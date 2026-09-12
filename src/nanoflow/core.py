"""The nanoflow runtime.

The runtime deliberately has one authoritative mutable object: a JSON run state.
Every completed step is followed by an atomic state write and an append-only event.
The event log is evidence for humans and tooling; it is never needed to resume.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
import hashlib
import json
from pathlib import Path
import tempfile
import time
from typing import Any, Callable, Mapping
import uuid


class RunStatus(StrEnum):
    READY = "ready"
    RUNNING = "running"
    PAUSED = "paused"
    FAILED = "failed"
    COMPLETED = "completed"


class WorkflowError(RuntimeError):
    """Raised when a saved run cannot safely be resumed."""


@dataclass(frozen=True)
class Budget:
    """Limits for one bounded execution segment, not for the whole run."""

    max_steps: int | None = None
    max_seconds: float | None = None

    def __post_init__(self) -> None:
        if self.max_steps is not None and self.max_steps < 1:
            raise ValueError("max_steps must be positive")
        if self.max_seconds is not None and self.max_seconds <= 0:
            raise ValueError("max_seconds must be positive")


StepFn = Callable[["StepContext"], Any]


@dataclass(frozen=True)
class Step:
    name: str
    run: StepFn
    retries: int = 0

    def __post_init__(self) -> None:
        if not self.name or "/" in self.name:
            raise ValueError("step names must be non-empty and path-safe")
        if self.retries < 0:
            raise ValueError("retries must be non-negative")


@dataclass(frozen=True)
class Workflow:
    name: str
    steps: tuple[Step, ...]
    version: str = "1"

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("workflow name is required")
        if not self.steps:
            raise ValueError("a workflow needs at least one step")
        names = [step.name for step in self.steps]
        if len(names) != len(set(names)):
            raise ValueError("step names must be unique")

    @property
    def fingerprint(self) -> str:
        payload = {"name": self.name, "version": self.version, "steps": [s.name for s in self.steps]}
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:16]


@dataclass
class StepContext:
    run_id: str
    workflow: str
    step: str
    input: Any
    outputs: Mapping[str, Any]
    workdir: Path
    _emit: Callable[[str, Mapping[str, Any]], None] = field(repr=False)
    _pause_reason: str | None = field(default=None, init=False, repr=False)

    def pause(self, reason: str = "paused by step") -> None:
        """Request a checkpoint immediately after this step returns."""
        self._pause_reason = reason

    @property
    def pause_reason(self) -> str | None:
        return self._pause_reason

    def event(self, kind: str, **data: Any) -> None:
        """Record a small, JSON-safe observation without changing run progress."""
        self._emit(kind, data)

    def write_artifact(self, name: str, value: str | bytes) -> Path:
        """Write durable step evidence below this run's private artifact directory."""
        artifact_root = (self.workdir / "artifacts").resolve()
        path = (artifact_root / name).resolve()
        try:
            path.relative_to(artifact_root)
        except ValueError as exc:
            raise ValueError("artifact name must stay below the run directory") from exc
        path.parent.mkdir(parents=True, exist_ok=True)
        data = value.encode() if isinstance(value, str) else value
        _atomic_bytes(path, data)
        self.event("artifact_written", path=str(path.relative_to(self.workdir.resolve())))
        return path


@dataclass
class RunState:
    run_id: str
    workflow: str
    workflow_version: str
    workflow_fingerprint: str
    status: RunStatus
    input: Any
    outputs: dict[str, Any]
    completed: list[str]
    next_step: str | None
    attempts: dict[str, int]
    metadata: dict[str, Any]
    event_seq: int = 0
    error: str | None = None
    pause_reason: str | None = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)

    def as_dict(self) -> dict[str, Any]:
        data = self.__dict__.copy()
        data["status"] = self.status.value
        return data

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "RunState":
        return cls(**{**data, "status": RunStatus(data["status"])})


class RunStore:
    """Filesystem store; state is replaceable, events are append-only."""

    def __init__(self, root: str | Path = ".nanoflow") -> None:
        self.root = Path(root)

    def run_dir(self, run_id: str) -> Path:
        return self.root / "runs" / run_id

    def state_path(self, run_id: str) -> Path:
        return self.run_dir(run_id) / "state.json"

    def events_path(self, run_id: str) -> Path:
        return self.run_dir(run_id) / "events.jsonl"

    def create(self, state: RunState) -> None:
        self.run_dir(state.run_id).mkdir(parents=True, exist_ok=True)
        self.save(state)

    def save(self, state: RunState) -> None:
        state.updated_at = time.time()
        path = self.state_path(state.run_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        _atomic_bytes(
            path,
            json.dumps(state.as_dict(), default=str, indent=2, sort_keys=True).encode(),
        )

    def load(self, run_id: str) -> RunState:
        try:
            data = json.loads(self.state_path(run_id).read_text())
        except FileNotFoundError as exc:
            raise WorkflowError(f"unknown run: {run_id}") from exc
        return RunState.from_dict(data)

    def events(self, run_id: str) -> list[dict[str, Any]]:
        path = self.events_path(run_id)
        if not path.exists():
            return []
        return [json.loads(line) for line in path.read_text().splitlines() if line]


class Engine:
    """Run and resume workflows in bounded, crash-tolerant segments."""

    def __init__(self, root: str | Path = ".nanoflow") -> None:
        self.store = RunStore(root)

    def start(
        self,
        workflow: Workflow,
        input: Any = None,
        *,
        run_id: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> RunState:
        run_id = run_id or uuid.uuid4().hex[:12]
        if self.store.state_path(run_id).exists():
            raise WorkflowError(f"run already exists: {run_id}; use resume")
        state = RunState(
            run_id=run_id,
            workflow=workflow.name,
            workflow_version=workflow.version,
            workflow_fingerprint=workflow.fingerprint,
            status=RunStatus.READY,
            input=input,
            outputs={},
            completed=[],
            next_step=workflow.steps[0].name,
            attempts={},
            metadata=dict(metadata or {}),
        )
        self.store.create(state)
        self._event(state, "run_created", workflow=workflow.name, version=workflow.version)
        return state

    def run(
        self,
        workflow: Workflow,
        run_id: str,
        *,
        budget: Budget | None = None,
        allow_workflow_change: bool = False,
    ) -> RunState:
        state = self.store.load(run_id)
        self._check_workflow(state, workflow, allow_workflow_change)
        if state.workflow_fingerprint != workflow.fingerprint and allow_workflow_change:
            state.workflow_version = workflow.version
            state.workflow_fingerprint = workflow.fingerprint
            self._event(state, "workflow_changed", version=workflow.version)
        if state.status is RunStatus.COMPLETED:
            return state
        budget = budget or Budget()
        started = time.monotonic()
        executed = 0
        state.status = RunStatus.RUNNING
        state.error = None
        state.pause_reason = None
        self._event(state, "run_resumed" if state.completed else "run_started")

        by_name = {step.name: step for step in workflow.steps}
        while state.next_step is not None:
            if self._budget_exhausted(budget, executed, started):
                self._pause(state, "segment budget exhausted")
                return state
            step = by_name[state.next_step]
            attempt = state.attempts.get(step.name, 0) + 1
            state.attempts[step.name] = attempt
            self._event(state, "step_started", step=step.name, attempt=attempt)
            context = StepContext(
                run_id=state.run_id,
                workflow=workflow.name,
                step=step.name,
                input=state.input,
                outputs=dict(state.outputs),
                workdir=self.store.run_dir(state.run_id),
                _emit=lambda kind, data: self._event(state, kind, step=step.name, **data),
            )
            try:
                result = step.run(context)
            except Exception as exc:  # a failed step is durable and resumable
                self._event(state, "step_failed", step=step.name, attempt=attempt, error=repr(exc))
                if attempt <= step.retries:
                    self.store.save(state)
                    continue
                state.status = RunStatus.FAILED
                state.error = f"{type(exc).__name__}: {exc}"
                self._event(state, "run_failed", step=step.name, error=state.error)
                self.store.save(state)
                return state

            state.outputs[step.name] = result
            state.completed.append(step.name)
            next_index = workflow.steps.index(step) + 1
            state.next_step = workflow.steps[next_index].name if next_index < len(workflow.steps) else None
            self._event(state, "step_succeeded", step=step.name)
            self.store.save(state)
            executed += 1
            if context.pause_reason:
                self._pause(state, context.pause_reason)
                return state

        state.status = RunStatus.COMPLETED
        self._event(state, "run_completed", steps=len(state.completed))
        self.store.save(state)
        return state

    def resume(self, workflow: Workflow, run_id: str, *, budget: Budget | None = None) -> RunState:
        return self.run(workflow, run_id, budget=budget)

    def status(self, run_id: str) -> RunState:
        return self.store.load(run_id)

    def summary(self, run_id: str) -> dict[str, Any]:
        """Return a public-safe progress projection without task input or step outputs."""
        state = self.status(run_id)
        return {
            "run_id": state.run_id,
            "workflow": state.workflow,
            "status": state.status.value,
            "completed": list(state.completed),
            "next_step": state.next_step,
            "attempts": dict(state.attempts),
            "event_count": state.event_seq,
            "error": state.error,
            "pause_reason": state.pause_reason,
            "updated_at": state.updated_at,
        }

    def _check_workflow(self, state: RunState, workflow: Workflow, allow_change: bool) -> None:
        if state.workflow != workflow.name:
            raise WorkflowError(f"run belongs to workflow {state.workflow!r}")
        if state.workflow_fingerprint != workflow.fingerprint and not allow_change:
            raise WorkflowError(
                "workflow definition changed; pass allow_workflow_change=True to resume explicitly"
            )

    @staticmethod
    def _budget_exhausted(budget: Budget, executed: int, started: float) -> bool:
        return (
            budget.max_steps is not None and executed >= budget.max_steps
        ) or (
            budget.max_seconds is not None and time.monotonic() - started >= budget.max_seconds
        )

    def _event(self, state: RunState, kind: str, **data: Any) -> None:
        state.event_seq += 1
        record = {
            "seq": state.event_seq,
            "at": time.time(),
            "run_id": state.run_id,
            "kind": kind,
            **data,
        }
        path = self.store.events_path(state.run_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, default=str, sort_keys=True) + "\n")
        self.store.save(state)

    def _pause(self, state: RunState, reason: str) -> None:
        state.status = RunStatus.PAUSED
        state.pause_reason = reason
        self._event(state, "run_paused", reason=reason, next_step=state.next_step)


def _atomic_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
        handle.write(data)
        temporary = Path(handle.name)
    temporary.replace(path)


__all__ = [
    "Budget",
    "Engine",
    "RunState",
    "RunStatus",
    "RunStore",
    "Step",
    "StepContext",
    "Workflow",
    "WorkflowError",
]
