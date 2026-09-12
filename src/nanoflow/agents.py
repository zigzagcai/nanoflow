"""Optional, dependency-free bridge from workflow steps to agent providers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Protocol

from .core import Step, StepContext


class AgentAdapter(Protocol):
    """The only provider contract the kernel needs."""

    def run(self, prompt: str, *, context: StepContext) -> Any:
        ...


@dataclass(frozen=True)
class FunctionAgent:
    """Adapt a plain function without forcing an SDK dependency."""

    fn: Callable[[str, StepContext], Any]

    def run(self, prompt: str, *, context: StepContext) -> Any:
        return self.fn(prompt, context)


@dataclass(frozen=True)
class AgentStep:
    """Turn one provider call into a normal durable :class:`Step`."""

    name: str
    prompt: str | Callable[[StepContext], str]
    agent: AgentAdapter
    retries: int = 0

    def as_step(self) -> Step:
        def run(context: StepContext) -> Any:
            prompt = self.prompt(context) if callable(self.prompt) else self.prompt
            context.event("agent_requested", adapter=type(self.agent).__name__)
            result = self.agent.run(prompt, context=context)
            context.event("agent_completed", adapter=type(self.agent).__name__)
            return result

        return Step(self.name, run, retries=self.retries)


__all__ = ["AgentAdapter", "AgentStep", "FunctionAgent"]
