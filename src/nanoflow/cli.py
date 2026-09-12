"""Tiny CLI for running a Python-defined workflow."""

from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
from typing import Any

from .core import Budget, Engine, Workflow


def _load(path: str) -> tuple[Workflow, Any]:
    module_path = Path(path).resolve()
    spec = importlib.util.spec_from_file_location("nanoflow_user_workflow", module_path)
    if spec is None or spec.loader is None:
        raise SystemExit(f"cannot load workflow: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    workflow = getattr(module, "workflow", None)
    if not isinstance(workflow, Workflow):
        raise SystemExit(f"{path} must define `workflow = Workflow(...)`")
    return workflow, getattr(module, "input_data", None)


def _print(state: Any) -> None:
    print(json.dumps(state.as_dict(), indent=2, sort_keys=True))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="nanoflow")
    parser.add_argument("--root", default=".nanoflow", help="run store directory")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="start or continue a workflow module")
    run.add_argument("--root", dest="command_root", help="run store directory")
    run.add_argument("workflow")
    run.add_argument("--run-id")
    run.add_argument("--max-steps", type=int)
    run.add_argument("--max-seconds", type=float)

    resume = sub.add_parser("resume", help="resume a saved run")
    resume.add_argument("--root", dest="command_root", help="run store directory")
    resume.add_argument("workflow")
    resume.add_argument("run_id")
    resume.add_argument("--max-steps", type=int)
    resume.add_argument("--max-seconds", type=float)

    status = sub.add_parser("status", help="show a saved run")
    status.add_argument("--root", dest="command_root", help="run store directory")
    status.add_argument("run_id")

    events = sub.add_parser("events", help="show the event ledger")
    events.add_argument("--root", dest="command_root", help="run store directory")
    events.add_argument("run_id")

    args = parser.parse_args(argv)
    engine = Engine(getattr(args, "command_root", None) or args.root)
    if args.command == "status":
        _print(engine.status(args.run_id))
        return
    if args.command == "events":
        print(json.dumps(engine.store.events(args.run_id), indent=2, sort_keys=True))
        return

    workflow, input_data = _load(args.workflow)
    budget = Budget(max_steps=args.max_steps, max_seconds=args.max_seconds)
    if args.command == "run":
        state = engine.start(workflow, input_data, run_id=args.run_id)
        state = engine.run(workflow, state.run_id, budget=budget)
    else:
        state = engine.resume(workflow, args.run_id, budget=budget)
    _print(state)


if __name__ == "__main__":
    main()
