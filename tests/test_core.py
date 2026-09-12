import json
from pathlib import Path
import tempfile
import unittest

from nanoflow import AgentStep, Budget, Engine, FunctionAgent, RunStatus, Step, Workflow
from nanoflow.core import WorkflowError


class EngineTests(unittest.TestCase):
    def test_budget_pause_and_resume_persist_every_step(self):
        with tempfile.TemporaryDirectory() as directory:
            seen = []
            workflow = Workflow(
                "demo",
                (
                    Step("one", lambda ctx: seen.append("one") or 1),
                    Step("two", lambda ctx: seen.append("two") or 2),
                ),
            )
            engine = Engine(directory)
            state = engine.start(workflow, "input")
            state = engine.run(workflow, state.run_id, budget=Budget(max_steps=1))
            self.assertEqual(state.status, RunStatus.PAUSED)
            self.assertEqual(state.completed, ["one"])
            self.assertEqual(seen, ["one"])
            state = engine.resume(workflow, state.run_id)
            self.assertEqual(state.status, RunStatus.COMPLETED)
            self.assertEqual(seen, ["one", "two"])
            events = engine.store.events(state.run_id)
            self.assertEqual(events[-1]["kind"], "run_completed")
            self.assertEqual([event["seq"] for event in events], list(range(1, len(events) + 1)))

    def test_retry_then_success_is_durable(self):
        with tempfile.TemporaryDirectory() as directory:
            attempts = {"count": 0}

            def flaky(ctx):
                attempts["count"] += 1
                if attempts["count"] == 1:
                    raise RuntimeError("temporary")
                return "ok"

            workflow = Workflow("retry", (Step("flaky", flaky, retries=1),))
            engine = Engine(directory)
            state = engine.start(workflow)
            state = engine.run(workflow, state.run_id)
            self.assertEqual(state.status, RunStatus.COMPLETED)
            self.assertEqual(state.attempts["flaky"], 2)
            self.assertEqual(state.outputs["flaky"], "ok")

    def test_final_failure_can_be_inspected_and_resumed(self):
        with tempfile.TemporaryDirectory() as directory:
            workflow = Workflow("failure", (Step("bad", lambda ctx: 1 / 0),))
            engine = Engine(directory)
            state = engine.start(workflow)
            state = engine.run(workflow, state.run_id)
            self.assertEqual(state.status, RunStatus.FAILED)
            self.assertIn("ZeroDivisionError", state.error)
            self.assertEqual(engine.status(state.run_id).status, RunStatus.FAILED)

    def test_workflow_change_requires_explicit_opt_in(self):
        with tempfile.TemporaryDirectory() as directory:
            engine = Engine(directory)
            first = Workflow("versioned", (Step("one", lambda ctx: 1),), version="1")
            changed = Workflow("versioned", (Step("one", lambda ctx: 2),), version="2")
            state = engine.start(first)
            state = engine.run(first, state.run_id, budget=Budget(max_steps=1))
            with self.assertRaises(WorkflowError):
                engine.resume(changed, state.run_id)
            state = engine.run(changed, state.run_id, allow_workflow_change=True)
            self.assertEqual(state.status, RunStatus.COMPLETED)

    def test_agent_step_keeps_provider_outside_the_kernel(self):
        with tempfile.TemporaryDirectory() as directory:
            calls = []

            def provider(prompt, context):
                calls.append((prompt, context.step))
                return "agent result"

            workflow = Workflow(
                "agent",
                (AgentStep("answer", lambda ctx: f"task: {ctx.input}", FunctionAgent(provider)).as_step(),),
            )
            engine = Engine(directory)
            state = engine.start(workflow, "ship it")
            state = engine.run(workflow, state.run_id)
            self.assertEqual(state.status, RunStatus.COMPLETED)
            self.assertEqual(state.outputs["answer"], "agent result")
            self.assertEqual(calls, [("task: ship it", "answer")])
            self.assertTrue(any(event["kind"] == "agent_completed" for event in engine.store.events(state.run_id)))

    def test_artifact_path_cannot_escape_run_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            workflow = Workflow("safe", (Step("write", lambda ctx: ctx.write_artifact("../escape", "x")),))
            engine = Engine(directory)
            state = engine.start(workflow)
            state = engine.run(workflow, state.run_id)
            self.assertEqual(state.status, RunStatus.FAILED)

    def test_start_does_not_overwrite_an_existing_run(self):
        with tempfile.TemporaryDirectory() as directory:
            engine = Engine(directory)
            workflow = Workflow("once", (Step("step", lambda ctx: "ok"),))
            state = engine.start(workflow, run_id="fixed")
            with self.assertRaises(WorkflowError):
                engine.start(workflow, run_id=state.run_id)


if __name__ == "__main__":
    unittest.main()
