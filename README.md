# nanoflow

nanoflow is a small durable workflow runtime for long-horizon agentic tasks. A workflow is a
Python list of named steps. Each bounded run segment executes some steps, writes state after
every successful step, and appends an event to an inspectable JSONL ledger. A killed process can
resume from the first unfinished step without replaying completed work.

The design takes two ideas from [humanize2](https://github.com/humanfia/humanize2): flows stay
ordinary Python, and the flow owns a small explicit state rather than a copied transcript;
checkpointing happens while work is in progress. From [loopx](https://github.com/huangruiteng/loopx)
it keeps the separation between durable state, an evidence event ledger, and bounded execution
segments. It intentionally leaves out a daemon, UI, provider registry, and process DSL.

## Quick start

```python
from nanoflow import Budget, Engine, Step, Workflow

workflow = Workflow("research", (
    Step("plan", lambda ctx: {"query": ctx.input}),
    Step("collect", lambda ctx: {"sources": ["a", "b"]}, retries=2),
    Step("write", lambda ctx: ctx.write_artifact("report.md", "done") or "report.md"),
))

engine = Engine(".nanoflow")
run = engine.start(workflow, input="long horizon task")
run = engine.run(workflow, run.run_id, budget=Budget(max_steps=1))
run = engine.resume(workflow, run.run_id)
assert run.status.value == "completed"
```

Run a workflow module from the command line with `nanoflow run examples/long_task.py --max-steps 2`,
then continue it with `nanoflow resume examples/long_task.py RUN_ID`. Inspect `state.json` and
`events.jsonl` under `.nanoflow/runs/RUN_ID/`.

Provider integrations stay outside the kernel. `AgentStep` adapts any object with a
`run(prompt, context=...)` method, while `FunctionAgent` adapts a plain Python function:

```python
from nanoflow import AgentStep, FunctionAgent, Workflow

agent = FunctionAgent(lambda prompt, ctx: f"answer for: {prompt}")
workflow = Workflow("agent-task", (AgentStep("answer", "Summarize the task", agent).as_step(),))
```

## Runtime contract

- `state.json` is the resume authority. It stores input, step outputs, attempts, the next step,
  status, and a workflow fingerprint.
- `events.jsonl` is append-only evidence for UIs, monitors, and post-run analysis. It is never
  required to reconstruct progress.
- `Budget` applies to one execution segment. A budget pause is a normal durable state, not a
  failed task; the next invocation gets a fresh segment budget.
- Step failures are recorded and retried up to the step's explicit retry count. A final failure
  is resumable after the caller changes the workflow or fixes the external cause.
- Workflow definition changes require an explicit `allow_workflow_change=True` on the Python API.

The first version is intentionally linear. A future adapter can produce or replace steps while
keeping the same state and event contracts; domain-specific agents and providers should remain
outside this kernel.
