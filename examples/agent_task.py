"""Provider-free agent workflow example; replace ``answer`` with an SDK adapter."""

from nanoflow import AgentStep, FunctionAgent, Workflow


def answer(prompt, context):
    # A real adapter can call any model API here; nanoflow only persists its result.
    return f"offline answer: {prompt}"


workflow = Workflow(
    "agent-task",
    (AgentStep("answer", lambda ctx: f"Task: {ctx.input}", FunctionAgent(answer), retries=1).as_step(),),
)
input_data = "summarize the repository"
