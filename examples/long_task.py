"""A resumable workflow that can be run with the nanoflow CLI."""

from nanoflow import Step, Workflow


def plan(ctx):
    return {"task": ctx.input, "planned": True}


def execute(ctx):
    result = {"planned": ctx.outputs["plan"], "result": "bounded work complete"}
    ctx.write_artifact("evidence.json", '{"result": "bounded work complete"}\n')
    return result


def review(ctx):
    return {"accepted": True, "reviewed": ctx.outputs["execute"]}


workflow = Workflow("long-task", (Step("plan", plan), Step("execute", execute), Step("review", review)))
input_data = "build a durable agent workflow"
