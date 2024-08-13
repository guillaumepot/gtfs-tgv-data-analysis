from prefect import flow, task, pause_flow_run, resume_flow_run
from prefect.runtime import flow_run
from prefect.deployments import Deployment
from prefect import serve

def generate_flow_run_name():
    flow_name = flow_run.flow_name

    parameters = flow_run.parameters
    name = parameters["name"]
    limit = parameters["limit"]

    return f"{flow_name}-with-{name}-and-{limit}"
    


@task(name="say_hello")
async def say_hello(name):
    print(f"hello {name}")

@task(name="say_goodbye")
async def say_goodbye(name):
    print(f"goodbye {name}")




@flow(name="test-flow",
      flow_run_name=generate_flow_run_name,
      description="this is a test flow",
      retries=3,
      retry_delay_seconds=60,
      version="0.0.1")
async def greetings(names=["arthur", "trillian", "ford", "marvin"]):
    for name in names:
        await say_hello(name)
        await pause_flow_run(timeout=600)  # pauses for 10 minutes
        await say_goodbye(name)

if __name__ == "__main__":

    deployment = Deployment.build_from_flow(
        flow=greetings,
        name="my-first-deployment",
        parameters={"names": ['Guillaume']},
        tags=["test"],
    )

    deployment.apply()

    serve(greetings)