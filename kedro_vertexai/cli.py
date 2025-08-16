import logging
import os
import webbrowser
from pathlib import Path

import click
from click import ClickException, Context, confirm

from .client import VertexAIPipelinesClient
from .config import PluginConfig, RunConfig, ScheduleConfig
from .constants import VERTEXAI_RUN_ID_TAG
from .context_helper import ContextHelper
from .utils import (
    docker_build,
    docker_push,
    materialize_dynamic_configuration,
    store_parameters_in_yaml,
)

logger = logging.getLogger(__name__)


def format_params(params: list):
    """
    Format parameter list into dictionary.
    Supports both colon format (compile-time: 'key:type') and equals format (runtime: 'key=value').
    """
    result = {}
    for p in params:
        if "=" in p:
            # Runtime format: key=value
            key, value = p.split("=", 1)
            result[key] = value
        elif ":" in p:
            # Compile-time format: key:type
            key, type_def = p.split(":", 1)
            result[key] = type_def
        else:
            # Invalid format
            raise ValueError(
                f"Invalid parameter format: '{p}'. Expected 'key=value' or 'key:type'"
            )
    return result


@click.group("VertexAI")
def commands():
    """Kedro plugin adding support for Vertex AI Pipelines"""
    pass


@commands.group(
    name="vertexai", context_settings=dict(help_option_names=["-h", "--help"])
)
@click.option(
    "-e",
    "--env",
    "env",
    type=str,
    default=lambda: os.environ.get("KEDRO_ENV", "local"),
    help="Environment to use.",
)
@click.pass_obj
@click.pass_context
def vertexai_group(ctx, metadata, env):
    """Interact with Google Cloud Platform :: Vertex AI Pipelines"""
    ctx.ensure_object(dict)
    ctx.obj["context_helper"] = ContextHelper.init(
        metadata,
        env,
    )


@vertexai_group.command()
@click.pass_context
def list_pipelines(ctx):
    """List deployed pipeline definitions"""
    context_helper = ctx.obj["context_helper"]
    click.echo(context_helper.vertexai_client.list_pipelines())


@vertexai_group.command()
@click.option(
    "--auto-build",
    type=bool,
    is_flag=True,
    default=False,
    help="Specify to docker build and push before scheduling a run.",
)
@click.option(
    "--yes",
    type=bool,
    is_flag=True,
    default=False,
    help="Auto answer yes confirm prompts.",
)
@click.option(
    "-i",
    "--image",
    type=str,
    help="Docker image to use for pipeline execution.",
)
@click.option(
    "-p",
    "--pipeline",
    "pipeline",
    type=str,
    help="Name of pipeline to run",
    default="__default__",
)
@click.option(
    "--param",
    "params",
    type=str,
    multiple=True,
    help="Parameters override in form of `key=value`",
)
@click.option("--wait-for-completion", type=bool, is_flag=True, default=False)
@click.pass_context
def run_once(
    ctx: Context,
    auto_build: bool,
    yes: bool,
    image: str,
    pipeline: str,
    params: list,
    wait_for_completion: bool,
):
    """Deploy pipeline as a single run within given experiment
    Config can be specified in kubeflow.yml as well."""
    context_helper = ctx.obj["context_helper"]
    config: RunConfig = context_helper.config.run_config
    client: VertexAIPipelinesClient = context_helper.vertexai_client
    image: str = image if image else config.image

    if auto_build:
        if (splits := image.split(":"))[-1] != "latest" and len(splits) > 1:
            logger.warning(
                f"This operation will overwrite the target image with {splits[-1]} tag at remote location."
            )

        if not yes and not confirm("Continue?", default=True):
            exit(1)

        if (rv := docker_build(str(context_helper.context.project_path), image)) != 0:
            exit(rv)
        if (rv := docker_push(image)) != 0:
            exit(rv)
    else:
        logger.warning(
            "Make sure that you've built and pushed your image to run the latest version remotely.\
 Consider using '--auto-build' parameter."
        )

    job = client.run_once(
        pipeline=pipeline,
        image=image,
        parameters=format_params(params),
    )

    if wait_for_completion:
        job.wait()


@vertexai_group.command()
@click.pass_context
def ui(ctx) -> None:
    """Open VertexAI Pipelines UI in new browser tab"""
    vertex_ai_url = (
        "https://console.cloud.google.com/vertex-ai/pipelines?project={}".format(
            ctx.obj["context_helper"].config.project_id
        )
    )
    webbrowser.open_new_tab(vertex_ai_url)


@vertexai_group.command()
@click.option(
    "-i",
    "--image",
    type=str,
    help="Docker image to use for pipeline execution.",
)
@click.option(
    "-p",
    "--pipeline",
    "pipeline",
    type=str,
    help="Name of pipeline to run",
    default="__default__",
)
@click.option(
    "-o",
    "--output",
    type=str,
    default="pipeline.yaml",
    help="Pipeline YAML definition file.",
)
@click.option(
    "--params",
    type=str,
    default="",
    help="""
Pipeline parameters to be specified at run time.
In a format <param name≥:<param type>, for example test_param:int.
Should be separated by comma.
""",
)
@click.pass_context
def compile(ctx, image, pipeline, output, params) -> None:
    """Translates Kedro pipeline into JSON file with VertexAI pipeline definition"""
    context_helper = ctx.obj["context_helper"]
    config = context_helper.config.run_config

    context_helper.vertexai_client.compile(
        pipeline=pipeline,
        image=image if image else config.image,
        output=output,
        params=params,
    )


@vertexai_group.command()
@click.option(
    "-p",
    "--pipeline",
    "pipeline",
    type=str,
    help="Name of pipeline to run",
    default="__default__",
)
@click.option(
    "-c",
    "--cron-expression",
    type=str,
    help="Cron expression for recurring run",
    required=False,
)
@click.option(
    "-t",
    "--timezone",
    type=str,
    help="Time zone of the crone expression.",
    required=False,
)
@click.option(
    "--start-time",
    type=str,
    help="Timestamp after which the first run can be scheduled.",
    required=False,
)
@click.option(
    "--end-time",
    type=str,
    help="Timestamp after which no more runs will be scheduled. ",
    required=False,
)
@click.option(
    "--allow-queueing",
    type=bool,
    help="Whether new scheduled runs can be queued when max_concurrent_runs limit is reached.",
    required=False,
)
@click.option(
    "--max-run-count",
    type=int,
    help="Maximum run count of the schedule.",
    required=False,
)
@click.option(
    "--max-concurrent-run-count",
    type=int,
    help="Maximum number of runs that can be started concurrently.",
    required=False,
)
@click.option(
    "--param",
    "params",
    type=str,
    multiple=True,
    help="Parameters override in form of `key=value`",
)
@click.pass_context
def schedule(
    ctx,
    pipeline: str,
    cron_expression: str,
    timezone: str,
    start_time: str = None,
    end_time: str = None,
    allow_queueing: bool = None,
    max_run_count: int = None,
    max_concurrent_run_count: int = None,
    params: list = [],
):
    """Schedules recurring execution of latest version of the pipeline"""
    context_helper = ctx.obj["context_helper"]
    client: VertexAIPipelinesClient = context_helper.vertexai_client
    config: RunConfig = context_helper.config.run_config

    schedule_config: ScheduleConfig = config.schedules.get(
        pipeline, config.schedules["default_schedule"]
    )

    schedule_config.cron_expression = (
        cron_expression if cron_expression else schedule_config.cron_expression
    )
    schedule_config.timezone = timezone if timezone else schedule_config.timezone
    schedule_config.start_time = (
        start_time if start_time else schedule_config.start_time
    )
    schedule_config.end_time = end_time if end_time else schedule_config.end_time
    schedule_config.allow_queueing = (
        allow_queueing if allow_queueing else schedule_config.allow_queueing
    )
    schedule_config.max_run_count = (
        max_run_count if max_run_count else schedule_config.max_run_count
    )
    schedule_config.max_concurrent_run_count = (
        max_concurrent_run_count
        if max_concurrent_run_count
        else schedule_config.max_concurrent_run_count
    )

    client.schedule(
        pipeline=pipeline,
        schedule_config=schedule_config,
        parameter_values=format_params(params),
    )


@vertexai_group.command()
@click.argument("project_id")
@click.argument("region")
@click.option(
    "--with-github-actions", is_flag=True, default=False
)  # TODO consider removing
@click.pass_context
def init(ctx, project_id, region, with_github_actions: bool):
    """Initializes configuration for the plugin"""
    context_helper = ctx.obj["context_helper"]
    project_name = context_helper.context.project_path.name
    if with_github_actions:
        image = f"gcr.io/${{oc.env:KEDRO_CONFIG_GOOGLE_PROJECT_ID}}/{project_name}:${{oc.env:KEDRO_CONFIG_COMMIT_ID, unknown-commit}}"  # noqa: E501
        run_name = f"{project_name}:${{oc.env:KEDRO_CONFIG_COMMIT_ID, unknown-commit}}"
    else:
        image = project_name
        run_name = project_name

    sample_config = PluginConfig.sample_config(
        project_id=project_id,
        image=image,
        project=project_name,
        run_name=run_name,
        region=region,
    )
    config_path = Path.cwd().joinpath("conf/base/vertexai.yml")
    with open(config_path, "w") as f:
        f.write(sample_config)
    # FIXME add docs link
    click.echo(
        f"""Configuration generated in {config_path}. Make sure to update settings.py to add
custom resolver for environment variables.
src/.../settings.py:```

```"""
    )

    if with_github_actions:
        PluginConfig.initialize_github_actions(
            project_name,
            where=Path.cwd(),
            templates_dir=Path(__file__).parent / "templates",
        )


@vertexai_group.command(hidden=True)
@click.argument("run_id", type=str)
@click.option(
    "--output",
    type=str,
    default="/tmp/mlflow_run_id",
)
@click.pass_context
def mlflow_start(ctx, run_id: str, output: str):
    import mlflow
    from kedro_mlflow.config.kedro_mlflow_config import KedroMlflowConfig

    try:
        kedro_context = ctx.obj["context_helper"].context
        mlflow_conf: KedroMlflowConfig = kedro_context.mlflow
    except AttributeError:
        raise ClickException("Could not read MLFlow config")

    run = mlflow.start_run(
        experiment_id=mlflow.get_experiment_by_name(
            mlflow_conf.tracking.experiment.name
        ).experiment_id,
        nested=False,
    )
    mlflow.set_tag(VERTEXAI_RUN_ID_TAG, run_id)
    with open(output, "w") as f:
        f.write(run.info.run_id)
    click.echo(f"Started run: {run.info.run_id}")


@vertexai_group.command(hidden=True)
@click.option("--params", type=str, default="")
@click.option("--output", type=str, default="config.yaml")
@click.pass_context
def initialize_job(ctx, params: str, output: str):
    """
    Initializes node in Vertex AI runtime

    Current responsibilities:

    1. Store run parameters as config.yaml, because we cannot pass lists
    as CLI args by default
    https://stackoverflow.com/questions/62492785/kedro-how-to-pass-list-parameters-from-command-line
    Bases on ideas from https://github.com/getindata/kedro-kubeflow/pull/90

    2. Generate dynamic config files (e.g. with credentials that need to be refreshed per-node)
    """
    logger.info("Initializing VertexAI job")

    context_helper: ContextHelper = ctx.obj["context_helper"]
    config: PluginConfig = context_helper.config

    # 1.
    store_parameters_in_yaml(params, output)

    # 2.
    materialize_dynamic_configuration(config, context_helper)
