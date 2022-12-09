import logging
import os
import webbrowser
from pathlib import Path

import click
from click import ClickException, Context, confirm

from .client import VertexAIPipelinesClient
from .config import PluginConfig, RunConfig
from .constants import KEDRO_VERTEXAI_BLOB_TEMP_DIR_NAME, VERTEXAI_RUN_ID_TAG
from .context_helper import ContextHelper
from .data_models import PipelineResult
from .utils import (
    docker_build,
    docker_push,
    materialize_dynamic_configuration,
    store_parameters_in_yaml,
)

logger = logging.getLogger(__name__)


def format_params(params: list):
    return dict((p[: p.find(":")], p[p.find(":") + 1 :]) for p in params)


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
@click.option(
    "--timeout-seconds",
    type=int,
    default=1800,
    help="If --wait-for-completion is used, "
    "this option sets timeout after which the plugin will return non-zero exit code "
    "if the pipeline does not finish in time",
)
@click.pass_context
def run_once(
    ctx: Context,
    auto_build: bool,
    yes: bool,
    image: str,
    pipeline: str,
    params: list,
    wait_for_completion: bool,
    timeout_seconds: int,
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

    run = client.run_once(
        pipeline=pipeline,
        image=image,
        image_pull_policy=config.image_pull_policy,
        parameters=format_params(params),
    )

    click.echo(
        f"Intermediate data datasets will be stored in {os.linesep}"
        f"gs://{config.root.strip('/')}/{KEDRO_VERTEXAI_BLOB_TEMP_DIR_NAME}/{run['displayName']}/*.bin"
    )

    if wait_for_completion:
        result: PipelineResult = client.wait_for_completion(
            timeout_seconds
        )  # blocking call
        if result.is_success:
            logger.info("Pipeline finished successfully!")
            exit_code = 0
        else:
            logger.error(f"Pipeline finished with status: {result.state}")
            exit_code = 1
        ctx.exit(exit_code)


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
    default="pipeline.json",
    help="Pipeline JSON definition file.",
)
@click.pass_context
def compile(ctx, image, pipeline, output) -> None:
    """Translates Kedro pipeline into JSON file with VertexAI pipeline definition"""
    context_helper = ctx.obj["context_helper"]
    config = context_helper.config.run_config

    context_helper.vertexai_client.compile(
        pipeline=pipeline,
        image_pull_policy=config.image_pull_policy,
        image=image if image else config.image,
        output=output,
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
    params: list,
):
    """Schedules recurring execution of latest version of the pipeline"""
    logger.warning(
        "Scheduler functionality was temporarily disabled, "
        "follow https://github.com/getindata/kedro-vertexai/issues/4 for updates"
    )


@vertexai_group.command()
@click.argument("project_id")
@click.argument("region")
@click.option("--with-github-actions", is_flag=True, default=False)
@click.pass_context
def init(ctx, project_id, region, with_github_actions: bool):
    """Initializes configuration for the plugin"""
    context_helper = ctx.obj["context_helper"]
    project_name = context_helper.context.project_path.name
    if with_github_actions:
        image = f"gcr.io/${{google_project_id}}/{project_name}:${{commit_id}}"
        run_name = f"{project_name}:${{commit_id}}"
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

    click.echo(f"Configuration generated in {config_path}")

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
