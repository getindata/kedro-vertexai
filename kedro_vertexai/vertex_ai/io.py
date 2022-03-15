"""
Pipeline input and output helper methods for spec generation
"""

from kedro.pipeline.node import Node
from kfp.components import structures

from ..utils import is_mlflow_enabled


def _find_input_node(input_name, nodes):
    return [node for node in nodes if input_name in node.outputs]


def get_output_type(output, catalog):
    """
    Returns Vertex output type based on the layer in Kedro catalog
    """
    if catalog[output].get("layer") == "models":
        return "Model"
    return "Dataset"


def generate_outputs(node: Node, catalog):
    """
    Generates outputs for a particular kedro node
    """
    data_mapping = {
        o: catalog[o]["filepath"]
        for o in node.outputs
        if o in catalog
        and "filepath" in catalog[o]
        and ":/" not in catalog[o]["filepath"]
    }
    output_specs = [
        structures.OutputSpec(o, get_output_type(o, catalog))
        for o in data_mapping.keys()
    ]
    output_copy_commands = " ".join(
        [
            f"&& mkdir --parents `dirname {{{{$.outputs.artifacts['{o}'].path}}}}` "
            f"&& cp /home/kedro/{filepath} {{{{$.outputs.artifacts['{o}'].path}}}}"
            for o, filepath in data_mapping.items()
        ]
    )
    output_placeholders = [
        structures.OutputPathPlaceholder(output_name=o)
        for o in data_mapping.keys()
    ]
    return output_specs, output_copy_commands, output_placeholders


def generate_mlflow_inputs():
    """
    Generates inputs that are required to correctly generate mlflow specific data.
    :return: mlflow_inputs, mlflow_tokens
    """
    mlflow_inputs = (
        [
            structures.InputSpec("mlflow_tracking_token", "String"),
            structures.InputSpec("mlflow_run_id", "String"),
        ]
        if is_mlflow_enabled()
        else []
    )
    mlflow_tokens = (
        "MLFLOW_TRACKING_TOKEN={{$.inputs.parameters['mlflow_tracking_token']}} "
        "MLFLOW_RUN_ID=\"{{$.inputs.parameters['mlflow_run_id']}}\" "
        if is_mlflow_enabled()
        else ""
    )

    return mlflow_inputs, mlflow_tokens
