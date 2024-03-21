from abc import ABC
from dataclasses import dataclass, field
from typing import Dict, Optional, Set

from kedro.framework.context import KedroContext
from kedro.pipeline.node import Node
from toposort import CircularDependencyError, toposort

TagsDict = Dict[str, Set[str]]
PipelineDependenciesDict = Dict[Node, Set[Node]]
GroupDependenciesDict = Dict[str, Set[str]]


@dataclass
class Grouping:
    nodes_mapping: Dict[str, Set[Node]] = field(default_factory=dict)
    dependencies: GroupDependenciesDict = field(default_factory=dict)

    # not sure if this is good idea to hook it to initialization, but for our limited
    # usage it should be fine
    def __post_init__(self):
        self.validate()

    def validate(self):
        try:
            [_ for _ in toposort(self.dependencies)]
        except CircularDependencyError as e:
            raise GroupingException(
                "Grouping has failed because of cyclic depedency after merging nodes. "
                f"Check your group settings. {str(e)}"
            )


class GroupingException(Exception):
    ...


class NodeGrouper(ABC):
    """Abstract base class for node grouping functions: grouping and validating the grouping
    The main argument to base grouping on is node_dependencies from kedro.pipeline.Pipeline
    For each node it tells which set of nodes are parents of them, based on nodes outputs
    """

    def __init__(self, kedro_context: Optional[KedroContext]):
        self.context = kedro_context

    def group(self, node_dependencies: PipelineDependenciesDict) -> Grouping:
        raise NotImplementedError

    def _get_tagging(self, node_dependencies: PipelineDependenciesDict) -> TagsDict:
        tagging = dict()
        # TODO make sure that node.name s are unique within pipeline
        for node in node_dependencies:
            tagging[node.name] = node.tags
        return tagging


class IdentityNodeGrouper(NodeGrouper):
    """Default class for grouping which puts each node into its own group,
    effectively not grouping anything at all."""

    def group(self, node_dependencies: PipelineDependenciesDict) -> Grouping:
        return Grouping(
            nodes_mapping={k.name: {k} for k in node_dependencies.keys()},
            dependencies={
                k.name: {dep.name for dep in deps}
                for k, deps in node_dependencies.items()
            },
        )


class TagNodeGrouper(NodeGrouper):
    """Grouping class that uses special tag prefix convention to aggregate
    nodes together. Only one such tag is allowed per node."""

    def __init__(self, kedro_context: KedroContext, tag_prefix="group.") -> None:
        super().__init__(kedro_context)
        self.tag_prefix = tag_prefix

    def group(self, node_dependencies: PipelineDependenciesDict) -> Grouping:
        group_mapping = {k.name: {k} for k in node_dependencies.keys()}
        group_belonging = {k.name: k.name for k in node_dependencies.keys()}
        node_names = [k for k in group_mapping.keys()]
        node_tagging = self._get_tagging(node_dependencies)

        # iterating over copy as we will modify group_mapping to reflect new state
        for name in node_names:
            node_tags = node_tagging[name]
            grouping_tags = [
                t for t in filter(lambda x: x.startswith(self.tag_prefix), node_tags)
            ]
            if len(grouping_tags) > 1:
                raise GroupingException(
                    "Inconsistent tagging for grouping, multiple tags"
                    f"with grouping prefix found in node {name}"
                )
            # 1 or 0 loop
            for tag in grouping_tags:
                group_name = tag[len(self.tag_prefix) :]
                if group_name not in group_mapping:
                    group_mapping[group_name] = set()
                group_mapping[group_name] = group_mapping[group_name].union(
                    group_mapping[name]
                )
                del group_mapping[name]
                group_belonging[name] = group_name

        group_dependencies: GroupDependenciesDict = dict()
        for child, parents in node_dependencies.items():
            group_name = group_belonging[child.name]
            # deduplication after gropuing thanks to sets and dicts properties
            if group_name not in group_dependencies:
                group_dependencies[group_name] = set()
            for parent in parents:
                if group_belonging[parent.name] != group_name:
                    group_dependencies[group_name].add(group_belonging[parent.name])

        return Grouping(
            nodes_mapping=group_mapping,
            dependencies=group_dependencies,
        )
