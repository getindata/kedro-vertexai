"""Test grouping"""

import unittest

from kedro.pipeline import Pipeline, node

from kedro_vertexai.grouping import (
    GroupingException,
    IdentityNodeGrouper,
    TagNodeGrouper,
)


def identity(input1: str):
    return input1  # pragma: no cover


class TestGenerator(unittest.TestCase):
    legal_groups = ["g1", "g2", "g3", "g4", "g5", "group"]
    illegal_groups = ["ig1"]
    node_names = ["node1", "node1a", "node2", "node3"]

    def create_pipeline_deps(self):
        return Pipeline(
            [
                node(
                    identity,
                    "A",
                    "B",
                    name="node1",
                    tags=[
                        "foo",
                        "group:group",
                        "g1:group",
                        "g2:group",
                        "ig1:group",
                        "g5:group",
                    ],
                ),
                node(
                    identity,
                    "B",
                    "B2",
                    name="node1a",
                    tags=[
                        "bar",
                        "group:group",
                        "g1:group",
                        "g3:group",
                        "g4:group",
                        "g5:group",
                    ],
                ),
                node(
                    identity,
                    "B",
                    "C",
                    name="node2",
                    tags=["baz", "group:group", "g2:group", "g4:group", "g5:group2"],
                ),
                node(
                    identity,
                    "C",
                    "D",
                    name="node3",
                    tags=["wag", "group:group", "g3:group", "ig1:group", "g5:group2"],
                ),
            ]
        ).node_dependencies

    def test_identity_grouping(self):
        # given
        deps = self.create_pipeline_deps()
        grouper = IdentityNodeGrouper(None)
        # when
        group = grouper.group(deps)
        for name in self.node_names:
            assert name in group.nodes_mapping
            assert name in group.dependencies
            assert (
                len(group.nodes_mapping[name]) == 1
                and next(i for i in group.nodes_mapping[name]).name == name
            )
        self.assertSetEqual(group.dependencies["node1"], set())
        self.assertSetEqual(group.dependencies["node1a"], {"node1"})
        assert group.dependencies["node2"] == {"node1"}
        assert group.dependencies["node3"] == {"node2"}

    def test_legal_tag_groups(self):
        # given
        deps = self.create_pipeline_deps()
        for prefix in self.legal_groups:
            with self.subTest(msg=f"test_{prefix}", group_prefix=prefix):
                grouper = TagNodeGrouper(None, prefix + ":")
                # when
                group = grouper.group(deps)
                # assert
                assert len(group.dependencies) < 4
                assert "group" in group.dependencies

                if prefix == "group":
                    assert len(group.dependencies) == 1
                    assert group.dependencies["group"] == set()
                    assert len(group.nodes_mapping["group"]) == 4
                else:
                    assert len(group.nodes_mapping["group"]) == 2

                # verify dependencies
                if prefix == "g1":
                    self.assertSetEqual(group.dependencies["group"], set())
                    self.assertSetEqual(group.dependencies["node2"], {"group"})
                    self.assertSetEqual(group.dependencies["node3"], {"node2"})

    def test_illegal_tag_groups(self):
        # given
        deps = self.create_pipeline_deps()
        for prefix in self.illegal_groups:
            with self.subTest(msg=f"test_{prefix}", group_prefix=prefix):
                grouper = TagNodeGrouper(None, prefix + ":")
                # when
                with self.assertRaises(GroupingException):
                    grouper.group(deps)
