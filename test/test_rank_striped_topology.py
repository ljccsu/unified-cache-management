"""Run the connector's topology configuration without importing vLLM/torch."""

import ast
import copy
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

SOURCE = Path(__file__).resolve().parents[1] / "ucm/integration/vllm/ucm_connector.py"
ROLE = types.SimpleNamespace(WORKER="worker", SCHEDULER="scheduler")
tree = ast.parse(SOURCE.read_text(encoding="utf-8-sig"))
method = next(
    node
    for node in ast.walk(tree)
    if isinstance(node, ast.FunctionDef)
    and node.name == "_configure_rank_striped_store"
)
namespace = {"Any": object, "KVConnectorRole": ROLE}
exec(
    compile(ast.Module(body=[method], type_ignores=[]), str(SOURCE), "exec"), namespace
)
configure = namespace[method.name]


class RankStripedTopologyTest(unittest.TestCase):
    def worker(self, dp=0, rank=0, pp=1):
        parallel = types.SimpleNamespace(
            tensor_parallel_size=8,
            pipeline_parallel_size=pp,
            data_parallel_rank=dp,
            rank=rank,
        )
        return types.SimpleNamespace(
            is_mla=True,
            tp_size=8,
            _role=ROLE.WORKER,
            _vllm_config=types.SimpleNamespace(parallel_config=parallel),
        )

    def config(self):
        return dict(
            share_buffer_rank_striped=True,
            share_buffer_enable=True,
            unique_id="instance",
            device_id=15,
        )

    def group(self, flags, rank=7):
        module = types.ModuleType("vllm.distributed.parallel_state")
        module.get_tp_group = Mock(
            return_value=types.SimpleNamespace(
                cpu_group=object(), world_size=8, rank_in_group=rank
            )
        )
        module.in_the_same_node_as = Mock(return_value=flags)
        return module

    def test_dp2_tp8_shares_shm_and_uses_group_rank(self):
        module = self.group([True] * 8)
        with patch.dict("sys.modules", {module.__name__: module}):
            first, second = self.config(), self.config()
            configure(self.worker(dp=0, rank=7), first)
            configure(self.worker(dp=1, rank=15), second)
        self.assertEqual(first["unique_id"], "instance")
        self.assertEqual(second["unique_id"], first["unique_id"])
        self.assertEqual(second["share_buffer_rank"], 7)
        self.assertEqual(second["local_rank_size"], 8)
        self.assertEqual(second["device_id"], 15)

    def test_cross_node_fails_before_store_creation(self):
        module = self.group([True] * 4 + [False] * 4)
        with patch.dict("sys.modules", {module.__name__: module}):
            with self.assertRaisesRegex(ValueError, "cross-node TP is unsupported"):
                configure(self.worker(), self.config())

    def test_probe_once_for_multiple_stores(self):
        module = self.group([True] * 8)
        worker = self.worker()
        with patch.dict("sys.modules", {module.__name__: module}):
            configure(worker, self.config())
            configure(worker, self.config())
        module.in_the_same_node_as.assert_called_once()

    def test_scheduler_preserves_shared_namespace_without_collective(self):
        scheduler = self.worker(dp=1)
        scheduler._role = ROLE.SCHEDULER
        config = self.config()
        configure(scheduler, config)
        self.assertEqual(config["unique_id"], "instance")
        self.assertNotIn("share_buffer_rank", config)

    def test_pipeline_groups_do_not_share_metadata(self):
        worker = self.worker(rank=8, pp=2)
        worker._rank_striped_topology = (0, 8)
        config = self.config()
        configure(worker, config)
        self.assertEqual(config["unique_id"], "instance_pp1")

    def test_disabled_leaves_config_unchanged(self):
        config = self.config()
        config["share_buffer_rank_striped"] = False
        before = copy.deepcopy(config)
        configure(self.worker(), config)
        self.assertEqual(config, before)

    def test_non_mla_fails_explicitly(self):
        worker = self.worker()
        worker.is_mla = False
        with self.assertRaisesRegex(ValueError, "requires MLA"):
            configure(worker, self.config())


if __name__ == "__main__":
    unittest.main()
