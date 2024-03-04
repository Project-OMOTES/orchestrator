import unittest
from omotes_orchestrator.config import OrchestratorConfig


class MyTest(unittest.TestCase):
    def test__construct_orchestrator_config__no_exception(self) -> None:
        # Arrange

        # Act
        result = OrchestratorConfig()

        # Assert
        self.assertIsNotNone(result)
