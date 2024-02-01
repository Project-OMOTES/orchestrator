from datetime import datetime
import unittest
from omotes_orchestrator import main


class MyTest(unittest.TestCase):
    def test__testable_function__is_correct(self) -> None:
        # Arrange
        current_time = datetime(1970, 1, 1, 13, 00)

        # Act
        result = main.testable_function(current_time)

        # Assert
        expected_result = datetime(1970, 1, 1, 14, 00)
        self.assertEqual(expected_result, result)

    def test_return_hello_world(self) -> None:
        # testing default argument value
        expected_result = main.return_hello_world()
        self.assertEqual(expected_result, "level=(info): Hello, world!")

        # testing different variations of the input argument
        expected_result = main.return_hello_world("debug")
        self.assertEqual(expected_result, "level=(debug): Hello, world!")
        expected_result = main.return_hello_world("error")
        self.assertEqual(expected_result, "level=(error): Hello, world!")
        expected_result = main.return_hello_world("warning")
        self.assertEqual(expected_result, "level=(warning): Hello, world!")

        # Testing invalid input and error handling
        with self.assertRaises(ValueError) as context:
            expected_result = main.return_hello_world("nope")

            self.assertTrue("Value nope is not a valid log level." in str(context.exception))
