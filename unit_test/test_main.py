import threading
import time
import unittest
from multiprocessing.pool import ThreadPool, MapResult
from unittest.mock import patch
from uuid import UUID

from omotes_orchestrator.config import OrchestratorConfig
from omotes_orchestrator.main import LifeCycleBarrierManager, BarrierTimeoutException


class LifeCycleBarrierManagerTest(unittest.TestCase):
    def test__ensure_barrier__barrier_is_created(self) -> None:
        # Arrange
        barrier_manager = LifeCycleBarrierManager()
        job_id = UUID("ab995599-a117-47b6-8da5-5d9488900858")

        # Act
        barrier = barrier_manager.ensure_barrier(job_id)

        # Assert
        self.assertEqual(barrier_manager._barriers[job_id], barrier)

    def test__ensure_barrier__barrier_is_already_ensured(self) -> None:
        # Arrange
        barrier_manager = LifeCycleBarrierManager()
        job_id = UUID("ab995599-a117-47b6-8da5-5d9488900858")
        barrier_1 = barrier_manager.ensure_barrier(job_id)

        # Act
        with patch("threading.Event") as event_mock:
            barrier_2 = barrier_manager.ensure_barrier(job_id)

        # Assert
        self.assertIs(barrier_1, barrier_2)
        event_mock.assert_not_called()

    def test__ensure_barrier__barrier_is_ensured_by_many_threads(self) -> None:
        # Arrange
        barrier_manager = LifeCycleBarrierManager()
        job_id = UUID("ab995599-a117-47b6-8da5-5d9488900858")

        num_of_threads = 100
        start_barrier = threading.Barrier(num_of_threads)

        def test_job(i: int) -> threading.Event:
            print(f"{i} is waiting for start barrier.")
            print(f"{start_barrier.n_waiting + 1}/{start_barrier.parties}")
            start_barrier.wait()
            print(f"{i} has passed start barrier.")
            return barrier_manager.ensure_barrier(job_id)

        # Act
        with ThreadPool(num_of_threads) as pool:
            async_results: MapResult[threading.Event] = pool.map_async(
                test_job, range(0, num_of_threads), error_callback=print
            )
            barriers = async_results.get(3.0)

        # Assert
        self.assertEqual(len(barriers), num_of_threads)
        self.assertIsInstance(barriers[0], threading.Event)
        for i in range(1, num_of_threads):
            self.assertIs(barriers[0], barriers[i])

    def test__set_barrier__first_set_the_barrier_then_wait(self) -> None:
        # Arrange
        barrier_manager = LifeCycleBarrierManager()
        job_id = UUID("ab995599-a117-47b6-8da5-5d9488900858")

        # Act / Assert
        barrier_manager.set_barrier(job_id)
        barrier_manager.wait_for_barrier(job_id)

    def test__set_barrier__wait_and_set_by_different_threads(self) -> None:
        # Arrange
        barrier_manager = LifeCycleBarrierManager()
        job_id = UUID("ab995599-a117-47b6-8da5-5d9488900858")

        def set_barrier() -> None:
            time.sleep(0.5)
            barrier_manager.set_barrier(job_id)

        other_thread = threading.Thread(target=set_barrier)

        # Act / Assert
        other_thread.start()
        barrier_manager.wait_for_barrier(job_id)

    def test__wait_barrier__takes_too_long(self) -> None:
        # Arrange
        barrier_manager = LifeCycleBarrierManager()
        job_id = UUID("ab995599-a117-47b6-8da5-5d9488900858")

        # Overwrite the timeout temporarily so the test doesn't take too long.
        previous_timeout = LifeCycleBarrierManager.BARRIER_WAIT_TIMEOUT
        LifeCycleBarrierManager.BARRIER_WAIT_TIMEOUT = 0.1

        # Act / Assert
        with self.assertRaises(BarrierTimeoutException):
            barrier_manager.wait_for_barrier(job_id)

        # Cleanup
        LifeCycleBarrierManager.BARRIER_WAIT_TIMEOUT = previous_timeout

    def test__cleanup_barrier__cleaning_non_existing_barrier(self) -> None:
        # Arrange
        barrier_manager = LifeCycleBarrierManager()
        job_id = UUID("ab995599-a117-47b6-8da5-5d9488900858")

        # Act / Assert
        barrier_manager.cleanup_barrier(job_id)

    def test__cleanup_barrier__cleaning_barrier(self) -> None:
        # Arrange
        barrier_manager = LifeCycleBarrierManager()
        job_id = UUID("ab995599-a117-47b6-8da5-5d9488900858")

        barrier_manager.ensure_barrier(job_id)
        self.assertIn(job_id, barrier_manager._barriers)

        # Act
        barrier_manager.cleanup_barrier(job_id)

        # Assert
        self.assertNotIn(job_id, barrier_manager._barriers)


class MyTest(unittest.TestCase):
    def test__construct_orchestrator_config__no_exception(self) -> None:
        # Arrange

        # Act
        result = OrchestratorConfig()

        # Assert
        self.assertIsNotNone(result)
