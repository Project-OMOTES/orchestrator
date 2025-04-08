import unittest
from datetime import datetime, timedelta

from omotes_orchestrator.db_models import SQLEsdlTimeSeriesInfoDB
from omotes_orchestrator.esdl_time_series_manager import EsdlTimeSeriesManager


class TimeSeriesManagerTest(unittest.TestCase):
    def test__sql_time_series_info_is_stale_on_registered_at__returns_true(self) -> None:
        # Arrange
        cur_time = datetime.now()
        esdl_time_series_info = SQLEsdlTimeSeriesInfoDB()
        esdl_time_series_info.esdl_id = "id1"
        esdl_time_series_info.registered_at = cur_time - timedelta(seconds=65)
        time_series_db_esdl_ids = ["id2", "id3"]

        # Act / Assert
        self.assertTrue(
            EsdlTimeSeriesManager.sql_esdl_time_series_info_is_stale(
                esdl_time_series_info=esdl_time_series_info,
                time_series_db_esdl_ids=time_series_db_esdl_ids,
                ref_time=cur_time,
                retention_sec=60,
            )
        )

    def test__sql_time_series_info_is_stale_on_registered__returns_false(self) -> None:
        # Arrange
        cur_time = datetime.now()
        esdl_time_series_info = SQLEsdlTimeSeriesInfoDB()
        esdl_time_series_info.esdl_id = "id1"
        esdl_time_series_info.registered_at = cur_time - timedelta(seconds=55)
        time_series_db_esdl_ids = ["id2", "id3"]

        # Act / Assert
        self.assertFalse(
            EsdlTimeSeriesManager.sql_esdl_time_series_info_is_stale(
                esdl_time_series_info=esdl_time_series_info,
                time_series_db_esdl_ids=time_series_db_esdl_ids,
                ref_time=cur_time,
                retention_sec=60,
            )
        )

    def test__sql_time_series_info_is_stale_esdl_in_list__returns_false(self) -> None:
        # Arrange
        cur_time = datetime.now()
        esdl_time_series_info = SQLEsdlTimeSeriesInfoDB()
        esdl_time_series_info.esdl_id = "id1"
        time_series_db_esdl_ids = ["id1", "id2"]

        # Act / Assert
        self.assertFalse(
            EsdlTimeSeriesManager.sql_esdl_time_series_info_is_stale(
                esdl_time_series_info=esdl_time_series_info,
                time_series_db_esdl_ids=time_series_db_esdl_ids,
                ref_time=cur_time,
                retention_sec=60,
            )
        )

    def test__sql_time_series_data_is_timed_out_deactivated_at_not_set__returns_false(self) -> None:
        # Arrange
        cur_time = datetime.now()
        esdl_time_series_info = SQLEsdlTimeSeriesInfoDB()

        # Act / Assert
        self.assertFalse(
            EsdlTimeSeriesManager.time_series_data_is_timed_out(
                esdl_time_series_info=esdl_time_series_info,
                ref_time=cur_time,
                retention_sec=60,
            )
        )

    def test__sql_time_series_data_is_timed_out_on_deactivated_at__returns_true(self) -> None:
        # Arrange
        cur_time = datetime.now()
        esdl_time_series_info = SQLEsdlTimeSeriesInfoDB()
        esdl_time_series_info.deactivated_at = cur_time - timedelta(seconds=65)

        # Act / Assert
        self.assertTrue(
            EsdlTimeSeriesManager.time_series_data_is_timed_out(
                esdl_time_series_info=esdl_time_series_info,
                ref_time=cur_time,
                retention_sec=60,
            )
        )

    def test__sql_time_series_data_is_timed_out_on_deactivated_at__returns_false(self) -> None:
        # Arrange
        cur_time = datetime.now()
        esdl_time_series_info = SQLEsdlTimeSeriesInfoDB()
        esdl_time_series_info.deactivated_at = cur_time - timedelta(seconds=55)

        # Act / Assert
        self.assertFalse(
            EsdlTimeSeriesManager.time_series_data_is_timed_out(
                esdl_time_series_info=esdl_time_series_info,
                ref_time=cur_time,
                retention_sec=60,
            )
        )
