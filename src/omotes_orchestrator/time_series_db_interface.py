import logging

from influxdb import InfluxDBClient

from omotes_orchestrator.config import InfluxDBConfig

LOGGER = logging.getLogger("omotes_orchestrator")


def initialize_db(config: InfluxDBConfig) -> InfluxDBClient:
    """Initialize the database connection.

    :param config: Configuration on how to connect to the time series database.
    """
    LOGGER.info(
        "Connecting to InfluxDB at %s:%s as user %s", config.host, config.port, config.username
    )
    logging.getLogger('urllib3.connectionpool').setLevel(logging.INFO)

    return InfluxDBClient(
        host=config.host,
        port=config.port,
        database=config.database,
        username=config.username,
        password=config.password,
    )


class TimeSeriesDBInterface:
    """Interface to the time series database for any queries to manage time series data."""

    influxdb_config: InfluxDBConfig
    """Configuration on how to connect to the database."""
    influxdb_client: InfluxDBClient
    """Database client."""

    def __init__(self, influxdb_config: InfluxDBConfig) -> None:
        """Create the time series database interface."""
        self.influxdb_config = influxdb_config

    def start(self) -> None:
        """Start the interface and connect to the database."""
        self.influxdb_client = initialize_db(self.influxdb_config)

    def stop(self) -> None:
        """Stop the interface and dispose of any connections."""
        if self.influxdb_client:
            self.influxdb_client.close()

    def delete_database_for_esdl(self, esdl_id: str) -> None:
        """Delete time series data for the specified ESDL ID.

        :param esdl_id: ID of the ESDL for which the time series data should be deleted.
        """
        self.influxdb_client.drop_database(esdl_id)

        LOGGER.debug("Time series database deleted: ", esdl_id)

    def get_esdl_ids_with_time_series_data(self) -> list[str]:
        """Get the ID's of all ESDL's for which time series data is present.

        :return: a list of ESDL ID's.
        """
        databases_to_omit = ["_internal", self.influxdb_config.database]
        return [
            db["name"]
            for db in self.influxdb_client.get_list_database()
            if db["name"] not in databases_to_omit
        ]
