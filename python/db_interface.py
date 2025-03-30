import abc
from typing import List, Dict, Any, Optional
import pandas as pd

class DatabaseInterface(abc.ABC):
    """Abstract Base Class for timeseries database interactions."""

    @abc.abstractmethod
    def connect(self, config: Dict[str, Any]) -> None:
        """Establish connection to the database."""
        pass

    @abc.abstractmethod
    def setup_schema(self, drop_existing: bool = True) -> None:
        """Create necessary database, table/measurement/collection, hypertables, etc."""
        pass

    @abc.abstractmethod
    def insert_individual(self, timestamp: pd.Timestamp, data: Dict[str, Any], recording_id: str) -> None:
        """Insert a single data point."""
        pass

    @abc.abstractmethod
    def insert_batch(self, data_batch_df: pd.DataFrame, recording_id: str) -> None:
        """Insert a batch of data points from a pandas DataFrame."""
        # DataFrame columns should include 'timestamp' and all sensor fields.
        pass

    @abc.abstractmethod
    def query_time_range(self, start: pd.Timestamp, end: pd.Timestamp, recording_id: str) -> Optional[pd.DataFrame]:
        """Query data within a specific time range for a given recording."""
        pass

    @abc.abstractmethod
    def query_filtered(self, start: pd.Timestamp, end: pd.Timestamp, recording_id: str, filter_condition: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """Query data with a filter condition within a time range."""
        # filter_condition example: {'field': 'temperature', 'op': '>', 'value': 30.0}
        pass

    @abc.abstractmethod
    def query_aggregate(self, start: pd.Timestamp, end: pd.Timestamp, recording_id: str, interval: str, aggregation_func: str, field: str) -> Optional[pd.DataFrame]:
        """Query aggregated data over a time range."""
        # interval example: '1m', '5m', '1h' (pandas offset alias)
        # aggregation_func example: 'mean', 'max', 'min', 'count', 'sum'
        pass

    @abc.abstractmethod
    def delete_time_range(self, start: pd.Timestamp, end: pd.Timestamp, recording_id: str) -> None:
        """Delete data within a specific time range for a given recording."""
        pass

    @abc.abstractmethod
    def delete_recording(self, recording_id: str) -> None:
        """Delete all data associated with a specific recording_id."""
        pass

    @abc.abstractmethod
    def close(self) -> None:
        """Close the database connection."""
        pass

    @abc.abstractmethod
    def get_connection_info(self) -> Dict[str, Any]:
        """Return basic information about the current connection (host, port, etc.)."""
        pass
