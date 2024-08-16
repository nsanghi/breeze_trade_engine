import os
import logging
from datetime import datetime
from unittest.mock import patch, MagicMock
import pytest
from breeze_trade_engine.executors.data.option_chain_fetcher import (
    OptionChainDataFetcher,
)


@pytest.fixture
def mock_env_vars():
    with patch.dict(
        os.environ, {"BREEZE_APP_KEY": "test_app_key", "FILE_PATH": "/tmp"}
    ):
        yield


@pytest.fixture
def mock_datetime():
    with patch(
        "breeze_trade_engine.executors.data.option_chain_fetcher.datetime"
    ) as mock_date:
        mock_date.now.return_value = datetime(2023, 10, 1)
        yield mock_date


@pytest.fixture
def mock_logger():
    with patch(
        "breeze_trade_engine.executors.data.option_chain_fetcher.logging.getLogger"
    ) as mock_logger:
        yield mock_logger


def test_process_day_begin(mock_env_vars, mock_datetime, mock_logger):
    fetcher = OptionChainDataFetcher("test_fetcher", "09:00", "15:30", 60)

    fetcher.process_day_begin()

    expected_file_path = "/tmp/nifty_chain_2023-10-01.csv"
    assert fetcher.file == expected_file_path
    mock_logger().info.assert_called_with("Day begin logic executed.")
