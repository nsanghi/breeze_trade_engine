import os
import logging
from datetime import datetime
from unittest.mock import patch, MagicMock
import pytest
import pytest_asyncio
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
        mock_date.now.return_value = datetime(2024, 8, 16, 10, 0)
        yield mock_date


@pytest.fixture
def mock_logger():
    with patch(
        "breeze_trade_engine.executors.data.option_chain_fetcher.logging.getLogger"
    ) as mock_logger:
        yield mock_logger


@pytest.mark.asyncio
async def test_process_day_begin(mock_env_vars, mock_datetime, mock_logger):
    fetcher = OptionChainDataFetcher("test_fetcher", "09:00", "15:30", 60)

    await fetcher.process_day_begin()

    expected_file_path = "/tmp/nifty_chain_2024-08-16.csv"
    assert fetcher.file == expected_file_path


def test_option_chain_fetcher_singleton():
    fetcher1 = OptionChainDataFetcher("test_fetcher1", "09:00", "15:30", 60)
    fetcher2 = OptionChainDataFetcher("test_fetcher2", "09:00", "15:30", 60)

    assert fetcher1 is fetcher2
