"""Unit-тесты для утилит S3."""

from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from tinvest_flow.common.s3 import ensure_bucket


def test_ensure_bucket_exists() -> None:
    mock_s3 = MagicMock()
    mock_s3.head_bucket.return_value = {}

    ensure_bucket(mock_s3, "test-bucket")

    mock_s3.head_bucket.assert_called_once_with(Bucket="test-bucket")
    mock_s3.create_bucket.assert_not_called()


def test_ensure_bucket_creates() -> None:
    mock_s3 = MagicMock()
    error = ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadBucket")
    mock_s3.head_bucket.side_effect = error

    ensure_bucket(mock_s3, "test-bucket")

    mock_s3.create_bucket.assert_called_once_with(Bucket="test-bucket")


def test_ensure_bucket_raises_forbidden() -> None:
    mock_s3 = MagicMock()
    error = ClientError({"Error": {"Code": "403", "Message": "Forbidden"}}, "HeadBucket")
    mock_s3.head_bucket.side_effect = error

    with pytest.raises(ClientError):
        ensure_bucket(mock_s3, "test-bucket")
