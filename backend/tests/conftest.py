import os

# Must happen before runner.py is imported so db.py reads the test URL
_test_db_url = os.environ.get("TEST_DATABASE_URL")
if _test_db_url is None:
    raise RuntimeError("TEST_DATABASE_URL env var is required to run tests")
os.environ["DATABASE_URL"] = _test_db_url

import pytest
from fastapi.testclient import TestClient
from runner import app


@pytest.fixture(scope="session")
def client():
    with TestClient(app) as c:
        yield c
