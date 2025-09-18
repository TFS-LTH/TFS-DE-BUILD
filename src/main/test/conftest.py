# this file helps pytest to know where the source code located
import sys
import os
import pytest
from pathlib import Path

# add dev and test dir to sys path for running pytest
dev_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../main/dev'))
test_path = os.path.abspath(os.path.dirname(__file__))

print("==> BEFORE sys.path:", sys.path)
print("==> Adding following dev_path to sys.path:", dev_path)
print("==> Adding following test_path to sys.path:", test_path)

if dev_path not in sys.path:
    sys.path.insert(0, dev_path)

if test_path not in sys.path:
    sys.path.insert(0, test_path)

print("==> AFTER sys.path:", sys.path)

@pytest.fixture(scope="session")
def test_root_dir():
    # Assuming conftest.py is in src/main/test/
    return Path(__file__).parent.resolve()

