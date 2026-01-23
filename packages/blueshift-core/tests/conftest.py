import sys
from os.path import dirname, abspath, join

# Add src to path so we can import blueshift without installing it in editable mode 
# (useful for local dev if not installed)
root_dir = dirname(dirname(abspath(__file__)))
src_dir = join(root_dir, 'src')
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

import pytest

@pytest.fixture(scope='session')
def blueshift_root():
    return root_dir
