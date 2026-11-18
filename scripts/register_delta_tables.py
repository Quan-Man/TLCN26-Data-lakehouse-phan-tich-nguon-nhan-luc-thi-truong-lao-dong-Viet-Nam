"""DQ-gated registration of catalog Delta tables in the Trino metastore."""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'jobs'))

from trino.register_tables import main


if __name__ == '__main__':
    raise SystemExit(main())
