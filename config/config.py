"""Central, environment-overridable settings. No secrets live here."""
import os
from pathlib import Path

try:  # python-dotenv is optional; plain environment variables work too
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
except ImportError:  # pragma: no cover
    pass

PROJECT_ROOT = Path(__file__).resolve().parent.parent

INPUT_DIR = Path(os.getenv("INPUT_DIR", PROJECT_ROOT / "data" / "input"))
INPUT_GLOB = os.getenv("INPUT_GLOB", "dataset_*.csv")
REFERENCE_FILE = Path(os.getenv("REFERENCE_FILE", PROJECT_ROOT / "data" / "reference" / "canonical_customers.csv"))
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", PROJECT_ROOT / "data" / "output"))
DB_PATH = Path(os.getenv("MAPPING_DB_PATH", OUTPUT_DIR / "mapping.db"))
SQL_DIR = PROJECT_ROOT / "sql"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# --- source contract -------------------------------------------------------
REQUIRED_COLUMNS = ["source_system", "source_id", "event_ts", "email", "phone", "full_name"]
VALID_SOURCE_SYSTEMS = ("crm", "web", "billing")
SOURCE_ID_PATTERN = r"^(?P<system>[a-z_]+)-\d{8}$"

# rows are written to SQLite in chunks of this size
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))
