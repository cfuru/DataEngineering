import sys
from pathlib import Path

# Add the utils directory so `import pipeline_helpers` works the same way
# as in Databricks (where `databricks` namespace conflicts with databricks-sdk).
UTILS_DIR = Path(__file__).resolve().parents[1] / "notebooks" / "utils"
if str(UTILS_DIR) not in sys.path:
    sys.path.insert(0, str(UTILS_DIR))
