"""Desktop entry point for the bounded, read-only Milestone 8 tools."""
from pathlib import Path
import runpy
import sys

source = Path(__file__).resolve().parents[1]/"powerbi"/"milestone8"
sys.path.insert(0, str(source))
runpy.run_path(str(source/"m8.py"), run_name="__main__")
