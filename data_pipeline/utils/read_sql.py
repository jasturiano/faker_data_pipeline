from pathlib import Path


def read_sql_file(filename: str) -> str:
    """Read SQL query from file."""
    # Go up two levels from utils to reach project root
    sql_dir = Path(__file__).parent.parent.parent / "sql"
    with open(sql_dir / filename) as f:
        return f.read()
