"""app/__main__.py — Makes `python -m app` invoke the CLI.

Usage:
    python -m app <command>
    python -m app --help
"""

from app.cli import main

if __name__ == "__main__":
    main()
