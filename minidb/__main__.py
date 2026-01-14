#!/usr/bin/env python3
"""
MiniDB - A Simple Relational Database Management System
Entry point script

Run the REPL:
    python -m minidb

Or use as a library:
    from minidb import Database
    db = Database()
    db.execute("SELECT 1 + 1;")
"""

from minidb.core.repl import main

if __name__ == '__main__':
    main()
