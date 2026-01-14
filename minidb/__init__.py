"""
MiniDB - A Simple Relational Database Management System
Built for the Pesapal Junior Dev Challenge '26

Author: Erick Aboge
"""

__version__ = "1.0.0"
__author__ = "Erick Aboge"

from .core.database import Database
from .core.repl import REPL

__all__ = ["Database", "REPL"]
