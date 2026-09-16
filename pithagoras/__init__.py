"""Pithagoras portal, rewritten in Python (standard library only).

A web front end for the pi coding agent. The server owns runs, appends every
event to SQLite, and replays them to reconnecting browsers over SSE.
"""

__version__ = "0.1.0"
