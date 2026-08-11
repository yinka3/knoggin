"""Shared data contracts grouped by lifecycle owner.

Subpackages:
- ``agent``: identity, runtime limits, stream events, and tool-call contracts.
- ``ingestion``: extraction, resolution, and graph-write handoffs.
- ``episode``: generated and persisted episodic-memory structures.
- ``source``: source coordinates and answer-attribution records.

Modules that are cross-cutting and already cohesive remain flat.
"""
