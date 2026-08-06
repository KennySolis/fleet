#!/usr/bin/env python3
"""Live dashboard over the JSONL job journal. Implementation omitted.

Serves a single page that tails jobs.jsonl and renders every job's stream
as it runs. Because every producer writes the same event schema, this
dashboard needed zero changes to also read journals written by other
tooling on the machine - one viewer for every stream.
"""
