#!/usr/bin/env python3
"""The dispatcher. A job asks for a capability, not a machine.

Public shell: the implementation is omitted from the public repo. What the
real module does, in the order it does it:

  probe    -> hit every node's health endpoint; classify up / cold / down.
              Cold is not down: a stopped renderer on a fast card still beats
              a running renderer on a card 25x slower.
  rank     -> healthy candidates for the job's capability, ordered by
              measured cost_s from the node registry.
  acquire  -> take the node's concurrency slot AND its mutex group, if any.
              Nodes sharing one GPU's VRAM share a mutex group and never run
              concurrently. Lifecycle hooks fire here: evict resident LLMs
              over HTTP (keep_alive: 0), drop a lock file so dependent
              services fail over instead of erroring.
  run      -> hand the job to its adapter (see adapters.py). Every line the
              worker prints is journaled to jobs.jsonl in one shared schema;
              nothing fabricates status.
  release  -> clear the lock, free the slot.

Entry points: probe | run <jobs.json> | demo
"""
