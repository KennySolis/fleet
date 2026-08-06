#!/usr/bin/env python3
"""Job adapters, one per job kind. Implementation omitted from the public repo.

Each adapter exposes:
    CAPABILITY                      the capability it needs from a node
    run(node, base, payload, log)   do the work, return a JSON-able result

`base` is the HTTP root the dispatcher resolved for the node. For a host
whose service binds loopback only, that is the near end of an SSH tunnel,
so adapters never care about transport.

Kinds in the real module:
    llm           prompt against an Ollama host (per-node default model)
    sdxl          ComfyUI render; on-demand service start on the far host
    upscale       waifu2x sidecar; retries through its single-job 429
    local         subprocess on this machine, output streamed live
    remote-python code executed on the cloud VPS over SSH

Kept in one file on purpose: small functions over a framework.
"""
