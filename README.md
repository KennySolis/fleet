# fleet

One dispatcher over every machine I own. A job asks for a **capability**
("llm", "sdxl", "upscale", "python"), not a machine; the dispatcher probes
what is alive, ranks the healthy candidates by measured cost, and runs the
work on the cheapest one.

## Why it exists

Two GPUs in the house, and both are too small for their two tenants. A card
serves LLM inference or image generation, never both at once. Ignoring that
rule cost a hard reset on one machine (video render + resident code model ->
out-of-memory, no crash dump) and, on the other, a 16-second render
stretching to 110 seconds while a chat reply took 2.5 minutes.

So the scheduler makes the rule structural instead of a habit:

- Nodes that share a card share a **mutex group** and never run concurrently.
- Acquiring a GPU node fires lifecycle hooks: resident LLMs are **evicted
  over HTTP** (`keep_alive: 0`, so no shell is needed on the far machine),
  and a **lock file** is dropped so dependent services fail over to a slower
  local model instead of going silent. The lock exists because the runtime's
  process list cannot prove a card is free after eviction.

## What it does

- **Capability routing** across 7 nodes on 3 machines: a workstation
  (CPU + GPU), a second GPU host, and an always-on cloud VPS.
- **Honest health probing**: per-node health endpoints; a cold service is
  classified cold, not down (or every render would be sent to the 25x
  slower card).
- **Transports**: local exec, HTTP, SSH, and SSH-tunneled HTTP for services
  that bind loopback only; adapters never care which. On-demand service
  start (~3 seconds) for hosts that do not keep the renderer running.
- **Structured journaling**: every line a worker prints lands in an
  append-only JSONL journal in one shared schema, so a single live web
  dashboard reads every stream. Nothing fabricates status.
- **Remote power control**: the second GPU host sleeps nightly by policy;
  wake is a magic packet over Wi-Fi with an SSH-verified round trip.
  (S3 sleep, not shutdown: Wi-Fi cannot wake a machine from full power-off.
  Two traps found and fixed along the way: Fast Startup silently breaks NIC
  wake state, and an adapter can report wake "enabled" without being armed.)

## Verified

`probe`: 7/7 nodes correctly classified (cold is not down). Mixed 6-job
sweep, all green:

| job | node | time | proves |
|---|---|---|---|
| render A | gpu host | 13.9s | warm render 12.4s |
| render B | gpu host | 27.8s | serialized behind A (concurrency 1) |
| llm | same card | 66.6s | waited for both renders: the mutex working |
| local build | workstation | 0.1s | ran in parallel |
| upscale 2x | sidecar | 4.0s | parallel, 1024px to true 2048px |
| remote python | vps | 2.0s | parallel |

## Tech

Python (stdlib-only dispatcher and adapters) · JSONL journaling · SSH
tunnels · Ollama + ComfyUI + waifu2x adapters · Wake-on-LAN · cloud VPS

## Status

This is the real system, running daily. The dispatcher (`fleet.py`), the
adapters (`adapters.py`), the dashboard (`dash.py`), and the power control
(`box_power.py`) are the actual modules, lightly sanitized: hosts,
credentials, and MAC addresses come from the environment, and the real node
registry stays private (`nodes.example.json` shows its shape).
