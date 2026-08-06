#!/usr/bin/env python3
"""The fleet dispatcher -- one scheduler over every machine in the fleet.

A job asks for a CAPABILITY ("sdxl", "llm", "upscale", "python"), not a machine.
The dispatcher probes what is alive, picks the cheapest healthy node that has the
capability, and runs the work there. Nodes are declared in nodes.json (private;
nodes.example.json shows its shape).

Everything a worker prints lands in jobs.jsonl in one shared event schema, so a
single live dashboard (dash.py) renders every stream with no changes.

THE RULE THIS EXISTS TO ENFORCE: a GPU serves LLM inference OR image generation,
never both at once. Nodes that share a card share a "mutex" group in nodes.json
and can never run concurrently. Both cards in this fleet are already too small
for their two tenants -- ignoring this cost a hard reset on one machine (video
render + a resident code model -> out-of-memory, no crash dump) and, on the
other, one render going 16s -> 110s while a chat reply took two and a half
minutes.

Taking the second GPU also takes the model that dependent services rely on, so
that node drops a lock file on acquire. Dependent services watch the lock and
fail over to a slower local model for the duration instead of erroring.

Usage:
  python fleet.py probe                 # health of every node, honestly reported
  python fleet.py run jobs.json         # run a JSON array of job specs
  python fleet.py demo                  # small mixed sweep, proves the wiring
"""
import json
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

import adapters

HERE = os.path.dirname(os.path.abspath(__file__))
NODES_FILE = os.path.join(HERE, "nodes.json")
JOBS_LOG = os.path.join(HERE, "jobs.jsonl")
RENDER_LOCK = os.environ.get(
    "FLEET_GPU_LOCK",
    os.path.join(os.path.expanduser("~"), ".fleet", "render.lock"))

# The log classifier, kept identical across tools so one dashboard reads all.
ERROR_MARKERS = ("error", "traceback", "exception", "blocked", "fail", "denied")

_log_lock = threading.Lock()


def load_nodes():
    with open(NODES_FILE, encoding="utf-8") as f:
        return json.load(f)["nodes"]


# ---------------------------------------------------------------- journaling --
def emit(event):
    """Append one event to jobs.jsonl. Same schema every producer writes."""
    event["ts"] = time.time()
    line = json.dumps(event)
    with _log_lock:
        with open(JOBS_LOG, "a", encoding="utf-8") as f:
            f.write(line + "\n")


def make_logger(job_id):
    """A per-job logger. Every line is something a worker actually produced --
    nothing here fabricates status, which is the whole point of the format."""
    def log(text):
        text = str(text).rstrip()
        if not text:
            return
        low = text.lower()
        kind = "error" if any(m in low for m in ERROR_MARKERS) else "line"
        emit({"job_id": job_id, "type": kind, "text": text})
        print("[%s] %s" % (job_id, text), flush=True)
    return log


# ------------------------------------------------------------------- plumbing --
def http_get(url, timeout=6):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.status, r.read()


def http_up(url, timeout=6):
    try:
        status, _ = http_get(url, timeout)
        return 200 <= status < 300
    except Exception:
        return False


def ollama_resident(endpoint, timeout=5):
    """Models currently holding VRAM on an Ollama host."""
    try:
        _, body = http_get(endpoint + "/api/ps", timeout)
        return [m["name"] for m in json.loads(body.decode()).get("models", [])]
    except Exception:
        return []


def ollama_evict(endpoint, log):
    """Unload every resident model. keep_alive=0 on a no-op generate is the
    documented way to make Ollama release VRAM immediately -- and unlike
    `ollama stop` over ssh it needs no shell on the far machine.

    Note /api/ps cannot tell you a card is free: after an evict the server still
    answers 200 with an empty list, which is exactly why the lock file exists.
    """
    for model in ollama_resident(endpoint):
        body = json.dumps({"model": model, "prompt": "", "keep_alive": 0}).encode()
        req = urllib.request.Request(endpoint + "/api/generate", data=body,
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                r.read()
            log("evicted %s from %s" % (model, endpoint))
        except Exception as ex:
            log("could not evict %s: %s" % (model, ex))


# --------------------------------------------------------- mutex side effects --
def _evict_llm_peers(node, nodes, log):
    """Free the card by unloading whatever LLM node shares this mutex group."""
    for peer in nodes:
        if peer is node or peer.get("mutex") != node.get("mutex"):
            continue
        if "llm" in peer.get("capabilities", []):
            ollama_evict(peer["endpoint"], log)


def _set_render_lock(node, nodes, log):
    """Signal dependent services that the shared card is busy. They watch this
    file (with a freshness window) and fail over to a slower local model."""
    os.makedirs(os.path.dirname(RENDER_LOCK), exist_ok=True)
    with open(RENDER_LOCK, "w", encoding="utf-8") as f:
        f.write(str(time.time()))
    log("render.lock set -- dependent services fail over until this clears")


def _clear_render_lock(node, nodes, log):
    try:
        os.remove(RENDER_LOCK)
        log("render.lock cleared -- dependent services return to the fast node")
    except OSError:
        pass


ACTIONS = {
    "evict_llm_peers": _evict_llm_peers,
    "set_render_lock": _set_render_lock,
    "clear_render_lock": _clear_render_lock,
}


# ----------------------------------------------------------------- ssh tunnel --
class Tunnel:
    """The render service on the GPU host binds 127.0.0.1 only (deliberately --
    its LAN port is firewalled), so reach it through `ssh -N -L`. Gives us the
    full HTTP API from here: upload, queue, history, download.

    SSH kills its process tree on disconnect, so the tunnel lives exactly as
    long as the `with` block and nothing is left running on the far host.
    """

    def __init__(self, node, log):
        self.node = node
        self.log = log
        self.port = int(node.get("local_port", 18188))
        self.proc = None

    @property
    def base(self):
        return "http://127.0.0.1:%d" % self.port

    def _port_open(self):
        with socket.socket() as s:
            s.settimeout(1.0)
            return s.connect_ex(("127.0.0.1", self.port)) == 0

    def __enter__(self):
        if self._port_open():
            self.log("tunnel :%d already open, reusing" % self.port)
            return self
        remote = self.node["endpoint"].replace("http://", "")
        cmd = ["ssh", "-N", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
               "-L", "%d:%s" % (self.port, remote), self.node["ssh"]]
        self.proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL,
                                     stderr=subprocess.DEVNULL)
        for _ in range(30):
            if self._port_open():
                self.log("tunnel :%d -> %s open" % (self.port, remote))
                return self
            time.sleep(0.5)
        raise RuntimeError("ssh tunnel to %s never came up" % self.node["ssh"])

    def __exit__(self, *exc):
        if self.proc:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        return False


def ssh_run(node, command, timeout=120):
    """One-shot remote command. Anything that must OUTLIVE the call needs a
    scheduled task or detached process on the far side -- ssh kills its tree."""
    cmd = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10"]
    if node.get("key"):
        cmd += ["-i", node["key"]]
    cmd += [node["ssh"], command]
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    return p.returncode, (p.stdout or "") + (p.stderr or "")


# ---------------------------------------------------------------------- probe --
def probe_node(node):
    """Honest health for one node. 'why' says what is actually wrong, because
    'down' covers three very different situations on a remote GPU host."""
    name, transport = node["name"], node["transport"]
    try:
        if transport == "local":
            return {"name": name, "up": True, "why": "local"}

        if transport == "http":
            url = node["endpoint"] + node.get("health", "/")
            if http_up(url):
                extra = ""
                if "llm" in node.get("capabilities", []):
                    res = ollama_resident(node["endpoint"])
                    extra = "resident: %s" % (", ".join(res) if res else "none")
                return {"name": name, "up": True, "why": extra or "ok"}
            return {"name": name, "up": False, "why": "no answer at " + url}

        if transport == "ssh":
            rc, out = ssh_run(node, "echo ok", timeout=20)
            return {"name": name, "up": rc == 0, "why": out.strip()[:60] or "ssh rc=%d" % rc}

        if transport == "ssh-http":
            rc, out = ssh_run(node, "echo ok", timeout=20)
            if rc != 0:
                return {"name": name, "up": False,
                        "why": "ssh refused (host asleep? nights-off power policy)"}
            with Tunnel(node, lambda _m: None) as t:
                if http_up(t.base + node.get("health", "/"), timeout=8):
                    return {"name": name, "up": True, "why": "renderer serving"}
            # Cold is not down: the host does not keep the renderer running, and
            # the sdxl adapter starts it on demand. Calling this DOWN would send
            # every render to the slow card instead -- 315s/image against 13s.
            if node.get("start_cmd"):
                return {"name": name, "up": True, "cold": True,
                        "why": "reachable, renderer cold (starts on demand, ~20s)"}
            return {"name": name, "up": False, "why": "host reachable, renderer stopped"}
    except Exception as ex:
        return {"name": name, "up": False, "why": "%s: %s" % (type(ex).__name__, ex)}
    return {"name": name, "up": False, "why": "unknown transport " + transport}


def probe_all(nodes=None):
    nodes = nodes or load_nodes()
    out = [None] * len(nodes)

    def work(i, n):
        out[i] = probe_node(n)

    threads = [threading.Thread(target=work, args=(i, n), daemon=True)
               for i, n in enumerate(nodes)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=90)
    return [o for o in out if o]


# ------------------------------------------------------------------ dispatch --
class Fleet:
    def __init__(self):
        self.nodes = load_nodes()
        self.sems = {n["name"]: threading.Semaphore(n.get("concurrency", 1))
                     for n in self.nodes}
        self.mutexes = {}
        for n in self.nodes:
            if n.get("mutex"):
                self.mutexes.setdefault(n["mutex"], threading.Lock())
        self.health = {}

    def refresh_health(self):
        self.health = {p["name"]: p for p in probe_all(self.nodes)}
        return self.health

    def candidates(self, capability):
        """Healthy nodes with the capability, cheapest first."""
        ok = [n for n in self.nodes
              if capability in n.get("capabilities", [])
              and self.health.get(n["name"], {}).get("up")]
        return sorted(ok, key=lambda n: n.get("cost_s", 999))

    def _side_effects(self, node, phase, log):
        for name in node.get(phase, []):
            fn = ACTIONS.get(name)
            if fn:
                fn(node, self.nodes, log)
            else:
                log("unknown %s action %r -- skipped" % (phase, name))

    def run_job(self, job, index=0):
        kind = job["kind"]
        adapter = adapters.ADAPTERS.get(kind)
        job_id = job.get("label") or "%s_%d_%d" % (kind, int(time.time()), index)
        log = make_logger(job_id)

        if not adapter:
            emit({"job_id": job_id, "type": "start", "script": kind, "args": []})
            log("error: no adapter for job kind %r" % kind)
            emit({"job_id": job_id, "type": "done", "exit_code": 2})
            return {"ok": False, "job_id": job_id, "error": "no adapter"}

        wanted = job.get("node")
        pool = [n for n in self.candidates(adapter.CAPABILITY)
                if not wanted or n["name"] == wanted]
        if not pool:
            emit({"job_id": job_id, "type": "start", "script": kind, "args": []})
            why = "; ".join("%s: %s" % (n["name"], self.health.get(n["name"], {}).get("why", "?"))
                            for n in self.nodes
                            if adapter.CAPABILITY in n.get("capabilities", []))
            log("error: no healthy node for %r (%s)" % (adapter.CAPABILITY, why))
            emit({"job_id": job_id, "type": "done", "exit_code": 3})
            return {"ok": False, "job_id": job_id, "error": "no node"}

        node = pool[0]
        emit({"job_id": job_id, "type": "start", "script": kind,
              "args": [node["name"]], "cwd": HERE})
        log("-> %s (%s)" % (node["name"], node.get("notes", "")[:60]))

        mutex = self.mutexes.get(node.get("mutex"))
        t0 = time.time()
        result, code = None, 0
        if mutex:
            mutex.acquire()
        try:
            self.sems[node["name"]].acquire()
            try:
                self._side_effects(node, "on_acquire", log)
                try:
                    if node["transport"] == "ssh-http":
                        with Tunnel(node, log) as t:
                            result = adapter.run(node, t.base, job.get("payload", {}), log)
                    else:
                        result = adapter.run(node, node.get("endpoint"),
                                             job.get("payload", {}), log)
                finally:
                    self._side_effects(node, "on_release", log)
            finally:
                self.sems[node["name"]].release()
        except Exception as ex:
            log("error: %s: %s" % (type(ex).__name__, ex))
            code = 1
        finally:
            if mutex:
                mutex.release()

        log("finished in %.1fs" % (time.time() - t0))
        emit({"job_id": job_id, "type": "done", "exit_code": code})
        return {"ok": code == 0, "job_id": job_id, "node": node["name"],
                "seconds": time.time() - t0, "result": result}

    def run(self, jobs, parallel=4):
        """Run jobs concurrently. Node semaphores and GPU mutexes do the real
        limiting, so `parallel` is only how many we are willing to have in
        flight -- oversubscribing just makes them queue, never collide."""
        self.refresh_health()
        results = [None] * len(jobs)
        gate = threading.Semaphore(max(1, parallel))

        def work(i, job):
            with gate:
                results[i] = self.run_job(job, i)

        threads = [threading.Thread(target=work, args=(i, j), daemon=True)
                   for i, j in enumerate(jobs)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return results


# -------------------------------------------------------------------- the CLI --
def cmd_probe():
    rows = probe_all()
    width = max(len(r["name"]) for r in rows)
    up = 0
    for r in rows:
        up += 1 if r["up"] else 0
        flag = "COLD" if r.get("cold") else ("UP  " if r["up"] else "DOWN")
        print("%s  %-*s  %s" % (flag, width, r["name"], r["why"]))
    print("\n%d/%d nodes up" % (up, len(rows)))
    return 0 if up else 1


def cmd_run(path):
    with open(path, encoding="utf-8") as f:
        jobs = json.load(f)
    if isinstance(jobs, dict):
        jobs = [jobs]
    results = Fleet().run(jobs, parallel=len(jobs))
    ok = sum(1 for r in results if r and r["ok"])
    print("\n%d/%d jobs ok" % (ok, len(results)))
    for r in results:
        if r:
            print("  %-28s %-12s %5.1fs %s" % (r["job_id"], r.get("node", "-"),
                                               r.get("seconds", 0),
                                               "ok" if r["ok"] else r.get("error", "FAILED")))
    return 0 if ok == len(results) else 1


def cmd_demo():
    """A mixed sweep that touches every transport we have."""
    jobs = [
        {"kind": "llm", "label": "demo_llm",
         "payload": {"prompt": "Reply with exactly: fleet online", "timeout": 300}},
        {"kind": "local", "label": "demo_local",
         "payload": {"cmd": [sys.executable, "-c",
                             "print('local worker ok')"]}},
    ]
    return cmd_run_jobs(jobs)


def cmd_run_jobs(jobs):
    results = Fleet().run(jobs, parallel=len(jobs))
    ok = sum(1 for r in results if r and r["ok"])
    print("\n%d/%d jobs ok" % (ok, len(results)))
    return 0 if ok == len(results) else 1


def main():
    argv = sys.argv[1:]
    if not argv or argv[0] in ("-h", "--help", "help"):
        print(__doc__)
        return 0
    if argv[0] == "probe":
        return cmd_probe()
    if argv[0] == "run" and len(argv) > 1:
        return cmd_run(argv[1])
    if argv[0] == "demo":
        return cmd_demo()
    print(__doc__)
    return 1


if __name__ == "__main__":
    sys.exit(main())
