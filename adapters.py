#!/usr/bin/env python3
"""Job adapters for fleet.py -- one per job kind.

Each adapter exposes:
    CAPABILITY                      the capability it needs from a node
    run(node, base, payload, log)   do the work, return a JSON-able result

`base` is the HTTP root fleet.py resolved for the node -- for a host whose
service binds loopback only, that is the near end of an ssh tunnel, so
adapters never care about transport.

Kept in one file on purpose: five small classes read better than five modules.
This is a dispatcher over services that already exist, not a framework.
"""
import base64
import json
import os
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request

OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "out")


def _post_json(url, payload, timeout=60):
    req = urllib.request.Request(url, data=json.dumps(payload).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read().decode() or "{}"
    return json.loads(raw)


def _get_json(url, timeout=60):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read().decode() or "{}")


def _get_bytes(url, timeout=120):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.read()


# --------------------------------------------------------------------- llm ----
class llm:
    CAPABILITY = "llm"

    @staticmethod
    def run(node, base, payload, log):
        model = payload.get("model") or node.get("default_model")
        messages = []
        if payload.get("system"):
            messages.append({"role": "system", "content": payload["system"]})
        messages.append({"role": "user", "content": payload["prompt"]})
        t0 = time.time()
        # Non-streaming: the fleet wants the answer, not a typing animation.
        out = _post_json(base + "/api/chat",
                         {"model": model, "stream": False, "messages": messages},
                         timeout=payload.get("timeout", 900))
        text = (out.get("message") or {}).get("content", "")
        log("%s answered %d chars in %.1fs" % (model, len(text), time.time() - t0))
        if payload.get("out"):
            os.makedirs(os.path.dirname(os.path.abspath(payload["out"])), exist_ok=True)
            with open(payload["out"], "w", encoding="utf-8") as f:
                f.write(text)
            log("wrote " + payload["out"])
        return {"model": model, "text": text}


# ------------------------------------------------------------------- local ----
class local:
    CAPABILITY = "python"

    @staticmethod
    def run(node, base, payload, log):
        cmd = payload["cmd"]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True, bufsize=1,
                                cwd=payload.get("cwd"), encoding="utf-8",
                                errors="replace")
        for line in iter(proc.stdout.readline, ""):
            log(line.rstrip())
        proc.wait()
        if proc.returncode != 0:
            raise RuntimeError("exit code %s" % proc.returncode)
        return {"exit_code": proc.returncode}


# ----------------------------------------------------------------- upscale ----
class upscale:
    CAPABILITY = "upscale"

    @staticmethod
    def run(node, base, payload, log):
        with open(payload["in"], "rb") as f:
            png = f.read()
        # The service runs one job at a time and says 429 rather than queueing,
        # so the queue is here.
        for attempt in range(payload.get("retries", 12)):
            req = urllib.request.Request(base + "/upscale", data=png,
                                         headers={"Content-Type": "image/png"})
            try:
                with urllib.request.urlopen(req, timeout=payload.get("timeout", 180)) as r:
                    out = r.read()
                break
            except urllib.error.HTTPError as ex:
                if ex.code != 429:
                    raise
                log("node busy (429), waiting -- attempt %d" % (attempt + 1))
                time.sleep(3)
        else:
            raise RuntimeError("upscale stayed busy after every retry")

        dest = payload.get("out") or os.path.join(OUT_DIR, os.path.basename(payload["in"]))
        os.makedirs(os.path.dirname(os.path.abspath(dest)), exist_ok=True)
        with open(dest, "wb") as f:
            f.write(out)
        log("upscaled %s -> %s (%d KB)" % (os.path.basename(payload["in"]),
                                           dest, len(out) // 1024))
        return {"out": dest, "bytes": len(out)}


# -------------------------------------------------------------------- sdxl ----
def _comfy_ready(base, log, node, timeout=120):
    """The renderer is not kept running on the GPU host -- start it if it is
    not serving. The start command is idempotent and launches detached (ssh
    kills its own tree, so a plain foreground start there would die with the
    session)."""
    try:
        _get_json(base + "/system_stats", timeout=6)
        return True
    except Exception:
        pass
    if not node.get("start_cmd"):
        return False
    log("renderer not serving -- starting it on %s" % node["name"])
    from fleet import ssh_run  # late import: fleet imports us at module load
    ssh_run(node, node["start_cmd"], timeout=120)
    deadline = time.time() + timeout
    while time.time() < deadline:
        time.sleep(3)
        try:
            _get_json(base + "/system_stats", timeout=6)
            log("renderer up after %ds" % int(timeout - (deadline - time.time())))
            return True
        except Exception:
            continue
    return False


def _upload_image(base, path, log):
    """Multipart upload into the renderer's input/ so LoadImage can see it."""
    name = os.path.basename(path)
    boundary = "----fleet%d" % int(time.time() * 1000)
    with open(path, "rb") as f:
        data = f.read()
    body = (
        ("--%s\r\nContent-Disposition: form-data; name=\"image\"; filename=\"%s\"\r\n"
         "Content-Type: image/png\r\n\r\n" % (boundary, name)).encode()
        + data
        + ("\r\n--%s\r\nContent-Disposition: form-data; name=\"overwrite\"\r\n\r\ntrue\r\n"
           "--%s--\r\n" % (boundary, boundary)).encode()
    )
    req = urllib.request.Request(
        base + "/upload/image", data=body,
        headers={"Content-Type": "multipart/form-data; boundary=" + boundary})
    with urllib.request.urlopen(req, timeout=120) as r:
        r.read()
    log("uploaded %s to the render node" % name)
    return name


def _default_graph(payload, image_name):
    """SDXL text-to-image with an optional img2img encode. Pass
    payload['graph'] to bypass this entirely and submit a raw graph."""
    ckpt = payload.get("ckpt", "animagine-xl-4.0.safetensors")
    steps = payload.get("steps", 16)
    graph = {
        "1": {"class_type": "CheckpointLoaderSimple", "inputs": {"ckpt_name": ckpt}},
        "4": {"class_type": "CLIPTextEncode",
              "inputs": {"text": payload["positive"], "clip": ["1", 1]}},
        "5": {"class_type": "CLIPTextEncode",
              "inputs": {"text": payload.get("negative", ""), "clip": ["1", 1]}},
        "6": {"class_type": "KSampler", "inputs": {
            "seed": payload.get("seed", 1234), "steps": steps,
            "cfg": payload.get("cfg", 6.0),
            "sampler_name": payload.get("sampler", "dpmpp_2m"),
            "scheduler": payload.get("scheduler", "karras"),
            "denoise": payload.get("denoise", 1.0),
            "model": ["1", 0], "positive": ["4", 0], "negative": ["5", 0],
            "latent_image": ["3", 0]}},
        "7": {"class_type": "VAEDecode", "inputs": {"samples": ["6", 0], "vae": ["1", 2]}},
        "8": {"class_type": "SaveImage",
              "inputs": {"filename_prefix": payload.get("prefix", "fleet/out"),
                         "images": ["7", 0]}},
    }
    if image_name:  # img2img: encode the uploaded picture as the start latent
        graph["2"] = {"class_type": "LoadImage", "inputs": {"image": image_name}}
        graph["3"] = {"class_type": "VAEEncode",
                      "inputs": {"pixels": ["2", 0], "vae": ["1", 2]}}
    else:           # txt2img: an empty latent at the requested size
        graph["3"] = {"class_type": "EmptyLatentImage", "inputs": {
            "width": payload.get("width", 1024), "height": payload.get("height", 1024),
            "batch_size": 1}}
    return graph


class sdxl:
    CAPABILITY = "sdxl"

    @staticmethod
    def run(node, base, payload, log):
        if not _comfy_ready(base, log, node):
            raise RuntimeError("renderer would not start on %s" % node["name"])

        image_name = None
        if payload.get("image"):
            image_name = _upload_image(base, payload["image"], log)

        graph = payload.get("graph") or _default_graph(payload, image_name)
        t0 = time.time()
        prompt_id = _post_json(base + "/prompt", {"prompt": graph})["prompt_id"]
        log("queued %s" % prompt_id)

        deadline = t0 + payload.get("timeout", 600)
        history = None
        while time.time() < deadline:
            time.sleep(1.5)
            h = _get_json(base + "/history/" + prompt_id)
            if h:
                history = h[prompt_id]
                break
        if history is None:
            raise RuntimeError("render timed out after %ds" % payload.get("timeout", 600))

        outdir = payload.get("outdir") or OUT_DIR
        os.makedirs(outdir, exist_ok=True)
        saved = []
        for out in (history.get("outputs") or {}).values():
            for img in out.get("images", []):
                url = "%s/view?filename=%s&subfolder=%s&type=%s" % (
                    base, urllib.parse.quote(img["filename"]),
                    urllib.parse.quote(img.get("subfolder", "")),
                    img.get("type", "output"))
                dest = os.path.join(outdir, img["filename"])
                with open(dest, "wb") as f:
                    f.write(_get_bytes(url))
                saved.append(dest)
        log("rendered %d image(s) in %.1fs -> %s" % (len(saved), time.time() - t0, outdir))
        if not saved:
            raise RuntimeError("render finished but produced no image")
        return {"images": saved, "seconds": time.time() - t0}


# ------------------------------------------------------------- remote python --
class remote_python:
    CAPABILITY = "python-cpu"

    @staticmethod
    def run(node, base, payload, log):
        from fleet import ssh_run  # late import, see _comfy_ready
        code = payload["code"]
        blob = base64.b64encode(code.encode()).decode()
        py = node.get("remote_python", "python3")
        cmd = ("%s -c \"import base64;exec(base64.b64decode('%s').decode())\""
               % (py, blob))
        rc, out = ssh_run(node, cmd, timeout=payload.get("timeout", 1800))
        for line in out.splitlines():
            log(line)
        if rc != 0:
            raise RuntimeError("remote exit %d" % rc)
        return {"stdout": out}


ADAPTERS = {
    "llm": llm,
    "local": local,
    "upscale": upscale,
    "sdxl": sdxl,
    "remote-python": remote_python,
}
