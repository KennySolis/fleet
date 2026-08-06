#!/usr/bin/env python3
"""
Remote power control for the fleet's second GPU host.

    python box_power.py status
    python box_power.py sleep      # S3 -- wakeable
    python box_power.py wake       # magic packet, then wait for SSH
    python box_power.py cycle      # sleep, wait, wake, verify  (the self-test)
    python box_power.py off        # FULL shutdown -- NOT wakeable, see below

*** WHY SLEEP AND NOT SHUTDOWN ***
That host has no wired network, and Wi-Fi cannot wake a machine that is in
S5 -- the radio is unpowered. So a `shutdown /s` there is a ONE-WAY trip:
somebody has to walk over and press the button. That happened once, and it
is the reason this file exists.

S3 sleep keeps the Wi-Fi radio powered and listening for a magic packet,
which makes power control round-trip-able with no cable, no BIOS change, and
nobody in the room. Two things had to be true for it to work, and both are
now set on the host:
  * Fast Startup OFF (HiberbootEnabled=0). With it on, "shutdown" is really a
    partial hibernate and the NIC comes down in a state that ignores wake.
    Windows updates re-enable it routinely.
  * The adapter ARMED, not merely capable. `Get-NetAdapterPowerManagement`
    reported WakeOnMagicPacket=Enabled while `powercfg /devicequery wake_armed`
    did NOT list it -- enabled != armed. `powercfg /deviceenablewake` fixes it.

Host, user, and MAC addresses come from the environment; nothing here is
specific to one machine.
"""
import argparse
import os
import socket
import subprocess
import sys
import time

HOST = os.environ.get("BOX_HOST", "gpu-host")
USER = os.environ.get("BOX_USER", "user")
WIFI_MAC = os.environ.get("BOX_WIFI_MAC", "00:00:00:00:00:00")
ETH_MAC = os.environ.get("BOX_ETH_MAC", "00:00:00:00:00:00")
WIFI_MATCH = os.environ.get("BOX_WIFI_MATCH", "Wireless")  # wake_armed grep
BROADCASTS = [b for b in (os.environ.get("BOX_BROADCASTS")
                          or "255.255.255.255").split(",") if b]
PORTS = [9, 7]


def magic(mac):
    b = bytes.fromhex(mac.replace(":", "").replace("-", ""))
    return b"\xff" * 6 + b * 16


def send_wol(mac=WIFI_MAC, rounds=3):
    """Spray the packet at every broadcast/port combination.

    Belt and braces on purpose: which of these actually lands depends on the
    router's broadcast handling, and a magic packet costs nothing."""
    pkt = magic(mac)
    sent = 0
    for _ in range(rounds):
        for addr in BROADCASTS:
            for port in PORTS:
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                    s.sendto(pkt, (addr, port))
                    s.close()
                    sent += 1
                except Exception:
                    pass
        time.sleep(0.3)
    return sent


def ssh(cmd, timeout=12, host=HOST):
    try:
        r = subprocess.run(["ssh", "-o", "BatchMode=yes",
                            "-o", f"ConnectTimeout={min(timeout,10)}",
                            f"{USER}@{host}", cmd],
                           capture_output=True, text=True, timeout=timeout)
        return r.returncode == 0, (r.stdout or "").strip()
    except Exception:
        return False, ""


def is_up(host=HOST):
    ok, out = ssh("echo UP", timeout=10, host=host)
    return ok and "UP" in out


def wait_for(state, limit=180, host=HOST):
    """poll until the host reaches `state` (True=up, False=down)"""
    t0 = time.time()
    while time.time() - t0 < limit:
        if is_up(host) == state:
            return time.time() - t0
        time.sleep(5)
    return None


def cmd_status():
    up = is_up()
    print(f"host: {'UP' if up else 'DOWN / asleep'}")
    if up:
        ok, out = ssh('powershell -NoProfile -Command "'
                      '(Get-CimInstance Win32_OperatingSystem).LastBootUpTime"')
        if ok:
            print(f"  last boot: {out}")
        ok, out = ssh('powershell -NoProfile -Command "'
                      f'((powercfg /devicequery wake_armed) -match \'{WIFI_MATCH}\') '
                      '-ne $null"')
        print(f"  wifi armed for wake: {out or '?'}")
    return 0 if up else 1


def cmd_sleep():
    if not is_up():
        print("already down")
        return 0
    print("sending the host to S3 ...")
    # rundll32 SetSuspendState is the only reliable non-interactive S3 trigger;
    # `shutdown /h` hibernates (S4, radio unpowered -> unwakeable)
    ssh('powershell -NoProfile -Command "Start-Process rundll32.exe '
        '-ArgumentList \'powrprof.dll,SetSuspendState 0,1,0\'"', timeout=15)
    took = wait_for(False, limit=90)
    if took is None:
        print("  it did NOT go down within 90s")
        return 1
    print(f"  asleep after {took:.0f}s")
    return 0


def cmd_wake(limit=180):
    if is_up():
        print("already up")
        return 0
    n = send_wol()
    print(f"magic packet -> {WIFI_MAC} ({n} sends). waiting for SSH ...")
    took = wait_for(True, limit=limit)
    if took is None:
        print(f"  NO WAKE within {limit}s.")
        print("  Fallbacks, in order: (1) re-run `wake`, the first packet after")
        print("  a long sleep is sometimes dropped; (2) somebody presses the")
        print("  power button; (3) plug the wired port into the router and")
        print(f"  use the wired MAC {ETH_MAC}, which wakes from full off too.")
        return 1
    print(f"  UP after {took:.0f}s")
    return 0


def cmd_off():
    print("WARNING: a full shutdown is NOT remotely wakeable on this host.")
    print("Use `sleep` unless you specifically want it dark until someone")
    print("presses the button. Proceeding in 5s -- Ctrl-C to abort.")
    time.sleep(5)
    ssh("shutdown /s /f /t 10", timeout=15)
    print("shutdown issued")
    return 0


def cmd_cycle():
    """The self-test. Proves the round trip before anyone relies on it."""
    print("=== ROUND-TRIP TEST: sleep -> wake ===")
    if not is_up():
        print("host is not up; cannot test. Wake it first.")
        return 1
    if cmd_sleep() != 0:
        return 1
    print("holding 20s to make sure it is properly settled in S3 ...")
    time.sleep(20)
    rc = cmd_wake()
    print("\nRESULT:", "PASS -- the host no longer needs a human hand"
          if rc == 0 else "FAIL -- see the fallbacks above")
    return rc


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=["status", "sleep", "wake", "off", "cycle"])
    a = ap.parse_args()
    return {"status": cmd_status, "sleep": cmd_sleep, "wake": cmd_wake,
            "off": cmd_off, "cycle": cmd_cycle}[a.action]()


if __name__ == "__main__":
    sys.exit(main())
