#!/usr/bin/env python3
"""Remote power control for the second GPU host. Implementation omitted.

    status | sleep | wake | cycle | off

The host sleeps nightly by policy and is woken on demand with a Wake-on-LAN
magic packet, then verified by waiting for SSH to answer. `cycle` is the
self-test: sleep, wait, wake, verify.

Why S3 sleep and not shutdown: the host is on Wi-Fi, and Wi-Fi cannot wake
a machine from full power-off (the radio is unpowered in S5) - a shutdown
there is a one-way trip that ends with someone pressing the button.

Two traps this file exists to remember:
  * Fast Startup must be OFF. With it on, "shutdown" is a partial hibernate
    and the NIC comes down in a state that ignores wake packets. Windows
    updates re-enable it routinely.
  * "Enabled" is not "armed": the adapter can report WakeOnMagicPacket
    enabled while the OS wake-armed list does not include it.
"""
