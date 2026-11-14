#!/usr/bin/env python3
"""Checks an strace log for outgoing connections (Requirements 75.1 to 75.5).

    egress_check.py LOG ALLOWED_PORT [ALLOWED_PORT ...]

Reads `strace -f -e trace=connect` output. Every `connect` to an Internet address
is an egress attempt, whether or not it succeeded (inside the test namespace only
loopback exists, so a connection to anywhere else fails but is still logged).

A connection passes only when its address is loopback and its port is one of the
configured destinations given on the command line. Anything else, including any
DNS lookup (port 53) and any non-loopback address, is a violation: it would be
telemetry, or traffic nobody configured.

Exit status: 0 all connections were configured, 1 a violation, 2 usage or an
empty log (a log with no connections at all means the test did not run).
"""
import re
import sys

CONNECT_V4 = re.compile(
    r'connect\(\d+, \{sa_family=AF_INET, sin_port=htons\((\d+)\), sin_addr=inet_addr\("([^"]+)"\)'
)
CONNECT_V6 = re.compile(
    r'connect\(\d+, \{sa_family=AF_INET6, sin6_port=htons\((\d+)\).*?inet_pton\(AF_INET6, "([^"]+)"'
)
LOOPBACK = {"127.0.0.1", "::1"}


def connections(lines):
    """Yields (address, port) for every Internet connect() in the log."""
    for line in lines:
        match = CONNECT_V4.search(line) or CONNECT_V6.search(line)
        if match:
            port, address = match.groups()
            yield (address, int(port))


def violations(lines, allowed_ports):
    """Connections that are not to a configured loopback destination."""
    allowed = set(allowed_ports)
    return sorted(
        {
            (address, port)
            for address, port in connections(lines)
            if address not in LOOPBACK or port not in allowed
        }
    )


def main(argv):
    if len(argv) < 3:
        print(__doc__, file=sys.stderr)
        return 2
    with open(argv[1], encoding="utf-8", errors="replace") as log:
        lines = log.readlines()
    seen = set(connections(lines))
    if not seen:
        print("no connections were logged: the test did not exercise the components", file=sys.stderr)
        return 2
    bad = violations(lines, [int(port) for port in argv[2:]])
    for address, port in bad:
        print(f"unexpected connection to {address}:{port}", file=sys.stderr)
    print(f"{len(seen)} destination(s) seen, {len(bad)} not configured")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
