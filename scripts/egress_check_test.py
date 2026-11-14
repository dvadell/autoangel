#!/usr/bin/env python3
"""Tests for egress_check.py: the checker must catch what it is there to catch."""
import os
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import egress_check  # noqa: E402


def v4(port, address="127.0.0.1", pid=10):
    return (
        f'{pid} connect(5, {{sa_family=AF_INET, sin_port=htons({port}), '
        f'sin_addr=inet_addr("{address}")}}, 16) = 0'
    )


def v6(port, address="::1"):
    return (
        f'11 connect(6, {{sa_family=AF_INET6, sin6_port=htons({port}), '
        f'sin6_flowinfo=htonl(0), inet_pton(AF_INET6, "{address}", &sin6_addr), '
        f'sin6_scope_id=0}}, 28) = 0'
    )


V6_REAL = (
    '11 connect(6, {sa_family=AF_INET6, sin6_port=htons(443), sin6_flowinfo=htonl(0), '
    'inet_pton(AF_INET6, "2606:4700::1111", &sin6_addr), sin6_scope_id=0}, 28) = -1 ENETUNREACH'
)


class EgressCheck(unittest.TestCase):
    def test_configured_loopback_destinations_pass(self):
        self.assertEqual(egress_check.violations([v4(9400), v4(9401), v4(8081)], [9400, 9401, 8081]), [])

    def test_a_loopback_port_nobody_configured_is_a_violation(self):
        self.assertEqual(egress_check.violations([v4(4369)], [9400]), [("127.0.0.1", 4369)])

    def test_any_address_outside_loopback_is_a_violation_even_when_it_failed(self):
        failed = '12 connect(7, {sa_family=AF_INET, sin_port=htons(443), sin_addr=inet_addr("34.1.2.3")}, 16) = -1 ENETUNREACH'
        self.assertEqual(egress_check.violations([failed], [443]), [("34.1.2.3", 443)])

    def test_a_dns_lookup_is_a_violation(self):
        self.assertEqual(egress_check.violations([v4(53)], [9400]), [("127.0.0.1", 53)])
        self.assertEqual(egress_check.violations([v4(53, "10.0.0.2")], [53]), [("10.0.0.2", 53)])

    def test_unix_sockets_and_other_lines_are_ignored(self):
        lines = [
            '13 connect(3, {sa_family=AF_UNIX, sun_path="/var/run/nscd/socket"}, 110) = -1 ENOENT',
            "14 +++ exited with 0 +++",
            "",
        ]
        self.assertEqual(list(egress_check.connections(lines)), [])

    def test_ipv6_is_checked_too(self):
        self.assertEqual(egress_check.violations([v6(9400)], [9400]), [])
        self.assertEqual(egress_check.violations([V6_REAL], [443]), [("2606:4700::1111", 443)])

    def test_each_violation_is_reported_once(self):
        lines = [v4(4369)] * 5 + [v4(4369, pid=99)]
        self.assertEqual(egress_check.violations(lines, []), [("127.0.0.1", 4369)])

    def run_cli(self, lines, *ports):
        with tempfile.NamedTemporaryFile("w", suffix=".log", delete=False) as log:
            log.write("\n".join(lines) + "\n")
        try:
            return subprocess.run(
                [sys.executable, os.path.join(HERE, "egress_check.py"), log.name, *map(str, ports)],
                capture_output=True,
                text=True,
            )
        finally:
            os.unlink(log.name)

    def test_exit_status(self):
        self.assertEqual(self.run_cli([v4(9400)], 9400).returncode, 0)
        self.assertEqual(self.run_cli([v4(9400), v4(80, "1.1.1.1")], 9400).returncode, 1)
        # A log with no connections is not a pass: the test did not run.
        self.assertEqual(self.run_cli(["nothing here"], 9400).returncode, 2)
        self.assertEqual(self.run_cli([v4(9400)]).returncode, 2)

    def test_the_report_names_the_destination(self):
        result = self.run_cli([v4(80, "1.1.1.1")], 9400)
        self.assertIn("unexpected connection to 1.1.1.1:80", result.stderr)


if __name__ == "__main__":
    unittest.main()
