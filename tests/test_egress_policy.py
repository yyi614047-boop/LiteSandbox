"""Policy decisions must stay in the networked helper on every hop."""

import socket
import unittest
from unittest.mock import Mock, patch

from litesandbox.egress import PublicHTTPS, fetch, normalize_hosts


class SourcePolicyTests(unittest.TestCase):
    def test_exact_domains(self):
        self.assertEqual(normalize_hosts(["PyPI.org", "pypi.org"]), ["pypi.org"])
        for value in ("*.example.org", "127.0.0.1", "evil.example:443", "localhost"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                normalize_hosts([value])

    def test_redirect_rechecked_before_connect(self):
        for destination in ("https://blocked.example/file", "http://allowed.example/file",
                            "https://allowed.example:8443/file", "https://user@allowed.example/file"):
            with self.subTest(destination=destination), patch("litesandbox.egress.PublicHTTPS") as connect:
                response = connect.return_value.getresponse.return_value
                response.status = 302
                response.getheader.return_value = destination
                with self.assertRaises(PermissionError):
                    fetch("https://allowed.example/start", {"allowed.example"}, "GET")
                self.assertEqual(connect.call_count, 1)

    def test_dns_mixed_with_private_address_is_rejected(self):
        rows = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, 443))
                for ip in ("1.1.1.1", "169.254.169.254")]
        with patch("socket.getaddrinfo", return_value=rows), patch("socket.create_connection") as dial:
            with self.assertRaises(PermissionError):
                PublicHTTPS("allowed.example").connect()
            dial.assert_not_called()

    def test_validated_ip_is_used_with_original_tls_hostname(self):
        tls = Mock()
        conn = PublicHTTPS("allowed.example", timeout=8, context=tls)
        rows = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("1.1.1.1", 443))]
        with patch("socket.getaddrinfo", return_value=rows), patch("socket.create_connection") as dial:
            conn.connect()
            dial.assert_called_once_with(("1.1.1.1", 443), timeout=8)
            tls.wrap_socket.assert_called_once_with(dial.return_value, server_hostname="allowed.example")
