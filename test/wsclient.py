# A bare websocket client for the tests: the handshake, masked frames out, whole
# frames in. No library, so a test sees exactly what the server sends.
import base64
import os
import socket
import struct


class WS:
    """a bare client: the handshake, masked frames out, whole frames in"""

    def __init__(self, port, path):
        self.reason = None
        self.s = socket.create_connection(("127.0.0.1", port), timeout=30)
        key = base64.b64encode(os.urandom(16)).decode()
        self.s.sendall(("GET %s HTTP/1.1\r\nHost: 127.0.0.1\r\nUpgrade: websocket\r\n"
                        "Connection: Upgrade\r\nSec-WebSocket-Key: %s\r\n"
                        "Sec-WebSocket-Version: 13\r\n\r\n" % (path, key)).encode())
        self.buf = b""
        while b"\r\n\r\n" not in self.buf:
            got = self.s.recv(4096)
            if not got:
                break
            self.buf += got
        head, _, self.buf = self.buf.partition(b"\r\n\r\n")
        self.status = head.split(b"\r\n", 1)[0]

    def send(self, data, opcode=1):
        if isinstance(data, str):
            data = data.encode()
        n = len(data)
        head = bytes([0x80 | opcode])
        if n < 126:
            head += bytes([0x80 | n])
        elif n < 65536:
            head += bytes([0x80 | 126]) + struct.pack(">H", n)
        else:
            head += bytes([0x80 | 127]) + struct.pack(">Q", n)
        mask = os.urandom(4)
        self.s.sendall(head + mask + bytes(b ^ mask[i % 4] for i, b in enumerate(data)))

    def close(self, code=1000):
        self.send(struct.pack(">H", code) + b"bye", opcode=8)

    def _take(self, n):
        while len(self.buf) < n:
            got = self.s.recv(65536)
            if not got:
                raise EOFError
            self.buf += got
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

    def recv(self):
        """(opcode, payload); a close frame's payload is its code, and its reason goes
        in self.reason; EOF is (None, None)"""
        try:
            b0, b1 = self._take(2)
            n = b1 & 0x7f
            if n == 126:
                n = struct.unpack(">H", self._take(2))[0]
            elif n == 127:
                n = struct.unpack(">Q", self._take(8))[0]
            payload = self._take(n)
        except (EOFError, ConnectionError):
            return None, None
        op = b0 & 0x0f
        if op == 8:
            self.reason = payload[2:].decode()
            return 8, struct.unpack(">H", payload[:2])[0] if len(payload) >= 2 else None
        return op, payload

    def end(self):
        self.s.close()
