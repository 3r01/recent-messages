#!/usr/bin/env python3
"""Advertise only a ready Recentmessages instance through its shared tunnel."""
import subprocess
import time
import urllib.error
import urllib.request

UNIT = "cloudflared-rm.service"
READY_URL = "http://127.0.0.1:2790/health/ready"
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


class Admission:
    def __init__(self):
        self.good = 0
        self.bad = 0
        self.admitted = False

    def observe(self, ready):
        self.good = self.good + 1 if ready else 0
        self.bad = 0 if ready else self.bad + 1
        if self.good >= 2 and not self.admitted:
            return "start"
        if self.bad >= 3 and self.admitted:
            return "stop"
        return None


def is_ready():
    try:
        with OPENER.open(READY_URL, timeout=3) as response:
            return response.status == 204
    except (OSError, urllib.error.URLError):
        return False


def action(verb):
    subprocess.run(["systemctl", verb, UNIT], check=True, timeout=35)


def main():
    state = Admission()
    action("stop")
    try:
        while True:
            verb = state.observe(is_ready())
            if verb:
                try:
                    action(verb)
                    state.admitted = verb == "start"
                    print(f"tunnel admission: {verb}", flush=True)
                except (subprocess.SubprocessError, OSError):
                    print(f"tunnel admission: {verb} failed; retrying", flush=True)
            elif state.admitted:
                if subprocess.run(["systemctl", "is-active", "--quiet", UNIT], timeout=5).returncode:
                    state.admitted = False
            time.sleep(5)
    finally:
        action("stop")


if __name__ == "__main__":
    main()
