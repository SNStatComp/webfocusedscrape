import argparse
import json
import os
import sys


def scan():
    found = {}
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            exe = os.readlink(f"/proc/{pid}/exe")
            args = [
                x.decode(errors="replace")
                for x in open(f"/proc/{pid}/cmdline", "rb").read().split(b"\0")
                if x
            ]
        except Exception:
            continue
        command = " ".join(args)
        name = os.path.basename(exe).lower()
        is_worker = any(a == "src.main" or a.endswith("src.main") for a in args)
        is_chrome = "chrome" in name or "chromium" in name or "headless_shell" in name
        is_driver = "playwright" in command.lower() and (
            "node" in name or "run-driver" in command.lower()
        )
        if is_worker or is_chrome or is_driver:
            found[pid] = {"exe": exe, "command": command[:300]}
    return found


def main():
    parser = argparse.ArgumentParser(description="Check for leftover crawler processes")
    parser.add_argument("--snapshot", help="write current matching PIDs to JSON")
    parser.add_argument("--check", help="fail if any new matching PIDs exist since snapshot")
    args = parser.parse_args()
    current = scan()
    if args.snapshot:
        with open(args.snapshot, "w", encoding="utf-8") as f:
            json.dump(current, f, indent=2, sort_keys=True)
        print(f"snapshot={args.snapshot} processes={len(current)}")
    if args.check:
        with open(args.check, encoding="utf-8") as f:
            before = json.load(f)
        new = {pid: info for pid, info in current.items() if pid not in before}
        print(f"baseline={len(before)} current={len(current)} new={len(new)}")
        for pid, info in new.items():
            print(f"ORPHAN {pid}: {info['command']}")
        if new:
            sys.exit(1)
    if not args.snapshot and not args.check:
        print(f"matching_processes={len(current)}")
        for pid, info in current.items():
            print(f"{pid}: {info['command']}")


if __name__ == "__main__":
    main()
