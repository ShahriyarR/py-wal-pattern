import argparse
import json

import requests

from pywalpattern.service.server.client import KVClient
from pywalpattern.service.server.server import KVServer


def run_server():
    parser = argparse.ArgumentParser(description="WAL Key-Value Store Server")
    parser.add_argument("--host", default="localhost", help="Host to bind to")
    parser.add_argument("--port", type=int, default=9090, help="Port to bind to")
    parser.add_argument("--data-dir", default="./data", help="Directory for data storage")
    parser.add_argument("--is-leader", action="store_true", help="Run as leader")
    parser.add_argument("--leader-address", help="Leader address for follower registration")

    args = parser.parse_args()

    server = KVServer(args.host, args.port, args.data_dir, args.is_leader)
    if not args.is_leader and args.leader_address:
        requests.post(f"http://{args.leader_address}/register_follower", json={"address": f"{args.host}:{args.port + 1}"}, timeout=10)

    try:
        server.start()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()


def run_client():  # noqa: CCR001 C901
    parser = argparse.ArgumentParser(description="WAL Key-Value Store Client")
    parser.add_argument("--host", default="localhost", help="Server host")
    parser.add_argument("--port", type=int, default=9090, help="Server port")

    args = parser.parse_args()

    client = KVClient(args.host, args.port)

    if not client.connect():
        return

    print("Connected to server. Type 'help' for commands.")

    try:
        while True:
            cmd = input("KV> ").strip().split()
            if not cmd:
                continue

            command = cmd[0].lower()

            if command == "help":
                print("Commands:")
                print("  get <key>            - Get a value")
                print("  put <key> <value>    - Store a value")
                print("  delete <key>         - Delete a key")
                print("  keys                 - List all keys")
                print("  checkpoint           - Create a checkpoint")
                print("  quit                 - Exit the client")

            elif command == "get":
                if len(cmd) < 2:
                    print("Usage: get <key>")
                    continue

                key = cmd[1]
                value = client.get(key)
                if value is not None:
                    print(f"Value: {value}")

            elif command == "put":
                if len(cmd) < 3:
                    print("Usage: put <key> <value>")
                    continue

                key = cmd[1]
                # Try to parse as JSON, otherwise use as string
                try:
                    value = json.loads(" ".join(cmd[2:]))
                except Exception:
                    value = " ".join(cmd[2:])

                if client.put(key, value):
                    print(f"Key: {key} stored")
                else:
                    print("Error storing value")

            elif command == "delete":
                if len(cmd) < 2:
                    print("Usage: delete <key>")
                    continue

                key = cmd[1]
                if client.delete(key):
                    print(f"Key: {key} deleted")
                else:
                    print(f"Key: {key} not found")

            elif command == "keys":
                keys = client.keys()
                if keys:
                    print("Keys:")
                    for key in keys:
                        print(f"  - {key}")
                else:
                    print("No keys found")

            elif command == "checkpoint":
                if client.checkpoint():
                    print("Checkpoint created")
                else:
                    print("Error creating checkpoint")

            elif command in ("quit", "exit"):
                break

            else:
                print(f"Unknown command: {command}")
                print("Type 'help' for available commands")

    except KeyboardInterrupt:
        print("\nExiting...")
    finally:
        client.disconnect()


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "server":
        # Remove 'server' argument
        sys.argv.pop(1)
        run_server()
    else:
        run_client()
