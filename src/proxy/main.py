import argparse
from src.proxy.proxyCommunication import ProxyCommunicator


def main():
    parser = argparse.ArgumentParser(description="Shopping List Server")
    parser.add_argument("--port", required=True, help="Port to run the server on")
    parser.add_argument("--seed", type=bool, default=False, help="Whether it is the first proxy to start")
    parser.add_argument("--known_server_port", type=int,required=False,help="Port of a known server to connect to ")
    parser.add_argument("--known_proxy_port", required=False, help="Port for the proxy to connect if not seeding")
    args = parser.parse_args()

    if args.seed and not args.known_server_port:
        print("[Fatal] Seed proxies must provide --known_server_port to connect to an existing server.")
        return

    if not args.seed and not args.known_proxy_port:
        print("[Fatal] Non-seed proxies must provide --known_proxy_port to connect to an existing proxy.")
        return

    comm = ProxyCommunicator(args.port, args.seed, args.known_server_port, args.known_proxy_port)
    comm.start()


if __name__ == "__main__":
    main()

