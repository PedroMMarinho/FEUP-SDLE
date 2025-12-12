import argparse
from fileinput import filename
import hashlib
from src.proxy.proxyCommunication import ProxyCommunicator


def main():
    parser = argparse.ArgumentParser(description="Shopping List Server")
    parser.add_argument("--port", required=True, help="Port to run the server on")
    parser.add_argument("--servers", type=str, help="file containing known servers", default=None)
    parser.add_argument("--proxies", type=str, help="file containing known proxies", default=None)
    args = parser.parse_args()

    filenameServer = args.servers
    filenameProxy = args.proxies

    known_server_ports = []
    known_proxy_ports = []
 
    if filenameServer:
        try:
            with open(filenameServer, "r") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        name, port_str = line.split(":")
                        known_server_ports.append((port_str, hashlib.sha256(f"server_{port_str}".encode()).hexdigest()))
        except Exception as e:
            print(f"[Warning] Could not read known servers/proxies file: {e}")


    for name, port in known_server_ports:
        print(f"[System] Known server: {name} at port {port}")

    if filenameProxy:
        try:
            with open(filenameProxy, "r") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        name, port_str = line.split(":")
                        known_proxy_ports.append((port_str, hashlib.sha256(f"proxy_{port_str}".encode()).hexdigest()))
        except Exception as e:
            print(f"[Warning] Could not read known servers/proxies file: {e}")

    for name, port in known_proxy_ports:
        print(f"[System] Known proxy: {name} at port {port}")


    comm = ProxyCommunicator(args.port, args.seed, known_server_ports, known_proxy_ports) 
    comm.start()


if __name__ == "__main__":
    main()

