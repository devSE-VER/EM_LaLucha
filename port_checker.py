import socket
import argparse
import sys


def check_port(host, port, timeout=2):
    """
    Check if a port is open on the specified host

    Args:
        host (str): The host/IP address to check
        port (int): The port number to check
        timeout (int): Connection timeout in seconds

    Returns:
        bool: True if port is open, False otherwise
    """
    try:
        # Create a socket object
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)

        # Attempt to connect to the host and port
        result = sock.connect_ex((host, port))
        sock.close()

        # If result is 0, the port is open
        return result == 0

    except socket.gaierror:
        print(f"Error: Could not resolve hostname '{host}'")
        return False
    except socket.error as e:
        print(f"Error: {e}")
        return False


def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Check if a port is open on a host')
    parser.add_argument('host', help='Host or IP address to check')
    parser.add_argument('port', type=int, help='Port number to check')
    parser.add_argument('-t', '--timeout', type=int, default=2, help='Connection timeout in seconds (default: 2)')

    # Parse arguments
    args = parser.parse_args()

    # Check if the port is open
    is_open = check_port(args.host, args.port, args.timeout)

    if is_open:
        print(f"Port {args.port} is OPEN on {args.host}")
        sys.exit(0)
    else:
        print(f"Port {args.port} is CLOSED on {args.host}")
        sys.exit(1)


if __name__ == "__main__":
    main()