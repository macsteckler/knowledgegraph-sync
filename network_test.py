import socket
import requests
import subprocess
import platform

def check_ip_version():
    print("\n=== Checking IP Version ===")
    # Try to connect to a known IPv4 address
    try:
        socket.create_connection(("8.8.8.8", 53), timeout=3)
        print("✓ IPv4 connectivity: Available")
    except OSError:
        print("✗ IPv4 connectivity: Not available")

    # Try to connect to a known IPv6 address
    try:
        socket.create_connection(("2001:4860:4860::8888", 53), timeout=3)
        print("✓ IPv6 connectivity: Available")
    except OSError:
        print("✗ IPv6 connectivity: Not available")

def check_supabase_connectivity():
    print("\n=== Checking Supabase Connectivity ===")
    host = "aws-0-us-west-1.pooler.supabase.com"
    
    # Try to resolve the hostname
    try:
        print(f"\nResolving {host}...")
        ip_info = socket.getaddrinfo(host, None)
        for info in ip_info:
            family = "IPv4" if info[0] == socket.AF_INET else "IPv6"
            print(f"✓ Resolved to {family} address: {info[4][0]}")
    except socket.gaierror as e:
        print(f"✗ Failed to resolve {host}: {e}")

    # Test connectivity to different ports
    ports = [5432, 6543]  # Session Pooler and Transaction Pooler ports
    for port in ports:
        print(f"\nTesting connection to port {port}...")
        try:
            sock = socket.create_connection((host, port), timeout=5)
            sock.close()
            print(f"✓ Port {port} is reachable")
        except Exception as e:
            print(f"✗ Port {port} is not reachable: {e}")

def run_traceroute():
    print("\n=== Running Traceroute ===")
    host = "aws-0-us-west-1.pooler.supabase.com"
    
    # Use different traceroute commands based on OS
    if platform.system() == "Windows":
        cmd = ["tracert", host]
    else:
        cmd = ["traceroute", "-n", host]
    
    try:
        print(f"\nTraceroute to {host}...")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        print(result.stdout)
    except subprocess.TimeoutExpired:
        print("Traceroute timed out")
    except Exception as e:
        print(f"Failed to run traceroute: {e}")

if __name__ == "__main__":
    print("Network Connectivity Test")
    print("========================")
    
    check_ip_version()
    check_supabase_connectivity()
    run_traceroute()
    
    print("\nTest complete!") 