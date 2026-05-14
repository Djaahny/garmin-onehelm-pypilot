import socket
import time

PI_IP = "172.16.85.190"
PORT = 8000
UUID = "99223b99-e4d3-4552-b43b-7b3360c937f7"

MCAST_GRP = "239.255.255.250"
MCAST_PORT = 1900

response_template = (
    "HTTP/1.1 200 OK\r\n"
    "CACHE-CONTROL: max-age=120\r\n"
    "EXT:\r\n"
    "LOCATION: http://{ip}:{port}/rootDesc.xml\r\n"
    "SERVER: Linux/6 UPnP/1.0 PiOneHelm/0.1\r\n"
    "ST: upnp:rootdevice\r\n"
    "USN: uuid:{uuid}::upnp:rootdevice\r\n"
    "\r\n"
)

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.bind(("", MCAST_PORT))

mreq = socket.inet_aton(MCAST_GRP) + socket.inet_aton(PI_IP)
sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

print("SSDP responder running...")
print(f"LOCATION: http://{PI_IP}:{PORT}/onehelm/upnp.xml")
print(f"UUID: {UUID}")

while True:
    data, addr = sock.recvfrom(2048)
    text = data.decode(errors="ignore")

    if "M-SEARCH" in text and "upnp:rootdevice" in text:
        print(f"Got M-SEARCH from {addr}")

        response = response_template.format(
            ip=PI_IP,
            port=PORT,
            uuid=UUID
        )

        time.sleep(0.2)
        sock.sendto(response.encode(), addr)
        print("Sent SSDP response")
