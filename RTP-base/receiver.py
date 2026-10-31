import argparse
import socket
import sys

from utils import PacketHeader, compute_checksum

def receiver(receiver_ip, receiver_port, window_size):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((receiver_ip, receiver_port))

    da_ket_noi = False
    sender_addr = None
    seq_cho = 1
    bo_dem = {}

    while True:
        raw_pkt, addr = sock.recvfrom(2048)
        
        hdr = PacketHeader(raw_pkt[:16])
        msg = raw_pkt[16 : 16 + hdr.len]

        cs_goc = hdr.checksum
        hdr.checksum = 0
        cs_tinh = compute_checksum(hdr / msg)
        if cs_goc != cs_tinh:
            continue

        if hdr.type == 0:
            if not da_ket_noi:
                da_ket_noi = True
                sender_addr = addr
                seq_cho = 1
                bo_dem.clear()
                
                ack = PacketHeader(type=3, seq_num=1, len=0, checksum=0)
                ack.checksum = compute_checksum(ack / b"")
                sock.sendto(bytes(ack), sender_addr)
            continue

        if not da_ket_noi or addr != sender_addr:
            continue

        if hdr.type == 2:
            if hdr.seq_num >= seq_cho + window_size:
                continue

            if hdr.seq_num < seq_cho:
                ack = PacketHeader(type=3, seq_num=seq_cho, len=0, checksum=0)
                ack.checksum = compute_checksum(ack / b"")
                sock.sendto(bytes(ack), sender_addr)
            else:
                bo_dem[hdr.seq_num] = msg
                while seq_cho in bo_dem:
                    chunk = bo_dem.pop(seq_cho)
                    sys.stdout.buffer.write(chunk)
                    sys.stdout.flush()
                    seq_cho = seq_cho + 1

                ack = PacketHeader(type=3, seq_num=seq_cho, len=0, checksum=0)
                ack.checksum = compute_checksum(ack / b"")
                sock.sendto(bytes(ack), sender_addr)

        elif hdr.type == 1:
            if hdr.seq_num == seq_cho:
                ack = PacketHeader(type=3, seq_num=hdr.seq_num + 1, len=0, checksum=0)
                ack.checksum = compute_checksum(ack / b"")
                sock.sendto(bytes(ack), sender_addr)
                break

    sock.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("receiver_ip", help="The IP address of the host that receiver is running on")
    parser.add_argument("receiver_port", type=int, help="The port number on which receiver is listening")
    parser.add_argument("window_size", type=int, help="Maximum number of outstanding packets")
    args = parser.parse_args()

    receiver(args.receiver_ip, args.receiver_port, args.window_size)

if __name__ == "__main__":
    main()