import argparse
import socket
import sys
import time

from utils import PacketHeader, compute_checksum

TIMEOUT = 0.5

def sender(receiver_ip, receiver_port, window_size):
    dich_den = (receiver_ip, receiver_port)
    
    du_lieu = sys.stdin.buffer.read()
    
    max_chunk = 1456
    danh_sach_chunk = []
    vi_tri = 0
    while vi_tri < len(du_lieu):
        danh_sach_chunk.append(du_lieu[vi_tri : vi_tri + max_chunk])
        vi_tri = vi_tri + max_chunk
        
    tong_so_goi = len(danh_sach_chunk)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setblocking(False)

    hdr_start = PacketHeader(type=0, seq_num=0, length=0, checksum=0)
    hdr_start.checksum = compute_checksum(hdr_start / b"")
    sock.sendto(bytes(hdr_start), dich_den)
    t_start = time.time()

    ket_noi_xong = False
    while not ket_noi_xong:
        try:
            res, _ = sock.recvfrom(2048)
            ack_hdr = PacketHeader(res[:16])
            cs_goc = ack_hdr.checksum
            ack_hdr.checksum = 0
            if cs_goc == compute_checksum(ack_hdr / b""):
                if ack_hdr.type == 3 and ack_hdr.seq_num == 1:
                    ket_noi_xong = True
        except BlockingIOError:
            pass

        if time.time() - t_start >= TIMEOUT:
            sock.sendto(bytes(hdr_start), dich_den)
            t_start = time.time()
        time.sleep(0.001)

    base = 1
    next_seq = 1
    timer_cua_so = None

    while base <= tong_so_goi:
        while (next_seq < base + window_size) and (next_seq <= tong_so_goi):
            payload = danh_sach_chunk[next_seq - 1]
            data_hdr = PacketHeader(type=2, seq_num=next_seq, length=len(payload), checksum=0)
            data_hdr.checksum = compute_checksum(data_hdr / payload)
            sock.sendto(bytes(data_hdr / payload), dich_den)

            if base == next_seq:
                timer_cua_so = time.time()
            next_seq = next_seq + 1

        try:
            while True:
                res, _ = sock.recvfrom(2048)
                ack_hdr = PacketHeader(res[:16])
                cs_goc = ack_hdr.checksum
                ack_hdr.checksum = 0
                if cs_goc == compute_checksum(ack_hdr / b""):
                    if ack_hdr.type == 3:
                        if ack_hdr.seq_num > base:
                            base = ack_hdr.seq_num
                            timer_cua_so = time.time()
        except BlockingIOError:
            pass

        if timer_cua_so is not None:
            if time.time() - timer_cua_so >= TIMEOUT:
                for k in range(base, next_seq):
                    pl = danh_sach_chunk[k - 1]
                    re_hdr = PacketHeader(type=2, seq_num=k, length=len(pl), checksum=0)
                    re_hdr.checksum = compute_checksum(re_hdr / pl)
                    sock.sendto(bytes(re_hdr / pl), dich_den)
                timer_cua_so = time.time()

        time.sleep(0.001)

    seq_end = tong_so_goi + 1
    end_hdr = PacketHeader(type=1, seq_num=seq_end, length=0, checksum=0)
    end_hdr.checksum = compute_checksum(end_hdr / b"")
    sock.sendto(bytes(end_hdr), dich_den)
    t_end = time.time()

    while True:
        try:
            res, _ = sock.recvfrom(2048)
            ack_hdr = PacketHeader(res[:16])
            cs_goc = ack_hdr.checksum
            ack_hdr.checksum = 0
            if cs_goc == compute_checksum(ack_hdr / b""):
                if ack_hdr.type == 3 and ack_hdr.seq_num == seq_end + 1:
                    break
        except BlockingIOError:
            pass

        if time.time() - t_end >= TIMEOUT:
            break
        time.sleep(0.001)

    sock.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("receiver_ip", help="The IP address of the host that receiver is running on")
    parser.add_argument("receiver_port", type=int, help="The port number on which receiver is listening")
    parser.add_argument("window_size", type=int, help="Maximum number of outstanding packets")
    args = parser.parse_args()

    sender(args.receiver_ip, args.receiver_port, args.window_size)

if __name__ == "__main__":
    main()