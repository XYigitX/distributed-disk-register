import grpc
from concurrent import futures
import sys
import os
import threading
import time

# Proto dosyalarini ice aktarabilmek icin path ayari
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(base_dir)
sys.path.append(os.path.join(base_dir, 'generated'))

from generated import family_pb2
from generated import family_pb2_grpc

class WorkerNode(family_pb2_grpc.FamilyServiceServicer):
    def __init__(self, node_id, storage_dir, io_mode="buffered"):
        self.node_id = node_id
        self.storage_dir = storage_dir
        self.io_mode = io_mode  # "buffered" veya "unbuffered"
        if not os.path.exists(storage_dir):
            os.makedirs(storage_dir)

    def StoreMessage(self, request, context):
        msg = request.chat_message
        file_path = os.path.join(self.storage_dir, f"{msg.message_id}.txt")
        
        if self.io_mode == "unbuffered":
            # Unbuffered IO: Doğrudan işletim sistemi çağrısı
            fd = os.open(file_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
            os.write(fd, msg.message.encode())
            os.close(fd)
            print(f"[NODE {self.node_id}] Mesaj kaydedildi (UNBUFFERED): ID={msg.message_id}")
        else:
            # Buffered IO: Python'un standart buffered write
            with open(file_path, "w", buffering=8192) as f:
                f.write(msg.message)
            print(f"[NODE {self.node_id}] Mesaj kaydedildi (BUFFERED): ID={msg.message_id}")
        
        return family_pb2.StoreResponse(success=True)

    def GetMessage(self, request, context):
        msg_id = request.message_id
        file_path = os.path.join(self.storage_dir, f"{msg_id}.txt")
        if os.path.exists(file_path):
            with open(file_path, "r") as f:
                content = f.read()
            return family_pb2.GetResponse(
                chat_message=family_pb2.ChatMessage(message_id=msg_id, message=content),
                found=True
            )
        return family_pb2.GetResponse(found=False)

    def ListMessages(self, request, context):
        """Node'daki tüm mesajları listele"""
        try:
            files = os.listdir(self.storage_dir)
            for filename in files:
                if filename.endswith('.txt'):
                    try:
                        msg_id = int(filename.replace('.txt', ''))
                        file_path = os.path.join(self.storage_dir, filename)
                        with open(file_path, "r") as f:
                            content = f.read()
                        yield family_pb2.ChatMessage(message_id=msg_id, message=content)
                    except:
                        pass
        except Exception as e:
            print(f"[NODE {self.node_id}] ListMessages hatası: {e}")

    def report_status(self):
        while True:
            time.sleep(5)
            files = os.listdir(self.storage_dir)
            # Terminal temizle
            os.system('cls' if os.name == 'nt' else 'clear')
            print("=" * 40)
            print(f"    NODE {self.node_id} - CANLI RAPOR")
            print("=" * 40)
            print(f"\nIO Modu: {self.io_mode.upper()}")
            print(f"Disk Klasoru: {self.storage_dir}")
            print(f"\nSaklanan Mesaj Sayisi: {len(files)}")
            print("=" * 40)

def serve(node_id, port, leader_addr="localhost:5550", io_mode="buffered"):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    worker = WorkerNode(node_id, f"storage_node_{node_id}", io_mode=io_mode)
    family_pb2_grpc.add_FamilyServiceServicer_to_server(worker, server)
    server.add_insecure_port(f'0.0.0.0:{port}')
    server.start()
    
    print(f"[NODE {node_id}] Baslatildi, Port: {port}")
    
    # Lidere kaydol
    channel = grpc.insecure_channel(leader_addr)
    stub = family_pb2_grpc.FamilyServiceStub(channel)
    node_info = family_pb2.NodeInfo(node_id=int(node_id), address=f"localhost:{port}")
    stub.RegisterNode(family_pb2.RegisterNodeRequest(node_info=node_info))
    
    # Raporlama thread'i
    threading.Thread(target=worker.report_status, daemon=True).start()
    
    server.wait_for_termination()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type=int, required=True)
    parser.add_argument("--port", type=str, required=True)
    parser.add_argument("--io-mode", type=str, default="buffered", choices=["buffered", "unbuffered"],
                        help="IO modu: buffered (varsayılan) veya unbuffered")
    args = parser.parse_args()
    serve(args.id, args.port, io_mode=args.io_mode)
