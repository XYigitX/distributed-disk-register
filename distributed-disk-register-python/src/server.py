import grpc
from concurrent import futures
import sys
import os
import socket
import threading
import time

# Proto dosyalarini ice aktarabilmek icin hem ust dizini hem de generated dizinini ekle
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(base_dir)
sys.path.append(os.path.join(base_dir, 'generated'))

from generated import family_pb2
from generated import family_pb2_grpc

class LeaderService(family_pb2_grpc.FamilyServiceServicer):
    def __init__(self, tolerance_level):
        self.tolerance_level = tolerance_level
        self.nodes = {}  # node_id -> {info: NodeInfo, stub: FamilyServiceStub}
        self.message_to_nodes = {}  # message_id -> list of node_ids
        self.lock = threading.Lock()
        self.leader_storage = "leader_metadata"
        self.leader_messages_dir = "leader_messages"  # Lider'in kendi mesaj storage'ı
        if not os.path.exists(self.leader_storage):
            os.makedirs(self.leader_storage)
        if not os.path.exists(self.leader_messages_dir):
            os.makedirs(self.leader_messages_dir)
        self._load_metadata()
        self._load_leader_messages()

    def _load_metadata(self):
        """Lider başlarken eski kayıtları yükler"""
        metadata_file = os.path.join(self.leader_storage, "message_mapping.txt")
        if os.path.exists(metadata_file):
            with open(metadata_file, "r") as f:
                for line in f:
                    parts = line.strip().split(":")
                    if len(parts) == 2:
                        msg_id = int(parts[0])
                        node_ids = [int(x) for x in parts[1].split(",") if x]
                        self.message_to_nodes[msg_id] = node_ids

    def _load_leader_messages(self):
        """Lider'in kendi diskindeki mesajları yükler"""
        try:
            files = os.listdir(self.leader_messages_dir)
            msg_count = len([f for f in files if f.endswith('.txt')])
            if msg_count > 0:
                print(f"[LIDER] Kendi diskinden {msg_count} mesaj yüklendi")
        except:
            pass

    def _save_message_to_leader(self, msg_id, message):
        """Liderin kendi diskine mesaj kaydet"""
        file_path = os.path.join(self.leader_messages_dir, f"{msg_id}.txt")
        with open(file_path, "w") as f:
            f.write(message)

    def _get_message_from_leader(self, msg_id):
        """Liderin kendi diskinden mesaj oku"""
        file_path = os.path.join(self.leader_messages_dir, f"{msg_id}.txt")
        if os.path.exists(file_path):
            with open(file_path, "r") as f:
                return f.read()
        return None

    def _save_metadata(self, msg_id, node_ids):
        """Lider her mesajın hangi node'larda olduğunu diske kaydeder"""
        metadata_file = os.path.join(self.leader_storage, "message_mapping.txt")
        with open(metadata_file, "a") as f:
            f.write(f"{msg_id}:{','.join(map(str, node_ids))}\n")

    def _discover_node_messages(self, node_id, stub):
        """Node'daki mevcut tum mesajlari kesfeder ve senkronize eder"""
        try:
            # Node'dan tüm mesaj listesini al (streaming)
            discovered_count = 0
            synced_to_leader = 0
            messages = stub.ListMessages(family_pb2.Empty(), timeout=5.0)
            
            for msg in messages:
                try:
                    msg_id = msg.message_id
                    message_content = msg.message
                    
                    # 1. Node'da var ama metadata'da yok -> Lider'e kaydet
                    with self.lock:
                        if msg_id not in self.message_to_nodes:
                            self.message_to_nodes[msg_id] = []
                            # Lider'in diskine de kaydet
                            self._save_message_to_leader(msg_id, message_content)
                            synced_to_leader += 1
                            print(f"[SYNC] Mesaj {msg_id} node {node_id}'den lider'e kopyalandı")
                        
                        if node_id not in self.message_to_nodes[msg_id]:
                            self.message_to_nodes[msg_id].append(node_id)
                            discovered_count += 1
                except Exception as e:
                    print(f"[LIDER] Mesaj işleme hatası: {e}")
            
            # 2. Metadata'da var ama node'da yok -> Lider'den node'a dağıt
            synced_to_node = 0
            with self.lock:
                for msg_id, node_list in list(self.message_to_nodes.items()):
                    if node_id in node_list:
                        # Bu node'da olması gerekiyor, kontrol et
                        try:
                            resp = stub.GetMessage(family_pb2.GetRequest(message_id=msg_id), timeout=1.0)
                            if not resp.found:
                                # Node'da yok ama metadata'da var -> Lider'den gönder
                                leader_msg = self._get_message_from_leader(msg_id)
                                if leader_msg:
                                    req = family_pb2.StoreRequest(
                                        chat_message=family_pb2.ChatMessage(message_id=msg_id, message=leader_msg)
                                    )
                                    stub.StoreMessage(req, timeout=2.0)
                                    synced_to_node += 1
                                    print(f"[SYNC] Mesaj {msg_id} lider'den node {node_id}'e gönderildi")
                        except:
                            pass
            
            if discovered_count > 0 or synced_to_leader > 0 or synced_to_node > 0:
                # Tum metadata'yi yeniden kaydet
                metadata_file = os.path.join(self.leader_storage, "message_mapping.txt")
                with open(metadata_file, "w") as f:
                    for msg_id in sorted(self.message_to_nodes.keys()):
                        node_ids = self.message_to_nodes[msg_id]
                        f.write(f"{msg_id}:{','.join(map(str, node_ids))}\n")
                print(f"[LIDER] Node {node_id} senkronizasyonu: {discovered_count} keşfedildi, {synced_to_leader} lider'e alındı, {synced_to_node} node'a gönderildi")
            else:
                print(f"[LIDER] Node {node_id} zaten senkronize")
        except Exception as e:
            print(f"[LIDER] Node {node_id} kesfinde hata: {e}")

    def RegisterNode(self, request, context):
        node_id = request.node_info.node_id
        addr = request.node_info.address
        
        # Node ile iletisim kurmak icin yeni bir kanal ac
        channel = grpc.insecure_channel(addr)
        stub = family_pb2_grpc.FamilyServiceStub(channel)
        
        with self.lock:
            self.nodes[node_id] = {
                "info": request.node_info,
                "stub": stub,
                "last_seen": time.time()  # Son görülme zamanı ekle
            }
        
        # Node'daki mevcut mesajlari kesfet ve metadata'ya ekle
        self._discover_node_messages(node_id, stub)
        
        print(f"[LIDER] Yeni uye kaydedildi: ID={node_id}, Adres={addr}")
        return family_pb2.RegisterNodeResponse(success=True)

    def _check_node_health(self):
        """Periyodik olarak node'ların sağlığını kontrol eder"""
        while True:
            time.sleep(5)  # 5 saniyede bir kontrol
            with self.lock:
                dead_nodes = []
                for node_id, data in list(self.nodes.items()):
                    try:
                        # Ping/health check yap
                        stub = data["stub"]
                        stub.GetMessage(family_pb2.GetRequest(message_id=-1), timeout=1.0)
                        # Başarılı - node aktif
                        self.nodes[node_id]["last_seen"] = time.time()
                    except:
                        # Başarısız - node ölü olabilir
                        last_seen = data.get("last_seen", 0)
                        if time.time() - last_seen > 10:  # 10 saniye cevap vermediyse
                            dead_nodes.append(node_id)
                
                # Ölü node'ları kaldır
                for node_id in dead_nodes:
                    print(f"[LIDER] Node {node_id} yanıt vermiyor, listeden çıkarılıyor...")
                    del self.nodes[node_id]

    def status_report(self):
        """Periyodik raporlama yapar - Terminal temizleyerek"""
        while True:
            time.sleep(10)
            with self.lock:
                total_msgs = len(self.message_to_nodes)
                # Terminal temizle (Windows)
                os.system('cls' if os.name == 'nt' else 'clear')
                print("=" * 50)
                print("       LIDER DURUM RAPORU (CANLI)")
                print("=" * 50)
                print(f"\nToplam Mesaj Sayisi: {total_msgs}")
                print(f"Aktif Node Sayisi: {len(self.nodes)}\n")
                print("-" * 50)
                for node_id, data in self.nodes.items():
                    count = sum(1 for msgs in self.message_to_nodes.values() if node_id in msgs)
                    print(f"  Node {node_id} ({data['info'].address}): {count} mesaj")
                print("=" * 50)

def handle_client(conn, addr, leader_service):
    print(f"[LIDER] Istemci baglandi: {addr}")
    try:
        while True:
            data = conn.recv(1024).decode().strip()
            if not data: break
            
            parts = data.split(' ', 2)
            command = parts[0].upper()
            
            if command == "SET" and len(parts) == 3:
                msg_id = int(parts[1])
                message = parts[2]
                
                with leader_service.lock:
                    # YUK DAGITIMI MANTIGI: Node'lari mesaj sayisina gore sirala
                    # En az mesaji olan node'lari secerek yuk dengelemis oluruz.
                    node_message_counts = []
                    for nid in leader_service.nodes.keys():
                        count = sum(1 for msgs in leader_service.message_to_nodes.values() if nid in msgs)
                        node_message_counts.append((nid, count))
                    
                    # Sayiya gore kucukten buyuge sirala
                    node_message_counts.sort(key=lambda x: x[1])
                    available_node_ids = [x[0] for x in node_message_counts]
                
                if len(available_node_ids) < leader_service.tolerance_level:
                    conn.sendall(b"ERROR: Yeterli aktif uye yok\n")
                    continue

                # En "bos" olan node'lari secelim
                target_node_ids = available_node_ids[:leader_service.tolerance_level]
                
                success_count = 0
                stored_ids = []
                for nid in target_node_ids:
                    try:
                        node_stub = leader_service.nodes[nid]["stub"]
                        req = family_pb2.StoreRequest(chat_message=family_pb2.ChatMessage(message_id=msg_id, message=message))
                        resp = node_stub.StoreMessage(req)
                        if resp.success:
                            success_count += 1
                            stored_ids.append(nid)
                    except Exception as e:
                        print(f"Node {nid} hatasi: {e}")

                if success_count >= leader_service.tolerance_level:
                    with leader_service.lock:
                        # Lider kendi diskine de kaydet
                        leader_service._save_message_to_leader(msg_id, message)
                        leader_service.message_to_nodes[msg_id] = stored_ids
                        leader_service._save_metadata(msg_id, stored_ids)  # Diske kaydet
                    conn.sendall(b"OK\n")
                else:
                    conn.sendall(b"ERROR: Kayit tamamlanamadi\n")

            elif command == "GET" and len(parts) == 2:
                msg_id = int(parts[1])
                
                # Önce lider'in kendi diskinden dene
                leader_msg = leader_service._get_message_from_leader(msg_id)
                if leader_msg:
                    conn.sendall(f"VALUE {leader_msg}\n".encode())
                    continue
                
                # Lider'de yoksa node'lardan ara
                with leader_service.lock:
                    target_nodes = leader_service.message_to_nodes.get(msg_id, [])
                    # Eger metadata'da yoksa, tum node'larda ara
                    if not target_nodes:
                        target_nodes = list(leader_service.nodes.keys())
                
                found = False
                found_node_ids = []
                for nid in target_nodes:
                    try:
                        if nid not in leader_service.nodes:
                            continue  # Node artik kayitli degil
                        node_stub = leader_service.nodes[nid]["stub"]
                        resp = node_stub.GetMessage(family_pb2.GetRequest(message_id=msg_id))
                        if resp.found:
                            if not found:  # Ilk bulunusta cevap gonder
                                conn.sendall(f"VALUE {resp.chat_message.message}\n".encode())
                                found = True
                                # Lider'in diskine de kaydet (senkronizasyon)
                                leader_service._save_message_to_leader(msg_id, resp.chat_message.message)
                            found_node_ids.append(nid)
                    except:
                        continue # Diger node'u dene (Hata toleransi burada devreye girer)
                
                # Eger bulunduysa ve metadata'da yoksa, metadata'yi guncelle
                if found and msg_id not in leader_service.message_to_nodes:
                    with leader_service.lock:
                        leader_service.message_to_nodes[msg_id] = found_node_ids
                        leader_service._save_metadata(msg_id, found_node_ids)
                
                if not found:
                    conn.sendall(b"ERROR: Mesaj bulunamadi\n")
            else:
                conn.sendall(b"ERROR: Gecersiz komut\n")
    except Exception as e:
        print(f"Istemci hatasi: {e}")
    finally:
        conn.close()

def run_socket_server(leader_service, port=50052):
    """Clientlar icin text tabanlı socket sunucusu"""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('0.0.0.0', port))
    s.listen(5)
    print(f"[LIDER] Istemci (Socket) sunucusu baslatildi, Port: {port}")
    while True:
        conn, addr = s.accept()
        threading.Thread(target=handle_client, args=(conn, addr, leader_service)).start()

def load_tolerance():
    try:
        with open(os.path.join(base_dir, 'tolerance.conf'), 'r') as f:
            for line in f:
                if line.startswith('tolerance='):
                    return int(line.split('=')[1])
    except:
        return 2 # Varsayilan

def serve(grpc_port="5550", socket_port=6666):
    tolerance = load_tolerance()
    print(f"[LIDER] Tolerans Seviyesi: {tolerance}")
    
    leader_service = LeaderService(tolerance)
    
    # gRPC Sunucusu (Aile ici haberlesme)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    family_pb2_grpc.add_FamilyServiceServicer_to_server(leader_service, server)
    server.add_insecure_port(f'0.0.0.0:{grpc_port}')
    server.start()
    print(f"[LIDER] Aile (gRPC) sunucusu baslatildi, Port: {grpc_port}")

    # Raporlama thread'ini baslat
    threading.Thread(target=leader_service.status_report, daemon=True).start()
    
    # Health check thread'ini baslat
    threading.Thread(target=leader_service._check_node_health, daemon=True).start()

    # Socket Sunucusu (Istemci haberlesmesi) - Ana thread'de kalsin
    run_socket_server(leader_service, port=socket_port)

if __name__ == "__main__":
    serve(grpc_port="5550", socket_port=6666)
