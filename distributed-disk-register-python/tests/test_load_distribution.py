#!/usr/bin/env python3
"""
Test scripti: Mesaj dağılımını test eder
- Tolerans: 2 (tolerance.conf)
- Node sayısı: 4
- Mesaj sayısı: 100 (hızlı test için)
"""
import time
import subprocess
import sys
import os
import socket
from pathlib import Path

def start_nodes(node_count=4):
    """4 node başlatır"""
    processes = []
    base_port = 50051
    base_dir = Path(__file__).parent.parent
    test_dir = Path(__file__).parent
    
    print(f"🚀 {node_count} node başlatılıyor...")
    for i in range(1, node_count + 1):
        port = base_port + i - 1
        io_mode = os.environ.get("IO_MODE", "buffered")
        # Node'u doğru parametrelerle başlat
        cmd = [
            sys.executable, 
            str(base_dir / "src" / "node.py"),
            "--id", str(i),
            "--port", str(port),
            "--io-mode", io_mode
        ]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=str(test_dir))
        processes.append((proc, port, f"storage_node_{i}"))
        print(f"  ✓ Node {i} başlatıldı (port: {port}, io_mode: {io_mode})")
        time.sleep(0.5)
    
    return processes

def start_leader():
    """Leader server başlatır"""
    print("\n👑 Leader server başlatılıyor...")
    base_dir = Path(__file__).parent.parent
    test_dir = Path(__file__).parent
    cmd = [sys.executable, str(base_dir / "src" / "server.py")]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=str(test_dir))
    time.sleep(2)  # Leader'ın başlaması için bekle
    print("  ✓ Leader başlatıldı")
    return proc

def send_messages(message_count=100):
    """Mesajları socket ile hızlıca gönderir"""
    print(f"\n📤 {message_count} mesaj gönderiliyor...")
    
    try:
        # Leader'a bağlan
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(('localhost', 6666))
        
        for i in range(message_count):
            msg_id = 100 + i
            msg_content = f"Test mesaji {i+1}"
            
            # SET komutu gönder
            command = f"SET {msg_id} {msg_content}"
            s.sendall(command.encode())
            
            # Yanıt al
            response = s.recv(1024).decode()
            
            if (i + 1) % 25 == 0:
                print(f"  ✓ {i+1}/{message_count} mesaj gönderildi")
        
        s.sendall(b"EXIT")
        s.close()
        
        print(f"  ✓ Tüm {message_count} mesaj gönderildi!")
        
    except Exception as e:
        print(f"  ❌ Mesaj gönderme hatası: {e}")

def count_messages_in_nodes(node_count=4, message_count=100):
    """Her node'daki mesaj sayısını sayar"""
    print("\n📊 Mesaj dağılımı analiz ediliyor...\n")
    
    test_dir = Path(__file__).parent
    total = 0
    for i in range(1, node_count + 1):
        # storage_node_1, storage_node_2, etc.
        storage_dir = test_dir / f"storage_node_{i}"
        if storage_dir.exists():
            files = list(storage_dir.glob("*.txt"))
            count = len(files)
            total += count
            
            percentage = (count / (message_count * 2) * 100) if message_count * 2 > 0 else 0
            print(f"  Node {i}: {count:4d} mesaj ({percentage:5.1f}%)")
        else:
            print(f"  Node {i}: HATA - Klasör bulunamadı")
    
    print(f"\n  Toplam: {total} mesaj yazıldı")
    print(f"  Beklenen: {message_count * 2} ({message_count} mesaj × tolerans 2)")
    
    # İdeal dağılım
    if total > 0:
        ideal_per_node = (message_count * 2) / node_count
        print(f"  İdeal her node: {ideal_per_node:.0f} mesaj")

def cleanup(processes):
    """Tüm process'leri temizler"""
    print("\n🧹 Temizlik yapılıyor...")
    for proc, port, storage_dir in processes:
        proc.terminate()
        proc.wait()
    print("  ✓ Tüm node'lar durduruldu")

if __name__ == "__main__":
    try:
        NODE_COUNT = 4
        MESSAGE_COUNT = 100
        durations = {}
        for io_mode in ["buffered", "unbuffered"]:
            print("=" * 60)
            print(f"      DAĞITIK DISK KAYIT SİSTEMİ - YÜKLEME TESTİ ({io_mode.upper()} MODE)")
            print("=" * 60)
            print(f"Node Sayısı   : {NODE_COUNT}")
            print(f"Mesaj Sayısı  : {MESSAGE_COUNT}")
            print(f"Tolerans      : 2 (tolerance.conf)")
            print(f"IO Modu       : {io_mode}")
            print("=" * 60)

            os.environ["IO_MODE"] = io_mode
            # Leader başlat
            leader_proc = start_leader()
            # Node'ları başlat
            node_processes = start_nodes(NODE_COUNT)
            print("\n⏳ Sistem hazırlanıyor...")
            time.sleep(3)
            # Mesajları gönder (süre ölçümü)
            start_time = time.time()
            send_messages(MESSAGE_COUNT)
            end_time = time.time()
            duration = end_time - start_time
            durations[io_mode] = duration
            print(f"\n⏳ Yazma işlemleri tamamlanıyor...")
            time.sleep(3)
            count_messages_in_nodes(NODE_COUNT, MESSAGE_COUNT)
            print("\n" + "=" * 60)
            print(f"✅ {io_mode.upper()} TEST tamamlandı! Süre: {duration:.2f} saniye")
            print("=" * 60)
            # Temizlik
            cleanup(node_processes + [(leader_proc, None, None)])
            time.sleep(2)
        print("\n" + "#" * 60)
        print("Süre Karşılaştırması:")
        print(f"Buffered IO:   {durations['buffered']:.2f} saniye")
        print(f"Unbuffered IO: {durations['unbuffered']:.2f} saniye")
        print("#" * 60)
    except KeyboardInterrupt:
        print("\n\n⚠️  Test kullanıcı tarafından iptal edildi")
    except Exception as e:
        print(f"\n\n❌ Hata: {e}")
