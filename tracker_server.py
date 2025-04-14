"""Máy chủ tracker dùng để theo dõi các peer trong mạng P2P."""
import os # Thư viện os để thao tác với hệ thống tệp
import json # Thư viện json để xử lý dữ liệu JSON
import logging # Thư viện logging để ghi lại thông tin
import time # Thư viện time để xử lý thời gian
import threading # Thư viện threading để xử lý đa luồng
from http.server import HTTPServer, BaseHTTPRequestHandler # Thư viện http.server để tạo máy chủ HTTP
from pathlib import Path # Thư viện pathlib để thao tác với đường dẫn tệp
import urllib.parse # Thư viện urllib.parse để phân tích URL
from config import METAINFO_DIR, TRACKER_PORT # Nhập METAINFO_DIR từ file config.py

# Đảm bảo METAINFO_DIR là một đối tượng Path
METAINFO_DIR = Path(METAINFO_DIR) 

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("tracker.log", 'w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Thư mục chứa thông tin torrent
os.makedirs(METAINFO_DIR, exist_ok=True) 

# Xóa tất cả các file trong thư mục METAINFO_DIR khi khởi động
def reset_tracker_state():
    """Reset trạng thái của tracker khi khởi động"""
    global peer_registry, registered_peers
    
    # Reset peer_registry
    peer_registry = {}
    registered_peers = set() 
    
    # Xóa tất cả các file trong thư mục metainfo
    try:
        if METAINFO_DIR.exists():
            for file in METAINFO_DIR.glob("*"):
                try:
                    file.unlink()  # Xóa file
                    logging.info(f"Đã xóa file: {file}")
                except Exception as e:
                    logging.error(f"Không thể xóa file {file}: {e}")
        
        # Tạo lại thư mục nếu cần
        os.makedirs(METAINFO_DIR, exist_ok=True)
        logging.info("Đã reset trạng thái tracker")
        
    except Exception as e:
        logging.error(f"Lỗi khi reset tracker: {e}")

# Gọi hàm reset
reset_tracker_state()

# Registry lưu thông tin các peer
peer_registry = {}  # {info_hash: [peer1, peer2, ...]}
registered_peers = set()  # Tập hợp các peer_id đã đăng ký
PEER_TIMEOUT = 1800  # 30 phút

def load_metainfo(info_hash):
    """
    Tìm và đọc file metainfo dựa trên info_hash
    parameter: 
        info_hash: info_hash của torrent
    return: 
        dữ liệu metainfo nếu tìm thấy, None nếu không tìm thấy
    """
    for file in METAINFO_DIR.glob("*.torrent.json"): 
        try:
            with open(file, 'r', encoding='utf-8') as f: 
                data = json.load(f)
                if data.get('info_hash') == info_hash:
                    return data
        except Exception as e:
            logging.error(f"Lỗi đọc file {file}: {e}")
    return None

##################
def cleanup_inactive_peers():
    """Xóa các peer không hoạt động"""
    now = time.time()
    removed_count = 0
    
    for info_hash in list(peer_registry.keys()):
        active_peers = [p for p in peer_registry[info_hash] 
                      if now - p.get("last_active", 0) < PEER_TIMEOUT]
        
        removed_count += len(peer_registry[info_hash]) - len(active_peers)
        peer_registry[info_hash] = active_peers
        
        if not peer_registry[info_hash]:
            del peer_registry[info_hash]
            
    if removed_count > 0:
        logging.info(f"Đã xóa {removed_count} peer không hoạt động")

def handle_registration(data):
    """
    Xử lý đăng ký peer với tracker
    
    Parameters:
        data: Thông tin từ peer gửi lên
    
    Returns:
        dict: Kết quả đăng ký
    """
    global registered_peers
    
    # Lấy thông tin cơ bản
    peer_id = data.get("peer_id")
    ip = data.get("ip", "127.0.0.1")
    port = int(data.get("port", 6881))
    
    # Kiểm tra dữ liệu đầu vào
    if not peer_id:
        return {"success": False, "reason": "Thiếu peer_id"}
    
    # Kiểm tra xem peer đã đăng ký chưa
    if peer_id in registered_peers:
        return {
            "success": True, 
            "message": "Peer đã đăng ký trước đó",
            "peer_id": peer_id
        }
    
    # Đăng ký peer mới
    registered_peers.add(peer_id)
    logging.info(f"Peer mới đăng ký: {peer_id[:8]} từ {ip}:{port}")
    
    return {
        "success": True,
        "message": "Đăng ký thành công",
        "peer_id": peer_id,
        "tracker_id": "simple_tracker"
    }

def handle_announce(data):
    """Xử lý announce request từ peer"""
    # Lấy thông tin cơ bản
    info_hash = data.get("info_hash")
    peer_id = data.get("peer_id")
    ip = data.get("ip", "127.0.0.1") 
    port = int(data.get("port", 6881))
    uploaded = int(data.get("uploaded", 0)) 
    downloaded = int(data.get("downloaded", 0))
    left = int(data.get("left", 0))
    event = data.get("event")
    
    # Kiểm tra dữ liệu đầu vào
    if not info_hash or not peer_id:
        return {"failure_reason": "Thiếu info_hash hoặc peer_id"}
    
     # Kiểm tra xem peer đã đăng ký chưa
    if peer_id not in registered_peers:
        return {"failure_reason": "Peer chưa đăng ký với tracker. Vui lòng đăng ký trước."}
    
    # Tạo hoặc lấy danh sách peer cho info_hash
    if info_hash not in peer_registry:
        peer_registry[info_hash] = []
    
    # Thông tin peer
    peer_info = {
        "peer_id": peer_id,
        "ip": ip,
        "port": port,
        "left": left,
        "last_active": time.time() 
    }
    
    # Xử lý theo loại event
    if event == "started":
        # Kiểm tra nếu peer đã tồn tại
        existing_peer = next((p for p in peer_registry[info_hash] if p["peer_id"] == peer_id), None)
        if existing_peer:
            existing_peer.update(peer_info)
        else:
            peer_registry[info_hash].append(peer_info)
        logging.info(f"Peer {peer_id[:8]} bắt đầu tải {info_hash[:8]}")
            
    elif event == "completed":
        existing_peer = next((p for p in peer_registry[info_hash] if p["peer_id"] == peer_id), None)
        if existing_peer:
            existing_peer.update(peer_info)
            existing_peer["left"] = 0
        else:
            peer_info["left"] = 0
            peer_registry[info_hash].append(peer_info)
        logging.info(f"Peer {peer_id[:8]} đã tải xong {info_hash[:8]}")
            
    elif event == "stopped":
        peer_registry[info_hash] = [p for p in peer_registry[info_hash] if p["peer_id"] != peer_id]
        logging.info(f"Peer {peer_id[:8]} đã dừng tải {info_hash[:8]}")
    else:  # Regular announce
        existing_peer = next((p for p in peer_registry[info_hash] if p["peer_id"] == peer_id), None)
        if existing_peer:
            existing_peer.update(peer_info)
        else:
            peer_registry[info_hash].append(peer_info)
    
    # Lấy danh sách peer khác
    other_peers = [p for p in peer_registry[info_hash] if p["peer_id"] != peer_id]
    
    # Chuẩn bị danh sách trả về
    peers_list = []
    for p in other_peers:
        try:
            peers_list.append({
                "peer_id": p["peer_id"],
                "ip": p["ip"],
                "port": p["port"]
            })
        except Exception as e:
            logging.warning(f"Lỗi định dạng peer: {e}")
    
    # Số lượng seeder và leecher
    # num_seeders = sum(1 for p in peer_registry[info_hash] if p["left"] == 0)
    # num_leechers = sum(1 for p in peer_registry[info_hash] if p["left"] > 0)
    
    # Phản hồi
    response = {
        "tracker_id": "simple_tracker",
        "peers": peers_list
    }
    
    return response

class TrackerHandler(BaseHTTPRequestHandler):
    """
    HTTP Handler cho tracker server
    """
    
    def _send_json_response(self, data, status=200):
        """Gửi JSON response"""
        self.send_response(status)
        self.send_header('Content-Type', 'application/json') 
        self.end_headers()
        self.wfile.write(json.dumps(data).encode('utf-8'))
    
    def do_GET(self):
        """Xử lý GET request"""
        # Hiển thị thông tin tổng quan khi truy cập trang chủ
        self._send_json_response({
            "torrents": len(peer_registry),
            "peers": sum(len(peers) for peers in peer_registry.values()), 
            "seeders": sum(sum(1 for p in peers if p["left"] == 0) for peers in peer_registry.values()),
            "leechers": sum(sum(1 for p in peers if p["left"] > 0) for peers in peer_registry.values())
        })
    
    def do_POST(self):
        """Xử lý POST request"""
        parsed_url = urllib.parse.urlparse(self.path) 
        path = parsed_url.path 
        
        # Đọc và phân tích body
        content_length = int(self.headers.get('Content-Length', 0)) 
        post_data = self.rfile.read(content_length) 

        try:
            data = json.loads(post_data.decode('utf-8'))
        except json.JSONDecodeError:
            self._send_json_response({"failure_reason": "Invalid JSON format"}, 400)
            return
        
        if path == '/register':
            # Xử lý đăng ký peer
            response = handle_registration(data)
            self._send_json_response(response)
        elif path == '/announce':
            # Xử lý announce từ peer
            response = handle_announce(data)
            self._send_json_response(response)
        else:
            # Đường dẫn không hợp lệ
            self._send_json_response({"error": "Not found"}, 404)

def start_cleanup_thread():
    """Bắt đầu thread định kỳ dọn dẹp các peer không hoạt động"""
    def cleanup_task():
        while True:
            try:
                cleanup_inactive_peers()
            except Exception as e:
                logging.error(f"Lỗi trong task dọn dẹp: {e}")
            time.sleep(300)  # Chạy mỗi 5 phút
    
    thread = threading.Thread(target=cleanup_task, daemon=True)
    thread.start()
    logging.info("Thread dọn dẹp peer đã khởi động")

def run_server(port=8000):
    """Khởi động HTTP server"""
    server_address = ('', port)
    httpd = HTTPServer(server_address, TrackerHandler)
    logging.info(f"Tracker đang chạy trên cổng {port}...")

    server = HTTPServer(('0.0.0.0', TRACKER_PORT), TrackerHandler)
    server.serve_forever()
    
    # Bắt đầu thread dọn dẹp
    start_cleanup_thread()
    
    # Khởi động server
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logging.info("Đã nhận lệnh tắt server...")
    finally:
        httpd.server_close()
        logging.info("Tracker đã dừng.")

if __name__ == "__main__":
    run_server(8000)