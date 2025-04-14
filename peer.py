"""Quản lý torrent, chia sẻ và tải file"""
import os
import json
import logging
import threading
import socket
import time
import random
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from pathlib import Path
import hashlib
import requests

# Thêm đường dẫn cha vào sys.path để import config
from config import TRACKER_URL, DEFAULT_PEER_PORT, DOWNLOAD_DIR, METAINFO_DIR, TRACKER_HOST, TRACKER_PORT, TRACKER_URL, WEB_SERVER_PORT
from piece_manager import PieceManager
from peer_connection import PeerConnection

# Cấu hình logging cơ bản
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("peer.log", 'w', 'utf-8'),
        logging.StreamHandler()
    ]
)

class Peer:
    """Quản lý việc chia sẻ và tải file trong mạng P2P"""
        
    def __init__(self, tracker_url=TRACKER_URL):
        """Khởi tạo peer"""
        # Tạo ID cho peer
        self.peer_id = self._generate_peer_id()
        self.tracker_url = tracker_url
        self.port = DEFAULT_PEER_PORT
        
        # Thiết lập thư mục
        self.METAINFO_DIR = Path(METAINFO_DIR)
        self.DOWNLOAD_DIR = Path(DOWNLOAD_DIR)
        os.makedirs(self.DOWNLOAD_DIR, exist_ok=True)
        os.makedirs(self.METAINFO_DIR, exist_ok=True)
        
        # Quản lý torrent
        self.torrents = {}  # {info_hash: {name, status, progress, files, piece_manager}}
        self.connections = {}  # {info_hash: ConnectionManager}
        self.lock = threading.Lock()
        
        # Khởi động server để lắng nghe kết nối từ các peer khác
        self.server_socket = None
        self._start_server()
        
        # Đăng ký với tracker khi khởi động
        if not self.register_with_tracker():
            logging.warning("Không thể đăng ký với tracker. Một số chức năng có thể bị hạn chế.")
        
        # Tải danh sách torrent từ tracker
        try:
            self.load_torrents_from_tracker()
        except Exception as e:
            logging.error(f"Không thể tải danh sách torrent từ tracker: {e}")
        
    def _generate_peer_id(self):
        """Tạo ID ngẫu nhiên cho peer"""
        import random
        import string
        random_part = ''.join(random.choices(string.ascii_letters + string.digits, k=12))
        return f"-PY0001-{random_part}"
    
    def _start_server(self):
        """Khởi động server socket để lắng nghe kết nối từ các peer khác"""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind(('0.0.0.0', self.port))
            self.server_socket.listen(10)
            
            # Khởi động thread xử lý kết nối đến
            server_thread = threading.Thread(target=self._handle_incoming_connections, daemon=True)
            server_thread.start()
            logging.info(f"Đã khởi động server trên cổng {self.port}")
        except Exception as e:
            logging.error(f"Không thể khởi động server: {e}")
            self.server_socket = None

    def register_with_tracker(self):
        """
        Đăng ký peer với tracker
        
        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        try:
            # Chuẩn bị dữ liệu đăng ký
            register_data = {
                "peer_id": self.peer_id,
                "ip": socket.gethostbyname(socket.gethostname()),
                "port": self.port
            }
            
            # Gửi POST request đến tracker
            logging.info(f"Đăng ký với tracker...")
            tracker_base_url = self.tracker_url.replace("/announce", "")
            register_url = f"{tracker_base_url}/register"
            
            # Gửi yêu cầu đăng ký
            response = requests.post(
                register_url,
                json=register_data,
            )
            
            # Kiểm tra phản hồi
            if response.status_code != 200:
                logging.error(f"Lỗi khi đăng ký với tracker: HTTP {response.status_code}")
                return False
            
            # Phân tích phản hồi
            response_data = response.json()
            if not response_data.get("success", False):
                logging.error(f"Đăng ký không thành công: {response_data.get('reason', 'Unknown error')}")
                return False
            
            logging.info(f"Đăng ký với tracker thành công!")
            return True
            
        except Exception as e:
            logging.error(f"Lỗi khi đăng ký với tracker: {str(e)}")
            return False
    
    def _handle_incoming_connections(self):
        """Xử lý các kết nối đến từ các peer khác"""
        while True:
            try:
                client_socket, address = self.server_socket.accept()
                logging.info(f"Kết nối mới từ {address}")
                
                # Khởi động thread mới để xử lý kết nối
                handler_thread = threading.Thread(
                    target=self._handle_peer_connection,
                    args=(client_socket, address),
                    daemon=True
                )
                handler_thread.start()
            except Exception as e:
                logging.error(f"Lỗi khi xử lý kết nối đến: {e}")
                break
    
    def _handle_peer_connection(self, client_socket, address):
        """Xử lý kết nối từ peer"""
        try:
            # Đọc handshake
            pstrlen_bytes = client_socket.recv(1)
            if not pstrlen_bytes:
                client_socket.close()
                return
            
            pstrlen = pstrlen_bytes[0]
            if pstrlen <= 0:
                client_socket.close()
                return
            
            # Đọc phần còn lại của handshake
            handshake_data = client_socket.recv(pstrlen + 48)  # 8 bytes reserved + 20 bytes info_hash + 20 bytes peer_id
            if len(handshake_data) < pstrlen + 48:
                client_socket.close()
                return
            
            # Trích xuất info_hash
            info_hash_start = pstrlen + 8
            info_hash_end = info_hash_start + 20
            info_hash_bytes = handshake_data[info_hash_start:info_hash_end]
            info_hash = info_hash_bytes.hex()
            
            # Kiểm tra xem chúng ta có torrent này không
            if info_hash not in self.torrents:
                logging.warning(f"Nhận kết nối cho torrent không xác định: {info_hash}")
                client_socket.close()
                return
            
            # Trích xuất peer_id
            peer_id_bytes = handshake_data[info_hash_end:info_hash_end + 20]
            peer_id = peer_id_bytes.decode('utf-8', errors='replace')
            
            logging.info(f"Handshake thành công từ peer {peer_id} cho torrent {info_hash}")
            
            # Phản hồi handshake
            protocol = b"BitTorrent protocol"
            response = bytes([len(protocol)]) + protocol + b"\x00" * 8 + info_hash_bytes + self.peer_id.encode()
            client_socket.sendall(response)
            
            # Tạo kết nối peer
            connection = PeerConnection(
                socket=client_socket,
                peer_id=peer_id,
                info_hash=info_hash,
                piece_manager=self.torrents[info_hash]["piece_manager"]
            )
            
            # Khởi động xử lý kết nối
            connection.start()
            
        except Exception as e:
            logging.error(f"Lỗi khi xử lý kết nối peer: {e}")
            try:
                client_socket.close()
            except:
                pass
    
    def load_torrents_from_tracker(self):
        """Tải danh sách torrent từ tracker"""
        try:
            tracker_base_url = self.tracker_url.replace("/announce", "")
            torrents_url = f"{tracker_base_url}/torrents"
            
            response = requests.get(torrents_url, timeout=10)
            
            if response.status_code == 200:
                torrents_data = response.json().get("torrents", [])
                loaded_count = 0
                
                # Đồng bộ metainfo files
                for torrent in torrents_data:
                    info_hash = torrent.get("info_hash")
                    name = torrent.get("name")
                    
                    # Bỏ qua nếu đã có torrent này
                    if info_hash in self.torrents:
                        continue
                    
                    # Kiểm tra xem đã có metainfo chưa
                    torrent_file = self.METAINFO_DIR / f"{name}.torrent.json"
                    
                    try:
                        # Cố gắng tải metainfo
                        metainfo = None
                        
                        # Nếu file metainfo đã tồn tại cục bộ
                        if torrent_file.exists():
                            with open(torrent_file, 'r', encoding='utf-8') as f:
                                metainfo = json.load(f)
                        else:
                            # Lấy metainfo từ tracker
                            metainfo_url = f"{tracker_base_url}/metainfo/{info_hash}"
                            metainfo_response = requests.get(metainfo_url, timeout=10)
                            
                            if metainfo_response.status_code == 200:
                                metainfo = metainfo_response.json()
                                
                                # Lưu metainfo vào file local
                                with open(torrent_file, 'w', encoding='utf-8') as f:
                                    json.dump(metainfo, f, indent=2)
                        
                        # Nếu đã có metainfo, tạo torrent trong hệ thống
                        if metainfo:
                            # Tạo piece manager
                            piece_manager = PieceManager(
                                info_hash=info_hash,
                                piece_length=metainfo.get("piece_length", 512*1024),
                                piece_hashes=metainfo.get("pieces", []),
                                files=metainfo.get("files", []),
                                DOWNLOAD_DIR=self.DOWNLOAD_DIR
                            )
                            
                            # Tải tiến độ nếu đã tải trước đó
                            piece_manager.load_progress()
                            
                            # Thêm vào danh sách torrent
                            self.torrents[info_hash] = {
                                "name": name,
                                "metainfo": metainfo,
                                "piece_manager": piece_manager,
                                "status": "stopped",
                                "size": sum(f.get("length", 0) for f in metainfo.get("files", []))
                            }
                            
                            # Kiểm tra nếu đã tải xong
                            if piece_manager.is_complete():
                                self.torrents[info_hash]["status"] = "seeding"
                            
                            loaded_count += 1
                    
                    except Exception as e:
                        logging.error(f"Lỗi khi tải metainfo cho {name}: {e}")
                
                logging.info(f"Đã tải {loaded_count} torrent từ tracker")
                return True
            else:
                logging.error(f"Không thể tải danh sách torrent từ tracker: HTTP {response.status_code}")
                return False
        except Exception as e:
            logging.error(f"Lỗi khi tải torrents từ tracker: {e}")
            return False
    
    def get_torrent_status(self):
        """Lấy danh sách và trạng thái các torrent"""
        result = []
        
        with self.lock:
            for info_hash, torrent in self.torrents.items():
                # Tính toán kích thước
                size = torrent.get("size", 0)
                
                # Tính toán tiến độ nếu đang tải
                progress = 0
                piece_manager = torrent.get("piece_manager")
                if piece_manager:
                    progress = piece_manager.progress * 100
                
                result.append({
                    "info_hash": info_hash,
                    "name": torrent.get("name", "Unknown"),
                    "status": torrent.get("status", "unknown"),
                    "size": size,
                    "progress": progress
                })
        
        return result

    def get_torrent_detail(self, info_hash):
        """Lấy thông tin chi tiết về một torrent"""
        with self.lock:
            if info_hash not in self.torrents:
                return None
            
            torrent = self.torrents[info_hash]
            metainfo = torrent.get("metainfo", {})
            piece_manager = torrent.get("piece_manager")
            
            # Tính toán các thông số
            size = torrent.get("size", 0)
            size_formatted = self._format_size(size)
            
            progress = 0
            download_speed = 0
            upload_speed = 0
            
            if piece_manager is not None:
                progress = piece_manager.progress * 100
                download_speed = getattr(piece_manager, "download_speed", 0)
                upload_speed = getattr(piece_manager, "upload_speed", 0)
            
            # Format các thông số
            download_speed_formatted = self._format_speed(download_speed)
            upload_speed_formatted = self._format_speed(upload_speed)
            
            # Format thông tin file
            files = []
            for file_info in metainfo.get("files", []):
                file_size = file_info.get("length", 0)
                files.append({
                    "path": file_info.get("path", "Unknown"),
                    "size": file_size,
                    "size_formatted": self._format_size(file_size)
                })
            
            return {
                "name": torrent.get("name", "Unknown"),
                "status": torrent.get("status", "unknown"),
                "size": size,
                "size_formatted": size_formatted,
                "progress": progress,
                "download_speed": download_speed,
                "download_speed_formatted": download_speed_formatted,
                "upload_speed": upload_speed,
                "upload_speed_formatted": upload_speed_formatted,
                "files": files,
                "creation_date": metainfo.get("creation_date"),
                "piece_count": metainfo.get("piece_count", 0),
                "piece_length": metainfo.get("piece_length", 0)
            }
       
    def _format_size(self, size_bytes):
        """Format kích thước file theo đơn vị phù hợp"""
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes/1024:.2f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes/(1024*1024):.2f} MB"
        else:
            return f"{size_bytes/(1024*1024*1024):.2f} GB"

    def _format_speed(self, speed_bytes):
        """Format tốc độ tải/chia sẻ theo đơn vị phù hợp"""
        return f"{self._format_size(speed_bytes)}/s"
   
    def create_torrent(self, file_paths, name=None, piece_length=512*1024):
        """Tạo torrent từ file và bắt đầu chia sẻ (upload)"""
        if not file_paths:
            return False, "Không có file nào được chọn"
        
        with self.lock:
            try:
                # Nếu không có tên, sử dụng tên file đầu tiên
                if not name and len(file_paths) == 1:
                    name = Path(file_paths[0]).name
                elif not name:
                    name = "my_torrent"
                
                # Thu thập thông tin về các file
                files_info = []
                total_size = 0
                
                for file_path in file_paths:
                    if not os.path.exists(file_path):
                        return False, f"File không tồn tại: {file_path}"
                    
                    size = os.path.getsize(file_path)
                    files_info.append({
                        "path": os.path.basename(file_path),
                        "length": size
                    })
                    total_size += size
                
                # Tính piece count và tạo piece hashes
                piece_count = (total_size + piece_length - 1) // piece_length
                piece_hashes = self._calculate_piece_hashes(file_paths, piece_length)
                
                # Tạo metainfo dictionary
                metainfo = {
                    "name": name,
                    "piece_length": piece_length,
                    "piece_count": piece_count,
                    "files": files_info,
                    "pieces": piece_hashes,
                    "tracker": self.tracker_url,
                    "creation_date": int(time.time())
                }
                
                # Tính info_hash
                info_hash = hashlib.sha1(json.dumps(metainfo, sort_keys=True).encode()).hexdigest()
                metainfo["info_hash"] = info_hash
                
                # Lưu metainfo vào file JSON
                json_path = self.METAINFO_DIR / f"{name}.torrent.json"
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(metainfo, f, indent=2)
                
                # Tạo thư mục cho torrent này trong DOWNLOAD_DIR
                torrent_dir = self.DOWNLOAD_DIR / info_hash
                os.makedirs(torrent_dir, exist_ok=True)
                
                # Copy các file vào thư mục torrent
                for file_path in file_paths:
                    dest_path = torrent_dir / os.path.basename(file_path)
                    if not dest_path.exists():
                        import shutil
                        shutil.copy2(file_path, dest_path)
                
                # Tạo PieceManager cho torrent này
                piece_manager = PieceManager(
                    info_hash=info_hash,
                    piece_length=piece_length,
                    piece_hashes=piece_hashes,
                    files=files_info,
                    DOWNLOAD_DIR=self.DOWNLOAD_DIR
                )
                
                # Đánh dấu tất cả piece là đã tải xong (vì đây là file upload)
                piece_manager.mark_all_complete()
                
                # Lưu thông tin torrent
                self.torrents[info_hash] = {
                    "name": name,
                    "status": "seeding",
                    "piece_manager": piece_manager,
                    "metainfo": metainfo
                }
                
                # Thông báo cho tracker
                self._announce_to_tracker(info_hash, "completed")

                # Gửi metainfo đến tracker
                try:
                    tracker_base_url = self.tracker_url.replace("/announce", "")
                    upload_url = f"{tracker_base_url}/upload_metainfo"
                    
                    # Chuẩn bị dữ liệu gửi đi
                    upload_data = {
                        "metainfo": metainfo,
                        "peer_id": self.peer_id
                    }
                    
                    # Gửi request
                    upload_response = requests.post(
                        upload_url,
                        json=upload_data,
                        timeout=10
                    )
                    
                    if upload_response.status_code == 200:
                        response_data = upload_response.json()
                        if response_data.get("success"):
                            logging.info(f"Đã gửi metainfo đến tracker thành công")
                        else:
                            logging.error(f"Lỗi khi gửi metainfo: {response_data.get('reason')}")
                    else:
                        logging.error(f"Lỗi khi gửi metainfo đến tracker: HTTP {upload_response.status_code}")
                except Exception as e:
                    logging.error(f"Lỗi khi gửi metainfo đến tracker: {e}")
                
                logging.info(f"Đã tạo torrent mới: {name} ({info_hash})")
                return True, info_hash
                
            except Exception as e:
                logging.error(f"Lỗi khi tạo torrent: {e}")
                return False, str(e)
    
    def _calculate_piece_hashes(self, file_paths, piece_length):
        """Tính toán SHA-1 hash cho mỗi piece của torrent"""
        piece_hashes = []
        current_piece = b''
        
        # Xử lý từng file
        for file_path in file_paths:
            with open(file_path, 'rb') as f:
                while True:
                    # Đọc phần dữ liệu còn thiếu cho piece hiện tại
                    bytes_needed = piece_length - len(current_piece)
                    chunk = f.read(bytes_needed)
                    
                    if not chunk:
                        break
                    
                    current_piece += chunk
                    
                    # Nếu piece đã đủ, tính hash
                    if len(current_piece) == piece_length:
                        piece_hash = hashlib.sha1(current_piece).hexdigest()
                        piece_hashes.append(piece_hash)
                        current_piece = b''
        
        # Xử lý piece cuối
        if current_piece:
            piece_hash = hashlib.sha1(current_piece).hexdigest()
            piece_hashes.append(piece_hash)
        
        return piece_hashes

    def start_torrent(self, info_hash):
        """Bắt đầu tải xuống torrent"""
        with self.lock:
            if info_hash not in self.torrents:
                logging.warning(f"Không tìm thấy torrent với info_hash: {info_hash}")
                return False
            
            torrent = self.torrents[info_hash]
            
            # Nếu đã đang tải hoặc đã hoàn thành
            if torrent["status"] in ["downloading", "seeding"]:
                return True
            
            # Thông báo cho tracker
            peers = self._announce_to_tracker(info_hash, "started")
            
            # Cập nhật trạng thái
            torrent["status"] = "downloading"
            
            # Khởi động quá trình tải xuống
            download_thread = threading.Thread(
                target=self._download_torrent,
                args=(info_hash, peers),
                daemon=True
            )
            download_thread.start()
            
            logging.info(f"Đã bắt đầu tải torrent: {torrent['name']}")
            return True
    
    def _download_torrent(self, info_hash, peers):
        """Tải xuống torrent từ danh sách peers"""
        torrent = self.torrents[info_hash]
        piece_manager = torrent["piece_manager"]
        
        # Kết nối đến các peer
        active_connections = []

        logging.info(f"Bắt đầu kết nối đến {len(peers)} peers cho {torrent['name']}")

        for peer in peers:
            try:
                peer_id = peer.get("peer_id")
                ip = peer.get("ip")
                port = peer.get("port")
                
                if not peer_id or not ip or not port:
                    continue
                
                logging.info(f"Kết nối đến peer {peer_id[:8]} ({ip}:{port})")

                # Tạo kết nối đến peer
                connection = PeerConnection(
                    peer_id=peer_id,
                    ip=ip,
                    port=port,
                    info_hash=info_hash,
                    piece_manager=piece_manager
                )
                
                # Kết nối và handshake
                if connection.connect(self.peer_id):
                    active_connections.append(connection)
                    
                    # Khởi động thread tải xuống từ peer này
                    download_thread = threading.Thread(
                        target=connection.download,
                        daemon=True
                    )
                    download_thread.start()
                    logging.info(f"Đã kết nối thành công đến peer {peer_id[:8]}")
            
            except Exception as e:
                logging.error(f"Lỗi khi kết nối đến peer {peer.get('peer_id')}: {e}")
        
        # Kiểm tra tiến độ tải xuống định kỳ
        last_announce_time = time.time()
        announce_interval = 15  # Seconds

        # Kiểm tra tiến độ tải xuống định kỳ
        while torrent["status"] == "downloading" and not piece_manager.is_complete():
            time.sleep(1)

            # Kiểm tra và log tiến độ
            if int(time.time()) % 5 == 0:  # Log mỗi 5 giây
                progress = piece_manager.progress * 100
                logging.info(f"Tiến độ download {torrent['name']}: {progress:.1f}%")
            
            # Cập nhật announce để nhận thêm peers nếu cần
            now = time.time()
            if (len(active_connections) < 3 or now - last_announce_time > announce_interval):
                try:
                    logging.info(f"Yêu cầu danh sách peers mới từ tracker...")
                    new_peers = self._announce_to_tracker(info_hash, "")
                    last_announce_time = now
                    
                    # Kết nối đến các peer mới
                    for peer in new_peers:
                        # Kiểm tra xem peer đã được kết nối chưa
                        peer_id = peer.get("peer_id")
                        if any(conn.peer_id == peer_id for conn in active_connections):
                            continue
                            
                        # Kết nối đến peer mới
                        try:
                            ip = peer.get("ip")
                            port = peer.get("port")
                            
                            if not peer_id or not ip or not port:
                                continue
                            
                            connection = PeerConnection(
                                peer_id=peer_id,
                                ip=ip,
                                port=port,
                                info_hash=info_hash,
                                piece_manager=piece_manager
                            )
                            
                            if connection.connect(self.peer_id):
                                active_connections.append(connection)
                                threading.Thread(
                                    target=connection.download,
                                    daemon=True
                                ).start()
                                logging.info(f"Đã kết nối thành công đến peer mới {peer_id[:8]}")
                        except Exception as e:
                            logging.error(f"Lỗi khi kết nối đến peer mới: {e}")
                except Exception as e:
                    logging.error(f"Lỗi khi cập nhật danh sách peers: {e}")
        
        # Nếu đã tải xong
        if piece_manager.is_complete():
            with self.lock:
                torrent["status"] = "seeding"
                # Thông báo completed cho tracker
                self._announce_to_tracker(info_hash, "completed")
                logging.info(f"Đã tải xong torrent: {torrent['name']}")
    
    def stop_torrent(self, info_hash):
        """Dừng tải xuống hoặc chia sẻ torrent"""
        with self.lock:
            if info_hash not in self.torrents:
                return False
            
            torrent = self.torrents[info_hash]
            
            # Nếu đã dừng rồi
            if torrent["status"] == "stopped":
                return True
            
            # Thông báo cho tracker
            self._announce_to_tracker(info_hash, "stopped")
            
            # Cập nhật trạng thái
            prev_status = torrent["status"]
            torrent["status"] = "stopped"
            
            logging.info(f"Đã dừng torrent: {torrent['name']} (trạng thái trước: {prev_status})")
            return True
    
    def clear_downloaded_data(self, info_hash=None):
        """Xóa dữ liệu đã tải xuống
        
        Args:
            info_hash (str, optional): Hash của torrent cụ thể cần xóa.
                                    Nếu None, sẽ xóa tất cả.
        
        Returns:
            bool: True nếu thành công, False nếu có lỗi
        """
        import shutil
        
        try:
            with self.lock:
                if info_hash:
                    # Xóa dữ liệu của một torrent cụ thể
                    if info_hash not in self.torrents:
                        logging.warning(f"Không tìm thấy torrent với info_hash: {info_hash}")
                        return False
                    
                    torrent_dir = self.DOWNLOAD_DIR / info_hash
                    if torrent_dir.exists():
                        shutil.rmtree(torrent_dir)
                        logging.info(f"Đã xóa dữ liệu torrent: {self.torrents[info_hash]['name']}")
                        
                        # Reset trạng thái piece manager
                        piece_manager = self.torrents[info_hash]["piece_manager"]
                        piece_manager.reset_progress()
                else:
                    # Xóa tất cả dữ liệu download
                    for item in self.DOWNLOAD_DIR.glob("*"):
                        if item.is_dir():
                            shutil.rmtree(item)
                    
                    # Tạo lại thư mục trống
                    self.DOWNLOAD_DIR.mkdir(exist_ok=True)
                    
                    # Reset trạng thái tất cả piece manager
                    for hash_id, torrent in self.torrents.items():
                        piece_manager = torrent["piece_manager"]
                        piece_manager.reset_progress()
                    
                    logging.info("Đã xóa tất cả dữ liệu download")
                
                return True
        except Exception as e:
            logging.error(f"Lỗi khi xóa dữ liệu: {e}")
            return False
    
    def _announce_to_tracker(self, info_hash, event):
        """Thông báo với tracker và nhận danh sách peers"""
        try:
            torrent = self.torrents[info_hash]
            piece_manager = torrent["piece_manager"]
            
            # Tính các thông số
            downloaded = piece_manager.bytes_downloaded
            uploaded = piece_manager.bytes_uploaded
            left = piece_manager.bytes_left
            
            # Chuẩn bị dữ liệu 
            announce_data = {
                "peer_id": self.peer_id,
                "info_hash": info_hash,
                "ip": socket.gethostbyname(socket.gethostname()),
                "port": self.port,
                "uploaded": uploaded,
                "downloaded": downloaded,
                "left": left,
                "event": event
            }
            
            # Gửi request qua HTTP thay vì socket
            response = requests.post(
                self.tracker_url,
                json=announce_data,
                timeout=10
            )
            
            if response.status_code != 200:
                logging.error(f"Tracker phản hồi HTTP {response.status_code}")
                return []
            
            # Phân tích response
            data = response.json()
            
            # Kiểm tra lỗi
            if "failure_reason" in data:
                logging.error(f"Tracker báo lỗi: {data['failure_reason']}")
                return []
            
            # Lấy danh sách peers
            peers = data.get("peers", [])
            logging.info(f"Nhận được {len(peers)} peers từ tracker cho {info_hash}")
            
            return peers
                
        except requests.exceptions.ConnectionError:
            logging.error(f"Không thể kết nối đến tracker (server không hoạt động?)")
            return []
        except requests.exceptions.Timeout:
            logging.error(f"Kết nối đến tracker bị timeout")
            return []
        except json.JSONDecodeError as e:
            logging.error(f"Lỗi khi parse JSON từ tracker: {e}")
            return []
        except Exception as e:
            logging.error(f"Lỗi khi kết nối đến tracker: {e}")
            return []

    
    def get_torrent_status(self):
        """Lấy danh sách và trạng thái các torrent"""
        result = []
        
        with self.lock:
            for info_hash, torrent in self.torrents.items():
                # Tính toán kích thước
                size = torrent.get("size", 0)
                
                # Tính toán tiến độ nếu đang tải
                progress = 0
                piece_manager = torrent.get("piece_manager")
                if piece_manager is not None:  # Thêm kiểm tra nếu piece_manager tồn tại
                    progress = piece_manager.progress * 100
                
                result.append({
                    "info_hash": info_hash,
                    "name": torrent.get("name", "Unknown"),
                    "status": torrent.get("status", "unknown"),
                    "size": size,
                    "progress": progress
                })
        
        return result

class PeerGUI:
    """Giao diện đồ họa cho ứng dụng P2P"""
    
    def __init__(self, master):
        self.master = master
        self.master.title("P2P File Sharing")
        self.master.geometry("800x600")  # Tăng kích thước cửa sổ
        
        # Khởi tạo peer
        self.peer = Peer()
        
        # Thiết lập giao diện
        self._setup_ui()
        
        # Tạo giao diện trạng thái download
        self._create_download_status_ui()
        
        # Thiết lập hàng chờ download
        self._setup_download_queue()
        
        # Thêm biến để theo dõi torrent đang chọn
        self.selected_info_hash = None
        
        # Thêm event binding cho việc lựa chọn torrent
        self.tree.bind("<<TreeviewSelect>>", self._on_torrent_select)
        self.tree.bind("<Double-1>", lambda event: self._start_torrent())  # Thêm double-click để download
    

    def _on_torrent_select(self, event):
        """Xử lý khi người dùng chọn một torrent"""
        selected = self.tree.selection()
        if selected:
            item = selected[0]
            tags = self.tree.item(item, "tags")
            if tags and tags[0] != "no_torrents":
                self.selected_info_hash = tags[0]
                self._update_torrent_details()

    def _update_torrent_details(self):
        """Cập nhật thông tin chi tiết về torrent đang chọn"""
        if not self.selected_info_hash:
            return
            
        # Lấy thông tin chi tiết về torrent
        torrent_info = self.peer.get_torrent_detail(self.selected_info_hash)
        if not torrent_info:
            return
        
        # Cập nhật text widget
        self.info_text.config(state=tk.NORMAL)
        self.info_text.delete("1.0", tk.END)
        
        # Hiển thị thông tin cơ bản
        details = [
            f"Tên: {torrent_info.get('name', 'N/A')}",
            f"Info Hash: {self.selected_info_hash}",
            f"Kích thước: {torrent_info.get('size_formatted', 'N/A')}",
            f"Trạng thái: {torrent_info.get('status', 'N/A')}",
            f"Tiến độ: {torrent_info.get('progress', 0):.1f}%",
            f"Tốc độ tải: {torrent_info.get('download_speed_formatted', 'N/A')}",
            f"Tốc độ chia sẻ: {torrent_info.get('upload_speed_formatted', 'N/A')}",
        ]
        
        # Hiển thị danh sách file
        files = torrent_info.get("files", [])
        if files:
            details.append("\nDanh sách file:")
            for i, file in enumerate(files, 1):
                details.append(f"{i}. {file.get('path', 'N/A')} - {file.get('size_formatted', 'N/A')}")
        
        self.info_text.insert("1.0", "\n".join(details))
        self.info_text.config(state=tk.DISABLED)
    
    def _setup_ui(self):
        """Thiết lập giao diện người dùng"""
        # Frame cho torrent list
        list_frame = ttk.LabelFrame(self.master, text="Danh sách torrent", padding=5)
        list_frame.pack(padx=10, pady=5, fill=tk.BOTH, expand=True)
        
        # Tạo treeview để hiển thị danh sách torrent
        columns = ("name", "size", "status")
        self.tree = ttk.Treeview(list_frame, columns=columns, show="headings", selectmode="browse")
        
        # Định nghĩa các cột
        self.tree.heading("name", text="Tên torrent")
        self.tree.heading("size", text="Kích thước")
        self.tree.heading("status", text="Trạng thái")
        
        # Thiết lập độ rộng cột
        self.tree.column("name", width=300, minwidth=200)
        self.tree.column("size", width=100, minwidth=80)
        self.tree.column("status", width=100, minwidth=80)
        
        # Thêm scrollbar
        scrollbar = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Frame cho thông tin chi tiết
        info_frame = ttk.LabelFrame(self.master, text="Thông tin chi tiết", padding=5)
        info_frame.pack(padx=10, pady=5, fill=tk.X)
        
        # Tạo Text widget để hiển thị thông tin chi tiết
        self.info_text = tk.Text(info_frame, height=8, wrap=tk.WORD)
        self.info_text.pack(fill=tk.X)
        self.info_text.config(state=tk.DISABLED)
        
        # Frame cho các nút điều khiển
        control_frame = ttk.Frame(self.master, padding=5)
        control_frame.pack(padx=10, pady=5, fill=tk.X)
        
        # Tạo các nút điều khiển - Loại bỏ nút "Dừng"
        ttk.Button(control_frame, text="Upload", command=self._upload_file).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="Download", command=self._start_torrent).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="Chi tiết", command=self._show_details).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="Refresh", command=self._refresh_torrents).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="Tìm kiếm", command=self._search_torrent).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="Thoát", command=self._exit_application).pack(side=tk.RIGHT, padx=5)
        
        # Thiết lập sự kiện khi chọn torrent
        self.tree.bind("<<TreeviewSelect>>", self._on_torrent_select)
        self.tree.bind("<Double-1>", lambda event: self._start_torrent())  # Thêm double-click để download
        
        # Khởi tạo các biến
        self.selected_info_hash = None
        
        # Cập nhật danh sách torrent ban đầu
        self._update_torrent_list()
    
    def _on_torrent_double_click(self, event):
        """Xử lý khi người dùng double-click vào một torrent"""
        # Lấy item được chọn
        item = self.tree.identify("item", event.x, event.y)
        if item:
            # Hiển thị chi tiết torrent
            self._show_details()

    def _refresh_torrents(self):
        """Cập nhật danh sách torrent từ tracker"""
        try:
            if self.peer.load_torrents_from_tracker():
                messagebox.showinfo("Thông báo", "Đã cập nhật danh sách torrent từ tracker")
            else:
                messagebox.showwarning("Cảnh báo", "Không thể cập nhật danh sách torrent từ tracker")
        except Exception as e:
            messagebox.showerror("Lỗi", f"Lỗi khi cập nhật danh sách torrent: {str(e)}")
        
        # Cập nhật giao diện
        self._update_torrent_list()
    
    def _upload_file(self):
        """Chọn file để upload và tạo torrent"""
        # Mở hộp thoại chọn file
        file_paths = filedialog.askopenfilenames(
            title="Chọn file để chia sẻ",
            filetypes=(("Tất cả các file", "*.*"),)
        )
        
        if not file_paths:
            return
        
        # Hiện hộp thoại nhập tên torrent
        name_dialog = tk.Toplevel(self.master)
        name_dialog.title("Tạo Torrent")
        name_dialog.geometry("400x150")
        name_dialog.transient(self.master)
        name_dialog.grab_set()
        
        # Tên mặc định (lấy từ tên file đầu tiên)
        default_name = os.path.basename(file_paths[0]) if len(file_paths) == 1 else "my_torrent"
        
        ttk.Label(name_dialog, text="Nhập tên cho torrent:").pack(pady=10)
        
        name_var = tk.StringVar(value=default_name)
        name_entry = ttk.Entry(name_dialog, textvariable=name_var, width=40)
        name_entry.pack(pady=10, padx=20, fill=tk.X)
        
        # Frame cho các nút
        buttons_frame = ttk.Frame(name_dialog)
        buttons_frame.pack(pady=10, fill=tk.X)
        
        # Nút Hủy
        ttk.Button(buttons_frame, text="Hủy", command=name_dialog.destroy).pack(side=tk.RIGHT, padx=5)
        
        # Hàm xử lý khi tạo torrent
        def on_create():
            torrent_name = name_var.get().strip()
            if not torrent_name:
                messagebox.showwarning("Cảnh báo", "Tên torrent không được để trống")
                return
            
            # Tạo torrent mới
            success, result = self.peer.create_torrent(file_paths, name=torrent_name)
            name_dialog.destroy()
            
            if success:
                messagebox.showinfo("Thành công", f"Đã tạo torrent '{torrent_name}' và bắt đầu chia sẻ!")
                # Cập nhật danh sách ngay lập tức
                self._update_torrent_list()  # Đã có trong code
            else:
                messagebox.showerror("Lỗi", f"Không thể tạo torrent: {result}")
        
        # Nút Tạo Torrent
        ttk.Button(buttons_frame, text="Tạo Torrent", command=on_create).pack(side=tk.RIGHT, padx=5)
        
        # Focus vào entry
        name_entry.focus_set()
    
    def _start_torrent(self):
        """Bắt đầu tải torrent đã chọn"""
        if not self.selected_info_hash:
            selected = self.tree.selection()
            if not selected:
                messagebox.showinfo("Thông báo", "Vui lòng chọn một torrent")
                return
            
            # Lấy info_hash từ tag của item đã chọn
            item = selected[0]
            tags = self.tree.item(item, "tags")
            if not tags or tags[0] == "no_torrents":
                messagebox.showinfo("Thông báo", "Vui lòng chọn một torrent")
                return
                
            self.selected_info_hash = tags[0]
        
        # Lấy thông tin torrent
        torrent_info = None
        with self.peer.lock:
            if self.selected_info_hash in self.peer.torrents:
                torrent_info = self.peer.torrents[self.selected_info_hash]
        
        if not torrent_info:
            messagebox.showwarning("Cảnh báo", "Không tìm thấy thông tin về torrent này")
            return
        
        # Nếu torrent đã đang tải hoặc đã hoàn thành, thông báo cho người dùng
        if torrent_info["status"] == "downloading":
            messagebox.showinfo("Thông báo", "Torrent đã đang được tải xuống")
            return
        elif torrent_info["status"] == "seeding":
            messagebox.showinfo("Thông báo", "Torrent đã được tải xuống hoàn tất")
            return
        
        # Bắt đầu tải với info_hash đã chọn
        if self.peer.start_torrent(self.selected_info_hash):
            messagebox.showinfo("Thông báo", f"Đã bắt đầu tải torrent: {torrent_info['name']}")
            # Cập nhật danh sách để hiển thị trạng thái mới
            self._update_torrent_list()
        else:
            messagebox.showerror("Lỗi", "Không thể bắt đầu tải torrent")
           

    """def _stop_torrent(self):
        selected = self.tree.selection()
        if not selected:
            messagebox.showinfo("Thông báo", "Vui lòng chọn một torrent")
            return
        
        # Lấy info_hash từ item đã chọn
        item = selected[0]
        info_hash = self.tree.item(item, "tags")[0]
        
        # Dừng torrent
        if self.peer.stop_torrent(info_hash):
            messagebox.showinfo("Thông báo", "Đã dừng torrent")
        else:
            messagebox.showerror("Lỗi", "Không thể dừng torrent")"""
    
    def _show_details(self):
        """Hiển thị chi tiết torrent"""
        if not self.selected_info_hash:
            selected = self.tree.selection()
            if not selected:
                messagebox.showinfo("Thông báo", "Vui lòng chọn một torrent")
                return
            
            item = selected[0]
            tags = self.tree.item(item, "tags")
            if not tags or tags[0] == "no_torrents":
                messagebox.showinfo("Thông báo", "Vui lòng chọn một torrent")
                return
                
            self.selected_info_hash = tags[0]
        
        # Lấy thông tin chi tiết và hiển thị
        torrent = self.peer.get_torrent_status(self.selected_info_hash)
        if not torrent:
            messagebox.showerror("Lỗi", "Không thể lấy thông tin chi tiết")
            return

    def _update_torrent_list(self):
        """Cập nhật danh sách torrent trong giao diện"""
        # Lưu thông tin về item đang chọn
        selected_hash = self.selected_info_hash
        
        # Xóa các item hiện tại
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        # Lấy danh sách torrent và cập nhật vào tree
        torrents = self.peer.get_torrent_status()
        if not torrents:
            self.tree.insert("", tk.END, values=("Không có torrent nào", "", ""), tags=("no_torrents",))
            return
        
        # Thêm các torrent vào tree
        for torrent in torrents:
            # Định dạng kích thước
            size_mb = torrent["size"] / (1024 * 1024) if torrent.get("size") else 0
            size_str = f"{size_mb:.2f} MB" if size_mb else "N/A"
            
            # Thêm vào tree
            item_id = self.tree.insert(
                "", 
                tk.END, 
                values=(torrent["name"], size_str, torrent["status"]), 
                tags=(torrent["info_hash"],)
            )
            
            # Nếu là torrent đang chọn trước đó, chọn lại
            if torrent["info_hash"] == selected_hash:
                self.tree.selection_set(item_id)
                self.tree.see(item_id)
        
        # Cập nhật theo chu kỳ (mỗi 2 giây)
        self.master.after(2000, self._update_torrent_list)
    
    def _setup_download_queue(self):
        """Thiết lập hàng chờ download tự động"""
        self.download_queue = []
        self.queue_processing = False
        
        def process_queue():
            if self.queue_processing or not self.download_queue:
                return
            
            self.queue_processing = True
            info_hash = self.download_queue.pop(0)
            
            # Bắt đầu tải torrent này
            if self.peer.start_torrent(info_hash):
                # Đợi đến khi tải xong hoặc thất bại
                def check_status():
                    torrent = None
                    with self.peer.lock:
                        if info_hash in self.peer.torrents:
                            torrent = self.peer.torrents[info_hash]
                    
                    if not torrent:
                        self.queue_processing = False
                        process_queue()
                        return
                    
                    if torrent["status"] == "seeding":
                        logging.info(f"Download hoàn tất: {torrent['name']}")
                        self.queue_processing = False
                        process_queue()
                    else:
                        self.master.after(5000, check_status)  # Kiểm tra lại sau 5 giây
                
                self.master.after(5000, check_status)
            else:
                logging.error(f"Không thể bắt đầu tải torrent: {info_hash}")
                self.queue_processing = False
                process_queue()
        
        # Gán phương thức để sử dụng từ bên ngoài
        self.add_to_download_queue = lambda info_hash: self.download_queue.append(info_hash) or process_queue()

    def _create_download_status_ui(self):
        """Tạo giao diện hiển thị trạng thái download"""
        status_frame = ttk.LabelFrame(self.master, text="Trạng thái Download", padding=5)
        status_frame.pack(padx=10, pady=5, fill=tk.X)
        
        # Tạo Progress bar và label
        self.progress_var = tk.DoubleVar(value=0)
        self.progress_bar = ttk.Progressbar(status_frame, variable=self.progress_var, maximum=100)
        self.progress_bar.pack(fill=tk.X, pady=5)
        
        self.status_label = ttk.Label(status_frame, text="Không có download đang diễn ra")
        self.status_label.pack(pady=5)
        
        # Khởi động thread cập nhật trạng thái
        def update_download_status():
            if self.selected_info_hash:
                torrent_info = self.peer.get_torrent_detail(self.selected_info_hash)
                if torrent_info and torrent_info["status"] in ["downloading", "seeding"]:
                    self.progress_var.set(torrent_info["progress"])
                    status_text = f"Đang tải: {torrent_info['name']} - {torrent_info['progress']:.1f}% - {torrent_info['download_speed_formatted']}"
                    self.status_label.config(text=status_text)
                else:
                    self.progress_var.set(0)
                    self.status_label.config(text="Không có download đang diễn ra")
            
            self.master.after(1000, update_download_status)
        
        self.master.after(1000, update_download_status)

    def _search_torrent(self):
        """Tìm kiếm torrent theo tên"""
        # Tạo hộp thoại tìm kiếm
        search_dialog = tk.Toplevel(self.master)
        search_dialog.title("Tìm kiếm Torrent")
        search_dialog.geometry("400x100")
        search_dialog.transient(self.master)
        search_dialog.grab_set()
        
        # Frame tìm kiếm
        search_frame = ttk.Frame(search_dialog, padding=10)
        search_frame.pack(fill=tk.X)
        
        # Entry và nút tìm kiếm
        ttk.Label(search_frame, text="Nhập tên torrent:").pack(side=tk.LEFT, padx=5)
        search_var = tk.StringVar()
        search_entry = ttk.Entry(search_frame, textvariable=search_var, width=30)
        search_entry.pack(side=tk.LEFT, padx=5)
        
        def do_search():
            search_term = search_var.get().strip()
            if not search_term:
                messagebox.showwarning("Cảnh báo", "Vui lòng nhập từ khóa tìm kiếm")
                return
            
            # Lấy danh sách torrent từ tracker
            try:
                self.peer.load_torrents_from_tracker()
                self._update_torrent_list()
                
                # Highlight các kết quả phù hợp
                found = False
                for item in self.tree.get_children():
                    name = self.tree.item(item, "values")[0].lower()
                    if search_term.lower() in name:
                        self.tree.selection_set(item)
                        self.tree.see(item)
                        found = True
                        break  # Chọn kết quả đầu tiên
                
                if found:
                    search_dialog.destroy()
                else:
                    messagebox.showinfo("Thông báo", "Không tìm thấy torrent phù hợp")
            except Exception as e:
                messagebox.showerror("Lỗi", f"Lỗi khi tìm kiếm: {str(e)}")
        
        ttk.Button(search_frame, text="Tìm", command=do_search).pack(side=tk.LEFT, padx=5)
        ttk.Button(search_frame, text="Hủy", command=search_dialog.destroy).pack(side=tk.LEFT, padx=5)
        
        search_entry.focus_set()
        search_entry.bind("<Return>", lambda e: do_search())
    

    def _start_update_thread(self):
        """Khởi động thread cập nhật UI"""
        def update_loop():
            while True:
                # Cập nhật UI trên main thread
                self.master.after(1000, self._update_torrent_list)
                time.sleep(2)  # Tăng thời gian giữa các lần cập nhật lên 2s
        
        threading.Thread(target=update_loop, daemon=True).start()
    
    def _stop_torrent(self):
        """Tạm dừng torrent đang chọn"""
        if not self.selected_info_hash:
            selected = self.tree.selection()
            if not selected:
                messagebox.showinfo("Thông báo", "Vui lòng chọn một torrent")
                return
            
            # Lấy info_hash từ tag
            item = selected[0]
            tags = self.tree.item(item, "tags")
            if not tags or tags[0] == "no_torrents":
                messagebox.showinfo("Thông báo", "Vui lòng chọn một torrent")
                return
                
            self.selected_info_hash = tags[0]
        
        # Tạm dừng torrent
        if self.peer.pause_torrent(self.selected_info_hash):
            messagebox.showinfo("Thông báo", "Đã tạm dừng torrent")
            # Cập nhật danh sách
            self._update_torrent_list()
        else:
            messagebox.showerror("Lỗi", "Không thể tạm dừng torrent")

    def _format_size(self, size_bytes):
        """Định dạng kích thước file"""
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes/1024:.1f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes/(1024*1024):.2f} MB"
        else:
            return f"{size_bytes/(1024*1024*1024):.2f} GB"

    def _exit_application(self):
        """Xử lý khi thoát ứng dụng"""
        # Hỏi người dùng có muốn xóa dữ liệu đã tải không
        response = messagebox.askyesnocancel(
            "Xóa dữ liệu",
            "Bạn có muốn xóa tất cả dữ liệu đã tải xuống không?\n\n"
            "- Yes: Xóa và thoát\n"
            "- No: Giữ lại dữ liệu và thoát\n"
            "- Cancel: Không thoát"
        )
        
        if response is None:  # Cancel
            return
        
        if response:  # Yes - xóa dữ liệu
            self.peer.clear_downloaded_data()
        
        # Thông báo cho tracker rằng peer đang rời đi
        for info_hash in [torrent["info_hash"] for torrent in self.peer.get_torrent_status()]:
            try:
                self.peer._announce_to_tracker(info_hash, "stopped")
            except:
                pass
        
        # Đóng ứng dụng
        self.master.quit()
if __name__ == "__main__":
    root = tk.Tk()
    app = PeerGUI(root)
    root.mainloop()