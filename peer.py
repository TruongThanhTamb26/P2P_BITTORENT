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
        self.tracker_url = tracker_url
        
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
        
        # Load danh sách torrent từ thư mục metainfo
        self._load_torrents()

        # Đăng ký với tracker khi khởi động
        if not self.register_with_tracker():
            logging.warning("Không thể đăng ký với tracker. Một số chức năng có thể bị hạn chế.")

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
    
    def _load_torrents(self):
        """Tải thông tin torrent từ thư mục metainfo"""
        # Đọc tất cả file .torrent.json từ thư mục metainfo
        for torrent_file in self.METAINFO_DIR.glob("*.torrent.json"):
            try:
                with open(torrent_file, 'r', encoding='utf-8') as f:
                    metainfo = json.load(f)
                
                info_hash = metainfo.get("info_hash")
                name = metainfo.get("name", "Unknown")
                
                if not info_hash:
                    logging.warning(f"File {torrent_file.name} không có info_hash")
                    continue
                
                # Tạo PieceManager cho torrent này
                piece_manager = PieceManager(
                    info_hash=info_hash,
                    piece_length=metainfo.get("piece_length", 512*1024),
                    piece_hashes=metainfo.get("pieces", []),
                    files=metainfo.get("files", []),
                    DOWNLOAD_DIR=self.DOWNLOAD_DIR
                )
                
                # Kiểm tra tiến độ tải xuống hiện tại
                piece_manager.load_progress()
                
                # Lưu thông tin torrent
                self.torrents[info_hash] = {
                    "name": name,
                    "status": "stopped",
                    "piece_manager": piece_manager,
                    "metainfo": metainfo
                }
                logging.info(f"Đã tải torrent: {name} ({info_hash})")
            
            except Exception as e:
                logging.error(f"Lỗi khi đọc file {torrent_file.name}: {e}")
    
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

    def get_torrent_status(self, info_hash=None):
        """Lấy trạng thái của một hoặc tất cả các torrent"""
        with self.lock:
            if info_hash:
                if info_hash not in self.torrents:
                    return None
                
                torrent = self.torrents[info_hash]
                piece_manager = torrent["piece_manager"]
                
                return {
                    "info_hash": info_hash,
                    "name": torrent["name"],
                    "status": torrent["status"],
                    "progress": piece_manager.progress * 100,  # Phần trăm
                    "downloaded": piece_manager.bytes_downloaded,
                    "uploaded": piece_manager.bytes_uploaded,
                    "left": piece_manager.bytes_left,
                    "files": torrent["metainfo"].get("files", [])
                }
            else:
                result = []
                for hash_id, torrent in self.torrents.items():
                    piece_manager = torrent["piece_manager"]
                    result.append({
                        "info_hash": hash_id,
                        "name": torrent["name"],
                        "status": torrent["status"],
                        "progress": piece_manager.progress * 100,  # Phần trăm
                    })
                return result

class PeerGUI:
    """Giao diện đồ họa cho ứng dụng P2P"""
    
    def __init__(self, master):
        self.master = master
        self.master.title("P2P File Sharing")
        self.master.geometry("800x500")
        
        # Khởi tạo peer
        self.peer = Peer()
        
        # Thiết lập giao diện
        self._setup_ui()
        
        # Khởi động thread cập nhật UI
        self._start_update_thread()

        # Thêm biến để theo dõi torrent đang chọn
        self.selected_info_hash = None
        
        # Thêm event binding cho việc lựa chọn torrent
        self.tree.bind("<<TreeviewSelect>>", self._on_torrent_select)
    

    def _on_torrent_select(self, event):
        """Xử lý khi người dùng chọn một torrent"""
        selected = self.tree.selection()
        if selected:
            item = selected[0]
            tags = self.tree.item(item, "tags")
            if tags and tags[0] != "no_torrents":
                # Chỉ log khi info_hash thay đổi
                if self.selected_info_hash != tags[0]:
                    self.selected_info_hash = tags[0]
                    logging.debug(f"Đã chọn torrent: {self.selected_info_hash}")
    
    
    def _setup_ui(self):
        """Thiết lập giao diện người dùng"""
        # Frame chính
        main_frame = ttk.Frame(self.master, padding=10)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Tiêu đề
        ttk.Label(main_frame, text="P2P File Sharing", font=("Arial", 16, "bold")).pack(pady=(0, 10))
        
        # Frame danh sách torrent
        list_frame = ttk.LabelFrame(main_frame, text="Danh sách Torrent")
        list_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        # Treeview hiển thị danh sách torrent
        columns = ("name", "status", "progress")
        self.tree = ttk.Treeview(list_frame, columns=columns, show="headings", selectmode="browse")
        
        # Thiết lập các cột
        self.tree.heading("name", text="Tên")
        self.tree.heading("status", text="Trạng thái")
        self.tree.heading("progress", text="Tiến độ")
        
        self.tree.column("name", width=350)
        self.tree.column("status", width=100)
        self.tree.column("progress", width=100)
        
        # Thanh cuộn cho treeview
        scrollbar = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        
        # Thêm bắt sự kiện khi double-click vào một torrent
        self.tree.bind("<Double-1>", self._on_torrent_double_click)
        
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Frame điều khiển
        control_frame = ttk.Frame(main_frame)
        control_frame.pack(fill=tk.X, pady=10)
        
        # Các nút điều khiển
        ttk.Button(control_frame, text="Upload", command=self._upload_file).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="Download", command=self._start_torrent).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="Dừng", command=self._stop_torrent).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="Chi tiết", command=self._show_details).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="Thoát", command=self._exit_application).pack(side=tk.RIGHT, padx=5)
            # Thêm phương thức mới để xử lý double-click
    def _on_torrent_double_click(self, event):
        """Xử lý khi người dùng double-click vào một torrent"""
        # Lấy item được chọn
        item = self.tree.identify("item", event.x, event.y)
        if item:
            # Hiển thị chi tiết torrent
            self._show_details()
    
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
        
        # Bắt đầu tải với info_hash đã chọn
        if self.peer.start_torrent(self.selected_info_hash):
            messagebox.showinfo("Thông báo", "Đã bắt đầu tải torrent")
            # Cập nhật danh sách để hiển thị trạng thái mới
            self._update_torrent_list()
        else:
            messagebox.showerror("Lỗi", "Không thể bắt đầu tải torrent")
            

    def _stop_torrent(self):
        """Dừng torrent đã chọn"""
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
            messagebox.showerror("Lỗi", "Không thể dừng torrent")
    
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
        
        selected_item = None
        
        # Thêm các torrent vào danh sách
        for torrent in torrents:  # torrents là list nên duyệt trực tiếp
            info_hash = torrent.get("info_hash")
            name = torrent.get("name", "Unknown")
            
            # Định dạng trạng thái
            status_text = {
                "stopped": "Dừng",
                "downloading": "Đang tải",
                "seeding": "Đang chia sẻ"
            }.get(torrent.get("status"), "Không xác định")
            
            # Định dạng tiến độ
            progress_text = f"{torrent.get('progress', 0):.1f}%"
            
            # Thêm vào tree
            item_id = self.tree.insert(
                "", tk.END,
                values=(name, status_text, progress_text),
                tags=(info_hash,)
            )
            
            # Nếu đây là item đã chọn trước đó, ghi nhớ để select lại
            if info_hash == selected_hash:
                selected_item = item_id
        
        
        # Khôi phục selection
        if selected_item:
            self.tree.selection_set(selected_item)
            self.tree.focus(selected_item)
            self.tree.see(selected_item)
    

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