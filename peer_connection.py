"""Quản lý kết nối giữa các peer"""
import socket
import struct
import logging
import time
import threading
import queue

class PeerConnection:
    """Xử lý kết nối đến một peer cụ thể"""
    
    def __init__(self, info_hash, piece_manager, peer_id=None, ip=None, port=None, socket=None):
        """
        Khởi tạo kết nối peer
        
        Parameters:
            info_hash: Hash của torrent
            piece_manager: PieceManager để quản lý dữ liệu
            peer_id: ID của peer (nếu kết nối đi)
            ip: Địa chỉ IP của peer (nếu kết nối đi)
            port: Cổng của peer (nếu kết nối đi)
            socket: Socket đã kết nối (nếu kết nối đến)
        """
        self.info_hash = info_hash
        self.piece_manager = piece_manager
        self.peer_id = peer_id
        self.ip = ip
        self.port = port
        self.socket = socket
        
        # Trạng thái kết nối
        self.connected = socket is not None
        self.am_choking = True  # Mặc định chặn peer
        self.am_interested = False
        self.peer_choking = True  # Mặc định bị peer chặn
        self.peer_interested = False
        
        # Bit field của peer (track những piece mà peer có)
        self.peer_bitfield = [False] * piece_manager.piece_count
        
        # Hàng đợi request
        self.request_queue = queue.Queue()
        
        # Thống kê
        self.last_active = time.time()
        self.bytes_downloaded = 0
        self.bytes_uploaded = 0
        
        # Lock để đồng bộ
        self.lock = threading.Lock()
    
    def connect(self, our_peer_id):
        """Kết nối đến peer và thực hiện handshake"""
        if self.connected:
            return True
        
        try:
            # Tạo socket và kết nối
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(10)  # Timeout 10 giây
            self.socket.connect((self.ip, self.port))
            
            # Thực hiện handshake
            protocol = b"BitTorrent protocol"
            reserved = b"\x00" * 8
            
            # Convert info_hash và peer_id thành bytes
            if isinstance(self.info_hash, str):
                info_hash_bytes = bytes.fromhex(self.info_hash)
            else:
                info_hash_bytes = self.info_hash
                
            our_peer_id_bytes = our_peer_id.encode()
            
            # Tạo handshake message
            handshake = bytes([len(protocol)]) + protocol + reserved + info_hash_bytes + our_peer_id_bytes
            
            # Gửi handshake
            self.socket.sendall(handshake)
            
            # Nhận handshake response
            pstrlen_bytes = self._recv_exact(1)
            if not pstrlen_bytes:
                return False
            
            pstrlen = pstrlen_bytes[0]
            if pstrlen <= 0 or pstrlen > 100:
                return False
            
            # Nhận phần còn lại của handshake
            rest = self._recv_exact(pstrlen + 48)  # 8 bytes reserved + 20 bytes info_hash + 20 bytes peer_id
            if not rest or len(rest) < pstrlen + 48:
                return False
            
            # Trích xuất info_hash từ response
            info_hash_start = pstrlen + 8
            info_hash_end = info_hash_start + 20
            recv_info_hash = rest[info_hash_start:info_hash_end].hex()
            
            # Kiểm tra info_hash
            if isinstance(self.info_hash, str) and recv_info_hash != self.info_hash:
                logging.warning(f"Received different info_hash: {recv_info_hash}")
                return False
            
            # Trích xuất peer_id từ response
            peer_id_start = info_hash_end
            peer_id_end = peer_id_start + 20
            recv_peer_id = rest[peer_id_start:peer_id_end].decode('utf-8', errors='replace')
            
            # Kiểm tra peer_id
            if self.peer_id and recv_peer_id != self.peer_id:
                logging.warning(f"Received different peer_id: {recv_peer_id}")
                # Vẫn tiếp tục kết nối vì có thể peer_id đã thay đổi
            
            # Cập nhật peer_id nếu chưa có
            if not self.peer_id:
                self.peer_id = recv_peer_id
            
            # Đánh dấu đã kết nối
            self.connected = True
            self.last_active = time.time()
            
            logging.info(f"Kết nối thành công đến peer {self.peer_id}")
            
            return True
            
        except Exception as e:
            logging.error(f"Lỗi kết nối đến peer {self.peer_id} ({self.ip}:{self.port}): {e}")
            self._close_connection()
            return False
    
    def start(self):
        """Khởi động thread nhận và gửi message"""
        if not self.connected:
            return False
        
        # Khởi động thread nhận message
        receiver_thread = threading.Thread(target=self._receiver_loop, daemon=True)
        receiver_thread.start()
        
        # Gửi message interested
        self._send_interested()
        
        return True
    
    def download(self):
        """Tải xuống từ peer này"""
        if not self.connected:
            return False
        
        # Khởi động thread nhận message
        receiver_thread = threading.Thread(target=self._receiver_loop, daemon=True)
        receiver_thread.start()
        
        # Khởi động thread gửi request
        request_thread = threading.Thread(target=self._request_loop, daemon=True)
        request_thread.start()
        
        # Gửi message interested
        self._send_interested()
        
        return True
    
    def _receiver_loop(self):
        """Nhận và xử lý message từ peer"""
        while self.connected:
            try:
                # Đọc length prefix
                length_prefix = self._recv_exact(4)
                if not length_prefix:
                    break
                
                # Giải mã length
                message_length = struct.unpack(">I", length_prefix)[0]
                
                # Nếu là keep-alive message (length = 0)
                if message_length == 0:
                    continue
                
                # Đọc message id và payload
                message = self._recv_exact(message_length)
                if not message:
                    break
                
                # Giải mã message id
                message_id = message[0]
                
                # Xử lý message
                self._handle_message(message_id, message[1:])
                
                # Cập nhật last_active
                self.last_active = time.time()
                
            except socket.timeout:
                # Gửi keep-alive và tiếp tục
                self._send_keepalive()
                continue
                
            except Exception as e:
                logging.error(f"Lỗi trong receiver_loop: {e}")
                break
        
        # Đóng kết nối
        self._close_connection()
    
    def _handle_message(self, message_id, payload):
        """Xử lý message từ peer"""
        # Choke: peer không gửi dữ liệu cho chúng ta
        if message_id == 0:
            self.peer_choking = True
            logging.debug(f"Peer {self.peer_id} choked us")
        
        # Unchoke: peer cho phép chúng ta download
        elif message_id == 1:
            self.peer_choking = False
            logging.debug(f"Peer {self.peer_id} unchoked us")
        
        # Interested: peer muốn tải từ chúng ta
        elif message_id == 2:
            self.peer_interested = True
            logging.debug(f"Peer {self.peer_id} is interested")
            
            # Unchoke peer nếu họ interested
            if self.am_choking:
                self._send_unchoke()
        
        # Not interested: peer không muốn tải từ chúng ta
        elif message_id == 3:
            self.peer_interested = False
            logging.debug(f"Peer {self.peer_id} is not interested")
        
        # Have: peer có thêm piece
        elif message_id == 4:
            if len(payload) < 4:
                return
            
            piece_index = struct.unpack(">I", payload[:4])[0]
            
            # Cập nhật bitfield
            if 0 <= piece_index < len(self.peer_bitfield):
                self.peer_bitfield[piece_index] = True
                logging.debug(f"Peer {self.peer_id} has piece {piece_index}")
            
            # Nếu chưa interested và chúng ta cần piece này
            if not self.am_interested and not self.piece_manager.has_piece(piece_index):
                self._send_interested()
        
        # Bitfield: danh sách piece peer có
        elif message_id == 5:
            # Kiểm tra độ dài bitfield có khớp với số piece không
            expected_length = (self.piece_manager.piece_count + 7) // 8  # Số byte cần thiết
            if len(payload) != expected_length:
                logging.warning(f"Received invalid bitfield length: {len(payload)}, expected: {expected_length}")
                return
            
            # Parse bitfield
            for i in range(self.piece_manager.piece_count):
                byte_index = i // 8
                bit_index = 7 - (i % 8)  # Bit 7 là MSB
                
                if byte_index < len(payload):
                    self.peer_bitfield[i] = bool((payload[byte_index] >> bit_index) & 1)
            
            logging.debug(f"Received bitfield from peer {self.peer_id}, has {sum(self.peer_bitfield)} pieces")
            
            # Nếu có piece chúng ta cần, gửi interested
            for i in range(self.piece_manager.piece_count):
                if self.peer_bitfield[i] and not self.piece_manager.has_piece(i):
                    self._send_interested()
                    break
        
        # Request: peer yêu cầu piece
        elif message_id == 6:
            if len(payload) < 12:
                return
                
            index, begin, length = struct.unpack(">III", payload[:12])
            
            # Nếu chúng ta đang choke peer, không gửi piece
            if self.am_choking:
                logging.debug(f"Ignoring request from peer {self.peer_id} (choked)")
                return
                
            # Nếu chúng ta có piece được yêu cầu, gửi nó
            if self.piece_manager.has_piece(index):
                block_data = self.piece_manager.read_block(index, begin, length)
                if block_data:
                    self._send_piece(index, begin, block_data)
                    
                    # Cập nhật thống kê upload
                    with self.lock:
                        self.bytes_uploaded += len(block_data)
                        
                    logging.debug(f"Sent piece {index}, begin {begin}, length {len(block_data)} to peer {self.peer_id}")
                else:
                    logging.warning(f"Could not read requested block: piece {index}, begin {begin}, length {length}")
            else:
                logging.debug(f"Peer {self.peer_id} requested piece {index} which we don't have")
        
        # Piece: nhận dữ liệu piece từ peer
        elif message_id == 7:
            if len(payload) < 8:
                return
                
            index, begin = struct.unpack(">II", payload[:8])
            block = payload[8:]
            
            # Lưu block vào piece manager
            result = self.piece_manager.write_block(index, begin, block)
            
            # Cập nhật thống kê download
            with self.lock:
                self.bytes_downloaded += len(block)
                
            logging.debug(f"Received piece {index}, begin {begin}, length {len(block)} from peer {self.peer_id}")
            
            # Nếu piece đã hoàn thành, gửi have message cho các peer khác
            if result == "piece_complete":
                self._send_have(index)
                
                # Nếu tất cả piece đã hoàn thành, gửi not interested
                if self.piece_manager.is_complete():
                    self._send_not_interested()
                    
            # Yêu cầu block tiếp theo
            self.request_queue.put((index, begin + len(block)))
        
        # Cancel: peer hủy yêu cầu
        elif message_id == 8:
            # Không cần xử lý vì chúng ta không theo dõi các request đang chờ xử lý
            pass
        
        # Các message ID khác: không xử lý
        else:
            logging.warning(f"Received unknown message ID: {message_id}")
    
    def _request_loop(self):
        """Gửi các request piece đến peer"""
        # Số request đang chờ tối đa
        MAX_PENDING_REQUESTS = 5
        pending_requests = 0
        
        while self.connected and not self.piece_manager.is_complete():
            try:
                # Nếu bị choke, đợi
                if self.peer_choking:
                    time.sleep(1)
                    continue
                    
                # Nếu có request trong hàng đợi, xử lý nó
                if not self.request_queue.empty():
                    index, begin = self.request_queue.get()
                    # Yêu cầu tiếp theo nếu chưa hoàn thành piece
                    if not self.piece_manager.is_piece_complete(index):
                        self._request_next_block(index, begin)
                        pending_requests += 1
                        
                # Ngược lại, tìm piece mới để tải
                elif pending_requests < MAX_PENDING_REQUESTS:
                    piece_index = self._select_piece_to_request()
                    if piece_index is not None:
                        # Bắt đầu tải piece từ đầu
                        self._request_next_block(piece_index, 0)
                        pending_requests += 1
                    else:
                        # Không có piece nào để tải
                        time.sleep(1)
                else:
                    # Đã đủ số request đang chờ, đợi
                    time.sleep(0.5)
                    pending_requests = 0  # Reset counter
            
            except Exception as e:
                logging.error(f"Lỗi trong request_loop: {e}")
                time.sleep(1)
                
    def _select_piece_to_request(self):
        """Chọn piece để yêu cầu dựa trên chiến lược rarest-first"""
        needed_pieces = []
        
        # Tìm các piece mà peer có và chúng ta cần
        for i in range(self.piece_manager.piece_count):
            if self.peer_bitfield[i] and not self.piece_manager.has_piece(i) and not self.piece_manager.is_piece_requested(i):
                needed_pieces.append(i)
                
        if not needed_pieces:
            return None
            
        # Nếu đây là phase đầu (dưới 4 piece), chọn ngẫu nhiên
        if self.piece_manager.complete_pieces_count < 4:
            import random
            return random.choice(needed_pieces)
            
        # Ngược lại, áp dụng rarest-first (đơn giản hóa: chọn piece đầu tiên)
        return needed_pieces[0]
        
    def _request_next_block(self, piece_index, begin):
        """Yêu cầu block tiếp theo của piece"""
        # Kích thước mỗi block (thường là 16KB)
        BLOCK_SIZE = 16 * 1024
        
        # Kích thước của piece
        piece_size = self.piece_manager.get_piece_size(piece_index)
        
        # Nếu đã hết piece, không yêu cầu nữa
        if begin >= piece_size:
            return
            
        # Tính kích thước block thực tế (có thể nhỏ hơn nếu là block cuối)
        length = min(BLOCK_SIZE, piece_size - begin)
        
        # Đánh dấu block này đang được yêu cầu
        self.piece_manager.request_block(piece_index, begin, length)
        
        # Gửi request
        self._send_request(piece_index, begin, length)
        
    def _recv_exact(self, length):
        """Nhận chính xác số byte chỉ định từ socket"""
        if not self.socket:
            return None
            
        data = b''
        remaining = length
        
        while remaining > 0:
            try:
                chunk = self.socket.recv(remaining)
                if not chunk:  # Kết nối đã đóng
                    return None
                    
                data += chunk
                remaining -= len(chunk)
                
            except socket.timeout:
                # Cho phép timeout và thử lại
                continue
                
            except Exception as e:
                logging.error(f"Error receiving data: {e}")
                return None
                
        return data
        
    def _send_keepalive(self):
        """Gửi keep-alive message"""
        if not self.connected:
            return False
            
        try:
            # Keep-alive: length prefix 0
            message = struct.pack(">I", 0)
            self.socket.sendall(message)
            return True
        except Exception as e:
            logging.error(f"Error sending keep-alive: {e}")
            self._close_connection()
            return False
            
    def _send_interested(self):
        """Gửi interested message"""
        if not self.connected:
            return False
            
        try:
            # Interested: <len=0001><id=2>
            message = struct.pack(">IB", 1, 2)
            self.socket.sendall(message)
            self.am_interested = True
            return True
        except Exception as e:
            logging.error(f"Error sending interested: {e}")
            self._close_connection()
            return False
            
    def _send_not_interested(self):
        """Gửi not interested message"""
        if not self.connected:
            return False
            
        try:
            # Not interested: <len=0001><id=3>
            message = struct.pack(">IB", 1, 3)
            self.socket.sendall(message)
            self.am_interested = False
            return True
        except Exception as e:
            logging.error(f"Error sending not interested: {e}")
            self._close_connection()
            return False
            
    def _send_choke(self):
        """Gửi choke message"""
        if not self.connected:
            return False
            
        try:
            # Choke: <len=0001><id=0>
            message = struct.pack(">IB", 1, 0)
            self.socket.sendall(message)
            self.am_choking = True
            return True
        except Exception as e:
            logging.error(f"Error sending choke: {e}")
            self._close_connection()
            return False
            
    def _send_unchoke(self):
        """Gửi unchoke message"""
        if not self.connected:
            return False
            
        try:
            # Unchoke: <len=0001><id=1>
            message = struct.pack(">IB", 1, 1)
            self.socket.sendall(message)
            self.am_choking = False
            return True
        except Exception as e:
            logging.error(f"Error sending unchoke: {e}")
            self._close_connection()
            return False
            
    def _send_have(self, piece_index):
        """Gửi have message"""
        if not self.connected:
            return False
            
        try:
            # Have: <len=0005><id=4><piece index>
            message = struct.pack(">IBI", 5, 4, piece_index)
            self.socket.sendall(message)
            return True
        except Exception as e:
            logging.error(f"Error sending have: {e}")
            self._close_connection()
            return False
            
    def _send_request(self, piece_index, begin, length):
        """Gửi request message"""
        if not self.connected:
            return False
            
        try:
            # Request: <len=0013><id=6><index><begin><length>
            message = struct.pack(">IBIII", 13, 6, piece_index, begin, length)
            self.socket.sendall(message)
            return True
        except Exception as e:
            logging.error(f"Error sending request: {e}")
            self._close_connection()
            return False
            
    def _send_piece(self, piece_index, begin, block):
        """Gửi piece message"""
        if not self.connected:
            return False
            
        try:
            # Piece: <len=0009+X><id=7><index><begin><block>
            message_len = 9 + len(block)
            header = struct.pack(">IBI", message_len, 7, piece_index)
            begin_bytes = struct.pack(">I", begin)
            
            # Gửi từng phần để tránh lỗi memory
            self.socket.sendall(header)
            self.socket.sendall(begin_bytes)
            self.socket.sendall(block)
            return True
        except Exception as e:
            logging.error(f"Error sending piece: {e}")
            self._close_connection()
            return False
            
    def _send_bitfield(self, bitfield):
        """Gửi bitfield message"""
        if not self.connected:
            return False
            
        try:
            # Chuyển đổi bitfield dạng list [True, False, ...] sang bytes
            byte_count = (len(bitfield) + 7) // 8
            bit_array = bytearray(byte_count)
            
            for i, bit in enumerate(bitfield):
                if bit:
                    byte_index = i // 8
                    bit_offset = 7 - (i % 8)  # MSB first
                    bit_array[byte_index] |= (1 << bit_offset)
            
            # Bitfield: <len=0001+X><id=5><bitfield>
            message_len = 1 + len(bit_array)
            message = struct.pack(">IB", message_len, 5) + bit_array
            
            self.socket.sendall(message)
            return True
        except Exception as e:
            logging.error(f"Error sending bitfield: {e}")
            self._close_connection()
            return False
            
    def _close_connection(self):
        """Đóng kết nối socket"""
        if self.connected:
            self.connected = False
            try:
                if self.socket:
                    self.socket.close()
                logging.info(f"Đóng kết nối với peer {self.peer_id}")
            except Exception as e:
                logging.error(f"Lỗi khi đóng kết nối: {e}")
    
    def get_stats(self):
        """Trả về thống kê kết nối"""
        with self.lock:
            return {
                "peer_id": self.peer_id,
                "ip": self.ip,
                "port": self.port,
                "connected": self.connected,
                "am_choking": self.am_choking,
                "am_interested": self.am_interested,
                "peer_choking": self.peer_choking,
                "peer_interested": self.peer_interested,
                "pieces_available": sum(self.peer_bitfield),
                "bytes_downloaded": self.bytes_downloaded,
                "bytes_uploaded": self.bytes_uploaded,
                "last_active": self.last_active
            }