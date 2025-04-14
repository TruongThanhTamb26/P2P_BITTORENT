"""Quản lý kết nối giữa các peer"""
import socket
import struct
import logging
import time
import threading
import queue

# Định nghĩa các loại message trong protocol BitTorrent
class MessageType:
    """Các loại message trong protocol BitTorrent"""
    KEEP_ALIVE = -1  # Không phải message type thực, chỉ để mã hóa keep-alive message
    CHOKE = 0
    UNCHOKE = 1
    INTERESTED = 2
    NOT_INTERESTED = 3
    HAVE = 4
    BITFIELD = 5
    REQUEST = 6
    PIECE = 7
    CANCEL = 8
    PORT = 9  # Cho DHT

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
        """Khởi động thread xử lý kết nối"""
        if not self.connected:
            return False
        
        # Chỉ sử dụng một cơ chế xử lý message - ưu tiên dùng download()
        download_thread = threading.Thread(target=self.download, daemon=True)
        download_thread.start()
        return True
    
    def _process_bitfield(self, payload):
        """Xử lý bitfield từ peer."""
        bitfield_length = len(payload) * 8
        for i in range(min(bitfield_length, len(self.peer_bitfield))):
            byte_index = i // 8
            bit_index = 7 - (i % 8)  # Bit thứ tự từ trái sang phải
            if byte_index < len(payload):
                has_piece = bool(payload[byte_index] & (1 << bit_index))
                self.peer_bitfield[i] = has_piece

    def _peer_has_pieces_we_need(self):
        """Kiểm tra xem peer có pieces mà chúng ta cần không."""
        for i in range(len(self.peer_bitfield)):
            if self.peer_bitfield[i] and not self.piece_manager.has_piece(i):
                return True
        return False

    def _handle_piece(self, payload):
        """Xử lý piece data"""
        try:
            if len(payload) < 8:
                logging.error(f"Piece payload quá ngắn: {len(payload)} bytes")
                return
                
            # Parse piece data
            piece_index = struct.unpack(">I", payload[0:4])[0]
            begin = struct.unpack(">I", payload[4:8])[0]
            block = payload[8:]
            
            logging.info(f"Nhận block: piece {piece_index}, offset {begin}, length {len(block)}")
            
            # Lưu block vào file tạm
            success = self.piece_manager.write_block(piece_index, begin, block)
            
            if success:
                logging.info(f"Đã lưu block: piece {piece_index}, offset {begin}")
                
                # Kiểm tra tiến độ download
                progress = self.piece_manager.progress
                if progress > 0:
                    logging.info(f"Tiến độ download: {progress*100:.1f}%")
                    
                # Yêu cầu piece tiếp theo
                if self.piece_manager.is_piece_complete(piece_index):
                    self._request_next_piece()
            else:
                logging.error(f"Lỗi khi lưu block: piece {piece_index}, offset {begin}")
        
        except Exception as e:
            logging.error(f"Lỗi khi xử lý piece data: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())

    def _request_next_piece(self):
        """Yêu cầu piece tiếp theo sau khi hoàn thành một piece"""
        # Tìm piece tiếp theo để yêu cầu
        piece_index = self._select_piece_to_request()
        
        if piece_index is not None:
            logging.info(f"Yêu cầu piece tiếp theo: {piece_index}")
            self._request_piece(piece_index)
            return True
        else:
            logging.info("Không còn piece nào để yêu cầu")
            return False
    
    def download(self):
        """Tải xuống dữ liệu từ peer."""
        try:
            # Gửi thông báo interested
            self._send_message(MessageType.INTERESTED)
            
            # Biến theo dõi trạng thái
            am_choking = True
            am_interested = True
            peer_choking = True
            peer_interested = False
            
            # Thời gian giữa các request piece
            request_interval = 0.1  # Thời gian giữa các request
            last_request_time = 0
            pending_requests = 0
            MAX_PENDING_REQUESTS = 5  # Số lượng request đồng thời tối đa
            
            # Thêm biến này để theo dõi các piece đã yêu cầu
            requested_pieces = set()
            
            # Timeout cho kết nối không hoạt động
            last_receive_time = time.time()
            INACTIVE_TIMEOUT = 60  # 60 giây
            
            logging.info(f"Bắt đầu download từ {self.peer_id[:8]}")
            
            # Gửi bitfield để peer biết chúng ta có những piece nào
            have_bitfield = self.piece_manager.get_bitfield()
            self._send_bitfield(have_bitfield)
            
            while True:
                # Kiểm tra timeout
                if time.time() - last_receive_time > INACTIVE_TIMEOUT:
                    logging.warning(f"Kết nối với {self.peer_id[:8]} timeout, ngắt kết nối")
                    break
                    
                # Đọc message type và length
                try:
                    self.socket.settimeout(5.0)  # Timeout ngắn để không bị treo
                    message_length_bytes = self._recv_exact(4)
                    if not message_length_bytes:
                        logging.warning(f"Kết nối với {self.peer_id[:8]} đã đóng")
                        break
                        
                    message_length = struct.unpack(">I", message_length_bytes)[0]
                    
                    # Keep-alive message
                    if message_length == 0:
                        logging.debug(f"Nhận keep-alive từ {self.peer_id[:8]}")
                        last_receive_time = time.time()
                        continue
                        
                    # Đọc message ID và payload
                    message_id_bytes = self._recv_exact(1)
                    if not message_id_bytes:
                        break
                    message_id = message_id_bytes[0]
                    
                    # Đọc payload nếu cần
                    payload = b""
                    remaining_length = message_length - 1
                    if remaining_length > 0:
                        payload = self._recv_exact(remaining_length)
                        if not payload:
                            break
                    
                    last_receive_time = time.time()
                    
                    # Xử lý message theo loại
                    if message_id == MessageType.CHOKE:
                        peer_choking = True
                        logging.debug(f"Peer {self.peer_id[:8]} choked us")
                    
                    elif message_id == MessageType.UNCHOKE:
                        peer_choking = False
                        logging.info(f"Peer {self.peer_id[:8]} unchoked us - có thể yêu cầu pieces")
                    
                    elif message_id == MessageType.INTERESTED:
                        peer_interested = True
                        # Nếu peer quan tâm, gửi unchoke
                        if am_choking:
                            self._send_message(MessageType.UNCHOKE)
                            am_choking = False
                    
                    elif message_id == MessageType.NOT_INTERESTED:
                        peer_interested = False
                    
                    elif message_id == MessageType.HAVE:
                        piece_index = struct.unpack(">I", payload)[0]
                        self.peer_bitfield[piece_index] = True
                        logging.debug(f"Peer {self.peer_id[:8]} có piece {piece_index}")
                        
                        # Nếu chúng ta cần piece này và chưa interested, gửi interested
                        if not am_interested and not self.piece_manager.has_piece(piece_index):
                            self._send_message(MessageType.INTERESTED)
                            am_interested = True
                    
                    elif message_id == MessageType.BITFIELD:
                        self._process_bitfield(payload)
                        logging.debug(f"Nhận bitfield từ peer {self.peer_id[:8]}")
                        
                        # Ngay sau khi nhận bitfield, gửi interested nếu peer có pieces chúng ta cần
                        if self._peer_has_pieces_we_need():
                            self._send_message(MessageType.INTERESTED)
                            am_interested = True
                            logging.info(f"Gửi INTERESTED tới peer {self.peer_id[:8]}")
                    
                    elif message_id == MessageType.REQUEST:
                        # Xử lý request từ peer nếu chúng ta không choked họ
                        if not am_choking:
                            self._handle_request(payload)
                    
                    elif message_id == MessageType.PIECE:
                        # Xử lý piece data
                        self._handle_piece(payload)
                        pending_requests -= 1
                        
                        # Log tiến độ download
                        if self.piece_manager.progress > 0:
                            logging.info(f"Tiến độ download: {self.piece_manager.progress*100:.1f}%")
                    
                    elif message_id == MessageType.CANCEL:
                        # Xử lý hủy request
                        pass
                    
                except socket.timeout:
                    # Timeout khi chờ đọc, không làm gì
                    pass
                
                # Nếu không bị choke và có pieces để tải
                if not peer_choking and self._peer_has_pieces_we_need():
                    now = time.time()
                    # Chỉ gửi request nếu đã đủ thời gian từ lần request trước
                    # và chưa đạt số lượng request tối đa
                    if now - last_request_time > request_interval and pending_requests < MAX_PENDING_REQUESTS:
                        piece_index = self._select_piece_to_request()
                        if piece_index is not None and piece_index not in requested_pieces:
                            self._request_piece(piece_index)
                            requested_pieces.add(piece_index)
                            pending_requests += 1
                            last_request_time = now
                            logging.info(f"Yêu cầu piece {piece_index} từ peer {self.peer_id[:8]}")
                
                # Kiểm tra nếu đã hoàn thành tất cả pieces
                if self.piece_manager.is_complete():
                    logging.info(f"Đã tải xong tất cả pieces từ {self.peer_id[:8]}")
                    break
                
                # Gửi keep-alive nếu cần
                if time.time() - last_receive_time > 30:  # 30 giây không có hoạt động
                    self._send_message(MessageType.KEEP_ALIVE)
        
        except Exception as e:
            logging.error(f"Lỗi trong quá trình download từ {self.peer_id[:8]}: {e}")
            import traceback
            logging.error(traceback.format_exc())
        
        finally:
            try:
                if self.socket:
                    self.socket.close()
            except:
                pass
            logging.info(f"Đã đóng kết nối với peer {self.peer_id[:8]}")

    

    def _handle_request(self, payload):
        """Xử lý REQUEST message từ peer"""
        if len(payload) < 12:
            logging.error("REQUEST message quá ngắn")
            return
            
        piece_index, begin, length = struct.unpack(">III", payload[:12])
        
        # Kiểm tra nếu chúng ta có piece được yêu cầu
        if not self.piece_manager.has_piece(piece_index):
            logging.debug(f"Peer yêu cầu piece {piece_index} mà chúng ta không có")
            return
        
        # Đọc dữ liệu từ piece manager
        block_data = self.piece_manager.read_block(piece_index, begin, length)
        
        if block_data:
            # Tạo và gửi PIECE message
            piece_msg_payload = struct.pack(">II", piece_index, begin) + block_data
            self._send_message(MessageType.PIECE, piece_msg_payload)
            
            # Cập nhật thống kê
            self.bytes_uploaded += len(block_data)
            
            logging.debug(f"Đã gửi piece {piece_index}, offset {begin}, length {len(block_data)}")
        else:
            logging.warning(f"Không thể đọc dữ liệu cho piece {piece_index}, offset {begin}")

    def _request_piece(self, piece_index):
        """Yêu cầu một piece từ peer"""
        if not self.connected:
            return
            
        try:
            # Kích thước của mỗi block trong một piece
            block_size = 16 * 1024  # 16KB
            
            # Xác định kích thước của piece được yêu cầu
            piece_size = self.piece_manager.get_piece_size(piece_index)
            
            # Chia thành các block và gửi nhiều request
            for offset in range(0, piece_size, block_size):
                # Block cuối có thể nhỏ hơn
                current_block_size = min(block_size, piece_size - offset)
                
                # Gửi request message 
                payload = struct.pack(">III", piece_index, offset, current_block_size)
                self._send_message(MessageType.REQUEST, payload)
                
                logging.debug(f"Gửi request piece {piece_index}, offset {offset}, length {current_block_size}")
                
        except Exception as e:
            logging.error(f"Lỗi khi request piece {piece_index}: {e}")

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

        while (self.connected) and (not self.piece_manager.is_complete()):
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
        """Chọn piece để yêu cầu từ peer"""
        # Danh sách các piece mà peer có và chúng ta cần
        needed_pieces = []
        
        for i in range(len(self.peer_bitfield)):
            # Kiểm tra nếu peer có piece này
            if self.peer_bitfield[i]:
                # Và chúng ta chưa có hoặc chưa yêu cầu
                if not self.piece_manager.has_piece(i) and not self.piece_manager.is_piece_requested(i):
                    needed_pieces.append(i)
        
        # Nếu không có piece nào cần
        if not needed_pieces:
            return None
        
        # Chiến lược chọn piece đơn giản: lấy piece đầu tiên cần
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
            
    def _send_bitfield(self, bitfield=None):
        """Gửi bitfield message tới peer
        
        Args:
            bitfield: Danh sách các piece mà chúng ta có (True/False cho mỗi piece)
        """
        # Nếu không có bitfield hoặc bitfield rỗng, không gửi
        if not bitfield:
            # Tạo bitfield từ piece_manager nếu có
            if hasattr(self.piece_manager, 'get_bitfield') and callable(self.piece_manager.get_bitfield):
                bitfield = self.piece_manager.get_bitfield()
            else:
                # Tạo bitfield rỗng dựa trên số lượng piece
                bitfield = [False] * self.piece_manager.piece_count
        
        # Chuyển bitfield thành bytes
        num_bytes = (len(bitfield) + 7) // 8
        bitfield_bytes = bytearray(num_bytes)
        
        # Đặt các bit tương ứng
        for i, has_piece in enumerate(bitfield):
            if has_piece:
                byte_index = i // 8
                bit_index = 7 - (i % 8)  # Bit đầu tiên là MSB
                bitfield_bytes[byte_index] |= (1 << bit_index)
        
        # Gửi BITFIELD message
        self._send_message(MessageType.BITFIELD, bytes(bitfield_bytes))
            
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

    def _send_message(self, message_id, payload=b''):
        """Gửi message đến peer
        
        Args:
            message_id: Loại message (từ class MessageType)
            payload: Dữ liệu của message (bytes)
        """
        try:
            if message_id == MessageType.KEEP_ALIVE:
                # Keep-alive là message đặc biệt không có id
                message = struct.pack('>I', 0)
            else:
                # Gói tin thông thường: <length prefix><message ID><payload>
                message = struct.pack('>IB', 1 + len(payload), message_id) + payload
            
            self.socket.sendall(message)
        except Exception as e:
            logging.error(f"Lỗi khi gửi message đến peer {self.peer_id[:8]}: {e}")
            raise