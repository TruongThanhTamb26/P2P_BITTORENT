"""Quản lý các piece của file trong quá trình tải xuống và chia sẻ"""
import os
import logging
import threading
import time
import hashlib
from pathlib import Path
from config import DOWNLOAD_DIR

class Piece:
    """Đại diện cho một piece của torrent"""
    
    def __init__(self, index, length):
        self.index = index
        self.length = length
        self.data = bytearray(length)
        self.downloaded_bytes = 0
        self.complete = False

class PieceManager:
    """Quản lý các piece của torrent và theo dõi tiến độ tải xuống"""
    
    def __init__(self, info_hash, piece_length, piece_hashes, files, DOWNLOAD_DIR):
        """Khởi tạo piece manager"""
        self.info_hash = info_hash
        self.piece_length = piece_length
        self.piece_hashes = piece_hashes
        self.files = files
        self.download_dir = DOWNLOAD_DIR
        
        # Thư mục torrent
        self.torrent_dir = self.download_dir / info_hash
        os.makedirs(self.torrent_dir, exist_ok=True)
        
        # Tính tổng kích thước và số piece
        self.total_size = sum(f['length'] for f in self.files)
        self.piece_count = len(piece_hashes) if piece_hashes else (self.total_size + piece_length - 1) // piece_length

        # Khởi tạo các piece objects
        self.pieces = []
        for i in range(self.piece_count):
            # Tính kích thước piece
            if i == self.piece_count - 1:  # Piece cuối cùng
                piece_size = self.total_size - (self.piece_count - 1) * self.piece_length
            else:
                piece_size = self.piece_length
            
            self.pieces.append(Piece(i, piece_size))
        
        
        # Trạng thái các piece
        self.have_pieces = [False] * self.piece_count
        self.requested_pieces = set()  # Các piece đang được yêu cầu
        
        # Thống kê
        self.bytes_downloaded = 0
        self.bytes_uploaded = 0
        self.start_time = time.time()
        
        # Cho thread safety
        self.lock = threading.Lock()    
        
        # Tạo thư mục lưu dữ liệu piece tạm thời
        self.pieces_dir = self.torrent_dir / "pieces"
        os.makedirs(self.pieces_dir, exist_ok=True)
    
    def mark_all_complete(self):
        """Đánh dấu tất cả piece đã tải xong (dùng cho upload)"""
        with self.lock:
            self.have_pieces = [True] * self.piece_count
            # Đánh dấu từng piece là hoàn thành
            for piece in self.pieces:
                piece.complete = True
                piece.downloaded_bytes = piece.length
            self.bytes_downloaded = self.total_size
            
    def load_progress(self):
        """Tải tiến độ tải xuống từ disk"""
        with self.lock:
            # Kiểm tra xem file nào đã tải xong
            for i in range(self.piece_count):
                piece_file = self.pieces_dir / f"piece_{i}"
                if piece_file.exists():
                    # Kiểm tra xem piece có hợp lệ không (nếu có hash)
                    if (self.piece_hashes) and (i < len(self.piece_hashes)):
                        with open(piece_file, 'rb') as f:
                            data = f.read()
                        
                        # Tính hash của piece
                        piece_hash = hashlib.sha1(data).hexdigest()
                        
                        # So sánh với hash đã biết
                        if piece_hash == self.piece_hashes[i]:
                            self.have_pieces[i] = True
                            self.pieces[i].complete = True  # Cập nhật Piece object
                            self.pieces[i].downloaded_bytes = len(data)
                            self.bytes_downloaded += len(data)
                    else:
                        # Nếu không có hash để kiểm tra, giả định piece hợp lệ
                        self.have_pieces[i] = True
                        piece_size = os.path.getsize(piece_file)
                        self.pieces[i].complete = True  # Cập nhật Piece object
                        self.pieces[i].downloaded_bytes = piece_size
                        self.bytes_downloaded += piece_size
            
            # Nếu đã tải xong tất cả, kiểm tra xem có file cuối cùng không
            if all(self.have_pieces) and self.files:
                # Kiểm tra xem đã có file cuối cùng không
                final_file = self.torrent_dir / self.files[-1]["path"]
                if final_file.exists():
                    # Đánh dấu đã hoàn thành
                    self._assemble_files()

    def reset_progress(self):
        """Reset tiến độ tải xuống về 0"""
        self.completed_pieces = set()
        self.requested_pieces = {}
        self.bytes_downloaded = 0
        self.bytes_uploaded = 0
        
        # Xóa file progress nếu có
        progress_file = Path(self.download_dir) / self.info_hash / "progress.json"
        if progress_file.exists():
            try:
                os.remove(progress_file)
            except:
                pass

    def has_piece(self, piece_index):
        """Kiểm tra xem có piece này không"""
        if 0 <= piece_index < len(self.have_pieces):
            return self.have_pieces[piece_index]
        return False

    def is_piece_requested(self, piece_index):
        """Kiểm tra xem piece đã được yêu cầu chưa"""
        return piece_index in self.requested_pieces

    def is_piece_complete(self, piece_index):
        """Kiểm tra xem piece đã hoàn thành chưa"""
        return self.have_pieces[piece_index] if 0 <= piece_index < len(self.have_pieces) else False
    
    
    @property
    def bytes_left(self):
        """Tính số byte còn phải tải"""
        return max(0, self.total_size - self.bytes_downloaded)
    
    @property
    def progress(self):
        """Tính tiến độ tải xuống (0.0 - 1.0)"""
        if self.total_size == 0:
            return 1.0
        with self.lock:
            return self.bytes_downloaded / self.total_size
    
    def is_complete(self):
        """Kiểm tra xem đã tải xong chưa"""
        with self.lock:
            return all(self.have_pieces)
        
    def get_piece_size(self, piece_index):
        """Trả về kích thước của piece"""
        with self.lock:
            if piece_index < 0 or piece_index >= self.piece_count:
                return 0
                
            # Với piece cuối, kích thước có thể nhỏ hơn
            if piece_index == self.piece_count - 1:
                remaining = self.total_size % self.piece_length
                if remaining > 0:
                    return remaining
            
            return self.piece_length
    
    def get_next_request(self, peer_has_pieces):
        """Lấy piece tiếp theo để yêu cầu từ peer"""
        with self.lock:
            # Sử dụng thuật toán "Rarest First": Chọn piece mà ít peer có nhất
            candidates = []
            for i in range(self.piece_count):
                if not self.have_pieces[i] and i not in self.requested_pieces and peer_has_pieces[i]:
                    candidates.append(i)
            
            if not candidates:
                return None
            
            # Trong triển khai đơn giản, chỉ chọn piece đầu tiên có sẵn
            piece_index = candidates[0]
            self.requested_pieces.add(piece_index)
            return piece_index
        
    def receive_block(self, piece_index, begin, data):
        """Nhận và lưu một block của piece"""
        with self.lock:
            # Kiểm tra tính hợp lệ
            if piece_index < 0 or piece_index >= self.piece_count:
                logging.error(f"Piece index không hợp lệ: {piece_index}")
                return False
                
            # Tạo thư mục pieces nếu chưa tồn tại
            os.makedirs(self.pieces_dir, exist_ok=True)
            
            # Đường dẫn đến file piece
            piece_file = self.pieces_dir / f"piece_{piece_index}"
            
            try:
                # Nếu file piece chưa tồn tại, tạo file trống có kích thước phù hợp
                if not piece_file.exists():
                    piece_size = self.get_piece_size(piece_index)
                    with open(piece_file, 'wb') as f:
                        f.write(b'\0' * piece_size)
                
                # Ghi block vào đúng vị trí trong file piece
                with open(piece_file, 'r+b') as f:
                    f.seek(begin)
                    f.write(data)
                
                # Kiểm tra xem piece đã hoàn thành chưa
                self._check_piece_complete(piece_index)
                
                return True
                
            except Exception as e:
                logging.error(f"Lỗi khi lưu block: {e}")
                return False
    
    def _check_piece_complete(self, piece_index):
        """Kiểm tra xem piece đã hoàn thành và hợp lệ chưa"""
        piece_file = self.pieces_dir / f"piece_{piece_index}"
        
        if not piece_file.exists():
            return False
            
        # Đọc toàn bộ piece
        with open(piece_file, 'rb') as f:
            data = f.read()
        
        # Kiểm tra kích thước
        expected_size = self.get_piece_size(piece_index)
        if len(data) != expected_size:
            return False
        
        # Kiểm tra hash nếu có
        if self.piece_hashes and piece_index < len(self.piece_hashes):
            piece_hash = hashlib.sha1(data).hexdigest()
            if piece_hash != self.piece_hashes[piece_index]:
                logging.warning(f"Piece {piece_index} hash không khớp")
                return False
        
        # Đánh dấu piece đã hoàn thành
        self.have_pieces[piece_index] = True
        self.pieces[piece_index].complete = True
        self.pieces[piece_index].downloaded_bytes = len(data)
        self.bytes_downloaded += len(data)
        
        if self.is_complete():
            # Nếu đã tải xong tất cả, lắp ghép các file
            self._assemble_files()
        
        return True

    def receive_piece(self, piece_index, data):
        """Nhận và lưu piece đã tải xuống"""
        with self.lock:
            # Kiểm tra piece_index hợp lệ
            if piece_index < 0 or piece_index >= self.piece_count:
                logging.error(f"Invalid piece index: {piece_index}")
                return False
            
            # Kiểm tra xem đã có piece này chưa
            if self.have_pieces[piece_index]:
                logging.debug(f"Piece {piece_index} đã có")
                self.requested_pieces.discard(piece_index)
                return True
            
            # Kiểm tra hash nếu có
            if self.piece_hashes and piece_index < len(self.piece_hashes):
                piece_hash = hashlib.sha1(data).hexdigest()
                if piece_hash != self.piece_hashes[piece_index]:
                    logging.warning(f"Piece {piece_index} không hợp lệ: hash không khớp")
                    self.requested_pieces.discard(piece_index)
                    return False
            
            # Lưu piece
            piece_file = self.pieces_dir / f"piece_{piece_index}"
            with open(piece_file, 'wb') as f:
                f.write(data)
            
            # Cập nhật trạng thái
            self.have_pieces[piece_index] = True
            self.requested_pieces.discard(piece_index)
            self.bytes_downloaded += len(data)
            
            # Log tiến độ
            logging.info(f"Đã nhận piece {piece_index}, tiến độ: {self.progress*100:.1f}%")
            
            # Kiểm tra xem đã hoàn thành chưa
            if self.is_complete():
                logging.info("Tải xuống hoàn thành, bắt đầu ghép file...")
                self._assemble_files()
            
            return True
    
    def _assemble_files(self):
        """Ghép các piece thành file hoàn chỉnh"""
        try:
            # Tạo các file theo mô tả trong metainfo
            for file_info in self.files:
                file_path = self.torrent_dir / file_info["path"]
                
                # Tạo thư mục cha nếu cần
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                
                # Mở file để ghi
                with open(file_path, 'wb') as f:
                    pass  # Chỉ tạo file trống
            
            # Lặp qua từng piece và ghi vào file thích hợp
            offset = 0
            for i in range(self.piece_count):
                piece_file = self.pieces_dir / f"piece_{i}"
                
                if not piece_file.exists():
                    logging.error(f"Piece file không tồn tại: {piece_file}")
                    continue
                
                with open(piece_file, 'rb') as f:
                    piece_data = f.read()
                
                # Tìm file và offset trong file để ghi piece này
                piece_offset = offset + i * self.piece_length
                remaining_data = piece_data
                
                for file_info in self.files:
                    file_size = file_info["length"]
                    file_path = self.torrent_dir / file_info["path"]
                    
                    # Kiểm tra xem piece này có thuộc về file không
                    if piece_offset < file_size:
                        # Tính offset trong file
                        file_offset = piece_offset
                        
                        # Tính số byte cần ghi vào file này
                        bytes_to_write = min(len(remaining_data), file_size - file_offset)
                        
                        # Ghi dữ liệu vào file
                        with open(file_path, 'r+b') as f:
                            f.seek(file_offset)
                            f.write(remaining_data[:bytes_to_write])
                        
                        # Cập nhật remaining_data cho file tiếp theo
                        remaining_data = remaining_data[bytes_to_write:]
                        
                        if not remaining_data:
                            break
                        
                        # Cập nhật offset cho file tiếp theo
                        piece_offset -= file_size
                    else:
                        # Dịch offset cho file tiếp theo
                        piece_offset -= file_size
            
            logging.info("Đã ghép file thành công!")
            
            # Tạo file hoàn thành để đánh dấu
            with open(self.torrent_dir / "completed", 'w') as f:
                f.write("1")
            
        except Exception as e:
            logging.error(f"Lỗi khi ghép file: {e}")
    
    def read_piece(self, piece_index, offset, length):
        """Đọc piece để upload cho peer khác"""
        with self.lock:
            # Kiểm tra piece_index hợp lệ
            if piece_index < 0 or piece_index >= self.piece_count:
                logging.error(f"Invalid piece index: {piece_index}")
                return None
            
            # Kiểm tra xem có piece này không
            if not self.have_pieces[piece_index]:
                logging.error(f"Piece {piece_index} chưa được tải xuống")
                return None
            
            try:
                # Đọc từ file piece
                piece_file = self.pieces_dir / f"piece_{piece_index}"
                if piece_file.exists():
                    with open(piece_file, 'rb') as f:
                        f.seek(offset)
                        data = f.read(length)
                    
                    # Cập nhật thống kê
                    self.bytes_uploaded += len(data)
                    
                    return data
                
                # Nếu không có file piece, đọc từ file gốc
                # Tính offset tổng cho piece này
                piece_start = piece_index * self.piece_length
                
                # Tìm file và offset trong file
                current_pos = 0
                for file_info in self.files:
                    file_size = file_info["length"]
                    file_end = current_pos + file_size
                    
                    # Nếu piece nằm trong file này
                    if piece_start < file_end:
                        file_path = self.torrent_dir / file_info["path"]
                        file_offset = piece_start - current_pos + offset
                        
                        with open(file_path, 'rb') as f:
                            f.seek(file_offset)
                            data = f.read(length)
                        
                        # Cập nhật thống kê
                        self.bytes_uploaded += len(data)
                        
                        return data
                    
                    current_pos = file_end
                
                return None
                
            except Exception as e:
                logging.error(f"Lỗi khi đọc piece {piece_index}: {e}")
                return None
            
    def get_bitfield(self):
        """Trả về mảng bit cho biết những piece nào đã có"""
        with self.lock:
            return self.have_pieces  # Sử dụng have_pieces thay vì pieces