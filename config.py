"""
Cấu hình hệ thống P2P

Sửa file này để thay đổi cấu hình khi chuyển sang máy khác.
Không cần sửa code trong các file khác.
"""
import os
# Địa chỉ IP máy đang chạy tracker server
# Trong môi trường thực tế, đây là địa chỉ IP public hoặc domain name
# Khi chạy trên cùng một máy, có thể để là localhost
TRACKER_HOST = "10.28.128.187"  # Thay đổi từ 192.168.110.35 sang localhost vì kết nối thực tế đang sử dụng localhost

# Cổng của tracker server
TRACKER_PORT = 8000

# Địa chỉ URL của tracker server
TRACKER_URL = f"http://{TRACKER_HOST}:{TRACKER_PORT}/announce"

# Cổng của web server
WEB_SERVER_PORT = 5000

# Cổng mặc định để lắng nghe kết nối từ các peer khác
DEFAULT_PEER_PORT = 6881

# Thư mục lưu trữ tệp tải về - đường dẫn tuyệt đối
DOWNLOAD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "downloads")

# Thư mục lưu metainfo - đường dẫn tuyệt đối
METAINFO_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "metainfo")  

# Kích thước piece mặc định (512 KB)
DEFAULT_PIECE_LENGTH = 512 * 1024