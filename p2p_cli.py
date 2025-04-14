"""Command line interface cho ứng dụng P2P"""
import os
import sys
import time
import argparse
import logging
from pathlib import Path

# Thêm đường dẫn cha vào sys.path để import các module
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import TRACKER_URL, DEFAULT_PEER_PORT

# Import các module từ file peer.py
from peer import Peer

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("p2p_cli.log", 'w', 'utf-8'),
        logging.StreamHandler()
    ]
)

def upload_command(args):
    """Tạo torrent từ file và upload"""
    peer = Peer()
    file_paths = [args.file_path]
    
    print(f"Đang tạo torrent từ file: {args.file_path}")
    success, result = peer.create_torrent(file_paths, name=args.name)
    
    if success:
        print(f"\nUpload thành công!")
        print(f"Info hash: {result}")
        print(f"File .torrent.json được lưu tại: {peer.metainfo_dir}/{args.name or Path(args.file_path).name}.torrent.json")
        print(f"File .torrent được lưu tại: {peer.metainfo_dir}/{args.name or Path(args.file_path).name}.torrent")
    else:
        print(f"Lỗi: {result}")

def download_command(args):
    """Tải xuống torrent theo info_hash"""
    peer = Peer()
    
    # Kiểm tra nếu torrent tồn tại
    torrents = peer.get_torrent_status()
    found = False
    
    for torrent in torrents:
        if torrent["info_hash"] == args.info_hash:
            found = True
            break
    
    if not found:
        print(f"Không tìm thấy torrent với info_hash: {args.info_hash}")
        return
    
    # Bắt đầu tải xuống
    print(f"Bắt đầu tải torrent: {args.info_hash}")
    if peer.start_torrent(args.info_hash):
        print("Đã bắt đầu tải torrent")
        
        # Hiển thị tiến độ
        try:
            while True:
                status = peer.get_torrent_status(args.info_hash)
                if not status:
                    print("Mất kết nối với torrent")
                    break
                
                if status["status"] == "seeding" or status["progress"] >= 100:
                    print(f"Tải xuống hoàn thành: {status['name']}")
                    break
                
                print(f"Tiến độ: {status['progress']:.1f}% - Đã tải: {format_size(status['downloaded'])}")
                time.sleep(1)
        except KeyboardInterrupt:
            print("Đã dừng theo yêu cầu của người dùng")
            peer.stop_torrent(args.info_hash)
    else:
        print("Không thể bắt đầu tải torrent")

def list_command(args):
    """Liệt kê tất cả torrent hiện có"""
    peer = Peer()
    torrents = peer.get_torrent_status()
    
    if not torrents:
        print("Không có torrent nào")
        return
    
    print(f"Tìm thấy {len(torrents)} torrent:")
    print("-" * 80)
    print(f"{'INFO HASH':<40} {'TÊN':<20} {'TRẠNG THÁI':<15} {'TIẾN ĐỘ'}")
    print("-" * 80)
    
    for torrent in torrents:
        info_hash = torrent["info_hash"]
        name = torrent["name"]
        status = torrent["status"]
        progress = f"{torrent['progress']:.1f}%"
        
        print(f"{info_hash:<40} {name[:20]:<20} {status:<15} {progress}")

def info_command(args):
    """Hiển thị thông tin chi tiết của torrent"""
    peer = Peer()
    status = peer.get_torrent_status(args.info_hash)
    
    if not status:
        print(f"Không tìm thấy torrent với info_hash: {args.info_hash}")
        return
    
    print(f"Thông tin chi tiết torrent: {status['name']}")
    print(f"Info Hash: {status['info_hash']}")
    print(f"Trạng thái: {status['status']}")
    print(f"Tiến độ: {status['progress']:.1f}%")
    print(f"Đã tải xuống: {format_size(status['downloaded'])}")
    print(f"Đã upload: {format_size(status['uploaded'])}")
    print(f"Còn lại: {format_size(status['left'])}")
    
    print("\nDanh sách file:")
    for file_info in status['files']:
        print(f" - {file_info['path']} ({format_size(file_info['length'])})")

def stop_command(args):
    """Dừng tải xuống torrent"""
    peer = Peer()
    if peer.stop_torrent(args.info_hash):
        print(f"Đã dừng torrent: {args.info_hash}")
    else:
        print(f"Không thể dừng torrent: {args.info_hash}")

def resume_command(args):
    """Tiếp tục tải xuống torrent đã dừng"""
    peer = Peer()
    if peer.start_torrent(args.info_hash):
        print(f"Đã tiếp tục torrent: {args.info_hash}")
    else:
        print(f"Không thể tiếp tục torrent: {args.info_hash}")

def format_size(size_bytes):
    """Định dạng kích thước file"""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes/1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes/(1024*1024):.2f} MB"
    else:
        return f"{size_bytes/(1024*1024*1024):.2f} GB"

def main():
    """Điểm chạy chính của chương trình"""
    parser = argparse.ArgumentParser(description="P2P BitTorrent Command Line Interface")
    subparsers = parser.add_subparsers(dest="command", help="Lệnh thực hiện")
    
    # Upload command
    upload_parser = subparsers.add_parser("upload", help="Tạo torrent và upload file")
    upload_parser.add_argument("file_path", help="Đường dẫn đến file cần upload")
    upload_parser.add_argument("--name", help="Tên cho torrent (mặc định: tên file)")
    
    # Download command
    download_parser = subparsers.add_parser("download", help="Tải xuống torrent")
    download_parser.add_argument("info_hash", help="Info hash của torrent cần tải")
    
    # List command
    list_parser = subparsers.add_parser("list", help="Liệt kê tất cả torrent")
    
    # Info command
    info_parser = subparsers.add_parser("info", help="Hiển thị thông tin chi tiết của torrent")
    info_parser.add_argument("info_hash", help="Info hash của torrent")
    
    # Stop command
    stop_parser = subparsers.add_parser("stop", help="Dừng tải xuống torrent")
    stop_parser.add_argument("info_hash", help="Info hash của torrent cần dừng")
    
    # Resume command
    resume_parser = subparsers.add_parser("resume", help="Tiếp tục tải xuống torrent đã dừng")
    resume_parser.add_argument("info_hash", help="Info hash của torrent cần tiếp tục")
    
    args = parser.parse_args()
    
    if args.command == "upload":
        upload_command(args)
    elif args.command == "download":
        download_command(args)
    elif args.command == "list":
        list_command(args)
    elif args.command == "info":
        info_command(args)
    elif args.command == "stop":
        stop_command(args)
    elif args.command == "resume":
        resume_command(args)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()