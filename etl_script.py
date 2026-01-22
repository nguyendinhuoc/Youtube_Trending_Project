import pandas as pd
from sqlalchemy import create_engine
import time

# --- CẤU HÌNH KẾT NỐI ---
# Lưu ý: 'localhost' chạy ở ngoài, nhưng nếu chạy script này trong docker network thì host là 'db'
# Vì bạn chạy script này trên Codespaces (máy host) nên dùng 'localhost'
db_connection_str = 'postgresql://user123:password123@localhost:5432/youtube_db'
db_connection = create_engine(db_connection_str)

file_path = 'data/trending_yt_videos_113_countries.csv'
chunk_size = 50000  # 50k dòng một lần

print("🚀 Bắt đầu quá trình nạp dữ liệu vào PostgreSQL...")

try:
    # Đọc file với engine python để tránh lỗi EOF/Encoding
    csv_reader = pd.read_csv(
        file_path,
        chunksize=chunk_size,
        engine='python',
        encoding='utf-8',
        encoding_errors='replace',
        on_bad_lines='skip'
    )

    total_rows = 0
    start_time = time.time()

    for i, chunk in enumerate(csv_reader):
        # 1. Lọc lấy VN và US (hoặc lấy hết nếu muốn)
        # Ở đây mình lọc luôn để DB đỡ rác, chỉ lưu cái cần thiết
        chunk_filtered = chunk[chunk['country'].isin(['VN', 'US'])]
        
        if not chunk_filtered.empty:
            # 2. Đẩy vào bảng 'youtube_trending'
            # if_exists='append': Nối tiếp vào bảng
            # index=False: Không lưu cột số thứ tự index
            chunk_filtered.to_sql('youtube_trending', db_connection, if_exists='append', index=False)
            
            total_rows += len(chunk_filtered)
            print(f"✅ Chunk {i}: Đã nạp thêm {len(chunk_filtered)} dòng (Tổng: {total_rows})")

    end_time = time.time()
    print(f"\n🎉 HOÀN TẤT! Đã nạp {total_rows} dòng vào bảng 'youtube_trending'.")
    print(f"⏱ Thời gian thực hiện: {round(end_time - start_time, 2)} giây")

except Exception as e:
    print(f"\n❌ Lỗi: {e}")