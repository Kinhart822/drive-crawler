# 🚀 G-Drive Direct Sync Pro

Giải pháp đồng bộ hóa G-Drive cao cấp, hỗ trợ copy trực tiếp server-to-server.

## ✨ Tính năng chính
- **Deep Sync & Auto-Heal**: Chỉ copy file thiếu, tự động quét lại lần 2 nếu lỗi.
- **Lọc ID thông minh**: Chọn ID cụ thể để copy (ví dụ: `1-10, 15, 20-30`).
- **Dọn dẹp Excel**: Tự xóa link chết khỏi file `data.xlsx` sau khi hoàn tất.
- **Tốc độ cao**: Sử dụng đa luồng (Multi-threading).

## 🚀 Cách chạy nhanh
1. **Cài đặt**: `pip install -r requirements.txt`
2. **Cấu hình**: Chỉnh sửa file `.env` (dựa trên `.env.example`).
3. **Thực thi**:
   ```powershell
   python drive_sync.py
   ```

## ⚙️ Cấu hình .env
```env
GOOGLE_CLIENT_SECRET_FILE=gdrive_credentials.json
GOOGLE_TOKEN_FILE=token.json
GOOGLE_SCOPES=https://www.googleapis.com/auth/drive
EXCEL_DATA_FILE=data.xlsx
ERROR_LOG_FILE=sync_errors.log
```

---
*Lưu ý: Đảm bảo tài khoản chạy script đã được thêm vào danh sách **Test Users** trong Google Cloud Console nếu dự án của bạn ở trạng thái Testing.*
