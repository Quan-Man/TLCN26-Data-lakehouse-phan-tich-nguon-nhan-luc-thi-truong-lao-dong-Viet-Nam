# Phạm vi kiểm tra

Ngày chuẩn bị gói: 2026-09-22.

## Đã kiểm tra trong môi trường tạo gói

| Kiểm tra | Kết quả |
| --- | --- |
| Parse 7 file Python bằng AST | Đạt |
| Kiểm tra 3 shell script bằng `bash -n`, không có CRLF | Đạt |
| Parse YAML, merge anchor, danh sách 9 dịch vụ | Đạt |
| Phụ thuộc dịch vụ không tự tham chiếu, không có chu trình | Đạt |
| File bind mount và Dockerfile tồn tại | Đạt |
| Biến Compose có trong `.env.example` | Đạt |
| Tạo `.env` thử trong thư mục tạm; khóa Fernet giải mã được 32 byte | Đạt |
| 8 mật khẩu riêng biệt, chạy lại setup không thay đổi file | Đạt |
| Dữ liệu mẫu: 5 dòng, 3 job hợp lệ duy nhất, tổng hợp 2 địa phương | Đạt bằng Python chuẩn |

Kiểm tra mẫu bằng Python chuẩn chỉ xác nhận kết quả mong đợi, không thay thế việc chạy Spark thực tế.

## Chưa kiểm tra do không có Docker Engine / CLI

- Compose schema/interpolation bằng `docker compose config` chính thức.
- Pull tất cả image tag trên registry và build hai image tùy chỉnh.
- Cài các dependency trong image, tải và kiểm tra checksum Maven.
- Migration Airflow/Superset và hành vi khởi tạo tài khoản khi chạy lại.
- Kết nối Spark S3A, ghi/đọc bảng Delta, ghi PostgreSQL và healthcheck.
- Import DAG bằng Airflow và thao tác dashboard trên trình duyệt.

## Xác nhận trên máy có Docker

Trong thư mục chứa `docker-compose.yml`:

```bash
python scripts/setup_env.py
docker compose config --quiet
docker compose up -d --build
docker compose ps -a
python scripts/verify.py
```

Chờ các dịch vụ dài hạn healthy trước khi chạy verify. Kỳ vọng ba dịch vụ init kết thúc code 0; mỗi lần demo in `SMOKE TEST PASSED`; Airflow báo không có lỗi import DAG. Verify chạy hai lần tuần tự để kiểm tra việc chạy lại demo không tích lũy bản ghi trùng. Không chạy đồng thời với DAG.

Tiếp theo, kích hoạt DAG `lakehouse_demo` từ giao diện Airflow; kiểm tra task thành công. Trên Superset, chạy `SELECT * FROM demo.job_counts` và xác nhận hai dòng kết quả như README.

Chưa được coi là kiểm thử tích hợp đạt nếu chỉ có kiểm tra YAML/Python thành công. Gói hiện là cấu hình đã rà soát tĩnh, cần xác nhận runtime trên máy triển khai.
