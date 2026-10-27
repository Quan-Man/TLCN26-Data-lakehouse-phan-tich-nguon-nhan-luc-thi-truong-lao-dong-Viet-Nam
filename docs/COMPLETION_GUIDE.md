# Hướng dẫn tích hợp bổ sung (25/10/2026)

## Phạm vi
- Giữ nguyên các DAG, jobs và dữ liệu hiện có.
- Sửa `00_master_pipeline.py` để gọi đúng `dag_id` Silver, Gold, DQ.
- Thêm `hive-db-init` (idempotent) để tạo PostgreSQL database `hive_metastore` kể cả khi volume PostgreSQL đã tồn tại.
- Thêm `hive-metastore` và `trino` vào Docker Compose; catalog `delta` trỏ đến MinIO và Hive Metastore.
- Không kèm `.env` hoặc thư mục `.git` để tránh chia sẻ bí mật và lịch sử Git.

## Chạy
1. Sao lưu project và volumes, thay các mật khẩu/secret đã từng chia sẻ; chạy `python scripts/setup_env.py` nếu cần tạo `.env` mới.
2. Chạy `docker compose config` để kiểm tra YAML và biến môi trường.
3. Chạy `docker compose build` rồi `docker compose up -d postgres minio minio-init hive-db-init`.
4. **Khởi tạo schema Hive Metastore trước khi chạy dịch vụ**. Tùy phiên bản image, chạy `schematool -dbType postgres -initSchema` trong container Hive có cùng `SERVICE_OPTS`/JDBC config. Không khởi tạo lại nếu schema đã tồn tại. Xem log Hive để kiểm tra.
5. Chạy `docker compose up -d hive-metastore trino airflow-webserver airflow-scheduler superset`.
6. Kiểm tra `docker compose ps`, log các dịch vụ, và từ Trino `SHOW CATALOGS;` / `SHOW SCHEMAS FROM delta;`.
7. Đăng ký các bảng Delta vào Hive Metastore nếu các Spark jobs hiện tại chỉ ghi trực tiếp vào đường dẫn S3A. Trino không tự liệt kê bảng chỉ vì file Delta tồn tại trong MinIO.
8. Cấu hình Superset kết nối Trino (cần cài driver Trino trong image Superset nếu chưa có), sau đó tạo datasets/dashboard.

## Giới hạn đã biết
- Đây là bản **tích hợp source code**; chưa chạy Docker engine hoặc kiểm thử end-to-end trên máy người dùng.
- Spark jobs hiện tại có thể cần cấu hình Hive catalog để tạo/đăng ký bảng; không thể khẳng định chỉ thêm Compose là chạy hết.
- Cần xác minh tương thích phiên bản Trino/Delta Lake/Hive trong môi trường thực tế, quyền S3, JAR Hadoop và đường dẫn bảng.
- ML không có trong sơ đồ kiến trúc gốc và chưa được bổ sung như một mô hình dự báo được huấn luyện/đánh giá; không nên tuyên bố đã có dự báo 2027–2030.
