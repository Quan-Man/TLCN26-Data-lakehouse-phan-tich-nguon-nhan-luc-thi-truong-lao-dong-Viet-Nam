# Test pipeline đơn giản

Đọc CSV tuyển sinh, JSON tuyển dụng và XLSX lao động → Bronze gốc → Silver đã làm sạch → Gold tổng hợp. Chạy một lệnh Docker; chưa cần bật Airflow DAG.

Bản này dùng Python + pandas + PyArrow + MinIO. Silver/Gold là **Parquet thông thường**, chưa phải bảng Delta Lake, chưa có transaction log hoặc time travel. Đây là bước chạy ETL tối giản; chưa triển khai toàn bộ star schema 9 dimensions/6 facts hay đưa Gold vào PostgreSQL/Superset.

## 1. Build Docker

Build và khởi động toàn bộ các service:

```bash
docker compose up -d --build
```

Kiểm tra trạng thái các service:

```bash
docker compose ps
```

Đảm bảo các container chính như `26tlcn-lakehouse-airflow-scheduler`, `26tlcn-lakehouse-airflow-webserver`, `26tlcn-lakehouse-minio`, `26tlcn-lakehouse-postgres`,`26tlcn-lakehouse-superset` đang ở trạng thái `Up`.

> Các container `*-init` không cần phải ở trạng thái `Up` sau khi quá trình khởi tạo hoàn tất.

---

## 2. Chạy kiểm thử Pipeline

Chạy pipeline kiểm thử:

```bash
docker compose exec airflow-scheduler python /opt/airflow/jobs/simple_pipeline.py --data-dir /opt/airflow/data --output-dir /opt/airflow/logs/simple_pipeline   
```

Sau khi pipeline chạy xong, kiểm tra kết quả:

```bash
docker compose exec airflow-scheduler python /opt/airflow/jobs/verify_simple_pipeline.py --run-dir /opt/airflow/logs/simple_pipeline/20261006T043348Z_0ffa2491
```

> Thay `20261006T043348Z_0ffa2491` bằng `run_id` được sinh ra từ lần chạy pipeline của bạn.

Nếu kết quả hiển thị tương tự như sau thì pipeline đã chạy thành công:

```text
PASS 20261006T043348Z_0ffa2491; local files verified; minio_verified_at_run=True
```

---

## 3. Kiểm tra Airflow DAG

### 3.1. Kiểm tra Airflow có nhận DAG hay không

Liệt kê các DAG mà Airflow đã load:

```bash
docker compose exec airflow-scheduler airflow dags list
```

Nếu muốn tìm DAG cụ thể:

```bash
docker compose exec airflow-scheduler airflow dags list | grep raw_bronze_silver_pipeline
```

Nếu xuất hiện:

```text
raw_bronze_silver_pipeline
```

thì Airflow đã nhận DAG thành công.

---

### 3.2. Kiểm tra lỗi import DAG

Chạy:

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
```

Nếu DAG không có lỗi, phần import error sẽ không xuất hiện lỗi liên quan đến DAG vừa tạo.

Có thể kiểm tra thêm file DAG trực tiếp bằng Python:

```bash
docker compose exec airflow-scheduler python /opt/airflow/dags/raw_to_bronze_to_silver.py
```

Nếu lệnh kết thúc mà không có exception thì file DAG không có lỗi Python cơ bản.

---

### 3.3. Test DAG bằng Airflow CLI

Chạy toàn bộ DAG ở chế độ test:

```bash
docker compose exec airflow-scheduler airflow dags test raw_bronze_silver_pipeline 2026-10-06
```

Lệnh này sẽ chạy các task theo thứ tự:

```text
load_raw_to_bronze
        ↓
etl_bronze_to_silver
```

Trong đó:

- `load_raw_to_bronze`: đọc dữ liệu thô và ghi vào Bronze.
- `etl_bronze_to_silver`: đọc dữ liệu Bronze, ETL và thêm cột số thứ tự trước khi ghi vào Silver.

Nếu cuối log không xuất hiện `FAILED`, `Exception` hoặc `Traceback` và các task hoàn thành thành công thì DAG hoạt động đúng.

---

### 3.4. Test từng task riêng lẻ

Có thể test task Bronze trước:

```bash
docker compose exec airflow-scheduler airflow tasks test \
  raw_bronze_silver_pipeline \
  load_raw_to_bronze \
  2026-10-06
```

Sau đó test task Silver:

```bash
docker compose exec airflow-scheduler airflow tasks test \
  raw_bronze_silver_pipeline \
  etl_bronze_to_silver \
  2026-10-06
```

Nếu cả hai task đều chạy thành công thì pipeline:

```text
RAW → BRONZE → SILVER
```

đã hoạt động đúng.

---

## 4. Trigger DAG trên Airflow

Sau khi test thành công, có thể trigger DAG trực tiếp bằng CLI:

```bash
docker compose exec airflow-scheduler airflow dags trigger raw_bronze_silver_pipeline
```

Kiểm tra các lần chạy:

```bash
docker compose exec airflow-scheduler airflow dags list-runs \
  -d raw_bronze_silver_pipeline
```

Hoặc truy cập Airflow Web UI, bật DAG `raw_bronze_silver_pipeline` và chọn **Trigger DAG**.

Pipeline hoàn chỉnh:

```text
Raw Data
   │
   ▼
load_raw_to_bronze
   │
   ▼
Bronze Layer
   │
   ▼
etl_bronze_to_silver
   │
   ▼
Silver Layer
```