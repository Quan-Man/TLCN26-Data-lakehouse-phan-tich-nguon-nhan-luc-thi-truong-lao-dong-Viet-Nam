# Docker cho Data Lakehouse nguồn nhân lực Việt Nam

Bộ môi trường phát triển cho đề tài **Xây dựng Data Lakehouse phân tích nguồn nhân lực và thị trường lao động tại Việt Nam**. Gói này cung cấp Docker Compose, image tùy chỉnh, khởi tạo cơ sở dữ liệu, DAG Airflow và pipeline minh họa Bronze → Silver → Gold → PostgreSQL.

**Phạm vi:** môi trường học tập trên một máy. Spark chạy `local[2]` trong container thực thi job; Airflow dùng LocalExecutor. Chưa triển khai cụm Spark master/worker, crawler thực tế, module SBERT, toàn bộ bảng fact/dim hoặc dashboard nghiệp vụ. Pipeline kèm theo chỉ dùng dữ liệu giả lập để kiểm tra kết nối.

**Trạng thái kiểm tra:** đã kiểm tra tĩnh cấu hình YAML, mã Python, shell và dữ liệu mẫu. Môi trường tạo gói không có Docker nên chưa thực hiện `docker compose build`, khởi động container hay kiểm thử tích hợp. Xem `docs/VALIDATION.md` và chạy `python scripts/verify.py` trên máy của bạn để xác nhận.

## 1. Các thành phần

| Thành phần | Phiên bản được ghim | Vai trò |
| --- | --- | --- |
| PostgreSQL | 16.8 | Ba database riêng: `airflow`, `superset`, `lakehouse`; tài khoản riêng |
| MinIO | RELEASE.2025-04-22T22-12-26Z | Object storage, ba bucket `bronze`, `silver`, `gold` |
| MinIO Client | RELEASE.2025-04-16T18-13-26Z | Chờ MinIO sẵn sàng và tạo bucket |
| Apache Airflow | 2.10.5, Python 3.11 | Webserver, scheduler, DAG và LocalExecutor |
| Apache Spark / PySpark | 3.5.5 | Xử lý dữ liệu với hai luồng local |
| Delta Lake | 3.3.0 | Đọc/ghi bảng Delta trên MinIO |
| Hadoop S3A | 3.3.4 | Kết nối Spark tới MinIO, khớp Hadoop trong PySpark |
| AWS Java SDK bundle | 1.12.262 | Dependency của Hadoop S3A 3.3.4 |
| Java | 17 | JVM cho Spark |
| Apache Superset | 4.1.2 | Khám phá dữ liệu và tạo dashboard |

Đây là bộ phiên bản cố định phục vụ bài thực hành, không phải cam kết sử dụng bản mới nhất. Không nâng riêng Spark, Delta Lake hoặc Hadoop S3A mà chưa kiểm tra tính tương thích. Delta Lake là thư viện/lớp quản lý bảng, không cần một container server riêng.

## 2. Chuẩn bị trên Windows / WSL2

1. Cài Docker Desktop, sử dụng Linux containers và WSL2 backend.
2. Cài Python 3.10 trở lên trên máy để chạy các script thiết lập; không cần cài Spark, Java hay Airflow trên Windows.
3. Mở Docker Desktop và chờ Docker Engine sẵn sàng.
4. Giải nén gói và mở terminal ngay trong thư mục `lakehouse-docker` chứa `docker-compose.yml`.

Khuyến nghị cho toàn bộ stack: cấp khoảng **8–12 GB RAM, 4 CPU và 15–20 GB dung lượng trống** cho Docker. Đây là ước lượng cho môi trường demo, cần điều chỉnh theo dữ liệu. Lần build đầu cần Internet để tải image, package Python, Java và JAR từ Maven Central.

Kiểm tra:

```bash
docker version
docker compose version
python --version
```

Dùng Docker Compose V2 (`docker compose`). Nếu chạy trong WSL và chỉ có `python3`, thay `python` bằng `python3` trong các lệnh dưới đây. Không cần chạy `sudo` cho script Python nếu tài khoản đã có quyền Docker phù hợp.

## 3. Khởi chạy

```bash
python scripts/setup_env.py
docker compose config --quiet
docker compose up -d --build
docker compose ps -a
```

`setup_env.py` tự sinh `.env` với mật khẩu ngẫu nhiên và khóa Fernet hợp lệ, không in mật khẩu ra terminal và không ghi đè nếu `.env` đã tồn tại. Mở `.env` bằng trình soạn thảo để xem thông tin đăng nhập. Không cần sao chép `.env.example` trước.

Compose sẽ:

1. Khởi động PostgreSQL, tạo database và các tài khoản.
2. Khởi động MinIO, chờ kết nối được rồi tạo ba bucket.
3. Chạy migration và tạo tài khoản Airflow.
4. Chạy migration, tạo tài khoản Superset và kết nối phân tích `Vietnam Lakehouse`.
5. Khởi động Airflow webserver, scheduler và Superset.

Các container `airflow-init`, `minio-init`, `superset-init` kết thúc với **Exited (0)** là bình thường. Các dịch vụ dài hạn cần ở trạng thái running/healthy. Nếu bước init lỗi, xem log trước khi tiếp tục.

## 4. Truy cập dịch vụ

| Dịch vụ | Địa chỉ trên máy | Đăng nhập |
| --- | --- | --- |
| Airflow | http://localhost:8080 | `AIRFLOW_ADMIN_USERNAME` / `AIRFLOW_ADMIN_PASSWORD` trong `.env` |
| MinIO Console | http://localhost:9001 | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` |
| MinIO S3 API | http://localhost:9000 | Endpoint cho client S3; không phải trang dashboard |
| Superset | http://localhost:8088 | `SUPERSET_ADMIN_USERNAME` / `SUPERSET_ADMIN_PASSWORD` |
| PostgreSQL | `localhost:5433` | Database `lakehouse`, user `lakehouse`, `LAKEHOUSE_DB_PASSWORD` |

Cổng mặc định PostgreSQL trên máy là `5433` để giảm khả năng trùng dịch vụ đã cài. Các cổng công bố chỉ bind `127.0.0.1`, dùng cho truy cập từ máy cá nhân.

**Từ bên trong container**, sử dụng tên dịch vụ: `postgres:5432` và `http://minio:9000`; không dùng `localhost:5433` hoặc `localhost:9000` để gọi container khác. Thay cổng trong `.env` chỉ thay cổng truy cập từ máy, không thay cổng nội bộ.

## 5. Chạy thử pipeline

Sau khi stack đã khởi động, chạy:

```bash
docker compose --profile tools run --rm spark-job
```

Job thực hiện:

- Lưu 5 dòng JSON giả lập vào `bronze/demo/job_postings`.
- Đọc lại Bronze, loại một bản ghi trùng và một bản ghi thiếu địa phương.
- Ghi 3 tin hợp lệ thành Delta tại `silver/demo/job_postings`.
- Tổng hợp và ghi Delta tại `gold/demo/job_counts`.
- Nạp kết quả vào bảng PostgreSQL `demo.job_counts` trong một transaction.
- Đọc lại kết quả để kiểm tra.

Kết quả mong đợi:

| location | posting_count |
| --- | ---: |
| Ha Noi | 1 |
| Ho Chi Minh | 2 |

Cuối log cần xuất hiện:

```text
SMOKE TEST PASSED: Bronze=5, Silver=3, Gold=2 locations, PostgreSQL=2 rows
```

Trong MinIO, thư mục bảng Delta phải có `_delta_log`. Demo sử dụng chế độ ghi đè snapshot trong các đường dẫn `demo/`; chạy lại không làm tăng số tin. Không chạy job trực tiếp và DAG cùng lúc: môi trường này chỉ thiết kế cho một Spark writer tại một thời điểm.

Để kiểm tra Compose và chạy demo hai lần:

```bash
python scripts/verify.py
```

## 6. Chạy bằng Airflow

1. Mở Airflow tại http://localhost:8080 và đăng nhập.
2. Tìm DAG `lakehouse_demo`.
3. Bật DAG và chọn **Trigger DAG**.
4. Mở task `bronze_silver_gold_postgres` để xem log và kết quả.

DAG có `schedule=None` nên chỉ chạy khi kích hoạt thủ công. `catchup=False` tránh tạo các lần chạy lịch sử. `max_active_runs=1` giữ một lần chạy DAG tại một thời điểm. Pipeline thử nghiệm được gói trong một task để việc cài đặt ban đầu dễ theo dõi; có thể tách thành nhiều task khi phát triển ETL thực tế.

Lệnh kiểm tra DAG:

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec airflow-scheduler airflow dags list
```

## 7. Xem dữ liệu trong Superset

Kết nối **Vietnam Lakehouse** được tạo sẵn khi init; dùng tài khoản PostgreSQL `bi_reader` chỉ có quyền đọc schema `demo` và `gold`.

Sau khi job thành công, mở SQL Lab, chọn kết nối đó và chạy:

```sql
SELECT location, posting_count
FROM demo.job_counts
ORDER BY posting_count DESC;
```

Để tạo biểu đồ, thêm dataset với database `Vietnam Lakehouse`, schema `demo`, bảng `job_counts`; chọn Bar Chart, dimension `location`, metric `SUM(posting_count)`, rồi lưu vào dashboard. Gói chưa chứa dashboard dựng sẵn.

Nếu kết nối ngoài Docker bằng DBeaver hoặc pgAdmin, dùng `localhost:5433`, database `lakehouse`; chọn `bi_reader` với `BI_DB_PASSWORD` để chỉ đọc, hoặc `lakehouse` để phát triển bảng dữ liệu.

## 8. Các file chính

| File / thư mục | Mục đích |
| --- | --- |
| `docker-compose.yml` | Dịch vụ, phụ thuộc, healthcheck, port và volume |
| `.env.example` | Tên biến cấu hình mẫu |
| `scripts/setup_env.py` | Tạo `.env` một lần bằng Python chuẩn |
| `scripts/verify.py` | Kiểm tra trên máy có Docker, chạy demo hai lần |
| `docker/airflow/Dockerfile` | Image Airflow + Java + Spark + Delta + S3A |
| `docker/airflow/download_jars.py` | Tải và kiểm tra checksum JAR khi build |
| `docker/airflow/bootstrap.sh` | Migration và tạo admin Airflow nếu chưa có |
| `docker/postgres/init.sh` | Tạo database, role, schema và bảng demo lần đầu |
| `docker/superset/` | Image, cấu hình và khởi tạo Superset |
| `dags/lakehouse_demo.py` | DAG chạy thử thủ công |
| `jobs/demo_pipeline.py` | Code Spark → Delta trên MinIO → PostgreSQL |
| `sample_data/job_postings.json` | Dữ liệu giả lập, định dạng JSON Lines |
| `docs/VALIDATION.md` | Phạm vi kiểm tra và phần cần xác nhận trên máy |

## 9. Gắn dữ liệu của đề tài

- Thêm chương trình thu thập vào dự án và lưu dữ liệu nguồn theo nguồn/năm/thời điểm thu thập tại Bronze.
- Tạo Spark job mới trong `jobs/`, tái sử dụng cấu hình kết nối của demo.
- Thiết kế schema Silver và Gold theo các fact/dim đã chốt trong README đề tài.
- Tạo bảng phân tích trong schema `gold` bằng tài khoản `lakehouse`; bảng mới tự được cấp quyền SELECT cho `bi_reader` nhờ default privileges.
- Thêm DAG mới trong `dags/` để điều phối job.

Thư mục `dags/`, `jobs/`, `sample_data/` được bind mount: sửa mã thì container thấy thay đổi, không cần rebuild. Thay Dockerfile hoặc dependency thì cần `docker compose build` và tạo lại container.

Module SBERT/HDBSCAN và giao diện kiểm duyệt sẽ cần image riêng với dependency ML. Chưa cài chúng trong gói này vì chưa có code module và chúng làm tăng đáng kể kích thước image.

## 10. Dừng và chạy lại

```bash
# Dung container, giu nguyen container va du lieu.
docker compose stop

# Chay lai cac container da tao.
docker compose start

# Xoa container/network, van giu named volumes.
docker compose down

# Tao lai container va su dung du lieu cu.
docker compose up -d
```

Giữ nguyên `.env` và tên project khi dùng lại volume. PostgreSQL chỉ chạy `init.sh` khi volume database còn trống; sửa mật khẩu trong `.env` không tự thay mật khẩu đã lưu trong database. Script tạo tài khoản giao diện cũng không tự đổi mật khẩu tài khoản đã tồn tại. Không thay khóa Fernet/SECRET_KEY tùy tiện khi metadata đã có dữ liệu.

`docker compose down -v` xóa cả named volumes, bao gồm PostgreSQL và MinIO. Chỉ dùng khi chủ động muốn xóa toàn bộ dữ liệu thực hành; đây không phải lệnh dừng thông thường.

## 11. Xử lý lỗi thường gặp

| Hiện tượng | Cách kiểm tra |
| --- | --- |
| Không kết nối Docker daemon | Mở Docker Desktop, kiểm tra `docker version` |
| Báo thiếu biến môi trường | Chạy `python scripts/setup_env.py` trong thư mục gói; nếu đã copy `.env.example`, thay tất cả giá trị mẫu bằng cấu hình hợp lệ |
| `port is already allocated` | Đổi cổng tương ứng trong `.env`, chạy lại `docker compose up -d` |
| Build không tải được image/package/JAR | Kiểm tra Internet/proxy tới Docker Hub, PyPI, Debian và Maven Central; xem log build để xác định nguồn lỗi |
| `Exited (1)` ở init | Xem log dịch vụ init và PostgreSQL; không bỏ qua migration thất bại |
| Airflow/Spark bị kill, mã 137 | Kiểm tra RAM được cấp cho Docker, giảm ứng dụng đang chạy |
| `ClassNotFoundException` liên quan Delta/S3A | Rebuild image; kiểm tra các JAR đã tải thành công và giữ nguyên bộ phiên bản |
| `password authentication failed` sau khi sửa `.env` | Mật khẩu trong volume cũ chưa đổi; dùng cấu hình cũ hoặc đổi mật khẩu bằng SQL có kiểm soát |
| Superset chưa thấy bảng demo | Chạy job, xác nhận kết quả thành công, chọn schema `demo` |
| Shell báo `$'\r'` / `bad interpreter` | Chuyển file `.sh` về LF; gói và `.gitattributes` đã cấu hình LF |

Xem log:

```bash
docker compose logs --tail=100 airflow-init superset-init minio-init
docker compose logs --tail=100 postgres airflow-scheduler
docker compose logs --tail=100 superset airflow-webserver
```

## 12. Tài liệu đối chiếu

- [Airflow 2.10.5 chạy bằng Docker](https://airflow.apache.org/docs/apache-airflow/2.10.5/howto/docker-compose/index.html).
- [Bảng tương thích Delta Lake và Spark](https://docs.delta.io/releases/).
- [Hadoop AWS / S3A 3.3.4](https://hadoop.apache.org/docs/r3.3.4/hadoop-aws/tools/hadoop-aws/index.html).
- [Superset Docker builds](https://superset.apache.org/admin-docs/installation/docker-builds/).
- [Các bản phát hành MinIO](https://github.com/minio/minio/releases).

Bộ cấu hình này là nền tảng để phát triển đề tài trên máy cá nhân. Trước khi triển khai chia sẻ hoặc production, cần thiết kế lại tài khoản dịch vụ, quản lý bí mật, sao lưu và vận hành phù hợp.
