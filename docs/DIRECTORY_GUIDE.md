# Cấu trúc thư mục dự án Data Lakehouse

**Đề tài:** Xây dựng Data Lakehouse phân tích nguồn nhân lực và thị trường lao động tại Việt Nam.

**Cập nhật:** 26/09/2026.

Tài liệu giải thích vai trò các thư mục trong dự án.

## 1. Tổng quan

| Đường dẫn | Vai trò chính |
| --- | --- |
| `configs/` | Cấu hình schema và ánh xạ dữ liệu |
| `dags/` | Điều phối pipeline bằng Airflow |
| `data/` | Lưu các tệp dữ liệu nguồn và dữ liệu mẫu |
| `docker/` | Dockerfile, cấu hình và script khởi tạo dịch vụ |
| `docs/` | Tài liệu hướng dẫn dự án |
| `jobs/` | Mã nguồn thu thập, làm sạch, chuẩn hóa và phân tích |
| `scripts/` | Công cụ thiết lập, kiểm tra và vận hành môi trường |
| `sql/` | Các câu lệnh SQL tạo bảng, view và kiểm tra dữ liệu |
| `docker-compose.yml` | Khai báo các dịch vụ Docker và cách chúng kết nối |
| `README.md` | Giới thiệu dự án và hướng dẫn bắt đầu nhanh |

## 2. `configs/` — Cấu hình dữ liệu

### `configs/mappings/`

Chứa các bảng ánh xạ giúp đưa giá trị từ nhiều nguồn về một danh mục thống nhất.

### `configs/schemas/`

Chứa mô tả cấu trúc dữ liệu: tên cột, kiểu dữ liệu, trường bắt buộc và các quy tắc mà chương trình sử dụng để kiểm tra.

## 3. `dags/` — Điều phối bằng Airflow

Chứa các file Python khai báo DAG. DAG xác định lịch chạy, task, thứ tự phụ thuộc, điều kiện chạy lại và retry.


## 4. `data/` — Dữ liệu trên máy

Đây là nơi đặt tệp nguồn để chương trình đọc và đưa vào Lakehouse, cùng các tệp mẫu phục vụ thử nghiệm.

| Thư mục | Nội dung | Ví dụ |
| --- | --- | --- |
| `data/education/` | Dữ liệu đại học, ngành/chương trình, tuyển sinh | `admission_2025.csv` |
| `data/jobs/` | Dữ liệu tin tuyển dụng đã thu thập | `topcv_jobs_2026-09-25.json` |
| `data/labor/` | Dữ liệu thống kê lao động, kinh tế và doanh nghiệp của đề tài | CSV về việc làm, GDP/GRDP, thu nhập, số doanh nghiệp |
| `data/sample/` | Tập dữ liệu nhỏ hoặc giả lập để thử pipeline | `job_postings.json` cho demo |


## 5. `docker/` — Đóng gói và khởi tạo dịch vụ

| Thư mục | Tác dụng |
| --- | --- |
| `docker/airflow/` | Cấu hình image và khởi tạo Airflow; bộ mẫu tích hợp Java, PySpark, Delta Lake và S3A |
| `docker/postgres/` | Script khởi tạo database, role và schema PostgreSQL |
| `docker/superset/` | Image, cấu hình và script khởi tạo Superset |


## 6. `docs/` — Tài liệu hướng dẫn

| File hiện có | Nội dung |
| --- | --- |
| `DIRECTORY_GUIDE.md` | Giải thích vai trò các thư mục và quy tắc đặt file |
| `TROUBLESHOOTING.md` | Lỗi thường gặp, cách kiểm tra và xử lý |
| `VALIDATION.md` | Những kiểm tra đã thực hiện, kết quả và giới hạn xác nhận |

## 7. `jobs/` — Mã nguồn nghiệp vụ

### Vai trò từng thư mục

| Đường dẫn hiện có | Nhiệm vụ | Ví dụ nội dung |
| --- | --- | --- |
| `jobs/crawlers/` | Thu thập dữ liệu website/API | Chương trình thu thập TopCV, CareerViet |
| `jobs/bronze/` | Nạp dữ liệu vào Bronze, giữ khả năng truy vết | Đọc CSV/JSON từ `data/`, ghi nguồn và thời điểm thu thập |
| `jobs/silver/` | Làm sạch, loại trùng và chuẩn hóa | Chuẩn hóa lương, kỹ năng, địa phương, tuyển sinh và chỉ tiêu thống kê |
| `jobs/gold/` | Xây dựng mô hình phân tích và chỉ số | Tạo dimension, fact, tổng hợp nhu cầu theo tháng/năm |
| `jobs/dq/` | Kiểm tra chất lượng từng tầng | Kiểm tra thiếu cột, trùng khóa, sai kiểu, sai tổng |
| `jobs/common/` | Tập trung hàm dùng chung | Tạo Spark session, đọc config, kết nối, ghi log |
| `jobs/demo_pipeline.py` | Chương trình thử pipeline | Kiểm tra luồng xử lý bằng dữ liệu mẫu |

### `jobs/bronze/`

Đưa dữ liệu nguồn vào vùng Bronze và lưu thông tin truy vết. Có thể lưu định dạng nguồn hoặc bảng tiếp nhận tùy thiết kế. Ưu tiên giữ thông tin gốc để xử lý lại; các quy tắc chuẩn hóa nghiệp vụ chủ yếu thực hiện ở Silver.

### `jobs/silver/`

Chuẩn hóa tên cột, kiểu dữ liệu, thời gian, đơn vị tính và danh mục. Áp dụng các bảng trong `configs/mappings/` và schema trong `configs/schemas/` khi code đã hỗ trợ.

Đối với dữ liệu kinh tế–lao động, giữ đúng tần suất nguồn: dữ liệu năm không tự biến thành dữ liệu quý. Giá trị thiếu và số 0 cần được phân biệt.

### `jobs/gold/`

Tổ chức dữ liệu phục vụ phân tích theo các bảng đã thống nhất:

| Nhóm | Các bảng theo thiết kế đề tài |
| --- | --- |
| Dimension | `dim_date`, `dim_location`, `dim_company`, `dim_occupation`, `dim_industry`, `dim_major`, `dim_skill`, `dim_university`, `dim_economic_indicator` |
| Fact | `fact_job_posting`, `fact_job_skill`, `fact_admission`, `fact_economic_indicator`, `fact_labour_market`, `fact_enterprise` |
| Tổng hợp | Nhu cầu tuyển dụng, kỹ năng, tuyển sinh và chỉ tiêu kinh tế–lao động theo kỳ |

### `jobs/dq/` và `jobs/common/`

`dq` là viết tắt của Data Quality. Các hàm kiểm tra cần được gọi ở đúng bước trong pipeline; chỉ có thư mục không khiến kiểm tra tự chạy.

`common` giúp các job dùng chung cấu hình kết nối và hàm tiện ích. Khi tổ chức module Python, thống nhất cách import và `PYTHONPATH`; có thể dùng `__init__.py` để tổ chức package thông thường.

## 8. `scripts/` — Công cụ vận hành

Chứa các chương trình phục vụ thiết lập và quản lý môi trường.

## 9. `sql/` — Câu lệnh tạo bảng và truy vấn

Chứa SQL phục vụ tạo cấu trúc bảng, view và kiểm tra dữ liệu. Các file có thể đặt:

## 10. Các file ở thư mục gốc

| File | Tác dụng |
| --- | --- |
| `.dockerignore` | Loại các file không cần đưa vào Docker build context |
| `.env` | Cấu hình thực trên máy, gồm các giá trị bí mật nếu sử dụng |
| `.env.example` | Mẫu các biến cần thiết để thành viên khác thiết lập |
| `.gitattributes` | Quy ước xử lý file văn bản và kiểu xuống dòng |
| `.gitignore` | Quy định file Git bỏ qua; không tự bỏ theo dõi file đã commit |
| `docker-compose.yml` | Khai báo dịch vụ, port, volume, network, môi trường và dependency |
| `README.md` | Giới thiệu đề tài, công nghệ và hướng dẫn chạy nhanh |
