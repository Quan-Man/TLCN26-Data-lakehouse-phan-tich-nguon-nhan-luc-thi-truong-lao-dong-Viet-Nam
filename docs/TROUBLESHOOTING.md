# Lỗi thường gặp và cách xử lý

Tài liệu dành cho đề tài Data Lakehouse nguồn nhân lực và thị trường lao động Việt Nam.

## 1. Kiểm tra nhanh trước khi sửa

```bash
docker version
docker compose version
docker compose config --quiet
docker compose ps -a
docker compose logs --tail=100 postgres airflow-scheduler superset
```

- `docker version`: kiểm tra cả client và kết nối tới Docker Engine.
- `config --quiet`: kiểm tra cấu hình Compose, không in toàn bộ cấu hình có thể chứa mật khẩu.
- `ps -a`: xem cả container đang chạy và đã dừng.
- `logs`: tìm thông báo lỗi đầu tiên và traceback liên quan.

Không dùng xóa volume như bước sửa lỗi mặc định. `docker compose down -v` sẽ xóa named volumes của stack, bao gồm dữ liệu PostgreSQL và MinIO trong cấu hình này.

## 2. Docker và khởi động dịch vụ

| Biểu hiện | Nguyên nhân có thể | Cách xử lý |
| --- | --- | --- |
| `docker` không được nhận diện | Chưa cài Docker hoặc terminal chưa nhận PATH | Cài/mở Docker Desktop, mở lại terminal, chạy `docker version` |
| `Cannot connect to the Docker daemon` | Docker Engine chưa chạy hoặc Docker context không đúng | Chờ Docker Desktop sẵn sàng; kiểm tra `docker context ls` và context đang chọn |
| `no configuration file provided` | Chạy lệnh sai thư mục | Chuyển đến thư mục chứa `docker-compose.yml` |
| Báo biến môi trường bắt buộc chưa được đặt | Chưa có `.env` hoặc thiếu giá trị | Chạy `python scripts/setup_env.py`; nếu `.env` đã tồn tại, kiểm tra và bổ sung biến thiếu |
| `.env` vẫn chứa `REPLACE_ME` | Đã copy file mẫu nhưng chưa điền cấu hình | Điền giá trị hợp lệ; script setup không tự ghi đè `.env` có sẵn |
| `port is already allocated` | Cổng trên máy đang được dùng | Đổi biến cổng trong `.env`, sau đó chạy `docker compose up -d` |
| `manifest unknown` | Image tag không tồn tại/không còn được cung cấp trên registry đó | Kiểm tra chính xác tên image, tag và tài liệu nhà cung cấp; chọn bản tương thích có chủ đích |
| `no matching manifest` | Image không hỗ trợ kiến trúc máy | Kiểm tra kiến trúc và các nền tảng image hỗ trợ; không đổi tag tùy tiện |
| Lỗi tải image/package/JAR | Mạng, proxy, DNS hoặc nguồn tải không truy cập được | Xác định URL lỗi trong log build; kiểm tra kết nối tới nguồn đó |
| `Exited (0)` ở dịch vụ `*-init` | Tác vụ khởi tạo đã hoàn tất | Bình thường nếu bước init thực sự thành công; xem log để xác nhận |
| `Exited (1)` ở dịch vụ init | Migration, tạo tài khoản hoặc tạo bucket thất bại | Đọc log đúng dịch vụ init và sửa nguyên nhân trước khi chạy lại |
| Container `unhealthy` | Healthcheck thất bại, dịch vụ chưa sẵn sàng hoặc khởi tạo lỗi | Xem log; kiểm tra dependency, tài nguyên và endpoint healthcheck |
| Mã thoát `137` | Tiến trình bị SIGKILL, có thể do thiếu RAM | Kiểm tra trạng thái OOM và tài nguyên Docker; không kết luận chỉ từ mã 137 |

Xem riêng log khởi tạo:

```bash
docker compose logs --tail=150 airflow-init superset-init minio-init
```

Sau khi sửa Dockerfile/dependency:

```bash
docker compose build
docker compose up -d
```

`docker compose restart` không áp dụng thay đổi cấu hình Compose hoặc biến môi trường vào container đã tạo. Dùng `up -d` để Compose áp dụng thay đổi cần thiết.

Compose cần healthcheck cùng `service_healthy` nếu phải chờ dịch vụ sẵn sàng, hoặc `service_completed_successfully` nếu phải chờ tác vụ init kết thúc thành công. Chỉ khởi động container chưa bảo đảm ứng dụng đã nhận kết nối được. [Tài liệu Docker](https://docs.docker.com/compose/how-tos/startup-order/).

## 3. Airflow không thấy DAG hoặc task bị lỗi

### DAG không xuất hiện / Broken DAG

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler ls -la /opt/airflow/dags
```

Kiểm tra file đã mount vào container, cú pháp Python, `dag_id` có trùng không và các thư viện được import. Không chạy crawl, Spark hoặc truy vấn dữ liệu lớn ở cấp đầu file DAG; để chúng chạy trong task.

### `ModuleNotFoundError` / VS Code gạch vàng import

- Lỗi trong log container: bổ sung dependency vào Dockerfile đúng dịch vụ, rebuild và tạo lại container.
- Chỉ là cảnh báo trong VS Code: kiểm tra Python interpreter đang chọn. Interpreter trên Windows có thể chưa có Airflow trong khi image Docker đã có.
- Với module của dự án: kiểm tra đường dẫn mount, cách import và `PYTHONPATH`.

Không cài thư viện chỉ trên Windows rồi kỳ vọng container tự sử dụng được. Không sửa bằng cách cài tạm vào container mà không cập nhật Dockerfile.

## 4. Spark, Delta Lake và MinIO

| Biểu hiện | Kiểm tra và xử lý |
| --- | --- |
| `JAVA_GATEWAY_EXITED` | Đọc lỗi JVM phía trước; kiểm tra Java, `JAVA_HOME`, bộ nhớ và dependency trong image chạy job |
| Không tìm thấy `S3AFileSystem` | Kiểm tra JAR `hadoop-aws` đã được cài vào image; version phải khớp Hadoop đi kèm Spark |
| `NoSuchMethodError` liên quan AWS/Hadoop | Có thể xung đột JAR; kiểm tra bộ Hadoop S3A và AWS SDK, tránh thêm nhiều version |
| Không tìm thấy Delta data source / extension | Kiểm tra JAR Delta và cấu hình Spark extension/catalog |
| `AccessDenied` / chữ ký S3 không hợp lệ | Kiểm tra access key, secret key, endpoint và quyền truy cập bucket |
| `NoSuchBucket` | Xem log `minio-init`, xác nhận bucket `bronze`, `silver`, `gold` đã tạo |
| Không kết nối MinIO từ container | Dùng `http://minio:9000`; kiểm tra network, path-style access và cấu hình HTTP/SSL |
| Đường dẫn không phải Delta table | Kiểm tra đúng vị trí có `_delta_log`; tệp Parquet đơn lẻ không tự trở thành bảng Delta |
| Lỗi ghi đồng thời / kết quả thay đổi bất thường | Tránh chạy demo trực tiếp đồng thời với DAG; thiết kế cơ chế một writer cho môi trường hiện tại |

Bộ mẫu ghim Spark 3.5.5, Delta Lake 3.3.0, Hadoop S3A 3.3.4 và AWS SDK bundle 1.12.262. Delta 3.3.x tương thích Spark 3.5.x theo [bảng tương thích chính thức](https://docs.delta.io/releases/). Không nâng từng dependency riêng lẻ mà chưa kiểm tra cả bộ.

Chạy demo khi stack đã sẵn sàng và DAG demo đang không chạy:

```bash
docker compose --profile tools run --rm spark-job
```

Mong đợi log `SMOKE TEST PASSED`. Demo chỉ ghi vào các đường dẫn `demo/` và bảng `demo.job_counts`.

## 5. PostgreSQL và Superset

### Không kết nối PostgreSQL

| Nơi kết nối | Host và cổng mặc định |
| --- | --- |
| DBeaver/pgAdmin trên máy | `localhost:5433` |
| Airflow/Superset trong Compose | `postgres:5432` |

Kiểm tra database `lakehouse`, username và mật khẩu đúng role. Airflow/Superset có database metadata riêng, không dùng chúng để lưu fact/dim nghiệp vụ.

### Superset init hoặc đăng nhập lỗi

```bash
docker compose logs --tail=150 superset-init superset
```

Kiểm tra migration, `SUPERSET_SECRET_KEY`, tài khoản admin và driver PostgreSQL. Không đổi SECRET_KEY để thử sửa lỗi khi metadata đã có dữ liệu mã hóa. Mật khẩu admin trong `.env` dùng khi tạo tài khoản; sửa biến không tự đổi mật khẩu tài khoản đã tồn tại.

## 6. Lỗi dữ liệu trong pipeline

| Hiện tượng | Hướng xử lý |
| --- | --- |
| Chạy lại làm tăng bản ghi trùng | Chốt khóa nghiệp vụ, deduplicate và cơ chế upsert/merge; kiểm tra bằng hai lần chạy cùng đầu vào |
| CSV đọc thành một cột | Kiểm tra delimiter thực tế, dấu ngoặc kép, header và encoding |
| Số có dấu phẩy/chấm bị đọc sai | Xác định quy ước thập phân và phân cách hàng nghìn của từng nguồn trước khi chuyển kiểu |
| Số 0 và dữ liệu thiếu bị đồng nhất | Giữ phân biệt `0`, null và ký hiệu không công bố; không tự thay toàn bộ thiếu thành 0 |
| Join làm tăng số dòng | Kiểm tra khóa dimension có duy nhất và quan hệ nhiều–nhiều; đối chiếu số dòng trước/sau join |
| Tổng chỉ tiêu tuyển sinh quá lớn | Tránh cộng cả chỉ tiêu tổng và chỉ tiêu thành phần theo phương thức/chương trình |
| Tỷ lệ thất nghiệp/CPI bị tổng hợp sai | Giữ đúng định nghĩa chỉ tiêu; không cộng tỷ lệ hoặc lấy trung bình các nhóm khi thiếu trọng số phù hợp |
| Dữ liệu năm bị biến thành bốn quý giống nhau | Giữ tần suất gốc; không tự nhân bản số liệu năm thành số liệu quý |
| Tên địa phương không nối được | Dùng ánh xạ có quản lý thời gian/phiên bản; kiểm tra thay đổi địa giới và phạm vi thống kê |
| Chuẩn hóa kỹ năng gộp sai | Kiểm duyệt ánh xạ; tương đồng ngữ nghĩa không luôn là cùng một kỹ năng |

Giữ bản nguồn Bronze để đối chiếu. Chỉ cho dữ liệu đi tiếp khi kiểm tra chất lượng của tầng tương ứng đạt quy tắc đã đặt.

## 7. Tài liệu tham khảo

- [Docker Compose: thứ tự khởi động](https://docs.docker.com/compose/how-tos/startup-order/).
- [Airflow 2.10.5: chạy bằng Docker](https://airflow.apache.org/docs/apache-airflow/2.10.5/howto/docker-compose/index.html).
- [Delta Lake: tương thích phiên bản](https://docs.delta.io/releases/).
