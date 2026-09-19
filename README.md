# Xây dựng Data Lakehouse phân tích nguồn nhân lực và thị trường lao động tại Việt Nam

## Nhóm thực hiện

| Thành viên | Mã số sinh viên |
| --- | --- |
| Quan Gia Mẫn | 23133042 |
| Trần Thảo Tiến | 23133076 |
| Nguyễn Thị Ngọc Trinh | 23133079 |

**Giảng viên hướng dẫn:** ThS. Trần Trọng Bình.

**Chuyên ngành:** Kỹ thuật dữ liệu.

> Tiểu luận chuyên ngành Kỹ thuật dữ liệu — Nhóm 3.
>
> README mô tả mục tiêu và thiết kế dự kiến của đề tài. Các chức năng, dashboard và mô hình dự báo bên dưới là phạm vi hướng đến; trạng thái hoàn thành cần được cập nhật theo kết quả triển khai thực tế.

## 1. Giới thiệu

Dự án hướng đến xây dựng một **Data Lakehouse** tích hợp dữ liệu tuyển dụng, giáo dục đại học, lao động, doanh nghiệp và kinh tế tại Việt Nam. Hệ thống tổ chức dữ liệu từ nhiều nguồn thành các tập dữ liệu thống nhất, phục vụ phân tích nhu cầu nhân lực, xu hướng kỹ năng và quy mô đào tạo.

Dữ liệu được xử lý theo kiến trúc **Medallion: Bronze → Silver → Gold**, từ dữ liệu tiếp nhận ban đầu đến dữ liệu đã chuẩn hóa và các bảng phục vụ phân tích. Kết quả được cung cấp cho dashboard nhằm hỗ trợ nghiên cứu thị trường lao động và định hướng đào tạo.

**Phạm vi thời gian theo đề cương:** thu thập dữ liệu giai đoạn 2020–2026 tùy khả năng cung cấp của từng nguồn; nghiên cứu dự báo giai đoạn 2027–2030 khi dữ liệu đáp ứng yêu cầu. Dữ liệu năm chưa kết thúc phải được ghi rõ thời điểm chốt số liệu.

## 2. Mục tiêu

- Tích hợp dữ liệu đa nguồn vào một nền tảng lưu trữ và xử lý thống nhất.
- Tự động hóa quy trình thu thập, làm sạch, chuẩn hóa và tổng hợp dữ liệu.
- Phân tích nhu cầu tuyển dụng theo thời gian, địa phương, ngành kinh tế và nghề nghiệp.
- Chuẩn hóa kỹ năng từ tin tuyển dụng, hạn chế trùng lặp do khác cách viết.
- Phân tích quy mô tuyển sinh theo trường, ngành đào tạo và địa phương.
- Đối chiếu xu hướng đào tạo với nhu cầu tuyển dụng trong phạm vi dữ liệu có thể so sánh.
- Xây dựng dashboard và nghiên cứu dự báo xu hướng nhân lực.

## 3. Nguồn dữ liệu

| Nhóm dữ liệu | Nguồn dự kiến | Nội dung khai thác |
| --- | --- | --- |
| Tuyển dụng | TopCV, VietnamWorks, CareerViet, ITviec, Glints và các nguồn phù hợp khác | Tin tuyển dụng, công ty, nghề nghiệp, địa điểm, mức lương, kinh nghiệm, kỹ năng |
| Giáo dục đại học | Website chính thức, đề án và thông báo tuyển sinh của các trường | Trường, ngành đào tạo, mã ngành, nhóm ngành, năm, chỉ tiêu tuyển sinh và các đặc trưng công bố được |
| Lao động | Cổng số liệu thống kê NSO, PX-Web, niên giám và báo cáo Điều tra Lao động Việc làm | Lực lượng lao động, việc làm, thất nghiệp, thu nhập và cơ cấu lao động |
| Kinh tế và doanh nghiệp | Các bảng và báo cáo thống kê chính thức | Chỉ tiêu kinh tế, tăng trưởng, đầu tư, số lượng doanh nghiệp theo thời gian, địa phương và ngành |
| Kỹ năng | Mô tả công việc và yêu cầu ứng viên trong tin tuyển dụng | Kỹ năng thô, tên kỹ năng chuẩn và các cách viết tương đương |

## 4. Kiến trúc hệ thống

<img width="1671" height="941" alt="Desing" src="https://github.com/user-attachments/assets/57abdad1-3384-404a-af83-6a5689188109" />

MinIO lưu trữ các đối tượng dữ liệu; Delta Lake cung cấp lớp quản lý bảng trên dữ liệu được lưu trữ; Spark thực hiện xử lý. PostgreSQL là lớp phục vụ truy vấn, nhận các bảng cần thiết từ Gold để Superset khai thác. Docker được dùng để đóng gói các dịch vụ.

### Các tầng dữ liệu

| Tầng | Mục đích | Dữ liệu dự kiến |
| --- | --- | --- |
| **Bronze** | Giữ dữ liệu tiếp nhận để truy vết và tái xử lý | Tệp nguồn JSON, CSV, HTML, tài liệu công bố và bảng tiếp nhận; kèm metadata thu thập |
| **Silver** | Làm sạch, chuẩn hóa schema và liên kết danh mục | Tin tuyển dụng đã loại trùng, địa phương chuẩn, danh mục ngành, nghề, kỹ năng, dữ liệu tuyển sinh và thống kê |
| **Gold** | Tổ chức dữ liệu nghiệp vụ và tính chỉ số | Các bảng dimension, fact và bảng tổng hợp phục vụ dashboard |

Các bảng Lakehouse được quản lý bằng Delta Lake; tệp nguồn giữ nguyên định dạng tại vùng lưu trữ thô. Bronze–Silver–Gold là cách tổ chức dữ liệu do dự án thiết kế, không phải các tầng Delta Lake tự tạo sẵn.

## 5. Công nghệ sử dụng

### 5.1. Python và bộ công cụ thu thập

**Python** là ngôn ngữ chính để viết chương trình thu thập, kiểm tra dữ liệu và các tác vụ xử lý.

| Công cụ | Giới thiệu | Vai trò trong đề tài |
| --- | --- | --- |
| Requests / HTTP client | Gửi yêu cầu HTTP để nhận dữ liệu | Thu thập API hoặc trang có nội dung trong phản hồi HTTP |
| Scrapy | Framework tổ chức các tác vụ crawl | Quản lý spider, phân trang và trích xuất dữ liệu từ nhiều trang |
| Playwright | Tự động hóa thao tác trình duyệt | Thu thập nguồn cần JavaScript hoặc tương tác để hiển thị nội dung |

### 5.2. Apache Airflow — Điều phối pipeline

Airflow mô tả workflow bằng DAG và quản lý lịch chạy, quan hệ phụ thuộc, thử lại và trạng thái tác vụ. Trong đề tài, Airflow điều phối các bước thu thập, xử lý Spark, kiểm tra chất lượng và nạp dữ liệu phục vụ phân tích. [Tài liệu Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/index.html).

### 5.3. MinIO — Lưu trữ đối tượng

MinIO là hệ thống object storage có giao diện tương thích S3. Dự án sử dụng MinIO để lưu tệp nguồn và dữ liệu của các tầng Lakehouse, tổ chức theo nguồn và thời gian để thuận tiện truy vết, đọc lại và xử lý bổ sung.

### 5.4. Apache Spark / PySpark — Xử lý dữ liệu

Spark là công cụ xử lý dữ liệu phân tán; PySpark cho phép sử dụng Spark bằng Python. Dự án dùng DataFrame và Spark SQL để chuẩn hóa dữ liệu, loại trùng, liên kết danh mục, tổng hợp chỉ tiêu và xây dựng bảng Gold. [Tài liệu Apache Spark](https://spark.apache.org/docs/latest/).

### 5.5. Delta Lake — Quản lý bảng Lakehouse

Delta Lake cung cấp giao dịch ACID, kiểm soát schema, cập nhật hoặc hợp nhất dữ liệu và truy vấn phiên bản lịch sử. Trong đề tài, các khả năng này hỗ trợ quản lý bảng trên MinIO, cập nhật dữ liệu theo khóa và tái lập kết quả phân tích. Việc truy vấn lịch sử phụ thuộc chính sách lưu giữ dữ liệu và log. [Tài liệu Delta Lake](https://docs.delta.io/index.html).

### 5.6. SBERT và DBSCAN/HDBSCAN — Chuẩn hóa kỹ năng

**SBERT / Sentence Transformers** mã hóa tên hoặc mô tả kỹ năng thành vector ngữ nghĩa, hỗ trợ so sánh mức độ tương đồng. [Tài liệu Sentence Transformers](https://sbert.net/).

**DBSCAN hoặc HDBSCAN** được dùng để thử nghiệm phân cụm các kỹ năng có vector gần nhau. Kết quả chỉ tạo ứng viên cho bước kiểm duyệt: kỹ năng gần nghĩa hoặc thường xuất hiện cùng nhau chưa chắc là một kỹ năng.

Ví dụ ánh xạ mong muốn: `ReactJS`, `React.js` và `React JS` được kiểm duyệt trước khi liên kết về một kỹ năng chuẩn. Các bản ghi chưa chắc chắn được giữ lại để xử lý tiếp.

### 5.7. Streamlit / FastAPI — Kiểm duyệt kỹ năng

**Streamlit** hỗ trợ xây dựng giao diện để người kiểm duyệt xem cụm, chọn tên chuẩn và chấp nhận hoặc từ chối ánh xạ. **FastAPI** có thể cung cấp API phục vụ tra cứu và cập nhật kết quả kiểm duyệt nếu cần tách giao diện với backend. Đây là các phương án triển khai theo đề cương, cần chốt khi xây dựng module.

### 5.8. PostgreSQL — Phục vụ truy vấn

PostgreSQL là hệ quản trị cơ sở dữ liệu quan hệ. Dự án nạp các bảng Gold hoặc dữ liệu tổng hợp cần thiết vào PostgreSQL để truy vấn bằng SQL và kết nối dashboard. Khóa chính, khóa ngoại và quy tắc nạp dữ liệu giúp duy trì tính nhất quán của lớp phục vụ.

### 5.9. Apache Superset — Trực quan hóa

Superset hỗ trợ khám phá dữ liệu và xây dựng dashboard. Dự án kết nối Superset với PostgreSQL để trình bày xu hướng tuyển dụng, nhu cầu kỹ năng, quy mô tuyển sinh và bối cảnh lao động theo các bộ lọc thời gian, địa phương, ngành và trường. [Tài liệu Apache Superset](https://superset.apache.org/docs/intro/).

### 5.10. Docker — Đóng gói môi trường

Docker đóng gói các dịch vụ cùng môi trường thực thi; Docker Compose có thể mô tả và khởi chạy nhiều dịch vụ trong môi trường phát triển. Dự án hướng đến cấu hình thống nhất cho Airflow, MinIO, Spark, PostgreSQL và Superset, kèm volume lưu trữ dữ liệu.

## 6. Mô hình dữ liệu phân tích

Tầng Gold được định hướng theo **star schema cho từng nghiệp vụ**. Nhiều bảng fact dùng chung các dimension tạo thành mô hình nhiều ngôi sao liên kết (fact constellation). Danh sách dưới đây là thiết kế dự kiến cần cụ thể hóa bằng schema và quy tắc ánh xạ.

### Bảng dimension

| Bảng | Nội dung |
| --- | --- |
| `dim_date` | Ngày, tháng, quý, năm và thông tin lịch |
| `dim_location` | Địa phương, vùng và mã địa lý chuẩn |
| `dim_company` | Danh mục doanh nghiệp xuất hiện trong dữ liệu tuyển dụng |
| `dim_occupation` | Danh mục nghề nghiệp và nhóm nghề |
| `dim_industry` | Danh mục ngành kinh tế |
| `dim_major` | Mã ngành, tên ngành và nhóm ngành đào tạo |
| `dim_economic_indicator` | Tên chỉ tiêu kinh tế, đơn vị tính và định nghĩa |
| `dim_skill` | Danh mục kỹ năng đã chuẩn hóa |
| `dim_university` | Danh mục trường đại học và các thông tin mô tả |

### Bảng fact

| Bảng | Mức chi tiết dự kiến của một bản ghi | Phân tích chính |
| --- | --- | --- |
| `fact_job_posting` | Một tin tuyển dụng đã định danh và loại trùng | Số tin, mức lương công bố, nhu cầu theo nghề và địa phương |
| `fact_job_skill` | Một cặp tin tuyển dụng – kỹ năng chuẩn, không lặp | Tần suất kỹ năng và cơ cấu kỹ năng theo nghề |
| `fact_admission` | Một trường – ngành/chương trình – năm – phương thức nếu nguồn tách riêng | Chỉ tiêu tuyển sinh theo trường, nhóm ngành và thời gian |
| `fact_economic_indicator` | Một chỉ tiêu – kỳ thống kê – địa phương – ngành nếu có | Mức và xu hướng các chỉ tiêu kinh tế |
| `fact_labour_market` | Một chỉ tiêu lao động – kỳ – địa phương – nhóm phân loại được công bố | Việc làm, thất nghiệp, thu nhập và cơ cấu lao động |
| `fact_enterprise` | Một chỉ tiêu doanh nghiệp – kỳ – địa phương – ngành nếu có | Quy mô, cơ cấu và biến động số doanh nghiệp |

`fact_job_skill` là bảng fact không có số đo, đồng thời biểu diễn quan hệ nhiều–nhiều giữa tin tuyển dụng và kỹ năng. Mỗi cặp tin–kỹ năng chỉ xuất hiện một lần sau chuẩn hóa.

Trước khi triển khai cần chốt mức chi tiết, khóa duy nhất và đơn vị tính cho từng fact. Dữ liệu theo năm/quý cần ghi rõ kỳ thống kê; không diễn giải như quan sát theo ngày. Không nối trực tiếp các fact có mức chi tiết khác nhau để cộng số liệu; cần tổng hợp về cùng mức so sánh trước.

## 7. Quy trình xử lý dự kiến

1. **Thu thập:** lấy dữ liệu từ API, website hoặc tệp công bố; ghi nhận nguồn và thời điểm thu thập.
2. **Lưu Bronze:** lưu dữ liệu gốc để đối chiếu và tái xử lý.
3. **Chuẩn hóa Silver:** xử lý dữ liệu thiếu, trùng lặp, sai kiểu; thống nhất thời gian, mức lương, địa phương và danh mục ngành nghề.
4. **Chuẩn hóa kỹ năng:** trích xuất kỹ năng, mã hóa vector, phân cụm, kiểm duyệt và lưu ánh xạ được chấp nhận.
5. **Xây dựng Gold:** tạo dimension, fact và các bảng tổng hợp theo nghiệp vụ.
6. **Kiểm tra chất lượng:** kiểm tra khóa, quan hệ tham chiếu, đơn vị tính và đối chiếu tổng với nguồn.
7. **Nạp PostgreSQL:** cập nhật lớp dữ liệu phục vụ theo cơ chế tránh sinh bản ghi trùng khi chạy lại.
8. **Trực quan hóa:** xây dựng dashboard và đối chiếu chỉ số với kết quả xử lý.

Airflow điều phối các bước tự động; các ánh xạ kỹ năng chờ kiểm duyệt cần có trạng thái riêng trước khi được sử dụng như kết quả đã xác nhận.

## 8. Chỉ số và dashboard dự kiến

| Nhóm phân tích | Chỉ số tiêu biểu | Lưu ý tính toán |
| --- | --- | --- |
| Tuyển dụng | Số tin duy nhất; tăng trưởng số tin; phân bố theo nghề, ngành và địa phương | Loại trùng trước khi đếm; kỳ so sánh phải tương đương |
| Mức lương | Trung vị hoặc trung bình mức lương đã chuẩn hóa | Chỉ tính tin có mức lương hợp lệ; công bố tỷ lệ tin có dữ liệu |
| Kỹ năng | Số tin yêu cầu từng kỹ năng; tỷ lệ xuất hiện kỹ năng | Mẫu số là số tin thuộc phạm vi phân tích, không phải tổng số dòng tin–kỹ năng |
| Tuyển sinh | Tổng chỉ tiêu; số trường; số ngành; biến động qua năm | Tránh cộng trùng tổng chỉ tiêu với chỉ tiêu thành phần theo phương thức |
| Lao động | Lực lượng lao động, việc làm, tỷ lệ thất nghiệp, thu nhập | Giữ định nghĩa nguồn; không cộng hoặc lấy trung bình đơn giản các tỷ lệ |
| Kinh tế và doanh nghiệp | Giá trị chỉ tiêu; số doanh nghiệp; mức tăng trưởng | Chỉ tổng hợp dữ liệu cùng đơn vị và phạm vi thống kê |

Các dashboard hướng đến gồm: tổng quan; nhu cầu nhân lực theo ngành/nghề; quy mô tuyển sinh; nhu cầu kỹ năng; bối cảnh kinh tế–lao động; đối chiếu xu hướng đào tạo và tuyển dụng.

Dashboard khoảng cách cung–cầu, đánh giá kỹ năng đào tạo và dự báo được phát triển khi có đủ dữ liệu tương ứng. Không tính Skill Gap Score chỉ từ tần suất kỹ năng trong tin tuyển dụng nếu chưa có dữ liệu hoặc quy ước đo lường phía cung.

## 9. Chất lượng dữ liệu và đánh giá

- **Đầy đủ:** tỷ lệ thiếu ở các trường dữ liệu bắt buộc và độ phủ theo nguồn/năm.
- **Duy nhất:** số bản ghi trùng trước và sau xử lý; tính duy nhất của khóa nghiệp vụ.
- **Hợp lệ:** kiểu dữ liệu, khoảng giá trị, đơn vị đo và mã danh mục.
- **Nhất quán:** quan hệ giữa fact và dimension; tổng chỉ tiêu so với nguồn công bố.
- **Truy vết:** nguồn dữ liệu, kỳ thống kê, thời điểm thu thập và phiên bản xử lý.
- **Chuẩn hóa kỹ năng:** độ bao phủ ánh xạ, tỷ lệ chưa xác định và độ chính xác trên tập mẫu được kiểm duyệt.
- **Vận hành:** thời gian chạy, tỷ lệ tác vụ thành công và khả năng chạy lại không làm tăng bản ghi trùng.

## 10. Tổ chức mã nguồn dự kiến

| Đường dẫn | Nội dung |
| --- | --- |
| `dags/` | DAG điều phối của Airflow |
| `src/collectors/` | Chương trình thu thập theo nguồn |
| `src/processing/` | Spark jobs xử lý Bronze, Silver và Gold |
| `src/skill_normalization/` | Trích xuất, embedding, phân cụm và ánh xạ kỹ năng |
| `src/serving/` | Nạp dữ liệu Gold vào PostgreSQL |
| `apps/skill_review/` | Giao diện hoặc API kiểm duyệt kỹ năng |
| `sql/` | DDL, view và truy vấn kiểm tra/chỉ số |
| `configs/` | Schema, quy tắc ánh xạ và cấu hình mẫu |
| `dashboards/` | Cấu hình hoặc bản xuất dashboard |
| `tests/` | Kiểm tra chất lượng dữ liệu và pipeline |
| `docs/` | Kiến trúc, từ điển dữ liệu và tài liệu triển khai |
| `docker-compose.yml` | Khai báo dịch vụ cho môi trường phát triển |
| `.env.example` | Danh sách biến môi trường mẫu, không chứa thông tin bí mật |

## 11. Định hướng triển khai

| Giai đoạn | Nội dung |
| --- | --- |
| Tuần 1–2 | Khảo sát bài toán, nguồn dữ liệu, fact/dim và chỉ số |
| Tuần 3–4 | Thiết kế kiến trúc, mô hình dữ liệu và môi trường triển khai |
| Tuần 5–6 | Xây dựng chương trình thu thập và DAG điều phối |
| Tuần 7–8 | Làm sạch dữ liệu, xây dựng các tầng Lakehouse |
| Tuần 9–10 | Triển khai và đánh giá module chuẩn hóa kỹ năng |
| Tuần 11–12 | Tổng hợp chỉ số, nạp PostgreSQL, xây dựng dashboard |
| Tuần 13 | Kiểm thử hệ thống, đánh giá kết quả và hoàn thiện báo cáo |# TLCN26-Data-lakehouse-phan-tich-nguon-nhan-luc-thi-truong-lao-dong-Viet-Nam
