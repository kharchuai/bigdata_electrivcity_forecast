# Electricity Forecasting — phần 01–05 theo cách làm của lab

Đã có bộ khung Python, cấu hình/hợp đồng dữ liệu, hạ tầng và khởi tạo bảng.
Các module ingestion, xử lý, đặc trưng, ML và script demo/benchmark vẫn là
khung cho phần 06 trở đi.

## 1. Môi trường Python dùng requirements.txt

Chạy trong thư mục chứa README này, với Python 3.10 trở lên:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
$env:PYTHONPATH = "$PWD\src"
.\.venv\Scripts\python.exe -m electricity_forecast.cli --help
```

Đặt lại PYTHONPATH khi mở terminal mới. Spark/PySpark lấy từ Docker image,
không cài thêm một bản PySpark vào requirements.txt.

## 2. Chuẩn bị cấu hình

```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
.\.venv\Scripts\python.exe -m electricity_forecast.cli validate-config --require-connections
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Biến môi trường tiến trình được ưu tiên hơn .env. Đường dẫn tương đối trong
YAML bắt đầu từ thư mục project. Mật khẩu nằm trong .env, không đưa vào Git.

Nếu đang dùng .env cũ, cập nhật SPARK_MASTER_URL thành local[2],
SPARK_UI_PORT thành 4040 và bổ sung các biến bootstrap theo .env.example.
Các cặp sau phải khớp; validate-config --require-connections sẽ báo nếu lệch:

| Biến trong .env / Compose | Khóa trong configs/local.yaml |
|---|---|
| HDFS_ROOT_URI | hdfs.root_uri |
| KAFKA_TOPIC | kafka.topic |
| KAFKA_PARTITIONS | kafka.partitions |
| KAFKA_REPLICATION_FACTOR | kafka.replication_factor |

Hợp đồng số đo trong contracts.py: timestamp giữ nguyên giờ nguồn, không kèm múi giờ,
mỗi khoảng dài 15 phút, đơn vị kW, giá trị 0 hợp lệ. Khóa số đo là
(run_id, customer_id, reading_end). Số đo kết thúc 09:00 thuộc giờ 08:00–09:00;
origin 09:00 dự báo trung bình khoảng 10:00–11:00.

Ví dụ timestamp mới: `2013-12-31T08:15:00`. Python dùng datetime không có
múi giờ; dữ liệu Spark/Iceberg dùng `TIMESTAMP_NTZ`; PostgreSQL dùng
`timestamp without time zone` và `timestamp[]`. Không chuyển UTC, không xử lý
DST, không thêm `Z` hoặc offset. Dữ liệu đầu vào có `Z`/offset bị từ chối để
tránh tự đổi giờ. `target_hour`/`target_day_of_week` lấy trực tiếp từ giờ nguồn.
Các mốc runtime do ứng dụng ghi cũng không kèm múi giờ; các thành phần demo
phải dùng cùng đồng hồ hệ thống khi đo latency. Chọn cửa sổ nguồn có các mốc
15 phút đều đặn; kiểm tra khoảng thiếu/trùng vẫn được giữ.

Tham khảo kiểu dữ liệu: [Spark TIMESTAMP_NTZ](https://spark.apache.org/docs/4.1.3/sql-ref-datatypes.html).

Fixture tests/readings_sample.csv có các ca đủ mẫu/trùng (trung bình 25 kW),
thiếu mẫu, cùng khóa khác giá trị, 0 hợp lệ, số âm/NaN và lỗi timestamp.
Cột case/expected_valid phục vụ test, không thuộc schema sự kiện.

Timestamp nguồn được quy ước là cuối khoảng đo. Adapter nguồn ở phần 06
đọc dữ liệu theo quy ước cố định này.

## 3. Tận dụng image và cấu hình của lab

| Thành phần | Phiên bản / cách chạy |
|---|---|
| Hadoop | de-labs/hadoop:3.4.3, image dùng chung của L01–L04 |
| Spark / Scala / Java | 4.1.3 / 2.13 / 17, local[2] như L09/L11 |
| Iceberg | runtime Spark 4.1 / Scala 2.13, phiên bản 1.11.0 |
| Kafka | apache/kafka:4.3.1, KRaft một node |
| PostgreSQL / JDBC | 18.6 / 42.7.8 |

Hadoop image đã chứa entrypoint.sh của lab. Nếu image đã có trên máy thì
không cần build lại. Nếu chưa có, build từ bộ lab của bạn (đổi đường dẫn nếu
chuyển sang máy khác); không sửa file trong bộ lab:

```powershell
docker build -t de-labs/hadoop:3.4.3 "E:\dowload\Big data\labs\images\hadoop"
```

Compose dùng pull_policy=never cho image lab vì đây là image build local.
Khi bàn giao project sang máy khác, cung cấp bộ build image của lab hoặc
chuyển image đó sang máy mới trước khi dựng dịch vụ.

Hadoop đọc core-site.xml và hdfs-site.xml được mount từ infra/. Đường dẫn
NameNode/DataNode là /hadoop/dfs/name và /hadoop/dfs/data như lab. Tên volume
namenode-data/datanode-data được giữ nguyên; chỉ thay điểm mount, không xóa
hay format lại dữ liệu đã có. NameNode và client Spark dùng user root trong
môi trường lab đơn giản này.

Dockerfile.spark chỉ cài thư viện Python, copy code và cấu hình. JAR được
khai báo bằng spark.jars.packages trong infra/spark-defaults.conf, giống
L09/L11. Lần chạy Spark đầu cần mạng để tải JAR; volume ivy-cache giữ cache
ở /tmp/.ivy2 cho các lần sau. Không dùng Maven build riêng.

## 4. Khởi động và thứ tự init

Cần Docker Desktop chạy Linux containers. Nếu đã có dữ liệu PostgreSQL 16,
đọc mục nâng PostgreSQL bên dưới trước khi tạo database 18 mới.

```powershell
docker compose config --quiet
docker compose build spark
docker compose up -d --remove-orphans
docker compose ps -a
docker compose logs hdfs-init kafka-init init
```

--remove-orphans dọn container của service cũ đã bỏ trong cùng project
(ví dụ spark-master/spark-worker); không xóa volume.

Luồng khởi tạo:

```text
NameNode + DataNode healthy → hdfs-init (hdfs dfs -mkdir -p) ─┐
Kafka healthy → kafka-init (kafka-topics.sh) ─────────────────┼→ init bảng → spark / runner
PostgreSQL healthy ─────────────────────────────────────────┘
```

hdfs-init và kafka-init là hai service chạy một lần, dùng image Hadoop/Kafka.
Các lệnh shell ngắn nằm ngay trong Compose. hdfs-init tạo source, warehouse,
models, checkpoints và spark-events. kafka-init tạo topic nếu chưa có và
kiểm tra số partition/replica hiện tại. Hai helper thất bại thì init không chạy.

Lệnh Python init chỉ đọc/kiểm tra cấu hình, chạy SQL PostgreSQL trong một
transaction, rồi tạo namespace/bảng Iceberg. Init dùng log và exit code,
không tạo run/job/attempt trong các bảng ops. Thư mục spark-events có trước
khi Spark bật event log. Runner chờ init thoát mã 0.

Để chạy lại các bước khởi tạo mà không xóa dữ liệu:

```powershell
docker compose run --rm hdfs-init
docker compose run --rm kafka-init
docker compose run --rm init
```

CREATE IF NOT EXISTS giữ bảng/dữ liệu đang có; không tự ALTER một schema cũ
bị lệch. Cấu trúc thay đổi cần SQL ALTER có chủ đích. Bảng ops.schema_migrations
của bản code cũ, nếu có, được để nguyên nhưng không còn dùng; bản mới không
tạo bảng này và không kiểm tra checksum/advisory lock.

### Nếu đã tạo bảng bằng bản dùng UTC trước đây

Đổi code không tự đổi kiểu cột của bảng đã tồn tại. `init` báo lỗi nếu các bảng
ứng dụng PostgreSQL còn dùng `timestamptz` hoặc bảng Iceberg còn dùng timestamp
có múi giờ. Dữ liệu và volume cũ được giữ nguyên.

Với dữ liệu cần giữ, sao lưu trước rồi chuyển schema và giá trị có chủ đích;
cần xác định lại giờ nguồn vì dữ liệu cũ có thể đã được đổi giờ. Nếu bắt đầu
bộ dữ liệu demo mới, dùng database ứng dụng và namespace Iceberg mới, đồng bộ
YAML/.env rồi chạy init. Không dùng `down -v` để xử lý lỗi schema. Database
catalog và metadata nội bộ của Iceberg vẫn do Iceberg quản lý.

Sau khi cập nhật code/cấu hình, build lại image Spark trước khi chạy init:

```powershell
docker compose build spark
docker compose run --rm init
```

## 5. Kiểm tra theo cách của lab

```powershell
# Đọc thư mục HDFS bằng CLI trong image Hadoop
docker compose exec namenode hdfs dfs -ls /electricity

# Mô tả topic bằng CLI trong image Kafka
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic electricity_readings

# Kiểm tra bảng PostgreSQL, Spark JDBC và đọc/ghi Iceberg trên HDFS
docker compose run --rm runner python3 -m electricity_forecast.cli check-infra
```

Đổi /electricity và tên topic trong lệnh kiểm tra nếu đã đổi cấu hình.
check-infra chỉ dùng một bảng thử Iceberg tên UUID, ghi/đọc 2 dòng, kiểm tra
snapshot rồi xóa chính bảng thử đó. Không tạo topic thử, không có client
WebHDFS Python hay lệnh Docker gọi từ Python trong container.

Kiểm tra tích hợp lớp lưu trữ khi stack đã sẵn sàng:

```powershell
docker compose run --rm -e RUN_STACK_TESTS=1 runner python3 -m unittest discover -s tests -v
```

Test dùng run_id/bảng thử riêng và dọn dữ liệu của chính test. Test local
thông thường bỏ qua các bài kiểm tra cần PostgreSQL/Spark thật.

Để chạy Spark trực tiếp như lab, dùng service spark:

```powershell
docker compose exec spark python3 -m electricity_forecast.cli check-infra
```

Spark UI ở http://localhost:4040 chỉ xuất hiện khi service spark đang chạy
một Spark application. Job trong init/runner có JVM local riêng, không
publish cổng UI ra host. HDFS UI: http://localhost:9870; PostgreSQL từ host:
localhost:55432; Kafka từ host: localhost:19092. Trong container dùng
postgres:5432, kafka:9092 và hdfs://namenode:8020.

## 6. Các bảng và logic được giữ

PostgreSQL: ops.runs, ops.job_runs, ops.forecast_status,
forecast.predictions, forecast.actual_hourly. Database catalog được tạo trên
volume PostgreSQL mới bởi infra/postgres-init.sh; Iceberg tự quản lý bảng
nội bộ của catalog. File SQL nghiệp vụ được chạy bởi lệnh init, nên chạy lại
được cả khi volume database đã tồn tại.

Năm bảng Iceberg: bronze.raw_events, silver.readings_15min,
silver.consumption_hourly, gold.forecast_features, ops.quarantine_events
trong catalog lakehouse. Vẫn giữ JDBC catalog trên PostgreSQL và file bảng
trên HDFS; không chuyển sang REST catalog/S3 của L11.

storage.py giữ các hàm MERGE theo khóa, từ chối giá trị xung đột và đọc
snapshot. Hourly chỉ được tăng độ đầy đủ từ WAITING_FOR_DATA; retry không
được đổi kết quả đã hoàn tất. metrics.py giữ theo dõi các job xử lý dữ liệu
ở các phần sau, không tham gia bootstrap hạ tầng.

## Xử lý lỗi và dừng dịch vụ

- Image de-labs/hadoop không có: build image từ bộ lab trước.
- .env cũ có spark://spark-master:7077: đổi sang local[2].
- Bootstrap khác YAML: đồng bộ HDFS_ROOT_URI/KAFKA_TOPIC/partition/replica.
- JAR chưa tải được: kiểm tra mạng và chạy lại init; giữ volume ivy-cache.
- PostgreSQL catalog database/role thiếu trên volume cũ: tạo/cập nhật có
  chủ đích; postgres-init.sh chỉ chạy khi volume database mới.
- Topic/schema không khớp: sửa cấu hình hoặc dùng lệnh ALTER phù hợp,
  không xóa topic, bảng hay checkpoint để làm lại demo.

```powershell
docker compose stop
```

Lệnh stop giữ dữ liệu trong volume. Chạy lại toàn bộ kiểm tra sau khi đổi
phiên bản phần mềm hoặc tái sử dụng dữ liệu/checkpoint của phiên bản cũ.

## Nâng PostgreSQL 16 lên 18 khi đã có dữ liệu

Compose mới dùng `electricity-forecast_postgres18-data` mount tại
`/var/lib/postgresql`, khớp image PostgreSQL 18. Volume cũ
`electricity-forecast_postgres-data` được giữ nguyên, không được mount vào 18.
Thay image không tự nâng định dạng database; cần dump/restore nếu muốn chuyển
dữ liệu. Không dùng `down -v` để nâng phiên bản.

Với cấu hình mặc định (hai database `electricity`, `iceberg_catalog`, user
`electricity`), backup khi container cũ còn chạy PostgreSQL 16, trước lần `up`
đầu tiên dùng image 18. Điều chỉnh tên database/user nếu đã đổi trong `.env`:

```powershell
New-Item -ItemType Directory -Force artifacts/pg16-backup | Out-Null
docker exec electricity-forecast-postgres-1 pg_dump -U electricity -d electricity -Fc -f /tmp/electricity.dump
docker exec electricity-forecast-postgres-1 pg_dump -U electricity -d iceberg_catalog -Fc -f /tmp/iceberg_catalog.dump
docker cp electricity-forecast-postgres-1:/tmp/electricity.dump artifacts/pg16-backup/electricity.dump
docker cp electricity-forecast-postgres-1:/tmp/iceberg_catalog.dump artifacts/pg16-backup/iceberg_catalog.dump
```

Chỉ tiếp tục sau khi cả hai lệnh dump và copy thành công. Đổi image có major
version khác không làm các checkpoint/model Spark cũ tự tương thích; kiểm tra
phục hồi trước khi dùng lại trong các phần triển khai tiếp theo.

Khởi động riêng PostgreSQL 18 trên volume mới, restore trước khi chạy service
`init`. Các lệnh sau dành cho database 18 mới, chưa được init/nạp dữ liệu:

```powershell
docker compose up -d --wait postgres
docker cp artifacts/pg16-backup/electricity.dump electricity-forecast-postgres-1:/tmp/electricity.dump
docker cp artifacts/pg16-backup/iceberg_catalog.dump electricity-forecast-postgres-1:/tmp/iceberg_catalog.dump
docker compose exec -T postgres pg_restore -U electricity -d electricity --clean --if-exists --no-owner --no-privileges --exit-on-error --single-transaction /tmp/electricity.dump
docker compose exec -T postgres pg_restore -U electricity -d iceberg_catalog --clean --if-exists --no-owner --no-privileges --exit-on-error --single-transaction /tmp/iceberg_catalog.dump
# Nếu dump chứa bảng timestamp cũ, xử lý theo mục schema phía trên trước khi init.
docker compose up --build -d
```

Nếu catalog dùng role khác, restore database catalog bằng role đó. Backup chứa
dữ liệu ứng dụng nên nằm trong `artifacts/`, đã được Git bỏ qua. Nếu container
16 đã được thay thế trước khi backup, vẫn giữ volume 16 để lấy lại bản dump;
không trỏ PostgreSQL 18 trực tiếp vào thư mục dữ liệu 16.

Tham khảo chính thức: [Spark Docker image](https://github.com/apache/spark-docker/tree/master/4.1.3),
[Iceberg JDBC catalog](https://iceberg.apache.org/docs/1.11.0/jdbc/),
[Kafka Docker](https://kafka.apache.org/43/getting-started/docker/),
[PostgreSQL Docker image](https://hub.docker.com/_/postgres),
[Compose startup order](https://docs.docker.com/compose/how-tos/startup-order/).
