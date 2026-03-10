# data-engineering-zoomcamp-2026

```plaintext
data-engineering-project/
├── dags/                   # Nơi chứa các file định nghĩa Workflow (Airflow DAGs)
│   ├── sql/                # Các file .sql riêng biệt để DAG gọi vào
│   └── main_pipeline.py
├── src/                    # Code logic chính (Trái tim của dự án)
│   ├── extract/            # Scripts lấy dữ liệu (API, Database, Web Scraping)
│   ├── transform/          # Logic xử lý, làm sạch (Spark, Pandas, dbt)
│   ├── load/               # Code đẩy dữ liệu vào Warehouse (BigQuery, Snowflake)
│   └── utils/              # Hàm dùng chung (Logger, kết nối DB, xử lý chuỗi)
├── config/                 # Chứa các file cấu hình (YAML, JSON, .env)
│   ├── config.yaml
│   └── db_params.json
├── data/                   # (Tùy chọn) Lưu dữ liệu tạm thời (Landing, Staging)
│   ├── raw/
│   └── processed/
├── tests/                  # Kiểm thử (Unit test cho các hàm transform/extract)
│   ├── test_extract.py
│   └── test_transform.py
├── notebooks/              # Nơi chứa file .ipynb để làm Prototype hoặc EDA
├── infra/                  # Quản lý hạ tầng (Terraform, CloudFormation)
├── docker/                 # Dockerfile và các cấu hình container
├── .gitignore              # Bỏ qua các file rác, dữ liệu nặng, file .env
├── requirements.txt        # Danh sách các thư viện Python cần thiết
├── README.md               # Hướng dẫn chạy dự án
└── Makefile                # Các lệnh tắt (build, run, test) để thao tác nhanh
```
