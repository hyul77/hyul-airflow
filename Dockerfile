FROM apache/airflow:2.8.4

# 필요한 패키지 설치
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# 데이터베이스 초기화 및 사용자 추가는 Airflow 시작 시 수행될 수 있도록 환경 변수 설정
ENV AIRFLOW__CORE__SQL_ALCHEMY_CONN="mysql+pymysql://hyul:ektmftkfkd231210@mysql_db/mydata"
ENV AIRFLOW__CORE__EXECUTOR="LocalExecutor"
ENV _AIRFLOW_DB_UPGRADE="true"
ENV _AIRFLOW_WWW_USER_CREATE="true"
ENV _AIRFLOW_WWW_USER_USERNAME="hyul2"
ENV _AIRFLOW_WWW_USER_PASSWORD="hyul2"
ENV _AIRFLOW_WWW_USER_ROLE="Admin"
ENV _AIRFLOW_WWW_USER_FIRSTNAME="Admin"
ENV _AIRFLOW_WWW_USER_LASTNAME="User"
ENV _AIRFLOW_WWW_USER_EMAIL="hyul@example.com"

# DAG 파일 추가
COPY Airflow_naver.py ./dags/
