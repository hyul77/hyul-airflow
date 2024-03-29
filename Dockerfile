FROM apache/airflow:2.8.4

USER root
RUN apt update
RUN apt install gcc -y


USER airflow
COPY requirements.txt /tmp
WORKDIR /tmp/
# 필요한 패키지 및 라이브러리 설치
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# DAG 및 기타 파일을 컨테이너 내의 워크 디렉토리로 복사
COPY /home/hyul/airflow /opt/airflow/dags/