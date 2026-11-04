FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY proto/ proto/
RUN python -m grpc_tools.protoc -Iproto --python_out=. --grpc_python_out=. proto/flight.proto

COPY common/ common/
COPY flight_service/ flight_service/
COPY booking_service/ booking_service/

USER 10001
