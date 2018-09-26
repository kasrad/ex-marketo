FROM quay.io/keboola/docker-custom-python:latest

RUN pip install  --upgrade --no-cache-dir --ignore-installed logging_gelf
RUN pip install  --upgrade --no-cache-dir --ignore-installed marketorestpython

COPY . /code/
WORKDIR /data/
CMD ["python", "-u", "/code/main.py"]
