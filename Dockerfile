FROM ghcr.io/infrasonar/python:3.12.9
ADD . /code
WORKDIR /code
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "main.py"]
