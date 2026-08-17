FROM python:3.13-alpine
WORKDIR /app
COPY generate.py .
USER nobody
ENTRYPOINT ["python3", "-u", "generate.py"]
