FROM python:3.13-alpine
WORKDIR /app
COPY fake.py .
USER nobody
EXPOSE 4000
ENTRYPOINT ["python3", "-u", "fake.py"]
