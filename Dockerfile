FROM python:3.9-slim
WORKDIR /app
COPY src/ /app/
RUN pip install flask requests
CMD ["python", "node.py", "0"]
