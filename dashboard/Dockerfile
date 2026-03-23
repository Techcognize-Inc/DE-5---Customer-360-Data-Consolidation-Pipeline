FROM python:3.11-slim

WORKDIR /opt/project

COPY dashboard/requirements-dashboard.txt /tmp/requirements-dashboard.txt
RUN pip install --no-cache-dir -r /tmp/requirements-dashboard.txt

EXPOSE 8501

CMD ["streamlit", "run", "/opt/project/dashboard/app.py", "--server.address", "0.0.0.0", "--server.port", "8501"]
