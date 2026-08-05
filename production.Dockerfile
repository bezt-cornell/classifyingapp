FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=5000

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    libffi-dev \
    libjpeg-dev \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements_aws.txt ./
RUN pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir -r requirements_aws.txt

COPY . .

RUN mkdir -p /app/static/light_curves /app/static/vlass_images /app/static/wise_plots

EXPOSE 5000

#CMD ["gunicorn", "--bind", "0.0.0.0:5000", "class_app:class_app"]
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--access-logfile", "-", "--error-logfile", "-", "--capture-output", "--log-level", "info", "class_app:class_app"]
