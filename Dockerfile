ARG base_image=amazonlinux:2

FROM ${base_image}

WORKDIR /app/package/

RUN ["yum", "install", "python3-pip", "zip", "-y"]

RUN ["python3", "-m", "pip", "install", "--no-cache-dir", "prometheus-client", "requests", "--target", "."]

COPY concourse_cloudwatch_metrics_publisher.py .

RUN ["zip", "-v", "-9", "-r", "../package.zip", "."]
