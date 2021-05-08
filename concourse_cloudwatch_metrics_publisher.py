"""
Publish Concourse metrics to CloudWatch
"""
from json import dumps
from os import getenv
from time import time
from typing import Dict, List

from boto3 import client

from prometheus_client.parser import text_string_to_metric_families

from requests import get

INTERESTING_METRICS = (
    "concourse_steps_waiting",
    "concourse_builds_running",
    "concourse_jobs_scheduling",
    "concourse_workers_containers",
    "concourse_workers_tasks",
)
IGNORE_LABELS = (
    "platform",
    "teamId",
    "team",
    "type",
)
TAG_LABELS = (
    "workerTags",
    "tags",
)

cloudwatch = client("cloudwatch")


def labels_to_dimensions(labels: Dict[str, str]) -> List[Dict[str, str]]:
    """
    Converts Prometheus labels to CloudWatch dimensions

    :param labels: Prometheus label format
    :return: CloudWatch dimension format
    """
    dimensions = []

    for key in labels.keys():
        value = labels[key]

        if key in IGNORE_LABELS:
            continue

        if key in TAG_LABELS and value == "":
            value = "none"

        dimensions.append(
            {
                "Name": key,
                "Value": value,
            }
        )

    return dimensions


def handler(event: None, context: None) -> None:  # pylint: disable=unused-argument
    """
    Publish data points for Concourse metrics
    """
    timestamp = time()
    response = get(url=getenv("CONCOURSE_METRICS_URL"))

    if response.status_code != 200:
        raise ValueError(f"Concourse returned {response.status_code}: {response.text}")

    metric_data = []

    for family in text_string_to_metric_families(response.text):
        if family.name in INTERESTING_METRICS:
            for sample in family.samples:
                metric_data.append(
                    {
                        "MetricName": sample.name,
                        "Dimensions": labels_to_dimensions(sample.labels),
                        "Timestamp": timestamp,
                        "Value": sample.value,
                        "Unit": "Count",
                        "StorageResolution": 60,
                    }
                )

    print(dumps(metric_data))

    cloudwatch.put_metric_data(
        Namespace="Concourse",
        MetricData=metric_data,
    )


if __name__ == "__main__":
    handler(None, None)
