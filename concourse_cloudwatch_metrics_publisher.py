"""
Publish Concourse metrics to CloudWatch
"""
from json import dumps
from os import environ
from time import time
from typing import Dict, List

from boto3 import client  # type: ignore

from prometheus_client.parser import text_string_to_metric_families  # type: ignore

from requests import get

INTERESTING_METRICS = (
    "concourse_steps_waiting",
    "concourse_builds_running",
    "concourse_jobs_scheduling",
    "concourse_workers_containers",
    "concourse_workers_tasks",
)
IGNORE_LABELS = ("platform",)
REMAP_LABELS = {
    "workerTags": "tags",
    "teamId": "team",
}

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

        if key in REMAP_LABELS:  # pylint: disable=consider-using-get
            key = REMAP_LABELS[key]

        if value == "":
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

    response = get(url=environ["CONCOURSE_METRICS_URL"])

    if response.status_code != 200:
        raise ValueError(f"Concourse returned {response.status_code}: {response.text}")

    parsed = text_string_to_metric_families(response.text)

    metric_data = []
    flattened_data = set()

    for family in parsed:
        if family.name in INTERESTING_METRICS:
            print(family)

            for sample in family.samples:
                dimensions = labels_to_dimensions(sample.labels)

                flattened = sample.name + "_" + "_".join([dim["Name"] + "_" + dim["Value"] for dim in dimensions])

                if flattened in flattened_data:
                    raise ValueError(f"Found duplicate value for {flattened}")

                flattened_data.add(flattened)

                metric_data.append(
                    {
                        "MetricName": sample.name,
                        "Dimensions": dimensions,
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
