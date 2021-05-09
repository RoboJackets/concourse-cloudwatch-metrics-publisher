"""
Publish Concourse metrics to CloudWatch
"""
from collections import Counter
from json import dumps
from os import environ
from time import time
from typing import Dict, List

from boto3 import client  # type: ignore

from prometheus_client.parser import text_string_to_metric_families  # type: ignore

from requests import get

cloudwatch = client("cloudwatch")


def handler(  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    event: None, context: None  # pylint: disable=unused-argument
) -> None:
    """
    Publish data points for Concourse metrics
    """
    timestamp = time()

    response = get(url=environ["CONCOURSE_METRICS_URL"])

    if response.status_code != 200:
        raise ValueError(f"Concourse returned {response.status_code}: {response.text}")

    parsed = text_string_to_metric_families(response.text)

    metric_data = []

    untagged_worker_count = 0
    concourse_builds_running = 0
    concourse_jobs_scheduling = 0

    for family in parsed:
        if family.type != "gauge":
            continue

        print(family)

        if family.name == "concourse_builds_running":
            concourse_builds_running = family.samples[0].value

        if family.name == "concourse_jobs_scheduling":
            concourse_jobs_scheduling = family.samples[0].value

        if family.name == "concourse_workers_containers":
            concourse_workers_containers: Dict[str, List[int]] = {}
            for sample in family.samples:
                tag = sample.labels["tags"]
                if tag == "":
                    tag = "none"
                    untagged_worker_count += 1

                if tag in concourse_workers_containers:
                    concourse_workers_containers[tag].append(sample.value)
                else:
                    concourse_workers_containers[tag] = [sample.value]

            for tag in concourse_workers_containers:
                values = []
                counts = []

                counter = Counter(concourse_workers_containers[tag])

                for key in counter:
                    values.append(key)
                    counts.append(counter[key])

                metric_data.append(
                    {
                        "MetricName": family.name,
                        "Dimensions": [{"Name": "tag", "Value": tag}],
                        "Timestamp": timestamp,
                        "Values": values,
                        "Counts": counts,
                        "Unit": "Count",
                        "StorageResolution": 60,
                    }
                )

        if family.name == "concourse_steps_waiting":
            concourse_steps_waiting: Dict[str, int] = {}
            for sample in family.samples:
                tag = sample.labels["workerTags"]
                if tag == "":
                    tag = "none"

                concourse_steps_waiting[tag] = concourse_steps_waiting.get(tag, 0) + sample.value

            for tag in concourse_steps_waiting:
                metric_data.append(
                    {
                        "MetricName": family.name,
                        "Dimensions": [{"Name": "tag", "Value": tag}],
                        "Timestamp": timestamp,
                        "Value": concourse_steps_waiting[tag],
                        "Unit": "Count",
                        "StorageResolution": 60,
                    }
                )

        if family.name == "concourse_workers_tasks":
            aggregated = []
            for sample in family.samples:
                aggregated.append(sample.value)

            values = []
            counts = []

            counter = Counter(aggregated)

            for key in counter:
                values.append(key)
                counts.append(counter[key])

            metric_data.append(
                {
                    "MetricName": family.name,
                    "Dimensions": [],
                    "Timestamp": timestamp,
                    "Values": values,
                    "Counts": counts,
                    "Unit": "Count",
                    "StorageResolution": 60,
                }
            )

    metric_data.append(
        {
            "MetricName": "concourse_builds_running_per_worker",
            "Dimensions": [],
            "Timestamp": timestamp,
            "Value": concourse_builds_running / (1 if untagged_worker_count == 0 else untagged_worker_count),
            "Unit": "Count",
            "StorageResolution": 60,
        }
    )

    metric_data.append(
        {
            "MetricName": "concourse_jobs_scheduling_per_worker",
            "Dimensions": [],
            "Timestamp": timestamp,
            "Value": concourse_jobs_scheduling / (1 if untagged_worker_count == 0 else untagged_worker_count),
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
