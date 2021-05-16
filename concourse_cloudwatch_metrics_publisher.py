"""
Publish Concourse metrics to CloudWatch
"""
from collections import Counter
from json import dumps
from os import environ
from time import time
from typing import Any, Dict, List, Tuple

from boto3 import client  # type: ignore

from prometheus_client import Metric  # type: ignore
from prometheus_client.parser import text_string_to_metric_families  # type: ignore

from requests import get

cloudwatch = client("cloudwatch")

worker_tags = {}


def all_samples_aggregated_by_tag(metric: Metric, timestamp: float) -> Tuple[int, List[Dict[str, Any]]]:
    """
    Maps a Prometheus metric to one or more CloudWatch metrics, publishing all values and aggregating by worker
    tag. Also counts the number of untagged workers for later use.

    :param metric: metric family object
    :param timestamp: when the data was retrieved
    :return: tuple of untagged worker count and CloudWatch metric data
    """
    metric_data = []
    samples_for_tag: Dict[str, List[int]] = {}
    untagged_workers = 0

    for sample in metric.samples:
        tag = sample.labels["tags"]
        if tag == "":
            untagged_workers += 1
            tag = "none"

        if tag in samples_for_tag:
            samples_for_tag[tag].append(sample.value)
        else:
            samples_for_tag[tag] = [sample.value]

        worker_tags[sample.labels["worker"]] = tag

    for tag in samples_for_tag:
        values = []
        counts = []

        counter = Counter(samples_for_tag[tag])

        for value in counter:
            values.append(value)
            counts.append(counter[value])

        metric_data.append(
            {
                "MetricName": metric.name,
                "Dimensions": [{"Name": "tag", "Value": tag}],
                "Timestamp": timestamp,
                "Values": values,
                "Counts": counts,
                "Unit": "Count",
                "StorageResolution": 60,
            }
        )

    return untagged_workers, metric_data


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

    metric_data: List[Dict[str, Any]] = []

    untagged_worker_count = 0
    concourse_builds_running = 0

    for metric in parsed:
        if metric.type != "gauge":
            continue

        print(metric)

        if metric.name == "concourse_builds_running":
            concourse_builds_running = metric.samples[0].value

        if metric.name == "concourse_workers_containers":
            (untagged_worker_count, concourse_workers_containers) = all_samples_aggregated_by_tag(metric, timestamp)
            print(concourse_workers_containers)
            metric_data.extend(concourse_workers_containers)

        if metric.name == "concourse_workers_volumes":
            (untagged_worker_count, concourse_workers_volumes) = all_samples_aggregated_by_tag(metric, timestamp)
            print(concourse_workers_volumes)
            metric_data.extend(concourse_workers_volumes)

        if metric.name == "concourse_steps_waiting":
            concourse_steps_waiting: Dict[str, int] = {}
            for sample in metric.samples:
                tag = sample.labels["workerTags"]
                if tag == "":
                    tag = "none"

                concourse_steps_waiting[tag] = concourse_steps_waiting.get(tag, 0) + sample.value

            for tag in concourse_steps_waiting:
                metric_data.append(
                    {
                        "MetricName": metric.name,
                        "Dimensions": [{"Name": "tag", "Value": tag}],
                        "Timestamp": timestamp,
                        "Value": concourse_steps_waiting[tag],
                        "Unit": "Count",
                        "StorageResolution": 60,
                    }
                )

        if metric.name == "concourse_workers_tasks":
            samples_for_tag: Dict[str, List[int]] = {}

            for sample in metric.samples:
                worker = sample.labels["worker"]
                if worker in worker_tags:
                    tag = worker_tags[worker]
                else:
                    continue

                if tag in samples_for_tag:
                    samples_for_tag[tag].append(sample.value)
                else:
                    samples_for_tag[tag] = [sample.value]

            for tag in samples_for_tag:
                values = []
                counts = []

                counter = Counter(samples_for_tag[tag])

                for value in counter:
                    values.append(value)
                    counts.append(counter[value])

                metric_data.append(
                    {
                        "MetricName": metric.name,
                        "Dimensions": [{"Name": "tag", "Value": tag}],
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

    print(dumps(metric_data))

    cloudwatch.put_metric_data(
        Namespace="Concourse",
        MetricData=metric_data,
    )


if __name__ == "__main__":
    handler(None, None)
