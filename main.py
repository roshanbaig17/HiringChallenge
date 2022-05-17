from services.metrics_service import MetricsService
from utilities import constants

def start_kafka_metrics():
    """
    Generate metrics from kafka
    :return:
    """
    metrics_service = MetricsService()
    metrics_service.generate_metrics_from_kafka(group_id=constants.KAFKA_CONSUMER_GROUP,seconds=5)

def manual_input():
    """
    Manually enter the json string to generate the metrics
    :return:
    """
    metrics_service = MetricsService()
    # We can enter {"ts": 1652767093,"uid": 1} as input
    text = input("Please enter json string")
    metrics_service.generate_metrics_from_json_string(json_str=text)


if __name__ == '__main__':
    start_kafka_metrics()