import datetime
import time
import pytz
import logging


def receive_real_time_events():
    '''
    assume we have a function to receive real-time events from a data stream
    simulated real-time events for demonstration
    '''
    events = [
        ("departed", datetime.datetime.utcnow(), "Event data 1"),
        ("arrived", datetime.datetime.utcnow(), "Event data 2"),
        # Add more real-time events as needed
    ]
    return events


def publish(publisher, topics, event_type, event_data):
    timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    topic = topics[event_type]
    logging.info(f'Publishing {event_type} {timestamp}')
    publisher.publish(topic, event_data.encode(), EventTimeStamp=timestamp)


def process_real_time_events(publisher, topics):
    while True:
        # receive real-time events
        real_time_events = receive_real_time_events()

        for event_type, notify_time, event_data in real_time_events:
            # process each real-time event
            publish(publisher, topics, event_type, event_data)

        # introduce dynamic sleep or remove sleep depending on your requirements
        time.sleep(1)  # Example: Sleep for 1 second between iterations


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Send simulated static flights events data from local file to Pub/Sub')
    parser.add_argument('--project', help='your project id, to create pubsub topic', required=True)

    # assume you have a dictionary of topics for different event types
    topics = {
        "departed": "departed_topic",
        "arrived": "arrived_topic",
        # add more topics as needed
    }

    publisher = pubsub.PublisherClient()

    # process real-time events
    process_real_time_events(publisher, topics)