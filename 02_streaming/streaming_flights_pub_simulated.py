import google.cloud.pubsub_v1 as pubsub
import argparse
import time
import logging
import datetime
import pytz


def notify(publisher, topics, rows, simStartTime, programStart, speedFactor):
    '''
    accumulating the rows into batches
    publishing a batch
    and sleeping until it is time to publish the next batch
    '''
    # sleep computation
    def compute_sleep_secs(notify_time):
        time_elapsed = (datetime.datetime.utcnow() - programStart).seconds
        sim_time_elapsed = (notify_time - simStartTime).seconds / speedFactor
        to_sleep_secs = sim_time_elapsed - time_elapsed
        return to_sleep_secs
    
    # accumulates events into a dictionary (tonotify) based on their event type.

    tonotify = {}
    for key in topics:
        tonotify[key] = list()

    for row in rows:
        event, notify_time, event_data = row

        # how much time should we sleep?
        if compute_sleep_secs(notify_time) > 1:
            publish(publisher, topics, tonotify, notify_time)

            to_sleep_secs = compute_sleep_secs(notify_time)
            if to_sleep_secs > 0:
                logging.info('Sleeping {} seconds'.format(to_sleep_secs))
                time.sleep(to_sleep_secs)
        
        tonotify[event].append(event_data)
    
    # left-over records; notify again
    publish(publisher, topics, tonotify, notify_time)


def publish(publisher, topics, allevents):
    '''
    publishing a batch of events
    '''
    for key in topics:
        topic = topics[key]
        events = allevents[key]
        logging.info('Publishing {} {} events'.format(len(events), key))

        for event_data in events:
            # THE ULTIMATE FUNCTION HERE
            # to publish the event data into the topic
            publisher.publish(topic, event_data.encode)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Send simulated static flights events data from local file to Pub/Sub')
    parser.add_argument('--startTime', help='Example: 2015-05-01 00:00:00 UTC', required=True)
    parser.add_argument('--endTime', help='Example: 2015-05-03 00:00:00 UTC', required=True)
    parser.add_argument('--endTime', help='Example: 2015-05-03 00:00:00 UTC', required=True)
    parser.add_argument('--rows', help='location of the file', required=True)
    parser.add_argument('--project', help='your project id, to create pubsub topic', required=True)
    parser.add_argument('--speedFactor', help='Example: 60 implies 1 hour of data sent to Cloud Pub/Sub in 1 minute', required=True, type=float)

    args = parser.parse_args()

    publisher = pubsub.PublisherClient()    # calling pubsub instance

    topics = {}

    for event_type in ['wheelsoff', 'departed', 'arrived']:
        topics[event_type] = publisher.topic_path(args.project, event_type)         # it creates the topic path as projects/your-project-id/topics/event_type
        try:
            publisher.get_topic(topic=topics[event_type])                           # check if the topics exist, must be a full path as "topics[event_type]" instead of "event_type"
            logging.info('{} already exist.'.format(topics[event_type]))            # if it does, then it will skip
        except:
            logging.info('{} as a new topic created'.format(topics[event_type]))    # if not, then it creates new topic
            publisher.create_topic(name=topics[event_type])

    # initiate the program
            
    TIME_FORMAT = '%Y-%m-%d %H:%M:%S %Z'
        
    programStartTime = datetime.datetime.utcnow()
    simStartTime = datetime.datetime.strptime(args.startTime, TIME_FORMAT).replace(tzinfo=pytz.UTC)
    logging.info('Simulation start time is {}'.format(simStartTime))
    notify(publisher, topics, args.rows, simStartTime, programStartTime, args.speedFactor)