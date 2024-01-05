import apache_beam as beam
import numpy as np
import csv
import json
import logging


logging.basicConfig(
    
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),                      # log to the console
        logging.FileHandler('app.log', mode='w')      # log to a file named 'app.log'
    ]
)

logger = logging.getLogger('logger')
logger.setLevel(logging.INFO)


def print_element(element):
    print(element)


def add_timezone(lat, lng):
    import timezonefinderL                      # using timezonefinderL instead timezonefinder to speed up the computation
    
    try:
        tf = timezonefinderL.TimezoneFinder()   # the syntax would be the same, except that it uses timezonefinderL instead of timezonefinder
        lat = float(lat)
        lng = float(lng)
        return (lat, lng, tf.timezone_at(lng=lng, lat=lat))
    
    except Exception as e:
        return (lat, lng, 'N/A')


def tz_correct(line, airport_timezones):
    '''
    Gets the departure airport ID from flight's data, 
    then looks up the timezone for that airport ID from the airport's data.
    ---
    line                : 1st param, input from the previous beam steps (flights pipeline)
    airport_timezones   : 2nd param, input from the airport_timezone dictionary (airports pipeline)
    '''
    
    try:
        dep_airport_id = line['ORIGIN_AIRPORT_SEQ_ID']
        arr_airport_id = line['DEST_AIRPORT_SEQ_ID']
        
        dep_timezone = airport_timezones[dep_airport_id][2]
        arr_timezone = airport_timezones[arr_airport_id][2]

        for f in ['CRS_DEP_TIME', 'DEP_TIME', 'WHEELS_OFF']:
            line[f], deptz = as_utc(line['FL_DATE'], line[f], dep_timezone)
        for f in ['WHEELS_ON', 'CRS_ARR_TIME', 'ARR_TIME']:
            line[f], arrtz = as_utc(line['FL_DATE'], line[f], arr_timezone)

        for f in ['WHEELS_OFF', 'WHEELS_ON', 'CRS_ARR_TIME', 'ARR_TIME']:
            line[f] = add_24h_if_before(line[f], line['DEP_TIME'])
        
        line['DEP_AIRPORT_TZOFFSET'] = deptz
        line['ARR_AIRPORT_TZOFFSET'] = arrtz
        
        yield json.dumps(line)
    
    except KeyError as e:
        # print('error at', dep_airport_id)
        logger.exception('Ignoring because airport is not known: ' + str(line))


def as_utc (date, hhmm, tzone):
    '''
    Convert each of the datetimes reported in that airport's time zone to UTC.
    '''
    try:
        if len(hhmm) > 0 and tzone is not None:
            import datetime, pytz
            
            loc_tz = pytz.timezone(tzone)
            loc_dt = loc_tz.localize(datetime.datetime.strptime(date, '%Y-%m-%d'), is_dst=False)
            loc_dt += datetime.timedelta(hours=int(hhmm[:2]),
                                         minutes=int(hhmm[2:]))
            utc_dt = loc_dt.astimezone(pytz.utc)
            return utc_dt.strftime('%Y-%m-%d %H:%M:%S'), loc_dt.utcoffset().total_seconds()
        
        else:
            return '', 0
    
    except ValueError as e:
        logger.exception('Wrong format for {} {} {}'.format(date, hhmm, tzone))
        return 'incorrect format at {}, {}, {}'.format(date, hhmm, tzone), 0
        # raise e
    

def add_24h_if_before(arrtime, deptime):
    '''
    Add 24h if arrtime is less than deptime due to timezone difference 
    between departure and arrival airport.
    '''
    import datetime

    if len(arrtime) > 0 and len(deptime) > 0 and arrtime < deptime:
        adt = datetime.datetime.strptime(arrtime, '%Y-%m-%d %H:%M:%S')
        adt += datetime.timedelta(hours=24)
        return adt.strftime('%Y-%m-%d %H:%M:%S')
    else:
        return arrtime
    

def get_next_event(line):
    
    line = json.loads(line)
    
    if len(line['DEP_TIME']) > 0:
        event = dict(line)
        event['EVENT_TYPE'] = 'departed'
        event['EVENT_TIME'] = line['DEP_TIME']

        for f in ['TAXI_OUT', 'WHEELS_OFF', 'WHEELS_ON',
                  'TAXI_IN', 'ARR_TIME', 'ARR_DELAY', 'DISTANCE']:
            event.pop(f, None)
        
        yield event


    if len(line['ARR_TIME']) > 0:
        event = dict(line)
        event['EVENT_TYPE'] = 'arrived'
        event['EVENT_TIME'] = line['ARR_TIME']
        
        yield event


    if len(line['WHEELS_OFF']) > 0:
        event = dict(line)
        event['EVENT_TYPE'] = 'wheelsoff'
        event['EVENT_TIME'] = line['WHEELS_OFF']
        
        for f in ['WHEELS_ON', 'TAXI_IN', 'ARR_TIME', 'ARR_DELAY', 'DISTANCE']:
            event.pop(f, None)
        
        yield event


def run():
    with beam.Pipeline('DirectRunner') as pipeline:

        airports = (pipeline
            | 'airports:read' >> beam.io.ReadFromText('airports.csv.gz', skip_header_lines=1)                       # reads lines from a text file named 'airport.csv.gz'
            | 'airports:parse' >> beam.Map(lambda line: next(csv.reader([line])))                                   # parse each line into a list of fields
            | 'airports:filter' >> beam.Filter(lambda line: "United States" in line)                                # filter only the US airport, as the flights only contain flights from/to the US
            | 'airports:timezone' >> beam.Map(lambda fields: (fields[0], add_timezone(fields[21], fields[26])))     # maps each list of fields to a tuple, taking the AIRPORT_SEQ_ID, and (LATITUDE, LONGITUDE), and take the timezone based on LAT and LNG using custom function
            # | 'airports:debug' >> beam.io.WriteToText('output_airport', file_name_suffix=".txt")                  
        )
        
        flights = (pipeline
            | 'flights:read' >> beam.io.ReadFromText('flights_sample.json')                                         # reads lines from a text file
            | 'flights:parse' >> beam.Map(lambda line: json.loads(line))                                            # parses each line of the flights data into a Python dictionary
            | 'flights:tzcorrect' >> beam.FlatMap(tz_correct, beam.pvalue.AsDict(airports))

            # ^ FlatMap is a transformation that applies a given function to each element in the input collection and produces zero or more output elements for each input element
            # while map is only 1-to-1 transformation, FlatMap is 0 to n transformation
            # (tz_correct) is the function being applied to each element, refer to tz_correct
            # (beam.pvalue.AsDict(airports)) is an argument being passed to tz_correct

            | 'flights:filter' >> beam.Filter(lambda line: 'incorrect format at' not in line)                       # for now, we will filter out data that has incorrect format
            # | 'flights:debug' >> beam.io.WriteToText('output_flights', file_name_suffix=".txt") 
        )
        
        events = flights | 'events:getevents' >> beam.FlatMap(get_next_event)

        # ^ different from the previous flatmap, this flatmap apply get_next_event to flights PCollection data
        # thus, that's the reason why it only takes one parameter but has "flights |" at the beginning of the syntax

        (events
            | 'events:tostring' >> beam.Map(lambda line: json.dumps(line))
            | 'events:write' >> beam.io.WriteToText('output_events', file_name_suffix='.txt')
        )


if __name__ == '__main__':
    run()