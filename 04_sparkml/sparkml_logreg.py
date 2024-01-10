#!/usr/bin/env python
# coding: utf-8

# ### Create and configure our spark session

from pyspark.sql import SparkSession
from pyspark import SparkContext

from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithLBFGS

import json
import logging


# setting up the logging level first

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


def run_sparkml(credential_loc, table_name_flights, table_name_traindays, jar_driver_path=None):
    
    # use sparkcontext to indicate the resource of our spark code
    # sparkcontext serves as the entry point for any spark functionalities
    # define it as "local", meaning that we'll use the local machine for our resource

    if jar_driver_path:
        '''
        add the extraClassPath configuration if jar_driver_path is defined
        specify the driver path for jar_driver_path variable
        '''
        spark = SparkSession \
            .builder \
            .config("spark.driver.extraClassPath", jar_driver_path) \
            .appName("Bayes classification using Spark") \
            .getOrCreate()
    else:
        spark = SparkSession \
        .builder \
        .appName("Bayes classification using Spark") \
        .getOrCreate()
    
    sc = spark.sparkContext


    with open(credential_loc) as json_file:
        postgres_prof = json.load(json_file)

    conn_params = {
            'host': postgres_prof['host'],              # local host
            'database': postgres_prof['database'],      # database name, we want to save it to learn_ds_deploy_gcp
            'user': postgres_prof['user'],              # user
            'password': postgres_prof['password']       # password
        }

    jdbc_url = f"jdbc:postgresql://localhost:5432/{conn_params['database']}"

    flights = spark.read \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name_flights) \
        .option("user", postgres_prof['user']) \
        .option("password", postgres_prof['password']) \
        .load()

    traindays = spark.read \
            .format("jdbc") \
            .option("driver", "org.postgresql.Driver")  \
            .option("url", jdbc_url) \
            .option("dbtable", table_name_traindays) \
            .option("user", postgres_prof['user']) \
            .option("password", postgres_prof['password']) \
            .load()

    flights.createOrReplaceTempView('flights')
    traindays.createOrReplaceTempView('traindays')

    logger.info('Successfully read the data from the PostgreSQL database.')


    query = '''
        SELECT
            f.DEP_DELAY, 
            f.TAXI_OUT, 
            f.ARR_DELAY, 
            f.DISTANCE
        FROM
            flights f
        JOIN
            traindays t
        ON
            f.FL_DATE == t.FL_DATE
        WHERE
            t.is_train_day == TRUE AND
            f.CANCELLED == 0 AND
            f.DIVERTED == 0
    '''

    traindata = spark.sql(query)    # execute the query


    ''' Label the data for y-variable '''

    def feat_tf(data):
        '''
        creating a LabeledPoint object where the label is a binary value 
        indicating whether 'ARR_DELAY' is less than 15
        '''
        label = LabeledPoint(
            float(data['ARR_DELAY'] < 15),
            [
                # list out all the features for the model here 
                # 'DEP_DELAY', 'TAXI_OUT', and 'DISTANCE'
                data['DEP_DELAY'],
                data['TAXI_OUT'],
                data['DISTANCE']
            ]   
        )
        return label

    # convert to rdd format first before applying the function
    # resillient distributed dataframe
    # before, it was only a spark dataframe, not an rdd

    traindata_tf = traindata.rdd.map(feat_tf)

    ''' Training the model '''

    # train the model using logisticregression
    # and print the intercept + weights

    lrmodel = LogisticRegressionWithLBFGS.train(traindata_tf, intercept=True)
    lrmodel.clearThreshold()

    # save the model by passing the sparkcontext and the path

    lrmodel.save(sc, path='model')

    logger.info('Successfully trained and saved the logistic regression model.')


    ''' Evaluating the model '''

    # load the test data

    query = '''
        SELECT
            f.DEP_DELAY, 
            f.TAXI_OUT, 
            f.ARR_DELAY, 
            f.DISTANCE
        FROM
            flights f
        JOIN
            traindays t
        ON
            f.FL_DATE == t.FL_DATE
        WHERE
            t.is_train_day == FALSE AND
            f.CANCELLED == 0 AND
            f.DIVERTED == 0
    '''

    testdata = spark.sql(query)

    # carry out the same pipeline like before
    # and predict all the datapoints using the model we built

    testdata_tf = testdata.rdd.map(feat_tf)
    labelpred = testdata_tf.map(lambda pred: (pred.label, lrmodel.predict(pred.features)))

    def eval(labelpred):

        # filtering cancel and nocancel 

        cancel = labelpred.filter(lambda data: data[1] < 0.7)
        nocancel = labelpred.filter(lambda data: data[1] >= 0.7)
        
        # calculate correct cancel and no cancel 

        corr_cancel = cancel.filter(lambda data: data[0] == int(data[1] >= 0.7)).count()
        corr_nocancel = nocancel.filter(lambda data: data[0] == int(data[1] >= 0.7)).count()

        cancel_denom = cancel.count()
        nocancel_denom = nocancel.count()

        # this is just to make sure that the denominator isn't zero
        # as numbers cannot be divided by zero

        if cancel_denom == 0:
            cancel_denom = 1
        if nocancel_denom == 0:
            nocancel_denom = 1

        # pass it into eval dictionary
            
        eval = {'total_cancel': cancel.count(),
                'correct_cancel': float(corr_cancel) / cancel_denom, 
                'total_noncancel': nocancel.count(), 
                'correct_noncancel': float(corr_nocancel) / nocancel_denom
            }
        
        return eval

    eval_json = eval(labelpred)

    # write the eval dictionary and save it into JSON file

    json_file_path = 'eval_result.json'

    with open(json_file_path, 'w') as jsonfile:
        json.dump(eval_json, jsonfile, indent=2)

    spark.stop()

    return logger.info('Successfully built the model and evaluate the result.')


if __name__ == '__main__':
    
    import argparse
    
    parser = argparse.ArgumentParser(description='Create Bayes model table to predict flight delays based on distance.')
    parser.add_argument('--credential', help='Location of credentials for PostgreSQL db. Include host, database, user, and password.', required=True)
    parser.add_argument('--jardriverpath', help='Location of jar driver path, if the Spark needs the Java driver to be included.')
    parser.add_argument('--tableflights', help='Table name where the flights data is located in the PostgreSQL.', required=True)
    parser.add_argument('--tabletrainday', help='Table name where the trainday data is located in the PostgreSQL.', required=True)
    
    args = parser.parse_args()

    logger.info('Starting to build the model.')
    run_sparkml(credential_loc=args.credential, 
                jar_driver_path=args.jardriverpath, 
                table_name_flights=args.tableflights, 
                table_name_traindays=args.tabletrainday)

    '''
    default value for the parameter to run this code
    --credential = 'cache/credential_postgres.json'
    --jardriverpath = 'cache/postgresql-42.7.1.jar'
    --tableflights = 'ex3_raw_flights_to2023'
    --tabletrainday = 'ex3_trainday'
    '''