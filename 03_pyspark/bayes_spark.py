#!/usr/bin/env python
# coding: utf-8

## Developing Bayes Model on Apache Spark

import pandas as pd
import json
from sqlalchemy import create_engine

from pyspark.sql import SparkSession
import pyspark.sql.functions as F   

import numpy as np
import seaborn as sns

def run_bayes(credential_loc, jar_driver_path=None):

    # create SparkSession

    if jar_driver_path:
        # add the extraClassPath configuration if jar_driver_path is defined
        # specify the driver path for jar_driver_path variable
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


    with open(credential_loc) as json_file:
        postgres_prof = json.load(json_file)

    conn_params = {
        'host': postgres_prof['host'],              # local host
        'database': postgres_prof['database'],      # database name, we want to save it to learn_ds_deploy_gcp
        'user': postgres_prof['user'],              # user
        'password': postgres_prof['password']       # password
    }

    table_name_flights = 'ex3_raw_flights_to2023'
    table_name_traindays = 'ex3_trainday'

    jdbc_url = f"jdbc:postgresql://localhost:5432/{conn_params['database']}"


    # load flights data and traindays (training records) from postgresql database
    # and create a temporary view so we can employ SQL on the dataframe

    flights = spark.read \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name_flights) \
        .option("user", postgres_prof['user']) \
        .option("password", postgres_prof['password']) \
        .load()

    flights.createOrReplaceTempView('flights')

    traindays = spark.read \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver")  \
        .option("url", jdbc_url) \
        .option("dbtable", table_name_traindays) \
        .option("user", postgres_prof['user']) \
        .option("password", postgres_prof['password']) \
        .load()

    traindays.createOrReplaceTempView('traindays')

    query = '''
        SELECT
            f.FL_DATE AS date,
            f.DISTANCE AS distance,
            f.DEP_DELAY AS dep_delay,
            f.ARR_DELAY AS arr_delay,
            IF(f.ARR_DELAY < 15, 1, 0) AS ontime
        FROM
            flights AS f
        JOIN
            traindays AS t
        ON
            f.FL_DATE == t.FL_DATE
        WHERE
            t.is_train_day == TRUE AND f.DEP_DELAY IS NOT NULL
        ORDER BY
            f.DEP_DELAY DESC
    '''

    # execut the query
    # and save it as a spark dataframe

    flights = spark.sql(query)     


    # the approxQuantile method is useful for calculating quantiles for spark df

    distthresh = flights.approxQuantile('distance',                      # params 1: column to make the quantiles
                                        list(np.arange(0, 1.0, 0.2)),    # params 2: create quantiles for 0.2, 0.4, 0.6, 0.8
                                        0.02                             # params 3: relative error, which controls the accuracy of the approximation
                                        )

    delaythresh = flights.approxQuantile('dep_delay',
                                        list(np.arange(0, 1.0, 0.2)),
                                        0.02
                                        )


    # import functions, allowing to use spark's built-in functions in the code.

    delaythresh = range(10, 20)

    df = pd.DataFrame(columns=['dist_thresh', 'delay_thresh', 'frac_ontime'])

    for m in range(0, len(distthresh) - 1):
        for n in range(0, len(delaythresh) - 1):
            bdf = flights[(flights['distance'] >= distthresh[m])
                & (flights['distance'] < distthresh[m+1])
                & (flights['dep_delay'] >= delaythresh[n])
                & (flights['dep_delay'] < delaythresh[n+1])]
            
            '''
            use sum to calculate number of ontime flights
            use count to calculate all flights in the data
            then divide the two numbers to get the ontime fraction
            use "collect" method to get all the numbers from spark distributed to the local machine
            see the reference below on what it would look like
            '''
            
            ontime_frac = bdf.agg(F.sum('ontime')).collect()[0][0] / bdf.agg(F.count('ontime')).collect()[0][0]
            print (m, n, ontime_frac)

            # append the result to a dataframe

            df = df.append({
                'dist_thresh': distthresh[m], 
                'delay_thresh': delaythresh[n],
                'frac_ontime': ontime_frac
            }, ignore_index=True)

    df['score'] = abs(df['frac_ontime'] - 0.7)
    bayes = df.sort_values(['score']).groupby('dist_thresh').head(1).sort_values('dist_thresh')
    bayes.to_csv('bayes.csv')


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Create Bayes model table to predict flight delays based on distance.')
    parser.add_argument('--credential', help='Location of credentials for PostgreSQL db. Include host, database, user, and password.', required=True)
    parser.add_argument('--jardriverpath', help='Location of jar driver path, if the Spark needs the Java driver to be included.')

    args = parser.parse_args()

    run_bayes(credential_loc=args.credential, jar_driver_path=args.jardriverpath)