#!/usr/bin/env python
# coding: utf-8

import tensorflow as tf
import logging
import numpy as np
import json


logging.basicConfig(

level=logging.INFO,                                  # set this to logging.DEBUG to print debug as well
format='%(asctime)s - %(levelname)s - %(message)s',
handlers=[
    logging.StreamHandler(),                         # log to the console
    logging.FileHandler('predict.log', mode='w')     # log to a file named 'predict.log'
]
)

logger = logging.getLogger('logger')
logger.setLevel(logging.INFO)    


def predict(INPUT, MODEL_PATH):
    
    model = tf.saved_model.load(MODEL_PATH)
    model = model.signatures['serving_default']

    logger.info('Successfully loading the model, now reading the input.')

    if isinstance(INPUT, str):
        # load data from JSON file
        with open(INPUT, 'r') as file:
            input_values = json.load(file)
    
    elif isinstance(INPUT, dict):
        # use provided dictionary
        input_values = INPUT
    
    else:
        raise ValueError("Input data should be either a JSON file path or a dictionary.")

    input_values = {
        'DEP_DELAY': np.array([input_values['DEP_DELAY']], dtype=np.float32),
        'TAXI_OUT': np.array([input_values['TAXI_OUT']], dtype=np.float32),
        'DISTANCE': np.array([input_values['DISTANCE']], dtype=np.float32),
        'CARRIER': np.array([input_values['CARRIER']]),
        'ORIGIN': np.array([input_values['ORIGIN']]),
        'DEST': np.array([input_values['DEST']]),
    }

    logger.info('Successfully reading the input, now will produce the prediction.')

    result_pred = model(**input_values)
    result_pred = list(result_pred.values())[0].numpy()[0, 0]

    print('The probability the flight will arrive on time is: ', + result_pred)
    
    return result_pred


if __name__ == '__main__':
    
    import argparse

    parser = argparse.ArgumentParser(description='Train an ML model to predict lateness of a flight.')
    parser.add_argument('--inputpath', help='Locate the input, must be in a JSON dictionary format.', required=True)
    parser.add_argument('--modelpath', help='Locate where the model is located.', required=True)

    args = parser.parse_args()

    logger.info(f"The program has been started. Will produce the prediction now.")
    logger.info(f"Tensorflow version -- {tf.__version__}")

    predict(args.inputpath, args.modelpath)