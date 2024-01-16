#!/usr/bin/env python
# coding: utf-8

# # Machine Learning Ops with TensorFlow Model

import json
import logging

import tensorflow as tf
from tensorflow.keras.callbacks import EarlyStopping


logging.basicConfig(

level=logging.INFO,                                 # set this to logging.DEBUG to print debug as well
format='%(asctime)s - %(levelname)s - %(message)s',
handlers=[
    logging.StreamHandler(),                        # log to the console
    logging.FileHandler('train.log', mode='w')      # log to a file named 'train.log'
]

)
logger = logging.getLogger('logger')
logger.setLevel(logging.INFO)    


def features_and_labels(features):
    '''
    function to separate between label and predictors.
    '''
    label = features.pop('ONTIME')
    return features, label


def read_dataset(pattern, batch_size, mode=tf.estimator.ModeKeys.TRAIN, truncate=None):
    '''
    read the dataset from csv file to tensor dataset.
    '''

    dataset = tf.data.experimental.make_csv_dataset(pattern,        # create tensor dataset from a csv file, where the format will be {keys (col_names): value (tensor)}
                                                    batch_size,
                                                    num_epochs=1) 
    dataset = dataset.map(features_and_labels)                      

    if mode == tf.estimator.ModeKeys.TRAIN:
        '''
        introduce randomness only if in training mode
        '''
        dataset = dataset.shuffle(batch_size*10)    # it will introduce randomness by shuffling the dataset
        dataset = dataset.repeat()                  # repeat the dataset indefenitely
    dataset = dataset.prefetch(1)                   # prefetches one batch of data to improve performance by overlapping the data loading and model execution
    
    if truncate is not None:
        '''
        if this set True, the function limits the dataset 
        to the specified number of elements using dataset.take(truncate)
        only for debugging, to see sample of data
        '''
        dataset = dataset.take(truncate)
    
    return dataset


def create_model(SIMPLE):
    '''
    feature engineering and architect the model.
    '''

    # create inputs first by separating between real and sparse value

    col_real = ['DEP_DELAY', 'TAXI_OUT', 'DISTANCE']

    real = {
        colname: tf.feature_column.numeric_column(colname)
            for colname in col_real
    }

    inputs = {
        colname: tf.keras.layers.Input(name=colname, shape=(), dtype='float32')
            for colname in real.keys()
    }

    carrier_list = ['9E', 'AA', 'AS', 'B6', 'DL', 'F9', 'WN', 
                    'G4', 'HA', 'MQ', 'NK','YX', 'OO', 'OH', 'UA']

    sparse = {
        # use vocabulary_list if the number of unique category is relatively small
        'CARRIER': tf.feature_column.categorical_column_with_vocabulary_list(key='CARRIER', 
                                                                            vocabulary_list=carrier_list),
        # use hash_bucket if the number of unique category is large / unknown
        'ORIGIN': tf.feature_column.categorical_column_with_hash_bucket('ORIGIN', hash_bucket_size=1000),
        'DEST': tf.feature_column.categorical_column_with_hash_bucket('DEST', hash_bucket_size=100)
    }

    embed = {
        colname : tf.feature_column.embedding_column(col, 3)
            for colname, col in sparse.items()
    }

    inputs.update({
        colname: tf.keras.layers.Input(name=colname, shape=(), dtype='string')
            for colname in embed.keys()
    })

    # define the features by passing the inputs to the features
    
    features = tf.keras.layers.DenseFeatures(
                                            feature_columns= list(embed.values()) + list(real.values()),    # has to be in list format
                                            name='features'                                                 # name of the features, helpful for debugging and visualization
                                            )(inputs)
    
    model = model_spec(features, inputs, SIMPLE)
    

    def rmse(y_true, y_pred):
        # define the rmse formula for model metric
        return tf.sqrt(tf.reduce_mean(tf.square(y_pred - y_true)))
    

    model.compile(optimizer='adam',
                  loss='binary_crossentropy',
                  metrics=['accuracy', rmse])

    return model


def model_spec(features, inputs, SIMPLE):
    if SIMPLE == True:

        output = tf.keras.layers.Dense(1, activation='sigmoid', name='pred')(features)
        model = tf.keras.Model(inputs, output)

        return model
        
    else:
        h1 = tf.keras.layers.Dense(64, activation='relu')(features)
        h2 = tf.keras.layers.Dense(8, activation='relu')(h1)
        output = tf.keras.layers.Dense(1, activation='sigmoid')(h2)

        model = tf.keras.Model(inputs, output)

        return model


def train_and_evaluate(TRAIN_FILE_PATH, EVAL_FILE_PATH, SIMPLE=True, DEVELOP_MODE=True):

    '''
    main function of this program.
    the program will work mainly on this as
    this function will execute other functions here.
    '''

    train_batch_size = 64
    num_examples = 5000*1000

    if DEVELOP_MODE == True:
        eval_batch_size = 100
        steps_per_epoch = 3
        epochs = 2
    else:
        eval_batch_size = 100
        steps_per_epoch = num_examples // train_batch_size
        epochs = 10

    train_dataset = read_dataset(TRAIN_FILE_PATH, train_batch_size)
    eval_dataset = read_dataset(EVAL_FILE_PATH, eval_batch_size)

    # fit the model (train)

    model = create_model(SIMPLE)
    logger.info(f"Model's been created, now going to train the model.")

    # create an early stopping function
    # will stop the training after having no improvement for 3 epochs in a row

    early_stopping = EarlyStopping(monitor='val_loss', patience=3)

    history = model.fit(train_dataset,
                        validation_data=eval_dataset,
                        epochs=epochs,
                        steps_per_epoch=steps_per_epoch,
                        validation_steps=10,
                        callbacks=[early_stopping]
                        )
    logger.info(f"Model's been trained successfully, now going to save all the output for the model.")
    
    # output -> model performance to performance_val.json and performance_plot.png, the model to the local directory, 
    
    model_perform_graph(history)
    
    export_dir = 'flights_trained_model/'
    tf.saved_model.save(model, export_dir)

    with open('performance_val.json', 'w') as json_file:
        json.dump(history.history, json_file)

    return logger.info(f"Program has been ran successfully. Please check inside the local file.")


def model_perform_graph(history):
    import matplotlib.pyplot as plt

    nrows = 1
    ncols = 2
    fig = plt.figure(figsize=(10, 5))

    for idx, key in enumerate(['loss', 'accuracy']):
        ax = fig.add_subplot(nrows, ncols, idx+1)
        plt.plot(history.history[key])
        plt.plot(history.history['val_{}'.format(key)])
        plt.title('model {}'.format(key))
        plt.ylabel(key)
        plt.xlabel('epoch')
        plt.legend(['train', 'validation'], loc='upper left')

    plt.savefig('performance_plot.png')


if __name__ == '__main__':
    
    import argparse

    parser = argparse.ArgumentParser(description='Train an ML model to predict lateness of a flight.')
    parser.add_argument('--trainfile', help='locate the path of the training file (must be in CSV).', required=True)
    parser.add_argument('--evalfile', help='locate the path of the evaluation file (must be in CSV).', required=True)
    parser.add_argument('--simplemodel', help="set to True for a simple log regression model, while False for a more complicated one.", required=True)
    parser.add_argument('--devmode', help="set to True if you want to limit the resource of computation in a development environment.")

    args = parser.parse_args()

    logger.info(f"The program has been started. Will create the model now.")
    logger.info(f"Tensorflow version -- {tf.__version__}")

    train_and_evaluate(args.trainfile, args.evalfile, args.simplemodel, args.devmode)