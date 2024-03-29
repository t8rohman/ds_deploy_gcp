# ds_deploy_gcp
Studying and exercising material to deploy data science and machine learning model on top of GCP. All the contents here are the exercise material taken from Data Science on the Google Cloud Platform book by Valliappa Lakshmanan (2017). Some of the codes are modified to be able to be ran on local machine. 

For the complete repository, please refer to: https://github.com/GoogleCloudPlatform/data-science-on-gcp

The exercise includes:
1. 01_ingest
   - Ingesting data from external, here we're using <a href="www.transtats.bts.gov">BTS data</a>.
   - Deploy the ingestion app using <b>flask</b>.
   - Containerize it into a docker container.

2. 02_streaming
   - Create streaming transformation using <b>Apache Beam</b> on local file.
   - Moving and running the local transformation to <b>Google Dataflow</b>.
   - Simulate the streaming data, and publish it using <b>Google Pub/Sub</b>.
  
3. 03_pyspark
   - Create a simple bayes model using <b>Apache Spark.</b>
   - Shell command editor to convert notebook to .py file using nbconvert.
   - Can be ran on Google Dataproc, please check the source's github.
  
4. 04_sparkml
   - Create a logistic regression model using MLLib from <b>Pyspark.</b>
   - Evaluate the model manually.

5. 05_mlopstf
   - Program for training the model and save it into local drive.
   - Use the model to make a prediction on an input data.
   - All done using <b>TensorFlow</b> library.
   - Both of the programs are containerized already in a <b>Docker</b>.
