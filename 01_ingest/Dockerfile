FROM python:3.10.9-slim

# statements and log messages to immediately appear in knative logs
ENV PYTHONUNBUFFERED True

# copy local code to the container image
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./

# install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# run the web service on container start up using gunicorn
# gunicorn offers performance, flexibility, and ease of use --suitable for django, flask, etc.
# set workers and threads according to the cpu power
# timeout is set to 0 to disable the timeouts of the workers to allow Cloud Run to handle instance scaling.
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app