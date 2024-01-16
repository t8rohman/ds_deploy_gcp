FROM python:3.10.9-slim

# statements and log messages to immediately appear in knative logs
ENV PYTHONUNBUFFERED True

# copy local code to the container image
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./

# install dependencies
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "05_callpredict.py", "--inputpath", "$INPUT_PATH", "--modelpath", "$MODEL_PATH"]