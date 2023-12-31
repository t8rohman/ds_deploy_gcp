import os
import logging
from flask import Flask
from flask import request, escape, jsonify
from ingest_flights import wrapper

app = Flask(__name__)

@app.route("/", methods=['POST'])
def ingest_flights():
    try:
        json = request.get_json()
        year = escape(json['year']) if 'year' in json else None
        month = escape(json['month']) if 'month' in json else None
        destdir = escape(json['destdir']) if 'destdir' in json else None

        wrapper(year, month, destdir)

        return jsonify({"message": "Flights data for {}_{} has been ingested successfully.".format(year, month)})
    
    except Exception as e:
        logging.exception("Failed to ingest, try again later!")


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))


'''
we can invoke this locally using terminal / cmd, by curl or httpie.

- curl: curl -X POST -H "Content-Type: application/json" -d '{"year": "xxxx", "month": "xx", "destdir": "xxxx"}' http://localhost:8080/
- httpie: check on google
'''