import os
import uuid
from datetime import datetime
from multiprocessing import Process, Queue, Manager
from flask import Flask, jsonify, make_response, request

from generate_report import generate_report_class

app = Flask(__name__)


@app.route("/trigger_report", methods=["GET"])
def trigger_report():
    report_id = str(uuid.uuid4())
    request_queue.put([report_id, datetime.now()])
    report_status[report_id] = {"STATUS": "RUNNING", "REPORT_PATH": ""}
    return jsonify({"REPORT_ID": report_id})


@app.route("/get_report/<id>", methods=["GET"])
def get_report(id):
    if id not in report_status.keys():
        return jsonify(
            {"STATUS": "AN ERROR OCCURRED", "ERROR": "THE ID USED DOESN'T EXIST"}
        )

    return jsonify(report_status[id])


def consumer(queue, report_status):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    print("dir_path", dir_path, os.path.join(dir_path, "/data/store status.csv"))
    obj = generate_report_class(
        os.path.join(dir_path, "data/store status.csv"),
        os.path.join(dir_path, "data/Menu hours.csv"),
        os.path.join(dir_path, "data/bq-results-20230125-202210-1674678181880.csv"),
        os.path.join(dir_path, "reports/"),
    )
    while 1:
        if not (queue.empty()):
            report_id, triggered_time = queue.get()
            try:
                report_path = obj.generate_detailed_report(triggered_time, report_id)
                report_status[report_id] = {
                    "STATUS": "DONE",
                    "REPORT_PATH": report_path,
                }
            except Exception as e:
                report_status[report_id] = {
                    "STATUS": "AN ERROR OCCURRED",
                    "ERROR": str(e),
                    "REPORT_PATH": "",
                }


if __name__ == "__main__":
    request_queue = Queue()
    manager = Manager()
    report_status = manager.dict()
    consumer_process = Process(
        target=consumer,
        args=(
            request_queue,
            report_status,
        ),
    )
    consumer_process.start()
    app.run(debug=True)
    consumer_process.join()
