from flask import Flask, request, render_template, Markup
import cPickle as pickle
import json
import socket
import requests
import pandas as pd
import numpy as np
import pymongo as mdb
from transformations import trans_func, fraud_risk, model_importance

app = Flask(__name__)

# helper function
@app.context_processor
def add_vars_to_context():
    return dict(site_title="Fraud Detector")

# home page
@app.route('/')
def index():
    return render_template('index.html',
                            page_title="Home")

# hello world
@app.route('/hello')
def hello():
    return render_template('page.html',
                            page_title="Hello",
                            contents="Hello world!")


# broadcast data endpoint
@app.route('/endpoint', methods=['POST'] )
def endpoint():
    # accept data
    data = json.loads(request.data)

    # write data to collection
    result = coll.insert(data)

    return str(result)

# prediction
@app.route('/score', methods=['POST'])
def score():
    # setup vars
    prediction = False

    # collect and clean json
    raw_data = request.json
    data = np.array(trans_func(raw_data))

    # make prediction
    prediction = model.predict_proba(data)
    prob = prediction[0][0]
    risk = fraud_risk(prob)

    # insert row to DB
    row = { "data": raw_data,
            "prob": prob,
            "risk": risk }

    coll.insert_one(row)

    return render_template('score.html',
                            page_title="Score",
                            prediction=risk )


# prediction
@app.route('/summary')
def summary():
    # get data
    total = coll.find().count()
    percent_high = coll.find({"risk": "High Risk"}).count() / float(total)
    percent_medium = coll.find({"risk": "Medium Risk"}).count() / float(total)
    percent_low = coll.find({"risk": "Low Risk"}).count() / float(total)

    # get performance metrics
    feature_importances = model_importance(model)


    # get feature importances
    importance_html = '<table class="importances">'
    for feature, importance in feature_importances:
        line = '<tr><td class="label">{}</td><td>{}</td></tr>'.format(feature, importance)
        importance_html += line
    importance_html += '</table>'


    # get model performance
    performance_html = '<table class="performance">'
    for metric, value in perf_metrics:
        line = '<tr><td class="label">{}</td><td>{}</td></tr>'.format(metric, value)
        performance_html += line
    performance_html += '</table>'


    return render_template('summary.html',
                            page_title="Model Summary",
                            high=round(percent_high, 3) * 100,
                            medium=round(percent_medium, 3) * 100,
                            low=round(percent_low, 3) * 100,
                            importances=Markup(importance_html),
                            performance=Markup(performance_html)
                            )


# workflow page
@app.route('/overview')
def overview():
    return render_template('overview.html',
                            page_title="App Overview")


# functions
def register_url(my_ip, my_port):
    # setup vars
    reg_url = 'http://10.5.3.92:5000/register'

    # make request
    response = requests.post(reg_url, data={'ip': my_ip, 'port': my_port})

    return response.content

if __name__ == '__main__':
    # setup global vars
    conn = mdb.MongoClient()
    db = conn.fraud
    coll = db.cases
    my_port = 8080
    my_ip = socket.gethostbyname(socket.gethostname())
    with open('./model.pkl') as f:
        model = pickle.load(f)
    with open('./model_perf.pkl') as f:
        perf_metrics = pickle.load(f)

    # run setup functions
    register_url(my_ip, my_port)

    app.run(host='0.0.0.0', port=my_port, debug=True)
