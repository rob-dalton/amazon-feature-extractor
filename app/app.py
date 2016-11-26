from flask import Flask, request, render_template, Markup
import cPickle as pickle
import json
import socket
import requests
import pandas as pd
import numpy as np
import pymongo as mdb

app = Flask(__name__)

# helper function
@app.context_processor
def add_vars_to_context():
    return dict(site_title="Amazon Product Recommender")

# home page
@app.route('/')
def index():
    return render_template('index.html',
                            page_title="Home")

if __name__ == '__main__':
    # setup global vars
    conn = mdb.MongoClient()
    db = conn.fraud
    coll = db.cases
    my_port = 8080
    my_ip = socket.gethostbyname(socket.gethostname())
    
    app.run(host='0.0.0.0', port=my_port, debug=True)
