from flask import Flask, request, render_template, Markup, url_for
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
    return dict(site_title="Amazon Feature Extractor")

# home page
@app.route('/')
def index():
    return render_template('index.html',
                            page_title="Home")

# search results page
@app.route('/search', methods=['GET', 'POST'])
def search():
    # create index
    coll.create_index([("title", "text")])

    # setup query
    query=request.form['queryText']

    # get search results
    search_results = coll.find({'$text':{'$search':query}},
                               {'score': {'$meta': 'textScore'}})

    # sort results
    search_results.sort([('score', {'$meta': 'textScore'})]).limit(15)

    # collect results
    results = [result for result in search_results]

    # extract output
    results_html = ""
    for result in results:
        title = result["title"]
        asin = result["asin"]

        # get avg rating
        sum_ratings = sum([result["ratings"][key]*int(key) for key in result["ratings"]])
        count_ratings = sum([result["ratings"][key] for key in result["ratings"]])
        avg_rating = sum_ratings * 1.0 / count_ratings

        #create html
        url = url_for("product", asin=asin)
        html = '<div class="search-result">'
        html += '<span class="result-title"><a href={}>{}</a></span>'.format(url, title)
        html += '<span class="avg-rating">{}</span></div>'.format(round(avg_rating, 2))

        results_html += html

    return render_template('search.html',
                            page_title="Search Results",
                            results=Markup(results_html)
                          )
# product page
@app.route('/product/<string:asin>')
def product(asin):
    # setup vars
    product = coll.find_one({'asin':asin})
    title = product["title"]

    # get avg rating
    sum_ratings = sum([product["ratings"][key]*int(key) for key in product["ratings"]])
    count_ratings = sum([product["ratings"][key] for key in product["ratings"]])
    avg_rating = round(sum_ratings * 1.0 / count_ratings, 2)


    # build html for ratings
    ratings_html = '<div class="avg-rating">{}</div>'.format(avg_rating)

    # build html for ratings (distribution)
    dist_bars_html = ""
    dist_bar_html = '<div class="bar-row"><span class="rating">{}</span>' \
            + '<span class="bar"><span class="fill" style="width:{}%;"></span></span>' \
            + '<span class="count">{}%</span></div>'

    """
    # 5 star dist
    for rating in reversed(range(1,6)):
        count = product["ratings"].get(str(rating), 0)
        bar_width = round(count * 100.0 / count_ratings, 1)
        dist_bars_html += dist_bar_html.format(rating,
                                             bar_width,
                                             bar_width)
    """

    # pos neg dist
    rating_types = {"+": (5,4), "-": (2,1)}
    for rating_type in rating_types:
        ratings = rating_types[rating_type]
        count = product["ratings"].get(str(ratings[0]), 0)
        count += product["ratings"].get(str(ratings[1]), 0)

        bar_width = round(count * 100.0 / count_ratings, 1)
        dist_bars_html += dist_bar_html.format(rating_type,
                                             bar_width,
                                             bar_width)

    
    # add dist bars html
    ratings_html += '<div class="dist-ratings">{}</div>'.format(dist_bars_html)


    # build html for posFeatures
    posFeatures_html = "None"
    if "posFeatures" in product.keys():
        temp = ""
        for feature in product["posFeatures"]:
            temp += feature+", "
        posFeatures_html = '<div class="posFeatures">{}</div>'.format(temp.strip(', '))

    # build html for negFeatures
    negFeatures_html = "None"
    if "negFeatures" in product.keys():
        temp = ""
        for feature in product["negFeatures"]:
            temp += feature+", "
        negFeatures_html = '<div class="negFeatures">{}</div>'.format(temp.strip(", "))

    return render_template('product.html',
                           page_title=title,
                           product_title=title,
                           product_ratings=Markup(ratings_html),
                           pos_features=Markup(posFeatures_html),
                           neg_features=Markup(negFeatures_html)
                          )


if __name__ == '__main__':
    # setup global vars
    conn = mdb.MongoClient()
    db = conn.reviews
    coll = db.products
    my_port = 8080
    my_ip = socket.gethostbyname(socket.gethostname())

    app.run(host='0.0.0.0', port=my_port, debug=True)
