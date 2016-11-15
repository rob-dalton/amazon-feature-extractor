"""
----------------------------------------
AMAZON REVIEWS CLEANER FUNCTIONS
----------------------------------------

Module containing helper functions for
data cleaning.

"""
import pyspark
import json
import pandas as pd
import numpy as np
from pyspark.sql.functions import udf
from pyspark.ml.feature import Tokenizer, CountVectorizer, StopWordsRemover, NGram, IDF
from nltk.corpus import stopwords

# TFIDF helper functions
def tokenize(df):
    tokenizer = Tokenizer(inputCol="reviewText", outputCol="raw_tokens")
    df_raw_tokens = tokenizer.transform(df)

    return df_raw_tokens


def remove_stop_words(df):
    remover = StopWordsRemover(inputCol="raw_tokens", outputCol="tokens", stopWords=stopwords.words("english"))
    df_tokens = remover.transform(df)

    return df_tokens


def add_tf_and_vocab(df):
    cv = CountVectorizer(inputCol="tokens", outputCol="tf_vector")
    tf_model = cv.fit(df)
    df_tf = tf_model.transform(df)

    vocab = tf_model.vocabulary

    return df_tf, vocab


def add_idf(df):
    idf = IDF(inputCol="tf_vector", outputCol="idf_vector")
    idf_model = idf.fit(df)
    df_idf = idf_model.transform(df)

    return df_idf


def add_tfidf(df):
    # tokenize
    df_raw_tokens = tokenize(df)

    # remove stopwords
    df_tokens = remove_stop_words(df_raw_tokens)

    # add tf vectors, get vocabulary
    df_tf, vocab = add_tf_and_vocab(df_tokens)

    # add idf vectors
    df_idf = add_idf(df_tf)

    return df_idf, vocab

# Extract features
"""
def extract_top_n_features(idf_vector, vocab, n):
    # Get indices of top n features
    # note - tfidf elements are pre-sorted by importance
    term_indices = tfidf_vector.indices[-n:]

    # Map features to terms
    features = [vocab[i] for i in term_indices]

    return features
"""

def add_top_features(df_tfidf, vocab, n=10):

    def extract_top_features(idf_vector, n):
        # Get indices of top n features
        # note - tfidf elements are pre-sorted by importance
        term_indices = idf_vector.indices[-n:]

        # Map features to terms
        features = [vocab[i] for i in term_indices]

        return features

    # Create udf function to extract top n features
    extract_features_udf = udf(lambda x: extract_top_features(x, n))

    # Apply udf, create new df with features column
    df_features = df_tfidf.withColumn("top_features",
                                    extract_features_udf(df_tfidf["idf_vector"]))

    return df_features


if __name__=="__main__":
    pass
