"""
========================================
----------------------------------------
AMAZON REVIEWS - DATA PROCESSING
----------------------------------------
========================================

Module containing helper functions for
data cleaning.

"""
import pyspark
import json
import pandas as pd
import numpy as np
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
from pyspark.ml.feature import Tokenizer, CountVectorizer, StopWordsRemover, NGram, IDF
from nltk.corpus import stopwords
from string import maketrans


"""
========================================
TOKENIZATION FUNCTIONS
========================================
Functions to tokenize reviewText

"""
def clean_reviewText(df):
    # create translation table for punctuation
    intab = '~!@#$%^&*()-_+=[]}{\|;:"<>,.?/'
    outtab = '                              '
    punc_tab = maketrans(intab, outtab)

    # remove punctuation
    punc_trans_udf = udf(lambda x: x.encode("utf-8").translate(punc_tab))
    df_clean = df.withColumn("cleanText", punc_trans_udf(df["reviewText"]))

    return df_clean


def remove_empty_tokens(df):
    remove_empty_udf = udf(lambda x: filter(None, x), ArrayType(StringType()))
    df_raw_tokens_clean = df.withColumn("raw_tokens", remove_empty_udf(df["raw_tokens"]))

    return df_raw_tokens_clean


def tokenize(df):
    # instantiate tokenizer
    tokenizer = Tokenizer(inputCol="cleanText", outputCol="raw_tokens")

    # create tokens
    df_raw_tokens = tokenizer.transform(df)

    # remove empty tokens
    df_raw_tokens_clean = remove_empty_tokens(df_raw_tokens)

    return df_raw_tokens


def remove_stop_words(df):
    remover = StopWordsRemover(inputCol="raw_tokens", outputCol="tokens", stopWords=stopwords.words("english"))
    df_tokens = remover.transform(df)

    return df_tokens


def add_tokens(df):
    # clean
    df_clean = clean_reviewText(df)

    # tokenize
    df_raw_tokens = tokenize(df_clean)

    # remove stopwords
    df_tokens = remove_stop_words(df_raw_tokens)

    return df_tokens


"""
========================================
TFIDF VECTORIZATION FUNCTIONS
========================================
Functions to create TFIDF vectors and
extract vocabulary for vectors

"""
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


def add_tfidf(df_tokens):
    # add tf vectors, get vocabulary
    df_tf, vocab = add_tf_and_vocab(df_tokens)

    # add idf vectors
    df_idf = add_idf(df_tf)

    return df_idf, vocab


"""
========================================
TFIDF MAPPING FUNCTIONS
========================================
Functions to map elements in TFIDF
vectors to terms in vocabularies

"""
def add_top_features(df, vocab, n=10):

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
    df_features = df.withColumn("top_features",
                                    extract_features_udf(df["idf_vector"]))

    return df_features


def add_top_features_by_category(df, vocab, n=10):
    pass


"""
========================================
METADATA FUNCTIONS
========================================
Functions to join product review data
with metadata

"""
def add_categories(df_products, df_meta):
    # select fields to join
    df_meta_subset = df_meta.select("asin", "categories")

    # join fields on product id asin
    df_cats = df_products.join(df_meta_subset, df_products.asin == df_meta_subset.asin).drop(df_meta_subset.asin)

    return df_cats


"""
========================================
MAIN
========================================
"""
if __name__=="__main__":
    pass
