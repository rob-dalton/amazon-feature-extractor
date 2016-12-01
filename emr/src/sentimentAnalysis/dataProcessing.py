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
    df_raw_tokens_clean = df.withColumn("rawTokens", remove_empty_udf(df["rawTokens"]))

    return df_raw_tokens_clean


def tokenize(df):
    # instantiate tokenizer
    tokenizer = Tokenizer(inputCol="cleanText", outputCol="rawTokens")

    # create tokens
    df_raw_tokens = tokenizer.transform(df)

    # remove empty tokens
    df_raw_tokens_clean = remove_empty_tokens(df_raw_tokens)

    return df_raw_tokens_clean


def remove_stop_words(df):
    remover = StopWordsRemover(inputCol="rawTokens", outputCol="tokens", stopWords=stopwords.words("english"))
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
    cv = CountVectorizer(inputCol="tokens", outputCol="tf_vector", minDF=2.0)
    tf_model = cv.fit(df)
    df_tf = tf_model.transform(df)

    vocab = tf_model.vocabulary

    return df_tf, vocab


def add_tfidf(df):
    idf = IDF(inputCol="tf_vector", outputCol="tfidf_vector")
    idf_model = idf.fit(df)
    df_tfidf = idf_model.transform(df)

    return df_tfidf


"""
========================================
TFIDF MAPPING FUNCTIONS
========================================
Functions to map elements in TFIDF
vectors to terms in vocabularies

"""
def extract_top_features(tfidf_vector, vocab, n):
    """
    INPUT: SparseVector, List, Int
    RETURN: List

    Take in TFIDF vector, vocabulary for vector,
    and number of terms. Return top n terms

    """
    # note - tfidf elements are pre-sorted by importance
    term_indices = tfidf_vector.indices[-n:]
    
    # map features to terms
    features = [[vocab[i], tfidf_vector[i]] for i in term_indices]

    return features


def add_top_features(df, vocab, n=10):
    """
    INPUT: PySpark DataFrame, List, Int
    RETURN: PySpark DataFrame

    Take in DataFrame with TFIDF vectors, list of vocabulary words,
    and number of features to extract. Map top features from TFIDF
    vectors to vocabulary terms. Return new DataFrame with terms
    
    """
    # Create udf function to extract top n features
    extract_features_udf = udf(lambda x: extract_top_features(x, vocab, n))

    # Apply udf, create new df with features column
    df_features = df.withColumn("top_features",
                                    extract_features_udf(df["tfidf_vectors_sum"]))


    return df_features


def add_pos_neg_features(df, vocab_pos, vocab_neg, n=10):
    """
    INPUT: Spark DataFrame, List, List, Int
    RETURN: Spark DataFrame

    Take in DataFrame grouped by asin, positive with tfidf vectors summed.
    Extract top positive and negative terms from each group, add features column

    """
    # split dataframe on postitive
    df_pos = df.where(df.positive==True)
    df_neg = df.where(df.positive==False)

    # add features
    df_pos_terms = add_top_features(df_pos, vocab_pos, n)
    df_neg_terms = add_top_features(df_neg, vocab_neg, n)
    
    return df_pos_terms.unionAll(df_neg_terms)


"""
========================================
METADATA FUNCTIONS
========================================
Functions to join product review data
with metadata

"""
def join_metadata(df_products, df_meta):
    # select fields to join
    df_meta_subset = df_meta.select("asin", "title", "categories")

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
