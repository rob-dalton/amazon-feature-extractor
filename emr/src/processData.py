"""
========================================================================
-----------------------------------------------------------------------
PROCESS DATA
-----------------------------------------------------------------------
========================================================================

Script that subsets review data by (asin, overall, reviewText) and 
creates tokens from reviewText. Joins reviews with categories, salesRank
from metadata. Saves resulting dataframe to S3.

"""
import pyspark as ps
from sentimentAnalysis import dataProcessing as dp


if __name__ == "__main__":

    # create spark session
    spark = ps.sql.SparkSession.builder \
            .appName("reviewProcessing") \
            .getOrCreate()


    # get dataframes
    # specify s3 as sourc with s3a://
    df = spark.read.json("s3a://amazon-review-data/user_dedup.json.gz")
    df_meta = spark.read.json("s3a://amazon-review-data/metadata.json.gz")


    # subset asin, reviewText
    df_subset = df.select("asin", "overall", "reviewText")
    
    
    # add tokens
    df_tokens = dp.add_tokens(df_subset)
    
    
    # add metadata
    df_s3 = dp.join_metadata(df_tokens, df_meta)

    # unpersist unused dataframes
    df_meta.unpersist()
    df.unpersist()
    df_subset.unpersist()
    df_tokens.unpersist()
    
    # write to s3
    df_s3.write.json("s3a://amazon-review-data/review-data")

