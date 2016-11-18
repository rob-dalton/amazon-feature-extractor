"""
========================================================================
-----------------------------------------------------------------------
ADD TOKENS AND CATEGORIES TO DATA
-----------------------------------------------------------------------
========================================================================

Script that subsets review data by asin and reviewText and creates
tokens for reviewText. Also adds product categories. Saves resulting
dataframe to S3.

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
    df_subset = df.select("asin", "reviewText")
    
    
    # add tokens
    df_tokens = dp.add_tokens(df_subset)
    
    
    # add categories
    df_cats = dp.add_categories(df_tokens, df_meta)
    
    # write to s3
    df_cats.write.mode('append').json("s3a://amazon-review-data/review-data")

