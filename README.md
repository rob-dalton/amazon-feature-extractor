# Amazon Product Recommender
## Overview
An application that extracts the most criticized attributes of negatively reviewed products and the most praised attributes of highly reviewed products. Eventually, the application will recommend products based on a user specified product type and desired attributes.

*PLEASE NOTE: The product review data used is not current. Many products will no longer exist.*

## Steps

### Model
1. Run <code>scripts/pyspark_emr.ipy</code>
2. Upload <code>scripts/jupyspark_emr.sh</code> to EMR cluster.
3. Install and configure <code>awscli</code> on cluster.
4. Run <code>src/clean_data.py</code> on the cluster.
5. Run <code>src/extract_features.py</code> on the cluster.
6. Save cleaned JSON data to S3 bucket.

### App
1. Install python dependencies. Easiest to just install anaconda2.
2. Install MongoDB if not already installed.
3. Create database <code>reviews</code>.
4. Import cleaned JSON from S3 to local directory.
4. Use functions in <code>loadJson.py</code> to load JSON files to database.
5. Run <code>app.py</code>

## Data
The data is sourced from Amazon, and was originally used in the following paper:<br>
<div style="margin-left: 1em">
  *Inferring networks of substitutable and complementary products*<br>
  <b>J. McAuley, R. Pandey, J. Leskovec</b><br>
  Knowledge Discovery and Data Mining, 2015<br>
  <a href="http://cseweb.ucsd.edu/~jmcauley/pdfs/kdd15.pdf">pdf</a>
</div>
