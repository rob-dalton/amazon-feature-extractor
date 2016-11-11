# Amazon Product Recommender
## Overview
An application that extracts the most criticized attributes of negatively reviewed products and the most praised attributes of highly reviewed products. The application then recommends products based on a user specified product type and desired attributes.

*PLEASE NOTE: The product review data used is not current. Many products will no longer exist.*

## Steps

### Model
1. Run <code>scripts/pyspark_emr.ipy</code>
2. Upload <code>scripts/jupyspark_emr.sh</code> to EMR cluster
3. Install and configure <code>awscli</code> on cluster
4. Run <code>src/clean_data.py</code>
5. Run <code>src/extract_features.py</code>
6. Run <code>src/build_model.py</code>

*Steps 4-6 in progress*

### App
*In progress*

## Data
The data is sourced from Amazon, and was originally used in the following paper:<br>
<div style="margin-left: 1em">
  *Inferring networks of substitutable and complementary products*<br>
  <b>J. McAuley, R. Pandey, J. Leskovec</b><br>
  Knowledge Discovery and Data Mining, 2015<br>
  <a href="http://cseweb.ucsd.edu/~jmcauley/pdfs/kdd15.pdf">pdf</a>
</div>
