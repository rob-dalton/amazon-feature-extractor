## EMR
### Overview
The code in the <code>emr</code> directory requires Spark to run. The files in this directory should be uploaded to an Amazon Web Services EMR instance running Spark and Hadoop.

### <code>.bash_profile</code>
- Bash profile
- Includes alias to run <code>jupyspark_emr.sh</code> from command line

### <code>bin/jupyspark_emr.sh</code>
- Bash script file
- Configures your spark cluster
- Spins up Jupyter notebook server
