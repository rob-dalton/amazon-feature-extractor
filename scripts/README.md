## Scripts
### Overview
The code in the <code>src</code> directory requires Spark to run. The files in this directory are used to setup a Amazon Web Services EMR instance running Spark and Hadoop.

It's recommended that this analysis be performed using distributed computing instead of a single local machine.

### <code>pyspark_emr.ipy</code>
iPython script file that spins up an EMR instance using AWS. Requires that you have <code>awscli</code> installed and configured with the proper credentials.

Also creates an alias for your cluster and appends the block for this alias to your <code>~/.ssh/config</code> file.

You must provide an authorized <code>ssh</code> key for authentication with AWS, and specify the name of the local file with the variable below:
```python
key_name = "<your-key-filename>"
```

You may also change the name and alias of the cluster by modifying these variables:
```python
cluster_alias = "<your-alias-name>"
cluster_name = "<your-cluster-name>"
```

### <code>jupyspark_emr.sh</code>
Bash script file that configures
