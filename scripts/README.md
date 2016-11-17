## Scripts
### Overview
The code in the <code>src</code> directory requires Spark to run. The files in this directory are used to setup a Amazon Web Services EMR instance running Spark and Hadoop.

It's recommended that this analysis be performed using distributed computing instead of a single local machine.

### <code>pyspark_emr.ipy</code>
- iPython script file. Spins up an AWS EMR instance

- Requires <code>awscli</code>

- Creates <code>ssh</code> ssh to login to cluster. Appends the block for this alias to your <code>~/.ssh/config</code> file.

- *NOTE:* Must provide an authorized <code>ssh</code> key for authentication with AWS by setting <code>key_name = "your-key-filename"</code>

- Can change the cluster name and alias by modifying:
<code>cluster_alias = "your-alias-name"</code> and <code>cluster_name = "your-cluster-name"</code>
