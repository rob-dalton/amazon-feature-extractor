{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "<hr>\n",
    "# Script Development - <code>groupByProd_AddFeatures.py</code>\n",
    "Development notebook for script to group by department, asin and add top positive and negative features.\n",
    "<hr>\n",
    "## Setup"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 1,
   "metadata": {
    "collapsed": true
   },
   "outputs": [],
   "source": [
    "import pyspark as ps\n",
    "from sentimentAnalysis import dataProcessing as dp"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "collapsed": false
   },
   "outputs": [],
   "source": [
    "# create spark session\n",
    "spark = ps.sql.SparkSession.builder \\\n",
    "    .appName(\"reviewProcessing\") \\\n",
    "    .getOrCreate()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "collapsed": false
   },
   "outputs": [],
   "source": [
    "# get dataframes\n",
    "# specify s3 as sourc with s3a://\n",
    "#df = spark.read.json(\"s3a://amazon-review-data/user_dedup.json.gz\")\n",
    "df_meta = spark.read.json(\"s3a://amazon-review-data/metadata.json.gz\")\n",
    "df = spark.read.json(\"s3a://amazon-review-data/reviews_Musical_Instruments_5.json.gz\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "collapsed": true
   },
   "outputs": [],
   "source": [
    "# subset asin, overall, reviewText\n",
    "df_subset = df.select(\"asin\", \"overall\", \"reviewText\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "collapsed": true
   },
   "outputs": [],
   "source": [
    "# add tokens\n",
    "df_tokens = dp.add_tokens(df_subset)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "collapsed": false
   },
   "outputs": [],
   "source": [
    "# add metadata\n",
    "df_meta_joined = dp.join_metadata(df_tokens, df_meta).select(\"asin\", \"overall\", \"salesRank\", \"reviewText\", \"tokens\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "collapsed": true
   },
   "outputs": [],
   "source": [
    "# unpersist old dataframes\n",
    "df_meta.unpersist()\n",
    "df.unpersist()\n",
    "df_tokens.unpersist()"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "collapsed": true
   },
   "source": [
    "<hr>\n",
    "## Development"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "collapsed": true
   },
   "source": [
    "### Group by Department"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "metadata": {
    "collapsed": false
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+----------+-------+--------------------+--------------------+--------------------+\n",
      "|      asin|overall|           salesRank|          reviewText|              tokens|\n",
      "+----------+-------+--------------------+--------------------+--------------------+\n",
      "|1384719342|    5.0|[null,null,null,n...|Not much to write...|[much, write, exa...|\n",
      "|1384719342|    5.0|[null,null,null,n...|The product does ...|[product, exactly...|\n",
      "|1384719342|    5.0|[null,null,null,n...|The primary job o...|[primary, job, de...|\n",
      "+----------+-------+--------------------+--------------------+--------------------+\n",
      "only showing top 3 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "# check df\n",
    "df_meta_joined.show(3)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "metadata": {
    "collapsed": false
   },
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "Traceback (most recent call last):\n",
      "  File \"/home/hadoop/anaconda/lib/python2.7/SocketServer.py\", line 290, in _handle_request_noblock\n",
      "    self.process_request(request, client_address)\n",
      "  File \"/home/hadoop/anaconda/lib/python2.7/SocketServer.py\", line 318, in process_request\n",
      "    self.finish_request(request, client_address)\n",
      "  File \"/home/hadoop/anaconda/lib/python2.7/SocketServer.py\", line 331, in finish_request\n",
      "    self.RequestHandlerClass(request, client_address, self)\n",
      "  File \"/home/hadoop/anaconda/lib/python2.7/SocketServer.py\", line 652, in __init__\n",
      "    self.handle()\n",
      "  File \"/usr/lib/spark/python/pyspark/accumulators.py\", line 235, in handle\n",
      "ERROR:root:Exception while sending command.\n",
      "Traceback (most recent call last):\n",
      "  File \"/usr/lib/spark/python/lib/py4j-0.10.3-src.zip/py4j/java_gateway.py\", line 883, in send_command\n",
      "    response = connection.send_command(command)\n",
      "  File \"/usr/lib/spark/python/lib/py4j-0.10.3-src.zip/py4j/java_gateway.py\", line 1040, in send_command\n",
      "    \"Error while receiving\", e, proto.ERROR_ON_RECEIVE)\n",
      "Py4JNetworkError: Error while receiving\n",
      "    num_updates = read_int(self.rfile)\n",
      "  File \"/usr/lib/spark/python/pyspark/serializers.py\", line 545, in read_int\n",
      "    raise EOFError\n",
      "EOFError\n",
      "ERROR:py4j.java_gateway:An error occurred while trying to connect to the Java server (127.0.0.1:35319)\n",
      "Traceback (most recent call last):\n",
      "  File \"/usr/lib/spark/python/lib/py4j-0.10.3-src.zip/py4j/java_gateway.py\", line 963, in start\n",
      "    self.socket.connect((self.address, self.port))\n",
      "  File \"/home/hadoop/anaconda/lib/python2.7/socket.py\", line 228, in meth\n",
      "    return getattr(self._sock,name)(*args)\n",
      "error: [Errno 111] Connection refused\n",
      "ERROR:py4j.java_gateway:An error occurred while trying to connect to the Java server (127.0.0.1:35319)\n",
      "Traceback (most recent call last):\n",
      "  File \"/usr/lib/spark/python/lib/py4j-0.10.3-src.zip/py4j/java_gateway.py\", line 963, in start\n",
      "    self.socket.connect((self.address, self.port))\n",
      "  File \"/home/hadoop/anaconda/lib/python2.7/socket.py\", line 228, in meth\n",
      "    return getattr(self._sock,name)(*args)\n",
      "error: [Errno 111] Connection refused\n",
      "ERROR:py4j.java_gateway:An error occurred while trying to connect to the Java server (127.0.0.1:35319)\n",
      "Traceback (most recent call last):\n",
      "  File \"/usr/lib/spark/python/lib/py4j-0.10.3-src.zip/py4j/java_gateway.py\", line 963, in start\n",
      "    self.socket.connect((self.address, self.port))\n",
      "  File \"/home/hadoop/anaconda/lib/python2.7/socket.py\", line 228, in meth\n",
      "    return getattr(self._sock,name)(*args)\n",
      "error: [Errno 111] Connection refused\n",
      "ERROR:py4j.java_gateway:An error occurred while trying to connect to the Java server (127.0.0.1:35319)\n",
      "Traceback (most recent call last):\n",
      "  File \"/usr/lib/spark/python/lib/py4j-0.10.3-src.zip/py4j/java_gateway.py\", line 963, in start\n",
      "    self.socket.connect((self.address, self.port))\n",
      "  File \"/home/hadoop/anaconda/lib/python2.7/socket.py\", line 228, in meth\n",
      "    return getattr(self._sock,name)(*args)\n",
      "error: [Errno 111] Connection refused\n",
      "ERROR:py4j.java_gateway:An error occurred while trying to connect to the Java server (127.0.0.1:35319)\n",
      "Traceback (most recent call last):\n",
      "  File \"/usr/lib/spark/python/lib/py4j-0.10.3-src.zip/py4j/java_gateway.py\", line 963, in start\n",
      "    self.socket.connect((self.address, self.port))\n",
      "  File \"/home/hadoop/anaconda/lib/python2.7/socket.py\", line 228, in meth\n",
      "    return getattr(self._sock,name)(*args)\n",
      "error: [Errno 111] Connection refused\n",
      "ERROR:py4j.java_gateway:An error occurred while trying to connect to the Java server (127.0.0.1:35319)\n",
      "Traceback (most recent call last):\n",
      "  File \"/usr/lib/spark/python/lib/py4j-0.10.3-src.zip/py4j/java_gateway.py\", line 963, in start\n",
      "    self.socket.connect((self.address, self.port))\n",
      "  File \"/home/hadoop/anaconda/lib/python2.7/socket.py\", line 228, in meth\n",
      "    return getattr(self._sock,name)(*args)\n",
      "error: [Errno 111] Connection refused\n",
      "ERROR:py4j.java_gateway:An error occurred while trying to connect to the Java server (127.0.0.1:35319)\n",
      "Traceback (most recent call last):\n",
      "  File \"/usr/lib/spark/python/lib/py4j-0.10.3-src.zip/py4j/java_gateway.py\", line 963, in start\n",
      "    self.socket.connect((self.address, self.port))\n",
      "  File \"/home/hadoop/anaconda/lib/python2.7/socket.py\", line 228, in meth\n",
      "    return getattr(self._sock,name)(*args)\n",
      "error: [Errno 111] Connection refused\n",
      "ERROR:py4j.java_gateway:An error occurred while trying to connect to the Java server (127.0.0.1:35319)\n",
      "Traceback (most recent call last):\n",
      "  File \"/usr/lib/spark/python/lib/py4j-0.10.3-src.zip/py4j/java_gateway.py\", line 963, in start\n",
      "    self.socket.connect((self.address, self.port))\n",
      "  File \"/home/hadoop/anaconda/lib/python2.7/socket.py\", line 228, in meth\n",
      "    return getattr(self._sock,name)(*args)\n",
      "error: [Errno 111] Connection refused\n",
      "ERROR:py4j.java_gateway:An error occurred while trying to connect to the Java server (127.0.0.1:35319)\n",
      "Traceback (most recent call last):\n",
      "  File \"/usr/lib/spark/python/lib/py4j-0.10.3-src.zip/py4j/java_gateway.py\", line 963, in start\n",
      "    self.socket.connect((self.address, self.port))\n",
      "  File \"/home/hadoop/anaconda/lib/python2.7/socket.py\", line 228, in meth\n",
      "    return getattr(self._sock,name)(*args)\n",
      "error: [Errno 111] Connection refused\n",
      "ERROR:py4j.java_gateway:An error occurred while trying to connect to the Java server (127.0.0.1:35319)\n",
      "Traceback (most recent call last):\n",
      "  File \"/usr/lib/spark/python/lib/py4j-0.10.3-src.zip/py4j/java_gateway.py\", line 963, in start\n",
      "    self.socket.connect((self.address, self.port))\n",
      "  File \"/home/hadoop/anaconda/lib/python2.7/socket.py\", line 228, in meth\n",
      "    return getattr(self._sock,name)(*args)\n",
      "error: [Errno 111] Connection refused\n",
      "ERROR:py4j.java_gateway:An error occurred while trying to connect to the Java server (127.0.0.1:35319)\n",
      "Traceback (most recent call last):\n",
      "  File \"/usr/lib/spark/python/lib/py4j-0.10.3-src.zip/py4j/java_gateway.py\", line 963, in start\n",
      "    self.socket.connect((self.address, self.port))\n",
      "  File \"/home/hadoop/anaconda/lib/python2.7/socket.py\", line 228, in meth\n",
      "    return getattr(self._sock,name)(*args)\n",
      "error: [Errno 111] Connection refused\n",
      "ERROR:py4j.java_gateway:An error occurred while trying to connect to the Java server (127.0.0.1:35319)\n",
      "Traceback (most recent call last):\n",
      "  File \"/usr/lib/spark/python/lib/py4j-0.10.3-src.zip/py4j/java_gateway.py\", line 963, in start\n",
      "    self.socket.connect((self.address, self.port))\n",
      "  File \"/home/hadoop/anaconda/lib/python2.7/socket.py\", line 228, in meth\n",
      "    return getattr(self._sock,name)(*args)\n",
      "error: [Errno 111] Connection refused\n"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "----------------------------------------\n",
      "Exception happened during processing of request from ('127.0.0.1', 42662)\n",
      "----------------------------------------\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "ERROR:py4j.java_gateway:An error occurred while trying to connect to the Java server (127.0.0.1:35319)\n",
      "Traceback (most recent call last):\n",
      "  File \"/usr/lib/spark/python/lib/py4j-0.10.3-src.zip/py4j/java_gateway.py\", line 963, in start\n",
      "    self.socket.connect((self.address, self.port))\n",
      "  File \"/home/hadoop/anaconda/lib/python2.7/socket.py\", line 228, in meth\n",
      "    return getattr(self._sock,name)(*args)\n",
      "error: [Errno 111] Connection refused\n",
      "ERROR:py4j.java_gateway:An error occurred while trying to connect to the Java server (127.0.0.1:35319)\n",
      "Traceback (most recent call last):\n",
      "  File \"/usr/lib/spark/python/lib/py4j-0.10.3-src.zip/py4j/java_gateway.py\", line 963, in start\n",
      "    self.socket.connect((self.address, self.port))\n",
      "  File \"/home/hadoop/anaconda/lib/python2.7/socket.py\", line 228, in meth\n",
      "    return getattr(self._sock,name)(*args)\n",
      "error: [Errno 111] Connection refused\n",
      "ERROR:py4j.java_gateway:An error occurred while trying to connect to the Java server (127.0.0.1:35319)\n",
      "Traceback (most recent call last):\n",
      "  File \"/usr/lib/spark/python/lib/py4j-0.10.3-src.zip/py4j/java_gateway.py\", line 963, in start\n",
      "    self.socket.connect((self.address, self.port))\n",
      "  File \"/home/hadoop/anaconda/lib/python2.7/socket.py\", line 228, in meth\n",
      "    return getattr(self._sock,name)(*args)\n",
      "error: [Errno 111] Connection refused\n",
      "ERROR:py4j.java_gateway:An error occurred while trying to connect to the Java server (127.0.0.1:35319)\n",
      "Traceback (most recent call last):\n",
      "  File \"/usr/lib/spark/python/lib/py4j-0.10.3-src.zip/py4j/java_gateway.py\", line 963, in start\n",
      "    self.socket.connect((self.address, self.port))\n",
      "  File \"/home/hadoop/anaconda/lib/python2.7/socket.py\", line 228, in meth\n",
      "    return getattr(self._sock,name)(*args)\n",
      "error: [Errno 111] Connection refused\n",
      "ERROR:py4j.java_gateway:An error occurred while trying to connect to the Java server (127.0.0.1:35319)\n",
      "Traceback (most recent call last):\n",
      "  File \"/usr/lib/spark/python/lib/py4j-0.10.3-src.zip/py4j/java_gateway.py\", line 963, in start\n",
      "    self.socket.connect((self.address, self.port))\n",
      "  File \"/home/hadoop/anaconda/lib/python2.7/socket.py\", line 228, in meth\n",
      "    return getattr(self._sock,name)(*args)\n",
      "error: [Errno 111] Connection refused\n",
      "ERROR:py4j.java_gateway:An error occurred while trying to connect to the Java server (127.0.0.1:35319)\n",
      "Traceback (most recent call last):\n",
      "  File \"/usr/lib/spark/python/lib/py4j-0.10.3-src.zip/py4j/java_gateway.py\", line 963, in start\n",
      "    self.socket.connect((self.address, self.port))\n",
      "  File \"/home/hadoop/anaconda/lib/python2.7/socket.py\", line 228, in meth\n",
      "    return getattr(self._sock,name)(*args)\n",
      "error: [Errno 111] Connection refused\n"
     ]
    },
    {
     "ename": "Py4JNetworkError",
     "evalue": "An error occurred while trying to connect to the Java server (127.0.0.1:35319)",
     "output_type": "error",
     "traceback": [
      "\u001b[0;31m---------------------------------------------------------------------------\u001b[0m",
      "\u001b[0;31mPy4JNetworkError\u001b[0m                          Traceback (most recent call last)",
      "\u001b[0;32m<ipython-input-8-5f8e345e9619>\u001b[0m in \u001b[0;36m<module>\u001b[0;34m()\u001b[0m\n\u001b[0;32m----> 1\u001b[0;31m \u001b[0mtest_row\u001b[0m \u001b[0;34m=\u001b[0m \u001b[0mdf_meta_joined\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mfirst\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m",
      "\u001b[0;32m/usr/lib/spark/python/pyspark/sql/dataframe.py\u001b[0m in \u001b[0;36mfirst\u001b[0;34m(self)\u001b[0m\n\u001b[1;32m    801\u001b[0m         \u001b[0mRow\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mage\u001b[0m\u001b[0;34m=\u001b[0m\u001b[0;36m2\u001b[0m\u001b[0;34m,\u001b[0m \u001b[0mname\u001b[0m\u001b[0;34m=\u001b[0m\u001b[0;34mu'Alice'\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    802\u001b[0m         \"\"\"\n\u001b[0;32m--> 803\u001b[0;31m         \u001b[0;32mreturn\u001b[0m \u001b[0mself\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mhead\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m\u001b[1;32m    804\u001b[0m \u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    805\u001b[0m     \u001b[0;34m@\u001b[0m\u001b[0mignore_unicode_prefix\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n",
      "\u001b[0;32m/usr/lib/spark/python/pyspark/sql/dataframe.py\u001b[0m in \u001b[0;36mhead\u001b[0;34m(self, n)\u001b[0m\n\u001b[1;32m    789\u001b[0m         \"\"\"\n\u001b[1;32m    790\u001b[0m         \u001b[0;32mif\u001b[0m \u001b[0mn\u001b[0m \u001b[0;32mis\u001b[0m \u001b[0mNone\u001b[0m\u001b[0;34m:\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0;32m--> 791\u001b[0;31m             \u001b[0mrs\u001b[0m \u001b[0;34m=\u001b[0m \u001b[0mself\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mhead\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0;36m1\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m\u001b[1;32m    792\u001b[0m             \u001b[0;32mreturn\u001b[0m \u001b[0mrs\u001b[0m\u001b[0;34m[\u001b[0m\u001b[0;36m0\u001b[0m\u001b[0;34m]\u001b[0m \u001b[0;32mif\u001b[0m \u001b[0mrs\u001b[0m \u001b[0;32melse\u001b[0m \u001b[0mNone\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    793\u001b[0m         \u001b[0;32mreturn\u001b[0m \u001b[0mself\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mtake\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mn\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n",
      "\u001b[0;32m/usr/lib/spark/python/pyspark/sql/dataframe.py\u001b[0m in \u001b[0;36mhead\u001b[0;34m(self, n)\u001b[0m\n\u001b[1;32m    791\u001b[0m             \u001b[0mrs\u001b[0m \u001b[0;34m=\u001b[0m \u001b[0mself\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mhead\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0;36m1\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    792\u001b[0m             \u001b[0;32mreturn\u001b[0m \u001b[0mrs\u001b[0m\u001b[0;34m[\u001b[0m\u001b[0;36m0\u001b[0m\u001b[0;34m]\u001b[0m \u001b[0;32mif\u001b[0m \u001b[0mrs\u001b[0m \u001b[0;32melse\u001b[0m \u001b[0mNone\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0;32m--> 793\u001b[0;31m         \u001b[0;32mreturn\u001b[0m \u001b[0mself\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mtake\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mn\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m\u001b[1;32m    794\u001b[0m \u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    795\u001b[0m     \u001b[0;34m@\u001b[0m\u001b[0mignore_unicode_prefix\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n",
      "\u001b[0;32m/usr/lib/spark/python/pyspark/sql/dataframe.py\u001b[0m in \u001b[0;36mtake\u001b[0;34m(self, num)\u001b[0m\n\u001b[1;32m    346\u001b[0m         \u001b[0;34m[\u001b[0m\u001b[0mRow\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mage\u001b[0m\u001b[0;34m=\u001b[0m\u001b[0;36m2\u001b[0m\u001b[0;34m,\u001b[0m \u001b[0mname\u001b[0m\u001b[0;34m=\u001b[0m\u001b[0;34mu'Alice'\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m,\u001b[0m \u001b[0mRow\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mage\u001b[0m\u001b[0;34m=\u001b[0m\u001b[0;36m5\u001b[0m\u001b[0;34m,\u001b[0m \u001b[0mname\u001b[0m\u001b[0;34m=\u001b[0m\u001b[0;34mu'Bob'\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m]\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    347\u001b[0m         \"\"\"\n\u001b[0;32m--> 348\u001b[0;31m         \u001b[0;32mreturn\u001b[0m \u001b[0mself\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mlimit\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mnum\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mcollect\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m\u001b[1;32m    349\u001b[0m \u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    350\u001b[0m     \u001b[0;34m@\u001b[0m\u001b[0msince\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0;36m1.3\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n",
      "\u001b[0;32m/usr/lib/spark/python/pyspark/sql/dataframe.py\u001b[0m in \u001b[0;36mcollect\u001b[0;34m(self)\u001b[0m\n\u001b[1;32m    308\u001b[0m         \"\"\"\n\u001b[1;32m    309\u001b[0m         \u001b[0;32mwith\u001b[0m \u001b[0mSCCallSiteSync\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mself\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0m_sc\u001b[0m\u001b[0;34m)\u001b[0m \u001b[0;32mas\u001b[0m \u001b[0mcss\u001b[0m\u001b[0;34m:\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0;32m--> 310\u001b[0;31m             \u001b[0mport\u001b[0m \u001b[0;34m=\u001b[0m \u001b[0mself\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0m_jdf\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mcollectToPython\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m\u001b[1;32m    311\u001b[0m         \u001b[0;32mreturn\u001b[0m \u001b[0mlist\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0m_load_from_socket\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mport\u001b[0m\u001b[0;34m,\u001b[0m \u001b[0mBatchedSerializer\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mPickleSerializer\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    312\u001b[0m \u001b[0;34m\u001b[0m\u001b[0m\n",
      "\u001b[0;32m/usr/lib/spark/python/pyspark/traceback_utils.py\u001b[0m in \u001b[0;36m__exit__\u001b[0;34m(self, type, value, tb)\u001b[0m\n\u001b[1;32m     76\u001b[0m         \u001b[0mSCCallSiteSync\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0m_spark_stack_depth\u001b[0m \u001b[0;34m-=\u001b[0m \u001b[0;36m1\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m     77\u001b[0m         \u001b[0;32mif\u001b[0m \u001b[0mSCCallSiteSync\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0m_spark_stack_depth\u001b[0m \u001b[0;34m==\u001b[0m \u001b[0;36m0\u001b[0m\u001b[0;34m:\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0;32m---> 78\u001b[0;31m             \u001b[0mself\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0m_context\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0m_jsc\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0msetCallSite\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mNone\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m",
      "\u001b[0;32m/usr/lib/spark/python/lib/py4j-0.10.3-src.zip/py4j/java_gateway.py\u001b[0m in \u001b[0;36m__call__\u001b[0;34m(self, *args)\u001b[0m\n\u001b[1;32m   1129\u001b[0m             \u001b[0mproto\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mEND_COMMAND_PART\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m   1130\u001b[0m \u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0;32m-> 1131\u001b[0;31m         \u001b[0manswer\u001b[0m \u001b[0;34m=\u001b[0m \u001b[0mself\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mgateway_client\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0msend_command\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mcommand\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m\u001b[1;32m   1132\u001b[0m         return_value = get_return_value(\n\u001b[1;32m   1133\u001b[0m             answer, self.gateway_client, self.target_id, self.name)\n",
      "\u001b[0;32m/usr/lib/spark/python/lib/py4j-0.10.3-src.zip/py4j/java_gateway.py\u001b[0m in \u001b[0;36msend_command\u001b[0;34m(self, command, retry, binary)\u001b[0m\n\u001b[1;32m    879\u001b[0m          \u001b[0;32mif\u001b[0m \u001b[0;34m`\u001b[0m\u001b[0mbinary\u001b[0m\u001b[0;34m`\u001b[0m \u001b[0;32mis\u001b[0m \u001b[0;34m`\u001b[0m\u001b[0mTrue\u001b[0m\u001b[0;34m`\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    880\u001b[0m         \"\"\"\n\u001b[0;32m--> 881\u001b[0;31m         \u001b[0mconnection\u001b[0m \u001b[0;34m=\u001b[0m \u001b[0mself\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0m_get_connection\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m\u001b[1;32m    882\u001b[0m         \u001b[0;32mtry\u001b[0m\u001b[0;34m:\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    883\u001b[0m             \u001b[0mresponse\u001b[0m \u001b[0;34m=\u001b[0m \u001b[0mconnection\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0msend_command\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mcommand\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n",
      "\u001b[0;32m/usr/lib/spark/python/lib/py4j-0.10.3-src.zip/py4j/java_gateway.py\u001b[0m in \u001b[0;36m_get_connection\u001b[0;34m(self)\u001b[0m\n\u001b[1;32m    827\u001b[0m             \u001b[0mconnection\u001b[0m \u001b[0;34m=\u001b[0m \u001b[0mself\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mdeque\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mpop\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    828\u001b[0m         \u001b[0;32mexcept\u001b[0m \u001b[0mIndexError\u001b[0m\u001b[0;34m:\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0;32m--> 829\u001b[0;31m             \u001b[0mconnection\u001b[0m \u001b[0;34m=\u001b[0m \u001b[0mself\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0m_create_connection\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m\u001b[1;32m    830\u001b[0m         \u001b[0;32mreturn\u001b[0m \u001b[0mconnection\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    831\u001b[0m \u001b[0;34m\u001b[0m\u001b[0m\n",
      "\u001b[0;32m/usr/lib/spark/python/lib/py4j-0.10.3-src.zip/py4j/java_gateway.py\u001b[0m in \u001b[0;36m_create_connection\u001b[0;34m(self)\u001b[0m\n\u001b[1;32m    833\u001b[0m         connection = GatewayConnection(\n\u001b[1;32m    834\u001b[0m             self.gateway_parameters, self.gateway_property)\n\u001b[0;32m--> 835\u001b[0;31m         \u001b[0mconnection\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mstart\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m\u001b[1;32m    836\u001b[0m         \u001b[0;32mreturn\u001b[0m \u001b[0mconnection\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    837\u001b[0m \u001b[0;34m\u001b[0m\u001b[0m\n",
      "\u001b[0;32m/usr/lib/spark/python/lib/py4j-0.10.3-src.zip/py4j/java_gateway.py\u001b[0m in \u001b[0;36mstart\u001b[0;34m(self)\u001b[0m\n\u001b[1;32m    968\u001b[0m                 \u001b[0;34m\"server ({0}:{1})\"\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mformat\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mself\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0maddress\u001b[0m\u001b[0;34m,\u001b[0m \u001b[0mself\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mport\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    969\u001b[0m             \u001b[0mlogger\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mexception\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mmsg\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0;32m--> 970\u001b[0;31m             \u001b[0;32mraise\u001b[0m \u001b[0mPy4JNetworkError\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mmsg\u001b[0m\u001b[0;34m,\u001b[0m \u001b[0me\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m\u001b[1;32m    971\u001b[0m \u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    972\u001b[0m     \u001b[0;32mdef\u001b[0m \u001b[0mclose\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mself\u001b[0m\u001b[0;34m,\u001b[0m \u001b[0mreset\u001b[0m\u001b[0;34m=\u001b[0m\u001b[0mFalse\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m:\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n",
      "\u001b[0;31mPy4JNetworkError\u001b[0m: An error occurred while trying to connect to the Java server (127.0.0.1:35319)"
     ]
    }
   ],
   "source": [
    "test_row = df_meta_joined.first()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "collapsed": true
   },
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "anaconda-cloud": {},
  "kernelspec": {
   "display_name": "Python [conda root]",
   "language": "python",
   "name": "conda-root-py"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 2
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython2",
   "version": "2.7.12"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
