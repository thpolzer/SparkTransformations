# SparkTransformations
This tutorial describes how to transform data coming from HDFS using Scala on Spark.
In order to enable remote connection to your HDFS file system make sure the below server settings are done:

1. core-site.xml
property: fs.defaultFS
make sure the server name is explicitely set (not localhost!); example: hdfs://hadoopserver:9000

2. hdfs-site.xml
enter new property: dfs.namenode.rpc-bind-host
value: 0.0.0.0

3. /etc/hosts
deactivate 127.0.1.1 setting
example:
127.0.0.1 localhost
# 127.0.1.1 hadoopserver
192.16874.2 hadoopserver


