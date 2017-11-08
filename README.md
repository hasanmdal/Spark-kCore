# Spark-kCore
A Implementation of k-core decomposition on Apache Spark on undirected graphs. It ignores directions if a directed graph is provided.
To use the code please follow the following instructions.
## Pre-Requisite

* [sbt_1.0.3](http://www.scala-sbt.org/download.html) -Dependency Management
* [Apache Spark 2.1.1](https://spark.apache.org/) - Distributed Processing framework
* [Apache Hadoop 2.7](https://hadoop.apache.org/docs/r2.7.0/) - Distributed File System

## Installing

```
git clone https://github.com/DMGroup-IUPUI/Spark-kCore.git
cd Spark-kCore
sbt clean
sbt package
```
## Execute

The program takes as input a tab delimited edgelistfile as input and emits the max k-core value. It also takes as input a maximum iteration count  as an upper bound for the pregel iteration.
```
.$SPARK_HOME/bin/spark-submit --class "KCore" --executor-memory 14G --executor-cores 4 --driver-memory 14G --master <SPARK_MASTER_URL> kcore_2.10-1.0.jar <MAX_ITERATION_VALUE> <HDFS_INPUT_LOCATION> <NO_OF_PARTITIONS>
```

## Authors

* **Aritra Mandal** - *amandal@iupui.edu*
* **Dr. Mohammad Al Hasan** - *alhasan@cs.iupui.edu*
