# README

------------------

### before run apps

set CLASSPATH to hadoop-core lib.
```
# MAIN
export CLASSPATH=$CLASSPATH:$HADOOP_HOME/hadoop-core.jar
export CLASSPATH=$CLASSPATH:$HADOOP_HOME/lib/commons-cli-1.2.jar
```

------------------

# Quick Start

-----------------

## Run app

This app is multistage MapReduce program, which use distributed cache.


1 STAGE
```
# hadoop jar CalcRegionAggregation.jar sample.mr.JoinSalesDataJob /data/sales_sample/sales /data/sales_sample/sales_detail /output/job1
```

2 STAGE
```
# hadoop jar CalcRegionAggregation.jar sample.mr.JoinRegionAndItemJob /output/job1 /output/job2 /data/sales_sample/shoplist/shoplist.tsv /data/sales_sample/itemlist/itemlist.tsv
```



!



---------------------


# MRUnit test

please set CLASSPATH.
```
# MAIN
export CLASSPATH=$CLASSPATH:$HADOOP_HOME/hadoop-core.jar

# MRUnit
export MRUNIT_LIB=$HADOOP_HOME/lib/mrunit
export CLASSPATH=$CLASSPATH:$MRUNIT_LIB/commons-logging-1.1.1.jar
export CLASSPATH=$CLASSPATH:$MRUNIT_LIB/hamcrest-core-1.1.jar
export CLASSPATH=$CLASSPATH:$MRUNIT_LIB/junit-4.10.jar
export CLASSPATH=$CLASSPATH:$MRUNIT_LIB/mockito-all-1.8.5.jar
export CLASSPATH=$CLASSPATH:$MRUNIT_LIB/log4j-1.2.15.jar
export CLASSPATH=$CLASSPATH:$MRUNIT_LIB/guava-r09-jarjar.jar
export CLASSPATH=$CLASSPATH:$MRUNIT_LIB/mrunit-1.0.0-hadoop1.jar
```


compile
```
# javac -classpath $CLASSPATH:/root/workspace/hadoop_mr_apps/WordCount.jar -d classes ./src/test/sample/mr/WordCount*.java
```

run test
```
# java -classpath $CLASSPATH org.junit.runner.JUnitCore test.sample.mr.WordCountMapperTest test.sample.mr.WordCountReducerTest test.sample.mr.WordCountMapReduceTest
```
