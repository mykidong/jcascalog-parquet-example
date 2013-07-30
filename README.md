# jcascalog-parquet-example

JCascalog Example with Parquet Data Format.

## Run Examples

Make sure that the native lzop is installed on your hadoop cluster,
for more details, please see https://github.com/twitter/hadoop-lzo.

### Write Parquet from Json
This example M/R Job output produces parquet data from the input json data.

Edit the namenode and jobtracker configuration in jcascalog.parquet.WriteParquetFromJson.main():
```
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "hdfs://hadoop01:9000");
    conf.set("mapred.job.tracker", "hadoop01:9001");
```
   
To run M/R Job:

```
mvn -e -DskipTests=true clean install assembly:single ;
hadoop fs -rmr parquet/json;
hadoop fs -rmr parquet/out;
hadoop fs -rmr /tmp/lib/jcascalog-parquet-0.1.0-SNAPSHOT-hadoop-job.jar;
hadoop fs -mkdir parquet/json;
hadoop fs -put target/jcascalog-parquet-0.1.0-SNAPSHOT-hadoop-job.jar /tmp/lib;
hadoop fs -put src/test/resources/electricPowerUsageGenerator.json parquet/json/
```

```
hadoop jar target/jcascalog-parquet-0.1.0-SNAPSHOT-hadoop-job.jar \
              jcascalog.parquet.WriteParquetFromJson \
              parquet/json/electricPowerUsageGenerator.json \
              parquet/out \
              /tmp/lib/jcascalog-parquet-0.1.0-SNAPSHOT-hadoop-job.jar;
```

### Read Specified Columns from Parquet data with raw M/R Job.
With this raw M/R Job, the specified column will be read from the parquet data.

Edit the namenode and jobtracker configuration in jcascalog.parquet.ReadSpecifiedColumns.main():
```
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "hdfs://hadoop01:9000");
    conf.set("mapred.job.tracker", "hadoop01:9001");
```
   
To run this M/R Job:

```
hadoop jar target/jcascalog-parquet-0.1.0-SNAPSHOT-hadoop-job.jar \
          jcascalog.parquet.ReadSpecifiedColumns \
          parquet/out/  
          parquet/specified-columns \
          /tmp/lib/jcascalog-parquet-0.1.0-SNAPSHOT-hadoop-job.jar;
```


### Read Specified Columns from Parquet data with JCascalog.
With JCascalog, the specified column will be read from the parquet data.

Edit the namenode and jobtracker configuration in jcascalog.parquet.ReadSpecifedColumnsWithParquetScheme.main():
```
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "hdfs://hadoop01:9000");
    conf.set("mapred.job.tracker", "hadoop01:9001");
```
   
To run this M/R Job:

```
hadoop fs -rmr parquet/specified-columns ;
```


```
hadoop jar target/jcascalog-parquet-0.1.0-SNAPSHOT-hadoop-job.jar \
          jcascalog.parquet.ReadSpecifedColumnsWithParquetScheme \
          parquet/out/  \
          parquet/specified-columns \
          /tmp/lib/jcascalog-parquet-0.1.0-SNAPSHOT-hadoop-job.jar;
```


### Verify parquet data with JCascalog.
To verify parquet data, read the parquet data, and produce it as text data.

Edit the namenode and jobtracker configuration in jcascalog.parquet.WriteTextFromParquet.main():
```
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "hdfs://hadoop01:9000");
    conf.set("mapred.job.tracker", "hadoop01:9001");
```
   
To run this M/R Job:
```
hadoop jar target/jcascalog-parquet-0.1.0-SNAPSHOT-hadoop-job.jar \
                  jcascalog.parquet.WriteTextFromParquet \
                  parquet/specified-columns  \
                  parquet/text-out \
                  /tmp/lib/jcascalog-parquet-0.1.0-SNAPSHOT-hadoop-job.jar;
```

