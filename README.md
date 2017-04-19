# spark_programs

Data collection command :
spark-submit --master yarn-cluster --conf spark.io.compression.codec=snappy --conf spark.yarn.queue=Aggregation --conf spark.driver.cores=5 --conf spark.dynamicAllocation.maxExecutors=10 --conf spark.shuffle.compress=true --conf spark.sql.tungsten.enabled=true --conf spark.shuffle.spill=true --conf spark.sql.parquet.compression.codec=snappy --conf spark.speculation=true --conf spark.kryo.referenceTracking=false --conf spark.hadoop.parquet.block.size=134217728 --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 --conf spark.executor.memory=25g --conf spark.hadoop.dfs.blocksize=134217728 --conf spark.shuffle.manager=sort --conf spark.driver.memory=30g --conf spark.hadoop.mapreduce.input.fileinputformat.split.minsize=134217728 --conf spark.akka.frameSize=1024 --conf spark.yarn.executor.memoryOverhead=3120 --conf spark.sql.parquet.filterPushdown=true --conf spark.sql.inMemoryColumnarStorage.compressed=true --conf spark.hadoop.parquet.enable.summary-metadata=false --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.rdd.compress=true --conf spark.yarn.max.executor.failures=30 --conf spark.default.parallelism=2001 --conf spark.network.timeout=1200s --conf spark.hadoop.dfs.client.read.shortcircuit=true --conf spark.dynamicAllocation.enabled=true --conf spark.executor.cores=5 --conf spark.yarn.driver.memoryOverhead=5024 --conf spark.shuffle.consolidateFiles=true --conf spark.sql.parquet.mergeSchema=false --conf spark.sql.avro.compression.codec=snappy --conf spark.hadoop.dfs.domain.socket.path=/var/lib/hadoop-hdfs/dn_xsocket --conf spark.shuffle.spill.compress=true --conf spark.sql.caseSensitive=true --conf spark.hadoop.mapreduce.use.directfileoutputcommitter=true --conf spark.shuffle.service.enabled=true --conf spark.driver.maxResultSize=0 --conf spark.sql.shuffle.partitions=2001 --packages com.databricks:spark-avro_2.10:2.0.1,com.databricks:spark-csv_2.10:1.3.0 --class com.org_name.dar.Main /path/to/base/folder/target/scala-2.10/dar_2.10-1.0.jar /hdfs/input/path /hdfs/output/path

Computation command :
	/opt/anaconda/bin/python generate_deal_aud_rev.py /path/to/input/csv/input_d_db_data.csv /hdfs/output/path /path/to/input/csv/input_dr_data.csv /path/to/output/final_output.csv