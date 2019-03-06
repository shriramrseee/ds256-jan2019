
# WCC
spark-submit --class in.ds256.Assignment1.Spark.wcc --master yarn --deploy-mode cluster --num-executors 1 --driver-memory 512m --executor-memory 8G --executor-cores 4  Assignment1-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs:///user/ds256/a1/cit-Patents.txt hdfs:///user/shriramr/output0.txt 8&
spark-submit --class in.ds256.Assignment1.Spark.wcc --master yarn --deploy-mode cluster --num-executors 2 --driver-memory 512m --executor-memory 8G --executor-cores 4  Assignment1-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs:///user/ds256/a1/soc-livj.txt hdfs:///user/shriramr/output1.txt 16&
spark-submit --class in.ds256.Assignment1.Spark.wcc --master yarn --deploy-mode cluster --num-executors 3 --driver-memory 512m --executor-memory 8G --executor-cores 4  Assignment1-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs:///user/ds256/a1/USA-road-d.USA.gr hdfs:///user/shriramr/output2.txt 32&

# Conductance



# PR


# Span