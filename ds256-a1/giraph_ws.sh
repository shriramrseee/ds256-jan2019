
# WCC
runGiraphJob.sh /home/shriramr/Assignment1-1.0-SNAPSHOT-jar-with-dependencies.jar in.ds256.Assignment1.Giraph.wccG "--workers 1 -eif in.ds256.Assignment1.Giraph.graphReader.graphReaderOne -eip /user/ds256/a1/cit-Patents.txt -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/shriramr/giraph/gow0.txt -ca giraph.pure.yarn.job=true,giraph.metrics.enable=true,giraph.metrics.directory=/user/shriramr/giraph/metricsw0.txt,giraph.logLevel=debug,replicate=1 -yh 12000"
runGiraphJob.sh /home/shriramr/Assignment1-1.0-SNAPSHOT-jar-with-dependencies.jar in.ds256.Assignment1.Giraph.wccG "--workers 4 -eif in.ds256.Assignment1.Giraph.graphReader.graphReaderOne -eip /user/ds256/a1/soc-livj.txt -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/shriramr/giraph/gow1.txt -ca giraph.pure.yarn.job=true,giraph.metrics.enable=true,giraph.metrics.directory=/user/shriramr/giraph/metricsw1.txt,giraph.logLevel=debug,replicate=1 -yh 12000"
runGiraphJob.sh /home/shriramr/Assignment1-1.0-SNAPSHOT-jar-with-dependencies.jar in.ds256.Assignment1.Giraph.wccG "--workers 7 -eif in.ds256.Assignment1.Giraph.graphReader.graphReaderOne -eip /user/ds256/a1/orkut.txt -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/shriramr/giraph/gow2.txt -ca giraph.pure.yarn.job=true,giraph.metrics.enable=true,giraph.metrics.directory=/user/shriramr/giraph/metricsw2.txt,giraph.logLevel=debug,replicate=1 -yh 12000"

# Conductance
runGiraphJob.sh /home/shriramr/Assignment1-1.0-SNAPSHOT-jar-with-dependencies.jar in.ds256.Assignment1.Giraph.conductanceG "--workers 1 -mc in.ds256.Assignment1.Giraph.masterCompute.conductanceMaster -eif in.ds256.Assignment1.Giraph.graphReader.graphReaderOne -eip /user/ds256/a1/cit-Patents.txt -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/shriramr/giraph/gow3.txt -ca giraph.pure.yarn.job=true,giraph.metrics.enable=true,giraph.metrics.directory=/user/shriramr/giraph/metricsw3.txt,giraph.logLevel=debug,replicate=1 -yh 12000"
runGiraphJob.sh /home/shriramr/Assignment1-1.0-SNAPSHOT-jar-with-dependencies.jar in.ds256.Assignment1.Giraph.conductanceG "--workers 4 -mc in.ds256.Assignment1.Giraph.masterCompute.conductanceMaster -eif in.ds256.Assignment1.Giraph.graphReader.graphReaderOne -eip /user/ds256/a1/soc-livj.txt -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/shriramr/giraph/gow4.txt -ca giraph.pure.yarn.job=true,giraph.metrics.enable=true,giraph.metrics.directory=/user/shriramr/giraph/metricsw4.txt,giraph.logLevel=debug,replicate=1 -yh 12000"
runGiraphJob.sh /home/shriramr/Assignment1-1.0-SNAPSHOT-jar-with-dependencies.jar in.ds256.Assignment1.Giraph.conductanceG "--workers 7 -mc in.ds256.Assignment1.Giraph.masterCompute.conductanceMaster -eif in.ds256.Assignment1.Giraph.graphReader.graphReaderOne -eip /user/ds256/a1/orkut.txt -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/shriramr/giraph/gow5.txt -ca giraph.pure.yarn.job=true,giraph.metrics.enable=true,giraph.metrics.directory=/user/shriramr/giraph/metricsw5.txt,giraph.logLevel=debug,replicate=1 -yh 12000"

# PR
runGiraphJob.sh /home/shriramr/Assignment1-1.0-SNAPSHOT-jar-with-dependencies.jar in.ds256.Assignment1.Giraph.prG "--workers 1 -mc in.ds256.Assignment1.Giraph.masterCompute.prMaster -eif in.ds256.Assignment1.Giraph.graphReader.graphReaderOne -eip /user/ds256/a1/cit-Patents.txt -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/shriramr/giraph/gow6.txt -ca giraph.pure.yarn.job=true,giraph.metrics.enable=true,giraph.metrics.directory=/user/shriramr/giraph/metricsw6.txt,giraph.logLevel=debug,tolerance=0.01,weight=0.85,replicate=0 -yh 12000"
runGiraphJob.sh /home/shriramr/Assignment1-1.0-SNAPSHOT-jar-with-dependencies.jar in.ds256.Assignment1.Giraph.prG "--workers 4 -mc in.ds256.Assignment1.Giraph.masterCompute.prMaster -eif in.ds256.Assignment1.Giraph.graphReader.graphReaderOne -eip /user/ds256/a1/soc-livj.txt -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/shriramr/giraph/gow7.txt -ca giraph.pure.yarn.job=true,giraph.metrics.enable=true,giraph.metrics.directory=/user/shriramr/giraph/metricsw7.txt,giraph.logLevel=debug,tolerance=0.01,weight=0.85,replicate=0 -yh 12000"
runGiraphJob.sh /home/shriramr/Assignment1-1.0-SNAPSHOT-jar-with-dependencies.jar in.ds256.Assignment1.Giraph.prG "--workers 7 -mc in.ds256.Assignment1.Giraph.masterCompute.prMaster -eif in.ds256.Assignment1.Giraph.graphReader.graphReaderOne -eip /user/ds256/a1/orkut.txt -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/shriramr/giraph/gow8.txt -ca giraph.pure.yarn.job=true,giraph.metrics.enable=true,giraph.metrics.directory=/user/shriramr/giraph/metricsw8.txt,giraph.logLevel=debug,tolerance=0.01,weight=0.85,replicate=0 -yh 12000"

# Span
runGiraphJob.sh /home/shriramr/Assignment1-1.0-SNAPSHOT-jar-with-dependencies.jar in.ds256.Assignment1.Giraph.spanG "--workers 1 -eif in.ds256.Assignment1.Giraph.graphReader.graphReaderOne -eip /user/ds256/a1/cit-Patents.txt -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/shriramr/giraph/gow9.txt -ca giraph.pure.yarn.job=true,giraph.metrics.enable=true,giraph.metrics.directory=/user/shriramr/giraph/metricsw9.txt,giraph.logLevel=debug,sourceId=1,replicate=1 -yh 12000"
runGiraphJob.sh /home/shriramr/Assignment1-1.0-SNAPSHOT-jar-with-dependencies.jar in.ds256.Assignment1.Giraph.spanG "--workers 4 -eif in.ds256.Assignment1.Giraph.graphReader.graphReaderOne -eip /user/ds256/a1/soc-livj.txt -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/shriramr/giraph/gow10.txt -ca giraph.pure.yarn.job=true,giraph.metrics.enable=true,giraph.metrics.directory=/user/shriramr/giraph/metricsw10.txt,giraph.logLevel=debug,sourceId=1,replicate=1 -yh 12000"
runGiraphJob.sh /home/shriramr/Assignment1-1.0-SNAPSHOT-jar-with-dependencies.jar in.ds256.Assignment1.Giraph.spanG "--workers 7 -eif in.ds256.Assignment1.Giraph.graphReader.graphReaderOne -eip /user/ds256/a1/orkut.txt -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/shriramr/giraph/gow11.txt -ca giraph.pure.yarn.job=true,giraph.metrics.enable=true,giraph.metrics.directory=/user/shriramr/giraph/metricsw11.txt,giraph.logLevel=debug,sourceId=1,replicate=1 -yh 12000"


for i in {0..11}
do
  hdfs dfs -ls /user/shriramr/giraph/metrics$i.txt | wc -l
done

cat t | while read line
do
   getYarnLog.sh $line > temp
   a=$(cat temp | grep "compute all partitions" | tr '\n' ' ' | sed -e 's/[^0-9]/ /g' -e 's/^ *//g' -e 's/ *$//g' | tr -s ' ' | sed 's/ /\n/g' | awk '{ total += $1; count++ } END { print total/count }')
   b=$(cat temp | grep "network communication time" | tr '\n' ' ' | sed -e 's/[^0-9]/ /g' -e 's/^ *//g' -e 's/ *$//g' | tr -s ' ' | sed 's/ /\n/g' | awk '{ total += $1; count++ } END { print total/count }')
   c=$(cat temp | grep "superstep time" | tr '\n' ' ' | sed -e 's/[^0-9]/ /g' -e 's/^ *//g' -e 's/ *$//g' | tr -s ' ' | sed 's/ /\n/g' | awk '{ total += $1; count++ } END { print total/count }')
   echo $a $b $c >> temp1
done
