# spark-test
Tool for automatic cluster testing with apache spark.

### pom.xml configuration
TBD document these:

* <kafka.consumer.zookeeper.url>10.16.31.200:2181</kafka.consumer.zookeeper.url>
* <kafka.consumer.topic>36m</kafka.consumer.topic>
* <kafka.producer.topic>sparkOutput</kafka.producer.topic>
* <kafka.producer.servers>10.16.31.201:9092</kafka.producer.servers>
* <spark.home>/root/spark/spark-bin-hadoop</spark.home>
* <spark.master>spark://sc-211:7077</spark.master>
* <spark.machines>5</spark.machines>
* <spark.testtype>CountTest</spark.testtype>
* <application.resultsTopic>sparkResults</application.resultsTopic>
### bin/setenv.sh configuration

### to run a set of tests
1.

### to run single test
1. Set producer and consumer topic names in `pom.xml` testbed profile
2. (Optional) Execute `bin/clean-cluster.sh`
3. Execute `bin/install-cluster.sh` to prepare (download and unpack) spark on every machine
4. Execute `bin/start-cluster.sh` to run spark in cluster mode, web-based user interface to monitor the cluster will be running at http://10.16.31.211:8080/ (master node) and http://10.16.31.212:8081/ http://10.16.31.213:8081/ http://10.16.31.214:8081/ http://10.16.31.215:8081/ (slaves)
5. Execute `bin/deploy-to-cluster.sh` to archive, copy, unpack and build at 10.16.31.211, app will then start automatically, ready to receive json from kafka
