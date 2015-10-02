# spark-test
Tool for automatic cluster testing with apache spark.

## Configuration
You should have kafka set up with topic containing data for test.
### pom.xml
notable properties that should be set:
* **kafka.consumer.zookeeper.url** - input kafka zookeeper url
* **kafka.consumer.topic** - mutual prefix of input topics (i.e. input if you have topics input-4part and input-5part, they do not need to have different amount of partitions but they need to have suffix 5part if used for 5 computers etc.)
* **kafka.producer.topic** - where test output goes
* **kafka.producer.resultsTopic** - where test results will be available (topic gets cleared if you run tests again)
* **kafka.producer.servers** - output kafka url
* **spark.home** - location of spark installation in test work directory on all machines, use ${WRK}/spark-bin-hadoop (**WRK** is set in `bin/setenv.sh`)
* **spark.master** - master node url, can be found in master node spark monitor
* *spark.machines* - only needed if you are submitting one test only, number of all machines used in test
* *spark.testtype* - only needed if you are submitting one test only, name of test to run
* other properties are equivalent to kafka or spark properties (see their docs), but you should not need to change these

### bin/setenv.sh
* **ALL_SERVERS[1]** - url of master node, any machine on your cluster
* **ALL_SERVERS[1+]** - other machines on your clusters, will be used as workers/slaves
* **KAFKA_INSTALL** - location of kafka installation where you will consume test results and test data
* **KAFKA_PRODUCER** - url of machine with kafka that data will be read from
* **KAFKA_CONSUMER** - url of kachine with kafka that data will be produced to
* **TESTING_TOPIC** - should match **kafka.producer.topic**
* **SERVICE_TOPIC** - should match **kafka.producer.resultsTopic**
* **MASTERURL** - should match **spark.master**
* **WRK** - folder used for everyting on all servers (installation of spark, project...)

### App.java
You should not need to change anything in the sources, unless you have different dataset. Then you may want to change TEST_DATA_RECORDS_SIZE (total number of records in dataset) or FILTER_TEST_IP (IP address used in filter test) variables.

## Testing
### to run a set of tests on topics with data
1. In `bin/all-test-read.sh` set the desired batch sizes, test types and amount of computers to test (+repetitions)
2. Execute `bin/all-test-read.sh`, results will be in **SERVICE_TOPIC**

Example:
```
TESTTYPES[1]=ReadWriteTest
TESTTYPES[2]=FilterIPTest
TESTTYPES[3]=CountTest
TESTTYPES[4]=AggregationTest
TESTTYPES[5]=TopNTest
TESTTYPES[6]=SynScanTest
COMPUTERS[1]=5
COMPUTERS[2]=4
REPEAT=2
```

runs all tests 2 times on 5 machines first, followed by all tests 2 times on 4 machines

### to run single test
1. Set producer and consumer topic names in `pom.xml` testbed profile
2. (Optional) Execute `bin/clean-cluster.sh`
3. Execute `bin/install-cluster.sh` to prepare (download and unpack) spark on every machine
4. Execute `bin/start-cluster.sh` to run spark in cluster mode, web-based user interface to monitor the cluster will be running at http://10.16.31.211:8080/ (master node) and http://10.16.31.212:8081/ http://10.16.31.213:8081/ http://10.16.31.214:8081/ http://10.16.31.215:8081/ (slaves)
5. Execute `bin/deploy-to-cluster.sh` to archive, copy, unpack and build at 10.16.31.211, app will then start automatically, ready to receive json from kafka
