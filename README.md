# spark-test

1. Clone or download the project

### configuration

2. Set producer and consumer topic names in `pom.xml` testbed profile

### cluster preparation

3. (Optional) Execute `bin/clean-cluster.sh`
4. Execute `bin/install-cluster.sh` to prepare (download and unpack) spark on every machine

### cluster start

5. Execute `bin/start-cluster.sh` to run spark in cluster mode, web-based user interface to monitor the cluster will be running at http://10.16.31.211:8080/ (master node) and http://10.16.31.212:8081/ http://10.16.31.213:8081/ http://10.16.31.214:8081/ http://10.16.31.215:8081/ (slaves)

### project deployment and start

6. Execute `bin/deploy-to-cluster.sh` to archive, copy, unpack and build at 10.16.31.211, app will then start automatically, ready to receive json from kafka
