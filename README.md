# spark-test

1. Clone or download the project
## configuration
2. Set producer and consumer topic names in `pom.xml` testbed profile
## cluster preparation
3. (Optional) Execute `scripts/clean-cluster.sh`
4. Execute `scripts/install-cluster.sh` to prepare (download and unpack) spark on every machine
## cluster start
5. Execute `scripts/start-cluster.sh` to run spark in cluster mode, web-based user interface to monitor the cluster will be running at http://10.16.31.211:8080/ (master node) and http://10.16.31.212-215:8081/ (slaves)
## project deployment and start
6. Execute `scripts/deploy-to-cluster.sh` to archive, copy, unpack and build at 10.16.31.211, app will then start automatically, ready to receive json from kafka
