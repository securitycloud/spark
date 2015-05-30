# spark-test

1. Download and unpack spark (built for hadoop) https://spark.apache.org/downloads.html
2. Set **spark_home** property in `pom.xml` to the unpacked spark folder location
3. Navigate to project root directory and execute `mvn package exec:exec -Pprofile_name`
