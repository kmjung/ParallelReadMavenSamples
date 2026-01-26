
## Build jar
Install java, maven.

Then run:
```bash
mvn clean install
```

## Run jar

First authentication to get ADC:
```bash
gcloud auth application-default login
```

Read data with Avro:
```bash
mvn compile exec:java \
    -Dexec.mainClass="BigQueryStorageSampler" \
    -Dexec.args="--parent 'bigquery-user-bensea' --table 'bigquery-user-bensea.bensea_readapi.users' --format 'Avro'"
```

Read data with Arrow:
```bash
export JDK_JAVA_OPTIONS="--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED"

mvn compile exec:java \
    -Dexec.mainClass="BigQueryStorageSampler" \
    -Dexec.args="--parent 'bigquery-user-bensea' --table 'bigquery-user-bensea.bensea_readapi.users' --format 'Arrow'"
```
