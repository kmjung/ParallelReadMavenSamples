
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

Run main function:
```bash
mvn compile exec:java \
    -Dexec.mainClass="BigQueryStorageSampler" \
    -Dexec.args="--parent 'bigquery-user-bensea' --table 'bigquery-user-bensea.bensea_readapi.users' --format 'Avro'"
```
