![Build Core](https://github.com/debezium/debezium-connector-spanner/actions/workflows/maven.yml/badge.svg)

## Debezium connector spanner

### Prerequisites

need to set google credentials file path \
`export GOOGLE_APPLICATION_CREDENTIALS="/home/user/Downloads/service-account-file.json"`

or set connector config parameter `googleApplicationCredentialsFile`

### Tests

Run Unit tests
```
mvn test
```

Run coverage check
```
mvn clean test jacoco:report -P test-coverage
```

- Coverage report for unit tests is available at ${module.path}/target/site/jacoco/index.html

### Integration tests

Run the full IT suite against the local Spanner emulator (default, no real GCP project needed):
```
mvn clean verify
```
Run the full IT suite 
executed against a real Cloud Spanner instance instead of the emulator:
```
mvn clean verify \
  -Preal-spanner \
  -Dgcp.spanner.project.id=YOUR_PROJECT \
  -Dgcp.spanner.instance.id=YOUR_INSTANCE \
  -Dgcp.spanner.credentials.path=/path/to/key.json   
```
