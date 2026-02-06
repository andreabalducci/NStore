# NStore.Persistence.Mongo.Tests

This project contains MongoDB integration tests and Mongo batch performance benchmarks.

## Prerequisites

- .NET SDK installed (project targets `net6.0` and `net10.0`)
- A reachable MongoDB instance:
  - local MongoDB (for example in Docker), or
  - cloud MongoDB (for example Atlas)

## Connection Configuration

Connection string resolution order used by tests:

1. `NSTORE_MONGODB` environment variable
2. If `NSTORE_MONGO_BATCH_PERF=1`:
   - `NStore:Mongo:Performance:ConnectionString`
   - `NStore:Mongo:Performance:AtlasConnectionString`
3. `NStore:Mongo:ConnectionString`

Config sources loaded by tests:

- `appsettings.json` (in this project)
- optional `appsettings.Local.json`
- user secrets (`UserSecretsId: nstore-persistence-mongo-tests`)

## Local Mongo Setup

Start local MongoDB:

```bash
docker run --name nstore-mongo -p 27017:27017 -d mongo:7
```

Run all Mongo tests against local MongoDB:

```bash
NSTORE_MONGODB='mongodb://localhost:27017/nstoredev' \
dotnet test src/NStore.Persistence.Mongo.Tests/NStore.Persistence.Mongo.Tests.csproj -c Release -f net10.0
```

## Cloud Mongo Setup (Atlas)

Store connection string in user secrets:

```bash
dotnet user-secrets --project src/NStore.Persistence.Mongo.Tests/NStore.Persistence.Mongo.Tests.csproj \
  set "NStore:Mongo:Performance:ConnectionString" "mongodb+srv://<user>:<password>@<cluster>/<db>?authSource=admin"
```

You can also set:

```bash
dotnet user-secrets --project src/NStore.Persistence.Mongo.Tests/NStore.Persistence.Mongo.Tests.csproj \
  set "NStore:Mongo:Performance:AtlasConnectionString" "mongodb+srv://<user>:<password>@<cluster>/<db>?authSource=admin"
```

## Performance Benchmark

Main perf test:

- `mongodb_batch_insert_performance_tests.should_measure_batch_insert_performance_degradation`

Enable and run perf suite:

```bash
NSTORE_MONGO_BATCH_PERF=1 \
dotnet test src/NStore.Persistence.Mongo.Tests/NStore.Persistence.Mongo.Tests.csproj -c Release -f net10.0 \
  --filter "FullyQualifiedName~mongodb_batch_insert_performance_tests.should_measure_batch_insert_performance_degradation"
```

### Scenarios

Scenarios are read from:

- `NStore:Mongo:Performance:TestParameters` in `appsettings.json` / `appsettings.Local.json` / user secrets

Each scenario supports:

- `Name` (string)
- `BatchSize` (int)
- `Writers` (int or `"unbounded"`)
- `TotalChunks` (long)

Current default scenarios in `appsettings.json`:

- `batch-100-writers-1-chunks-2000`
- `batch-50-writers-2-chunks-2000`
- `batch-25-writers-4-chunks-2000`
- `batch-25-writers-unbounded-chunks-2000`

Execution order is:

1. `Writers` ascending (unbounded last)
2. `BatchSize` ascending
3. `Name` ascending

Each scenario starts from an empty test database (`Create(true)` / `DropOnInit`).

### Run One Scenario

Use `NSTORE_MONGO_BATCH_PERF_SCENARIO` with one or more comma-separated names:

```bash
NSTORE_MONGO_BATCH_PERF=1 \
NSTORE_MONGO_BATCH_PERF_SCENARIO='batch-25-writers-unbounded-chunks-2000' \
dotnet test src/NStore.Persistence.Mongo.Tests/NStore.Persistence.Mongo.Tests.csproj -c Release -f net10.0 \
  --filter "FullyQualifiedName~mongodb_batch_insert_performance_tests.should_measure_batch_insert_performance_degradation"
```

### Useful Perf Environment Variables

- `NSTORE_MONGO_BATCH_PERF=1` enable benchmark
- `NSTORE_MONGO_BATCH_PERF_LOG_FILE=/path/to/suite.csv` set output suite CSV path
- `NSTORE_MONGO_BATCH_PERF_SCENARIO=<name[,name2]>` run only selected scenarios
- `NSTORE_MONGO_BATCH_PERF_PARTITIONS=<int>` partition count (default `100`)
- `NSTORE_MONGO_BATCH_PERF_WARMUP=<int>` warmup batches (default `3`)
- `NSTORE_MONGO_BATCH_PERF_PROGRESS_EVERY_BATCHES=<int>` progress interval override
- `NSTORE_MONGO_BATCH_PERF_MAX_DEGRADATION=<double>` optional assertion threshold

When no config scenarios are found, env fallback is used:

- `NSTORE_MONGO_BATCH_PERF_TOTAL_CHUNKS` (default `2000`)
- `NSTORE_MONGO_BATCH_PERF_BATCH_SIZE` (default `1000`)
- `NSTORE_MONGO_BATCH_PERF_WRITERS` (default `1`, accepts `"unbounded"`)

## Output

Perf run writes:

- one suite CSV (summary of all executed scenarios)
- one scenario CSV per scenario (detailed progress + scenario summary)

Note:

- in suite CSV, unbounded writers are currently represented as `writers=0`
- in scenario CSV header, both `writers=unbounded` and `effective_writers=<n>` are written

By default output is under `TestResults/` in the test process working directory.
