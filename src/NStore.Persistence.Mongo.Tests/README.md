# NStore.Persistence.Mongo.Tests

This project contains MongoDB integration tests and Mongo batch performance benchmarks.

## Prerequisites

- .NET SDK installed (project targets `net6.0` and `net10.0`)
- A reachable MongoDB instance:
  - local MongoDB (for example in Docker), or
  - cloud MongoDB (for example Atlas)

## Connection Configuration

Config sources loaded by tests:

- `appsettings.json` (in this project)
- optional `appsettings.Local.json`
- user secrets (`UserSecretsId: nstore-persistence-mongo-tests`)

Connection string resolution order:

- Standard tests (`NStore:Mongo:Performance:Enabled` is not `true`):
  1. `NSTORE_MONGODB` environment variable
  2. `NStore:Mongo:ConnectionString`
- Perf tests (`NStore:Mongo:Performance:Enabled=true`):
  1. `NStore:Mongo:Performance:ConnectionString`
  2. `NStore:Mongo:ConnectionString`

Note: perf mode is config-driven; environment variables are not required to run perf tests.

## Durable Writes (Flush Guarantees)

To require server-side durability acknowledgements for benchmark writes, include write concern
and journaling settings in the connection string:

- local standalone MongoDB: `w=1&journal=true&wtimeoutMS=30000`
- replica set / Atlas: `w=majority&journal=true&wtimeoutMS=30000`

Example local user secret:

```bash
dotnet user-secrets --project src/NStore.Persistence.Mongo.Tests/NStore.Persistence.Mongo.Tests.csproj \
  set "NStore:Mongo:ConnectionString" "mongodb://localhost:27017/nstoredev?w=1&journal=true&wtimeoutMS=30000"
```

## Local Mongo Setup

Start local MongoDB:

```bash
docker run --name nstore-mongo -p 27017:27017 -d mongo:7
```

Run all Mongo tests against local MongoDB:

```bash
dotnet test src/NStore.Persistence.Mongo.Tests/NStore.Persistence.Mongo.Tests.csproj -c Release -f net10.0
```

## Cloud Mongo Setup (Atlas)

Store perf connection string in user secrets:

```bash
dotnet user-secrets --project src/NStore.Persistence.Mongo.Tests/NStore.Persistence.Mongo.Tests.csproj \
  set "NStore:Mongo:Performance:ConnectionString" "mongodb+srv://<user>:<password>@<cluster>/<db>?authSource=admin&w=majority&journal=true&wtimeoutMS=30000"
```

## Performance Benchmark

Perf benchmark tests:

- `mongodb_batch_insert_performance_tests.should_measure_batch_insert_performance_degradation`
- `mongodb_parallel_extension_batch_insert_performance_tests.should_measure_parallel_extension_batch_insert_performance_degradation`

Enable perf mode in configuration (user secrets example):

```bash
dotnet user-secrets --project src/NStore.Persistence.Mongo.Tests/NStore.Persistence.Mongo.Tests.csproj \
  set "NStore:Mongo:Performance:Enabled" "true"
```

Run only the channel-workers perf benchmark test:

```bash
dotnet test src/NStore.Persistence.Mongo.Tests/NStore.Persistence.Mongo.Tests.csproj -c Release -f net10.0 \
  --filter "FullyQualifiedName~mongodb_batch_insert_performance_tests.should_measure_batch_insert_performance_degradation"
```

Run only the extension-method perf benchmark test:

```bash
dotnet test src/NStore.Persistence.Mongo.Tests/NStore.Persistence.Mongo.Tests.csproj -c Release -f net10.0 \
  --filter "FullyQualifiedName~mongodb_parallel_extension_batch_insert_performance_tests.should_measure_parallel_extension_batch_insert_performance_degradation"
```

Run the normal suite without perf tests:

```bash
dotnet test src/NStore.Persistence.Mongo.Tests/NStore.Persistence.Mongo.Tests.csproj -c Release -f net10.0 \
  --filter "Category!=Performance"
```

Run both perf suites and print a consolidated summary table:

```bash
bash scripts/run-mongo-perf-tests.sh
```

The script enables perf mode for the test process via:
`NStore__Mongo__Performance__Enabled=true`.

### Scenarios

Scenarios are read from:

- `NStore:Mongo:Performance:TestParameters` in `appsettings.json` / `appsettings.Local.json` / user secrets

Each scenario supports:

- `BatchSize` (int)
- `Writers` (int or `"unbounded"`)
- `TotalChunks` (long, optional when `DefaultTotalChunks` is configured)
- `Name` is derived automatically as: `chunks-<TotalChunks>-batch-<BatchSize>-writers-<Writers|unbounded>`

Execution order is:

1. `Writers` ascending (unbounded last)
2. `BatchSize` ascending
3. `TotalChunks` ascending

Each scenario starts from an empty test database (`Create(true)` / `DropOnInit`).
The suite waits 5 seconds between scenarios to reduce transient server/oplog pressure.

### Run One Scenario

Set scenario filter in config (user secrets example):

```bash
dotnet user-secrets --project src/NStore.Persistence.Mongo.Tests/NStore.Persistence.Mongo.Tests.csproj \
  set "NStore:Mongo:Performance:ScenarioFilter" "chunks-1000-batch-25-writers-unbounded"
```

Then run the perf test command.

### Useful Perf Configuration Keys

- `NStore:Mongo:Performance:Enabled` (bool) enable benchmark tests
- `NStore:Mongo:Performance:ConnectionString` (string) perf Mongo target
- `NStore:Mongo:Performance:SuiteLogFile` (string, optional) explicit suite CSV path
- `NStore:Mongo:Performance:ScenarioFilter` (string, optional) comma-separated scenario names
- `NStore:Mongo:Performance:PartitionCount` (int, default `100`)
- `NStore:Mongo:Performance:DefaultTotalChunks` (long, optional fallback for scenario `TotalChunks`)
- `NStore:Mongo:Performance:WarmupBatches` (int, default `3`)
- `NStore:Mongo:Performance:ProgressEveryBatches` (int, optional)
- `NStore:Mongo:Performance:MaxDegradation` (double, optional, informational only; does not fail tests)
- `NStore:Mongo:Performance:ParallelBatchSize` (int, optional) parallel extension mode only
- `NStore:Mongo:Performance:ParallelWriters` (int, optional) parallel extension mode only

## Output

Perf run writes:

- one suite CSV (summary of all executed scenarios)
- one scenario CSV per scenario (detailed progress + scenario summary)
- first CSV lines include metadata comments:
  - `# mongodb_url=<scheme://server-or-cluster/database>` (credentials removed)
  - `# started_utc=<timestamp>`
  - suite also includes `inter_scenario_delay_s=5`

Notes:

- in suite CSV, `writers` is the configured value (`0` for unbounded) and `effective_writers` is the resolved runtime writer count
- in scenario CSV header, both `writers=unbounded` and `effective_writers=<n>` are written

By default output is under `TestResults/` in the test process working directory.
