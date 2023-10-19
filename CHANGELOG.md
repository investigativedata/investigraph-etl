# investigraph-etl

## CHANGELOG

### 0.4.0 (2023-10-19)

- Drop support for python 3.10
- Fix `ftmq` dependency

### 0.3.4 (2023-10-16)

- Fix `investigraph inspect` command
- Move some util code to `ftmq`
- Update dependencies

### 0.3.3 (2023-10-03)

- Update dependencies

### 0.3.2 (2023-09-08)
- introduce optional seed stage before extract stage
- minor improvements and dependencies updates

### 0.3.1 (2023-07-31)
- refactor aggregation to its own stage

### 0.3.0 (2023-07-28)
- implement incremental task caching
- re-model [nomenklatura](https://github.com/opensanctions/nomenklatura) in pydantic
- improve catalog building

### 0.2.0 (2023-07-19)
- integrate [runpandarun](https://github.com/simonwoerpel/runpandarun) into extract stage

### 0.1.0 (2023-07-13)
- breaking changes in:
    - cli invocation for `investigraph run`
    - `config.yaml` structure
- make all three stages customizable via *bring your own code*
- many small fixes and improvements
