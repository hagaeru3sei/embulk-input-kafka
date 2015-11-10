# Kafka input plugin for Embulk

## Supported data format

- CSV
- TSV
- LTSV
- JSON

## Overview

* **Plugin type**: input
* **Resume supported**: yes
* **Cleanup supported**: yes
* **Guess supported**: yes

## Configuration

- **host**: description (string, required)
- **port**: description (integer, default: `2181`)
- **topic**: description (string, default: `test`)
- **data.format**: description (string, default: `null`)
- **data.column.enclosedChar**: description (string, default: ``)
- **data.columns**: description (string, default: `null`)
- **ignore.lines**: description (integer, default: `0`)
- **preview.sampling.count**: description (integer, default: `10`)

## Example

Kafka data format sample (JSON)

```json
{"version":"v1", "timestamp":"%Y-%m-%d %H:%M:%S", "key":"k1", "value":"v1"}
{"version":"v1", "timestamp":"%Y-%m-%d %H:%M:%S", "key":"k2", "value":"v2"}
.
.
.
```

```yaml
in:
  type: kafka
  host: localhost
  port: 2181
  topic: topic1
  group: group1
  data.format: json
  data.columns:
      - {name: "version", type: string}
      - {name: "timestamp", type: timestamp}
      - {name: "key", type: string}
      - {name: "value", type: string}
  ignore.lines: 0
  preview.sampling.count: 10
out:
  type: file
  path_prefix: kafka-input
  file_ext: csv
  formatter:
    type: csv
    column_options:
      version: {type: string}
      datetime: {format: '%Y-%m-%d %H:%M:%S'}
      key: {type: string}
      value: {type: string}
```

```
$ cp config.yml.sample config.yml
```


## Build

```
$ ./gradlew gem
```

```
$ ./gradlew package
```

## Run

```
$ embulk run -I./lib config.yml
```

## Guess

```
$ embulk guess -I./lib -g kafka config.yml -o config-guess.yml
```

## Preview

```
$ embulk preview -I./lib config.yml
```

