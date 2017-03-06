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

- **zookeepers**: description (string, required)
- **topic**: description (string, default: `test`)
- **data.format**: description (string, default: `null`)
- **data.column.enclosedChar**: description (string, default: ``)
- **data.columns**: description (string, default: `null`)
- **ignore.lines**: description (integer, default: `0`)
- **preview.sampling.count**: description (integer, default: `10`)
- **thread.count**: description (integer, default: `1`)

## Example

Kafka data format sample (JSON)

```json
{"version":"v1", "timestamp":"%Y-%m-%d %H:%M:%S", "key":"k1", "value":"v1"}
{"version":"v1", "timestamp":"%Y-%m-%d %H:%M:%S", "key":"k2", "value":"v2"}
.
.
.
```

Config

```yaml
in:
  type: kafka
  zookeepers: localhost:2181
  topic: topic1
  group: group1
  data.format: json
  data.column.enclosedChar: ""
  data.columns:
      - {name: "version", type: string}
      - {name: "timestamp", type: timestamp}
      - {name: "key", type: string}
      - {name: "value", type: string}
  ignore.lines: 0
  preview.sampling.count: 10
  thread.count: 1
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

- zookeepers   - host1:host1_port,host2:host2_port,..
- data.format  - (csv|tsv|ltsv|json)
- ignore.lines - Ignore header lines. this is for csv,tsv with header column names.


## Build

```
$ ./gradlew gem
```

```
$ ./gradlew package
```

## Guess

```
$ cp config.yml.sample config.yml
```

Edit "zookeepers, topic, group, data.format" section

```
$ embulk guess -I./lib config.yml -o config-guess.yml
```

## Preview

```
$ embulk preview -I./lib config.yml
```

## Run

```
$ embulk run -I./lib config.yml
```

