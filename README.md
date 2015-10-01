# Kafka input plugin for Embulk

TODO: Write short description here and build.gradle file.

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
- **data.columns**: description (string, default: `null`)
- **ignore.lines**: description (integer, default: `0`)

## Example

Kafka data format sample

```json
{
  "version"  : "v1",
  "datetime" : "%Y-%m-%d %H:%M:%S",
  "key"      : "k",
  "value"    : "v1"
}
```

```yaml
in:
  type: kafka
  host: localhost
  port: 2181
  topic: test
  data.format: json
  data.columns:
      - {name: "version", type: string}
      - {name: "datetime", type: string}
      - {name: "key", type: string}
      - {name: "value", type: string}
   ignore.lines: 0
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

