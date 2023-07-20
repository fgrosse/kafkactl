# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
- Fix bug in `kafkactl get topics` that caused wrong partition IDs in JSON output
- Add `--with-config` flag to `kafkactl create topic`

## [v1.2.0] - 2023-04-30
- Added support for username & password authentication (plaintext SASL)
- Added support for client certificate authentication (TLS)
- Significantly improve query performance of `kafkactl get topics`, thus making it an option for larger Kafka clusters

## [v1.1.0] - 2023-04-09
- Fix Inconsistent meaning of --output=raw in get message and consume command (see fgrosse/kafkactl#2)
- Make `github.com/fgrosse/kafkactl/pkg` internal
- Integration with the [Confluent Schema Registry]
- `kafkactl consume`: Support decoding Avro messages
- `kafkactl get message`: Support decoding Avro messages
- `kafkactl get topic`: Treat topics with single `_` prefix as internal (instead of double `_` prefix)
- `kafkactl get topic`: Show warning about missing topic metadata only in verbose mode

## [v1.0.0] - 2023-03-07
- Initial release

[Unreleased]: https://github.com/fgrosse/kafkactl/compare/v1.2.0...HEAD
[v1.2.0]: https://github.com/fgrosse/kafkactl/compare/v1.1.0...v1.2.0
[v1.1.0]: https://github.com/fgrosse/kafkactl/compare/v1.0.0...v1.1.0
[v1.0.0]: https://github.com/fgrosse/kafkactl/releases/tag/v1.0.0

[schema-registry]: https://docs.confluent.io/platform/current/schema-registry/index.html
