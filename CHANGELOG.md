# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
- Support decoding Avro messages
- `kafkactl get topic`: Treat topics with single `_` prefix as internal (instead of double `_` prefix)
- `kafkactl get topic`: Show warning about missing topic metadata only in verbose mode

## [v1.0.0] - 2023-03-07
- Initial release

[Unreleased]: https://github.com/fgrosse/kafkactl/compare/v1.0.0...HEAD
[v1.0.0]: https://github.com/fgrosse/kafkactl/releases/tag/v1.0.0

