**Releases:**
[![Release Version](https://img.shields.io/github/v/release/downfa11-org/go-broker?label=go-broker)](https://github.com/downfa11-org/go-broker/releases/latest)

[![GitHub](https://img.shields.io/github/stars/downfa11-org/go-broker.svg?style=social)](https://github.com/downfa11-org/go-broker)
[![Contributors](https://img.shields.io/github/contributors/downfa11-org/go-broker.svg)](https://github.com/downfa11-org/go-broker/contributors)
[![Commits](https://img.shields.io/github/commit-activity/m/downfa11-org/go-broker.svg)](https://github.com/downfa11-org/go-broker/pulse)

[![Build Status](https://github.com/downfa11-org/go-broker/actions/workflows/ci-build.yml/badge.svg?branch=main)](https://github.com/downfa11-org/go-broker/actions/workflows/ci-build.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/downfa11-org/go-broker)](https://goreportcard.com/report/github.com/downfa11-org/go-broker)
[![CodeCov](https://img.shields.io/codecov/c/github/downfa11-org/go-broker)](https://codecov.io/gh/downfa11-org/go-broker)
[![Godoc](http://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)](https://godoc.org/github.com/downfa11-org/go-broker)

# Go-broker

## What is Go-broker?

Go-broker is a **lightweight message broker** inspired by Kafka’s design philosophy —
_logically separated but physically distributed data management_.

It aims to provide a minimal, efficient, and extensible messaging backbone for small-scale environments.

## Why Go-broker?

While Kafka’s persistence and fault-tolerant architecture are powerful,
its operational complexity and infrastructure cost can become heavy burdens for smaller deployments.

Go-broker was created to address these challenges by reimagining Kafka’s core concepts in a simpler form:

- Single-node architecture with minimal dependencies

- Clear logical data isolation and topic partitioning

<br>

In future releases, we will evolve into a distributed cluster
using `etcd` for coordination and metadata management.


## Documentation

To learn more about [documentation](docs/README.md).

## Community

### Contribution, Discussion and Support

This project is currently maintained by a single developer.

As the project grows and becomes more mature, we plan to establish more structured community such as a Slack and regular contributor meetings.

For now, you can:

- Ask questions or share feedback via GitHub Issues

- Reach out directly through email or GitHub

We truly welcome early contributors and feedback during this development phase.