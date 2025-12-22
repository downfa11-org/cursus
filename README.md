# Cursus

<br>

<div align="center">

[![GitHub](https://img.shields.io/github/stars/downfa11-org/go-broker.svg?style=social)](https://github.com/downfa11-org/go-broker)
[![Release Version](https://img.shields.io/github/v/release/downfa11-org/go-broker?label=go-broker)](https://github.com/downfa11-org/go-broker/releases/latest)
[![Contributors](https://img.shields.io/github/contributors/downfa11-org/go-broker.svg)](https://github.com/downfa11-org/go-broker/contributors)
[![Commits](https://img.shields.io/github/commit-activity/m/downfa11-org/go-broker.svg)](https://github.com/downfa11-org/go-broker/pulse)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://github.com/downfa11-org/go-broker/blob/main/LICENSE)

[![Build Status](https://github.com/downfa11-org/go-broker/actions/workflows/ci-build.yml/badge.svg?branch=main)](https://github.com/downfa11-org/go-broker/actions/workflows/ci-build.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/downfa11-org/go-broker)](https://goreportcard.com/report/github.com/downfa11-org/go-broker)
[![CodeCov](https://img.shields.io/codecov/c/github/downfa11-org/go-broker)](https://codecov.io/gh/downfa11-org/go-broker)
[![Godoc](http://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)](https://godoc.org/github.com/downfa11-org/go-broker)

</div>

<br>

Cursus is a **lightweight message broker** inspired by design philosophy —
_logically separated but physically distributed data management_.

It aims to provide a minimal, efficient, and extensible messaging backbone for small-scale environments.

## Key Features:

Simple configuration with fast startup capability, high throughput and low latency

**Topic-based Messaging**:
- Parallel processing by partition unit
- Synchronous, asynchronous, and batch-based message publishing with idempotent producers
- Pull/Stream model consumption, consumer groups with automatic rebalancing

**Persistence**:
- Asynchronous disk writes with batching
- Segment rotation (1MB default), efficient reads through mmap

**Flexibility**:
- Platform-specific optimizations (Linux: sendfile, fadvise)
- Standalone (single node) and Distributed Cluster (Raft) mode selection possible

## Documentation

To learn more about [documentation](docs/README.md).

## Community

This project is currently maintained by a single developer.
We truly welcome early contributors and feedback during this development phase.

As the project grows and becomes more mature, we plan to establish more structured community such as a Slack and regular contributor meetings.

For now, you can:
- Ask questions or share feedback via GitHub Issues
- Reach out directly through email or GitHub
