---
id: intro
title: Introduction
slug: /
---

![Streamstone Logo](https://github.com/yevhen/Streamstone/blob/master/Logo.Wide.png?raw=true)

# Streamstone

Streamstone is a tiny, embeddable .NET library for building scalable event-sourced applications on top of Azure Table Storage. It provides a simple, functional API inspired by Greg Young's Event Store.

## Features

- Fully ACID compliant
- Optimistic concurrency support
- Duplicate event detection (based on identity)
- Automatic continuation for both writes and reads (over WATS limits)
- Custom stream and event properties you can query on
- Synchronous (inline) projections and snapshots
- Change tracking support for inline projections
- Friendly for multi-tenant designs
- Sharding support (jump consistent hashing)
- Compatible with .NET Standard 2.0 and .NET Framework 4.6

## Quick Links

- [GitHub Repository](https://github.com/yevhen/Streamstone)
- [NuGet Package](https://www.nuget.org/packages/Streamstone/)
- [Gitter Community](https://gitter.im/yevhen/Streamstone)

## What is Streamstone?

Streamstone is a thin library layer (not a server) on top of Windows Azure Table Storage. It implements the low-level mechanics for dealing with event streams, while all heavy lifting is done by the underlying provider. The API is stateless and all exposed objects are immutable once constructed. Streamstone does not dictate the payload serialization protocol, so you are free to choose any protocol you want.

Optimistic concurrency is implemented by always including a stream header entity with every write, making it impossible to append to a stream without first having the latest Etag. Duplicate event detection is done by automatically creating an additional entity for every event, with RowKey set to a unique identifier of the source event (consistent secondary index).

---

Continue to the [Getting Started](getting-started.md) guide to learn how to install and use Streamstone. 