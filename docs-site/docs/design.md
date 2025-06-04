---
id: design
title: Design
---

Streamstone is a thin library layer on top of Windows Azure Table Storage. It implements the low-level mechanics for dealing with event streams, while all heavy lifting is done by the underlying provider.

- The API is stateless and all exposed objects are immutable once constructed.
- Streamstone does not dictate the payload serialization protocol, so you are free to choose any protocol you want.
- Optimistic concurrency is implemented by always including a stream header entity with every write, making it impossible to append to a stream without first having the latest Etag.
- Duplicate event detection is done by automatically creating an additional entity for every event, with RowKey set to a unique identifier of the source event (consistent secondary index).

## Schema

![Schema](/img/Schema.png)

---

![Schema for virtual partitions](/img/Schema_VP.png) 