---
id: limitations
title: Limitations
---

While Streamstone allows you to pass any number of events to `Stream.Write`, Azure Table Storage imposes the following limits:

- **Max batch size:** 100 entities
- **Batch will be flushed for every 99 events** (100 - 1 header entity)
- **Batch will be flushed for every 49 events with id set** (100/2 - 1 header entity)
- **InvalidOperationException** will be thrown if an event (with includes) exceeds the max batch size
- **Event payload size is not checked**; Azure Table Storage limits still apply

### Azure Table Storage API Limits

- Maximum size of batch: 4MB
- Maximum size of entity: 1MB
- Maximum size of property: 64KB
- Maximum length of property name: 255 characters
- An entity can have up to 255 custom properties

For more details, see the [WATS limitations on MSDN](http://msdn.microsoft.com/en-us/library/azure/dd179338.aspx). 