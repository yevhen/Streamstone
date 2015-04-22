![Streamstone](Logo.Wide.png)

Streamstone is a small library targeted at building scalable event-sourced solutions on top of Windows Azure Table Storage. The API is specifically tailored for ease of consumption from within DDD/CQRS style applications.

[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/yevhen/Streamstone?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Build status](https://ci.appveyor.com/api/projects/status/3rsmwblor11b6inq/branch/master?svg=true)](https://ci.appveyor.com/project/yevhen/streamstone/branch/master)

## Main features

+ All stream operations are fully consistent
+ Duplicate event detection (based on unique id of an event)
+ Supports custom event and stream metadata
+ Batching support for writes
+ Allows including custom table operations within batch 
+ Easy-to-use, functional style immutable API
+ Virtual streams

## License

Apache 2 License
