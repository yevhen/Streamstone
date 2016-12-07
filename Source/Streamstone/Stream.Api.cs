using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    public sealed partial class Stream
    {
        /// <summary>
        /// Initiates an asynchronous operation that provisions new stream in the specified partition.
        /// </summary>
        /// <param name="partition">The partition.</param>
        /// <returns>The promise, that wil eventually return stream header or will fail with exception</returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="partition"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ConcurrencyConflictException">
        ///     If stream already exists in the partition
        /// </exception>
        public static Task<Stream> ProvisionAsync(Partition partition)
        {
            return ProvisionAsync(new Stream(partition));
        }

        /// <summary>
        /// Initiates an asynchronous operation that provisions new stream with the given properties in the specified partition.
        /// </summary>
        /// <param name="partition">The partition.</param>
        /// <param name="properties">The stream properties</param>
        /// <returns>The promise, that wil eventually return stream header or will fail with exception</returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="partition"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="properties"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ConcurrencyConflictException">
        /// If stream already exists in the partition
        /// </exception>        
        public static Task<Stream> ProvisionAsync(Partition partition, StreamProperties properties)
        {
            return ProvisionAsync(new Stream(partition, properties));
        }

        /// <summary>
        /// Initiates an asynchronous operation that provisions specified stream.
        /// </summary>
        /// <param name="stream">The transient stream header.</param>
        /// <returns>The promise, that wil eventually return updated, persistent stream header or will fail with exception</returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="stream"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ConcurrencyConflictException">
        ///     If stream already exists in the partition
        /// </exception>
        static Task<Stream> ProvisionAsync(Stream stream)
        {
            Requires.NotNull(stream, "stream");
            return new ProvisionOperation(stream).ExecuteAsync();
        }

        /// <summary>
        /// Initiates an asynchronous operation that writes the given array of events to a stream using specified stream header.
        /// </summary>
        /// <param name="stream">The stream header.</param>
        /// <param name="events">The events to write.</param>
        /// <returns>
        ///     The promise, that wil eventually return the result of the stream write operation 
        ///     containing updated stream header or will fail with exception
        /// </returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="stream"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="events"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ArgumentOutOfRangeException">
        ///    If <paramref name="events"/> array is empty
        /// </exception>
        /// <exception cref="DuplicateEventException">
        ///     If event with the given id already exists in a storage
        /// </exception>
        /// <exception cref="IncludedOperationConflictException">
        ///     If included entity operation has conflicts
        /// </exception>
        /// <exception cref="ConcurrencyConflictException">
        ///     If write operation has conflicts
        /// </exception>
        public static Task<StreamWriteResult> WriteAsync(Stream stream, params EventData[] events)
        {
            Requires.NotNull(stream, "stream");
            Requires.NotNull(events, "events");

            if (events.Length == 0)
                throw new ArgumentOutOfRangeException("events", "Events have 0 items");

            return new WriteOperation(stream, events).ExecuteAsync();
        }

        /// <summary>
        /// Initiates an asynchronous operation that writes the given array of events to a partition using specified expected version.
        /// </summary>
        /// <remarks>For new stream specify expected version as 0</remarks>
        /// <param name="partition">The partition.</param>
        /// <param name="expectedVersion">The expected version of the stream.</param>
        /// <param name="events">The events to write.</param>
        /// <returns>
        ///     The result of the stream write operation containing updated stream header
        /// </returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="partition"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="events"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ArgumentOutOfRangeException">
        ///    If <paramref name="expectedVersion"/> is less than 0
        /// </exception> 
        /// <exception cref="ArgumentOutOfRangeException">
        ///    If <paramref name="events"/> array is empty
        /// </exception>
        /// <exception cref="DuplicateEventException">
        ///     If event with the given id already exists in a storage
        /// </exception>
        /// <exception cref="IncludedOperationConflictException">
        ///     If included entity operation has conflicts
        /// </exception>
        /// <exception cref="ConcurrencyConflictException">
        ///     If write operation has conflicts
        /// </exception>
        public static async Task<StreamWriteResult> WriteAsync(Partition partition, int expectedVersion, params EventData[] events)
        {
            Requires.NotNull(partition, nameof(partition));
            Requires.GreaterThanOrEqualToZero(expectedVersion, nameof(expectedVersion));

            var stream = expectedVersion == 0
                ? new Stream(partition)
                : await OpenAsync(partition);

            if (stream.Version != expectedVersion)
                throw ConcurrencyConflictException.StreamChangedOrExists(partition);

            return await WriteAsync(stream, events);
        }

         /// <summary>
        /// Initiates an asynchronous operation that sets the given stream properties (metadata).
        /// </summary>
        /// <param name="stream">The stream header.</param>
        /// <param name="properties">The properties.</param>
        /// <returns>The promise, that wil eventually return updated stream header or will fail with exception</returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="stream"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="properties"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ArgumentException">
        ///     If given stream header represents a transient stream
        /// </exception>
        /// <exception cref="ConcurrencyConflictException">
        ///     If stream has been changed in storage after the given stream header has been read
        /// </exception>
        public static Task<Stream> SetPropertiesAsync(Stream stream, StreamProperties properties)
        {
            Requires.NotNull(stream, "stream");
            Requires.NotNull(properties, "properties");

            if (stream.IsTransient)
                throw new ArgumentException("Can't set properties on transient stream", "stream");

            return new SetPropertiesOperation(stream, properties).ExecuteAsync();
        }

        /// <summary>
        /// Initiates an asynchronous operation that opens the stream in specified partition. Basically, it just return a stream header.
        /// </summary>
        /// <param name="partition">The partition.</param>
        /// <returns>
        ///     The promise, that wil eventually return the stream header or wil fail with exception
        /// </returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="partition"/> is <c>null</c>
        /// </exception>
        /// <exception cref="StreamNotFoundException">
        ///     If there is no stream in a given partition
        /// </exception>
        public static async Task<Stream> OpenAsync(Partition partition)
        {
            var result = await TryOpenAsync(partition).Really();

            if (result.Found)
                return result.Stream;

            throw new StreamNotFoundException(partition);
        }
        
        /// <summary>
        /// Initiates an asynchronous operation that tries to open the stream in a specified partition.
        /// </summary>
        /// <param name="partition">The partition.</param>
        /// <returns>
        ///     The promise, that wil eventually return the result of stream open operation, 
        ///     which could be further examined for stream existence;  or wil fail with exception
        /// </returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="partition"/> is <c>null</c>
        /// </exception>
        public static Task<StreamOpenResult> TryOpenAsync(Partition partition)
        {
            Requires.NotNull(partition, "partition");

            return new OpenStreamOperation(partition).ExecuteAsync();
        }

        /// <summary>
        /// Initiates an asynchronous operation that checks if there is a stream exists in the specified partition.
        /// </summary>
        /// <param name="partition">The partition.</param>
        /// <returns>
        ///     The promise, that wil eventually return <c>true</c>
        ///     if stream header was found in the specified partition,  <c>false</c> otherwise
        /// </returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="partition"/> is <c>null</c>
        /// </exception>
        public static async Task<bool> ExistsAsync(Partition partition)
        {
            return (await TryOpenAsync(partition).Really()).Found;
        }

        const int DefaultSliceSize = 1000;
        
        /// <summary>
        /// Initiates an asynchronous operation that reads the events from a stream in a specified partition.
        /// </summary>
        /// <typeparam name="T">The type of event entity to return</typeparam>
        /// <param name="partition">The partition.</param>
        /// <param name="startVersion">The start version.</param>
        /// <param name="sliceSize">Size of the slice.</param>
        /// <returns>
        ///     The promise, that wil eventually return the slice of the stream, 
        ///     which contains events that has been read; or will fail with exception
        /// </returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="partition"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ArgumentOutOfRangeException">
        ///     If <paramref name="startVersion"/> &lt; 1
        /// </exception>
        /// <exception cref="ArgumentOutOfRangeException">
        ///     If <paramref name="sliceSize"/> &lt; 1
        /// </exception>       
        /// <exception cref="StreamNotFoundException">
        ///     If there is no stream in a given partition
        /// </exception>
        public static Task<StreamSlice<T>> ReadAsync<T>(
            Partition partition, 
            int startVersion = 1, 
            int sliceSize = DefaultSliceSize) 
            where T : class, new()
        {
            Requires.NotNull(partition, "partition");
            Requires.GreaterThanOrEqualToOne(startVersion, "startVersion");
            Requires.GreaterThanOrEqualToOne(sliceSize, "sliceSize");

            return new ReadOperation<T>(partition, startVersion, sliceSize)
                .ExecuteAsync(BuildEntity<T>());
        }

        /// <summary>
        /// Initiates an asynchronous operation that reads the events from a stream in a specified partition.
        /// </summary>
        /// <param name="partition">The partition.</param>
        /// <param name="startVersion">The start version.</param>
        /// <param name="sliceSize">Size of the slice.</param>
        /// <returns>
        ///     The promise, that wil eventually return the slice of the stream, 
        ///     which contains events that has been read; or will fail with exception
        /// </returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="partition"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ArgumentOutOfRangeException">
        ///     If <paramref name="startVersion"/> &lt; 1
        /// </exception>
        /// <exception cref="ArgumentOutOfRangeException">
        ///     If <paramref name="sliceSize"/> &lt; 1
        /// </exception>       
        /// <exception cref="StreamNotFoundException">
        ///     If there is no stream in a given partition
        /// </exception>
        public static Task<StreamSlice<EventProperties>> ReadAsync(
            Partition partition,
            int startVersion = 1,
            int sliceSize = DefaultSliceSize)
        {
            Requires.NotNull(partition, "partition");
            Requires.GreaterThanOrEqualToOne(startVersion, "startVersion");
            Requires.GreaterThanOrEqualToOne(sliceSize, "sliceSize");

            return new ReadOperation<EventProperties>(partition, startVersion, sliceSize)
                .ExecuteAsync(BuildEventProperties);
        }

        static Func<DynamicTableEntity, T> BuildEntity<T>() where T : class, new()
        {
            if (typeof(T) == typeof(DynamicTableEntity))
                return e => e as T;

            return e =>
            {
                var t = new T();
                TableEntity.ReadUserObject(t, e.Properties, new OperationContext());
                return t;
            };
        }

        static EventProperties BuildEventProperties(DynamicTableEntity e)
        {
            return EventProperties.ReadEntity(e.Properties);
        }
    }
}