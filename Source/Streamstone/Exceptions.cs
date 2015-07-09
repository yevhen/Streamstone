using System;
using System.Linq;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

using Newtonsoft.Json;

namespace Streamstone
{
    using Annotations;

    public abstract class StreamstoneException : Exception
    {
        [StringFormatMethod("message")]
        protected StreamstoneException(string message, params object[] args)
            : base(args.Length > 0 ? string.Format(message, args) : message)
        {}
    }

    public sealed class StreamNotFoundException : StreamstoneException
    {
        public readonly CloudTable Table;
        public readonly Partition Partition;

        internal StreamNotFoundException(CloudTable table, Partition partition)
            : base("Stream header was not found in partition '{1}' which resides in '{0}' table located at {2}",
                   table, partition, table.StorageUri)
        {
            Table = table;
            Partition = partition;
        }
    }

    public sealed class DuplicateEventException : StreamstoneException
    {
        public readonly CloudTable Table;
        public readonly Partition Partition;
        public readonly string Id;

        internal DuplicateEventException(CloudTable table, Partition partition, string id)
            : base("Found existing event with id '{3}' in partition '{1}' which resides in '{0}' table located at {2}",
                   table, partition, table.StorageUri, id)
        {
            Table = table;
            Partition = partition;
            Id = id;
        }
    }    
    
    public sealed class IncludedOperationConflictException : StreamstoneException
    {
        public readonly CloudTable Table;
        public readonly Partition Partition;
        public readonly ITableEntity Entity;

        IncludedOperationConflictException(CloudTable table, Partition partition, ITableEntity entity, string message)
            : base(message)
        {
            Table = table;
            Partition = partition;
            Entity = entity;
        }

        internal static IncludedOperationConflictException Create(CloudTable table, Partition partition, EntityOperation include)
        {
            var dump = JsonConvert.SerializeObject(include.Entity, Formatting.Indented);

            var message = string.Format(
                "Included '{3}' operation had conflicts in partition '{1}' which resides in '{0}' table located at {2}\n" +
                "Dump of conflicting [{5}] contents follows: \n\t{4}",
                table, partition, table.StorageUri, include.GetType().Name, dump, include.Entity.GetType());

            return new IncludedOperationConflictException(table, partition, include.Entity, message);
        }
    }

    public sealed class ConcurrencyConflictException : StreamstoneException
    {
        public readonly CloudTable Table;
        public readonly Partition Partition;

        internal ConcurrencyConflictException(CloudTable table, Partition partition, string details)
            : base("Concurrent write detected for partition '{1}' which resides in table '{0}' located at {2}. See details below.\n{3}",
                   table, partition, table.StorageUri, details)
        {
            Table = table;
            Partition = partition;
        }

        internal static Exception EventVersionExists(CloudTable table, Partition partition, int version)
        {
            return new ConcurrencyConflictException(table, partition, string.Format("Event with version '{0}' is already exists", version));            
        }

        internal static Exception StreamChanged(CloudTable table, Partition partition)
        {
            return new ConcurrencyConflictException(table, partition, "Stream header has been changed in a storage");
        }

        internal static Exception StreamChangedOrExists(CloudTable table, Partition partition)
        {
            return new ConcurrencyConflictException(table, partition, "Stream header has been changed or already exists in a storage");
        }
    }

    public sealed class UnexpectedStorageResponseException : StreamstoneException
    {
        public readonly StorageExtendedErrorInformation Error;

        UnexpectedStorageResponseException(StorageExtendedErrorInformation error, string details)
            : base("Unexpected Table Storage response. Details: " + details)
        {
            Error = error;
        }

        internal static Exception ErrorCodeShouldBeEntityAlreadyExists(StorageExtendedErrorInformation error)
        {
            return new UnexpectedStorageResponseException(error, "Erorr code should be indicated as 'EntityAlreadyExists' but was: " + error.ErrorCode);
        }

        internal static Exception ConflictExceptionMessageShouldHaveExactlyThreeLines(StorageExtendedErrorInformation error)
        {
            return new UnexpectedStorageResponseException(error, "Conflict exception message should have exactly 3 lines");
        }

        internal static Exception ConflictExceptionMessageShouldHaveSemicolonOnFirstLine(StorageExtendedErrorInformation error)
        {
            return new UnexpectedStorageResponseException(error, "Conflict exception message should have semicolon on first line");
        }

        internal static Exception UnableToParseTextBeforeSemicolonToInteger(StorageExtendedErrorInformation error)
        {
            return new UnexpectedStorageResponseException(error, "Unable to parse text on first line before semicolon as integer");
        }
    }
}