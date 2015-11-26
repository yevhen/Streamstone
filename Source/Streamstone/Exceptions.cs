using System;
using System.Text;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    using Annotations;

    /// <summary>
    /// Represents errors thrown by Streamstone itself.
    /// </summary>
    public abstract class StreamstoneException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="StreamstoneException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="args">The arguments.</param>
        [StringFormatMethod("message")]
        protected StreamstoneException(string message, params object[] args)
            : base(args.Length > 0 ? string.Format(message, args) : message)
        {}
    }

    /// <summary>
    /// This exception is thrown when opening stream that doesn't exist 
    /// </summary>
    public sealed class StreamNotFoundException : StreamstoneException
    {
        /// <summary>
        /// The target partition
        /// </summary>
        public readonly Partition Partition;

        internal StreamNotFoundException(Partition partition)
            : base("Stream header was not found in partition '{1}' which resides in '{0}' table located at {2}",
                   partition.Table, partition, partition.Table.StorageUri)
        {
            Partition = partition;
        }
    }

    /// <summary>
    /// This exception is thrown when duplicate event is detected
    /// </summary>
    public sealed class DuplicateEventException : StreamstoneException
    {
        /// <summary>
        /// The target partition
        /// </summary>
        public readonly Partition Partition;

        /// <summary>
        /// The id of duplicate event
        /// </summary>
        public readonly string Id;

        internal DuplicateEventException(Partition partition, string id)
            : base("Found existing event with id '{3}' in partition '{1}' which resides in '{0}' table located at {2}",
                   partition.Table, partition, partition.Table.StorageUri, id)
        {
            Partition = partition;
            Id = id;
        }
    }

    /// <summary>
    /// This exception is thrown when included entity operation has conflicts in a partition
    /// </summary>
    public sealed class IncludedOperationConflictException : StreamstoneException
    {
        /// <summary>
        /// The target partition
        /// </summary>
        public readonly Partition Partition;

        /// <summary>
        /// The included entity
        /// </summary>
        public readonly ITableEntity Entity;

        IncludedOperationConflictException(Partition partition, ITableEntity entity, string message)
            : base(message)
        {
            Partition = partition;
            Entity = entity;
        }

        internal static IncludedOperationConflictException Create(Partition partition, EntityOperation include)
        {
            var dump = Dump(include.Entity);

            var message = string.Format(
                "Included '{3}' operation had conflicts in partition '{1}' which resides in '{0}' table located at {2}\n" +
                "Dump of conflicting [{5}] contents follows: \n\t{4}",
                partition.Table, partition, partition.Table.StorageUri, 
                include.GetType().Name, dump, include.Entity.GetType());

            return new IncludedOperationConflictException(partition, include.Entity, message);
        }

        static string Dump(ITableEntity entity)
        {
            var result = new StringBuilder();

            foreach (var property in entity.WriteEntity(new OperationContext()))
                result.Append($"\"{property.Key}\" : {property.Value}");

            return result.ToString();
        }
    }

    /// <summary>
    /// This exception is thrown when stream write/povision operation has conflicts in a partition
    /// </summary>
    public sealed class ConcurrencyConflictException : StreamstoneException
    {
        /// <summary>
        /// The target partition
        /// </summary>
        public readonly Partition Partition;

        internal ConcurrencyConflictException(Partition partition, string details)
            : base("Concurrent write detected for partition '{1}' which resides in table '{0}' located at {2}. See details below.\n{3}",
                   partition.Table, partition, partition.Table.StorageUri, details)
        {
            Partition = partition;
        }

        internal static Exception EventVersionExists(Partition partition, int version)
        {
            return new ConcurrencyConflictException(partition, string.Format("Event with version '{0}' is already exists", version));            
        }

        internal static Exception StreamChanged(Partition partition)
        {
            return new ConcurrencyConflictException(partition, "Stream header has been changed in a storage");
        }

        internal static Exception StreamChangedOrExists(Partition partition)
        {
            return new ConcurrencyConflictException(partition, "Stream header has been changed or already exists in a storage");
        }
    }

    /// <summary>
    /// This exception is thrown when Streamstone receives unexpected response from underlying WATS layer.
    /// </summary>
    public sealed class UnexpectedStorageResponseException : StreamstoneException
    {
        /// <summary>
        /// The error information
        /// </summary>
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