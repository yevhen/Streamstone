﻿using System;
using System.Text.Json;
using System.Text.Json.Serialization;

using Azure.Data.Tables;

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
                   partition.Table, partition, partition.Table.Uri)
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

        internal DuplicateEventException(Partition partition)
            : base("Found existing event in partition '{1}' which resides in '{0}' table located at {2}",
                   partition.Table, partition, partition.Table.Uri)
        {
            Partition = partition;
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
                partition.Table, partition, partition.Table.Uri, 
                include.GetType().Name, dump, include.Entity.GetType());

            return new IncludedOperationConflictException(partition, include.Entity, message);
        }

        static string Dump(ITableEntity entity)
        {
            return JsonSerializer.Serialize(entity, new JsonSerializerOptions
            {
                IncludeFields = false,
                WriteIndented = true,
                DefaultIgnoreCondition = JsonIgnoreCondition.Never,
                Converters = { new JsonStringEnumConverter() }
            });
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
                   partition.Table, partition, partition.Table.Uri, details)
        {
            Partition = partition;
        }

        internal static Exception EventVersionExists(Partition partition)
        {
            return new ConcurrencyConflictException(partition, "Event version already exists");            
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
}