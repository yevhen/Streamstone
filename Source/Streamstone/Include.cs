using System;
using System.Linq;

using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    /// <summary>
    /// Specifies the type of included entity operation
    /// </summary>
    public enum IncludeType
    {
        /// <summary>
        /// The insert operation
        /// </summary>
        Insert,
        
        /// <summary>
        /// The replace operation
        /// </summary>
        Replace,

        /// <summary>
        /// The delete operation
        /// </summary>
        Delete,

        /// <summary>
        /// The insert or merge operation
        /// </summary>
        InsertOrMerge,
        InsertOrReplace
    }

    /// <summary>
    /// Represents  included entity operation
    /// </summary>
    public sealed class Include
    {
        /// <summary>
        /// Inserts the specified entity.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns>A new instance of <see cref="Include"/> class</returns>
        /// <exception cref="ArgumentNullException">If given <paramref name="entity"/> is <c>null</c>.</exception>
        public static Include Insert(ITableEntity entity)
        {
            Requires.NotNull(entity, "entity");
            return new Include(IncludeType.Insert, new EntityOperation.Insert(entity));
        }

        /// <summary>
        /// Replaces the specified entity.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns>A new instance of <see cref="Include"/> class</returns>
        /// <exception cref="ArgumentNullException">If given <paramref name="entity"/> is <c>null</c>.</exception>
        public static Include Replace(ITableEntity entity)
        {
            Requires.NotNull(entity, "entity");
            return new Include(IncludeType.Replace, new EntityOperation.Replace(entity));
        }

        /// <summary>
        /// Deletes the specified entity.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns>A new instance of <see cref="Include"/> class</returns>
        /// <exception cref="ArgumentNullException">If given <paramref name="entity"/> is <c>null</c>.</exception>
        public static Include Delete(ITableEntity entity)
        {
            Requires.NotNull(entity, "entity");
            return new Include(IncludeType.Delete, new EntityOperation.Delete(entity));
        }

        /// <summary>
        /// Inserts or merges the specified entity.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns>A new instance of <see cref="Include"/> class</returns>
        /// <exception cref="ArgumentNullException">If given <paramref name="entity"/> is <c>null</c>.</exception>
        public static Include InsertOrMerge(ITableEntity entity)
        {
            Requires.NotNull(entity, "entity");
            return new Include(IncludeType.InsertOrMerge, new EntityOperation.InsertOrMerge(entity));
        }

        /// <summary>
        /// Inserts or replace the specified entity.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns>A new instance of <see cref="Include"/> class</returns>
        /// <exception cref="ArgumentNullException">If given <paramref name="entity"/> is <c>null</c>.</exception>
        public static Include InsertOrReplace(ITableEntity entity)
        {
            Requires.NotNull(entity, "entity");
            return new Include(IncludeType.InsertOrReplace, new EntityOperation.InsertOrReplace(entity));
        }

        Include(IncludeType type, EntityOperation operation)
        {
            Type = type;
            Operation = operation;
        }

        /// <summary> Gets the type of this include.  </summary>
        /// <value> The type of include operation. </value>
        public IncludeType Type
        {
            get; private set;
        }

        /// <summary> Gets the included entity. </summary>
        /// <value> The table entity. </value>
        public ITableEntity Entity
        {
            get { return Operation.Entity; }
        }

        internal EntityOperation Operation
        {
            get; private set;
        }
    }
}