using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    static class TaskExtensions
    {
        public static ConfiguredTaskAwaitable Really(this Task task)
        {
            return task.ConfigureAwait(false);
        }

        public static ConfiguredTaskAwaitable<T> Really<T>(this Task<T> task)
        {
            return task.ConfigureAwait(false);
        }
    }

    static class ExceptionExtensions
    {
        public static Exception PreserveStackTrace(this Exception ex)
        {
            var remoteStackTraceString = typeof(Exception)
                .GetField("_remoteStackTraceString", BindingFlags.Instance | BindingFlags.NonPublic);

            Debug.Assert(remoteStackTraceString != null);
            remoteStackTraceString.SetValue(ex, ex.StackTrace);

            return ex;
        }
    }

    /// <summary>
    /// Helper extensions for working with <see cref="EntityProperty"/> and <see cref="ITableEntity"/>
    /// </summary>
    public static class EntityPropertyExtensions
    {
        static readonly object[] noargs = new object[0];

        /// <summary>
        /// Converts given object to an <see cref="IDictionary{TKey,TValue}"/> of entity properties
        /// </summary>
        /// <param name="obj">An object instance</param>
        /// <returns>Dictionary of entity properties</returns>
        public static IDictionary<string, EntityProperty> Props(this object obj)
        {
            Requires.NotNull(obj, "obj");
            
            var result = new Dictionary<string, EntityProperty>();
            
            foreach (PropertyInfo property in obj.GetType().GetProperties())
            {
                var value = property.GetValue(obj, noargs);
                result.Add(property.Name, ToEntityProperty(property.Name, value, property.PropertyType));
            }

            return result;
        }

        /// <summary>
        /// Converts given <see cref="ITableEntity"/> to an <see cref="IDictionary{TKey,TValue}"/> of entity properties
        /// </summary>
        /// <param name="entity">An instance of table entity</param>
        /// <returns>Dictionary of entity properties</returns>
        public static IDictionary<string, EntityProperty> Props(this ITableEntity entity)
        {
            Requires.NotNull(entity, "entity");
            return entity.WriteEntity(new OperationContext());
        }

        static EntityProperty ToEntityProperty(string key, object value, Type type)
        {
            if (type == typeof(byte[])) 
                return new EntityProperty((byte[])value);
            
            if (type == typeof(bool) || type == typeof(bool?)) 
                return new EntityProperty((bool?)value);
            
            if (type == typeof(DateTimeOffset) || type == typeof(DateTimeOffset?)) 
                return new EntityProperty((DateTimeOffset?)value);
            
            if (type == typeof(DateTime)) 
                return new EntityProperty((DateTime?)value);

            if (type == typeof(double) || type == typeof(double?)) 
                return new EntityProperty((double?)value);

            if (type == typeof(Guid) || type == typeof(Guid?)) 
                return new EntityProperty((Guid?)value);

            if (type == typeof(int) || type == typeof(int?)) 
                return new EntityProperty((int?)value);

            if (type == typeof(long) || type == typeof(long?)) 
                return new EntityProperty((long?)value);
            
            if (type == typeof(string)) 
                return new EntityProperty((string)value);
            
            throw new NotSupportedException("Not supported entity property type '" + value.GetType() + "' for '" + key + "'");
        }
    
        internal static IEnumerable<KeyValuePair<string, EntityProperty>> Clone(
                   this IEnumerable<KeyValuePair<string, EntityProperty>> properties)
        {
            return properties.Select(Clone);
        }

        static KeyValuePair<string, EntityProperty> Clone(KeyValuePair<string, EntityProperty> x)
        {
            return new KeyValuePair<string, EntityProperty>(x.Key, x.Value.Clone());
        }

        static EntityProperty Clone(this EntityProperty source)
        {
            return EntityProperty.CreateEntityPropertyFromObject(source.PropertyAsObject);
        }
    }
}