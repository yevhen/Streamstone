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
}