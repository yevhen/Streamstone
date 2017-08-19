using System.Runtime.CompilerServices;
using System.Threading.Tasks;

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
}