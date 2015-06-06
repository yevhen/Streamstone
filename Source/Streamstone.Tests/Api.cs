using System;
using System.Linq;

namespace Streamstone
{
    public static class Api
    {
        public const int AzureMaxBatchSize = 100;

        public const string StreamRowKey = "SS-HEAD";
        public const string EventRowKeyPrefix = "SS-SE-";
        public const string EventIdRowKeyPrefix = "SS-UID-";

        public static string FormatEventRowKey(this int version)
        {
            return string.Format("{0}{1:d10}", EventRowKeyPrefix, version);
        }

        public static string FormatEventIdRowKey(this string id)
        {
            return string.Format("{0}{1}", EventIdRowKeyPrefix, id);
        }
    }
}