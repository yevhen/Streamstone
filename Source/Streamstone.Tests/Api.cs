namespace Streamstone
{
    public static class Api
    {
        public const int AzureMaxBatchSize = 100;

        public const string StreamRowKey = "SS-HEAD";
        public const string EventRowKeyPrefix = "SS-SE-";
        public const string EventIdRowKeyPrefix = "SS-UID-";

        public static string FormatEventRowKey(this int version) => $"{EventRowKeyPrefix}{version:d10}";
        public static string FormatEventIdRowKey(this string id) => $"{EventIdRowKeyPrefix}{id}";
    }
}