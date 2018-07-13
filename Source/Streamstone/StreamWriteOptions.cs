namespace Streamstone
{
    /// <summary>
    /// Represent set of possible options which can be used with stream write operation
    /// </summary>
    public class StreamWriteOptions
    {
        internal static readonly StreamWriteOptions Default = new StreamWriteOptions();

        /// <summary>
        /// Signals whether built-in entity change tracking should be used for includes.
        /// If set to <c>true</c> all included operations will be chained
        /// via built-in change tracker. Default is <c>true</c>
        /// </summary>
        /// <remarks>
        /// Entity change tracking is required during stream replays
        /// but could be disabled during normal writes.
        /// </remarks>
        public bool TrackChanges { get; set; } = true;
    }
}