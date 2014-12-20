using System;
using System.Linq;

namespace Streamstone
{
    public interface IVersionedEntity
    {
        int Version     { get; set; }
    }
}