using System;
using System.Linq;

namespace Example
{
    public class InventoryItemCreated
    {
        public readonly string Id;
        public readonly string Name;

        public InventoryItemCreated(string id, string name)
        {
            Id = id;
            Name = name;
        }
    }

    public class InventoryItemRenamed
    {
        public readonly string Id;
        public readonly string OldName;
        public readonly string NewName;

        public InventoryItemRenamed(string id, string oldName, string newName)
        {
            Id = id;
            NewName = newName;
            OldName = oldName;
        }
    }

    public class InventoryItemCheckedIn
    {
        public readonly string Id;
        public readonly int Count;

        public InventoryItemCheckedIn(string id, int count)
        {
            Id = id;
            Count = count;
        }
    }

    public class InventoryItemCheckedOut
    {
        public readonly string Id;
        public readonly int Count;

        public InventoryItemCheckedOut(string id, int count)
        {
            Id = id;
            Count = count;
        }
    }

    public class InventoryItemDeactivated
    {
        public readonly string Id;

        public InventoryItemDeactivated(string id)
        {
            Id = id;
        }
    }
}

