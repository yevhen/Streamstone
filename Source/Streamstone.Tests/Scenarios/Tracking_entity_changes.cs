using System;
using System.Collections.Generic;
using System.Linq;

using NUnit.Framework;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone.Scenarios
{
    [TestFixture]
    public class Tracking_entity_changes
    {
        const string EntityRowKey = "INV-0001";
        Partition partition;
        CloudTable table;
        Stream stream;

        [SetUp]
        public void SetUp()
        {
            table = Storage.SetUp();
            partition = new Partition(table, "test");
            stream = Stream.Provision(partition);
        }

        [Test]
        public void When_normal_flow()
        {
            var entity = new TestEntity(EntityRowKey) { Data = "12" };
            var insert = Include.Insert(entity);

            entity.Data = "34";
            var replace = Include.Replace(entity);

            EventData[] events =
            {
                CreateEvent(insert), 
                CreateEvent(replace)
            };

            Assert.DoesNotThrow(() => 
                Stream.Write(stream, events),
                    "Should choose (use) the one with higher priority. Insert in this case");

            var stored = RetrieveTestEntity(entity.RowKey);
            Assert.That(stored, Is.Not.Null);
            Assert.That(stored.Data, Is.EqualTo("34"),
                "Should insert latest entity data");
        }

        [Test]
        public void When_including_different_entity_instances_for_the_same_entity_row_key()
        {
            var e1 = new TestEntity(EntityRowKey);
            var e2 = new TestEntity(EntityRowKey);

            EventData[] events =
            {
                CreateEvent(Include.Insert(e1)), 
                CreateEvent(Include.Replace(e2))
            };

            Assert.Throws<InvalidOperationException>(()=> 
                Stream.Write(stream, events),
                    "Entity equality is by reference, since we need Etag from previous operation on the entity \n" + 
                    "and since chunking may split events in separate batches its easier to have 'by-ref' equality.\n" + 
                    "It also more memory efficient and less error-prone than 'by-value'");
        }

        [Test]
        public void When_including_mutually_soluble_operations()
        {
            var entity = new TestEntity(EntityRowKey) { Data = "12" };

            EventData[] events =
            {
                CreateEvent(Include.Insert(entity)), 
                CreateEvent(Include.Delete(entity))
            };

            Stream.Write(stream, events);

            var stored = RetrieveTestEntity(entity.RowKey);
            Assert.That(stored, Is.Null);
        }

        [Test]
        public void When_including_mutually_soluble_operations_chained_indirectly()
        {
            var entity = new TestEntity(EntityRowKey) { Data = "12" };

            EventData[] events =
            {
                CreateEvent(Include.Insert(entity)), 
                CreateEvent(Include.Replace(entity)), 
                CreateEvent(Include.Delete(entity))
            };

            Stream.Write(stream, events);

            var stored = RetrieveTestEntity(entity.RowKey);
            Assert.That(stored, Is.Null);
        }

        [Test]
        public void When_including_replace_with_empty_or_null_Etag()
        {
            var entity = new TestEntity(EntityRowKey) { ETag = null };

            Assert.Throws<InvalidOperationException>(() =>
                Stream.Write(stream, CreateEvent(Include.Replace(entity))));

            entity.ETag = "";

            Assert.Throws<InvalidOperationException>(() =>
                Stream.Write(stream, CreateEvent(Include.Replace(entity))));
        }

        [Test]
        public void When_including_unconditional_replace()
        {
            var entity = new TestEntity(EntityRowKey) { Data = "123" };
            InsertTestEntity(entity);

            entity = new TestEntity(EntityRowKey)
            {
                Data = "456",
                ETag = "*"
            };

            Assert.DoesNotThrow(() => 
                Stream.Write(stream, CreateEvent(Include.Replace(entity))),
                    "Will be always executed and will fully replace contents");

            var stored = RetrieveTestEntity(EntityRowKey);
            Assert.That(stored.Data, Is.EqualTo("456"));
        }

        [Test]
        public void When_including_unconditional_replace_for_transient_entity()
        {
            var entity = new TestEntity(EntityRowKey) { ETag = "*" };

            Assert.Throws<StorageException>(() =>
                Stream.Write(stream, CreateEvent(Include.Replace(entity))),
                    "Will be always executed and exception will be thrown by the storage");
        }

        [Test]
        public void When_including_unconditional_delete()
        {
            var entity = new TestEntity(EntityRowKey) { Data = "123" };
            InsertTestEntity(entity);

            entity = new TestEntity(EntityRowKey) { ETag = "*" };
            Assert.DoesNotThrow(() =>
                Stream.Write(stream, CreateEvent(Include.Delete(entity))),
                    "Will be always executed and will delete row");

            var stored = RetrieveTestEntity(EntityRowKey);
            Assert.That(stored, Is.Null);
        }

        [Test]
        public void When_including_unconditional_delete_for_transient_entity()
        {
            var entity = new TestEntity(EntityRowKey) { ETag = "*" };

            Assert.Throws<StorageException>(() =>
                Stream.Write(stream, CreateEvent(Include.Delete(entity))),
                        "Will be always executed and exception will be thrown by the storage");
        }

        [Test]
        public void When_including_unconditional_replace_after_insert()
        {
            var entity = new TestEntity(EntityRowKey)
            {
                Data = "911",
                ETag = "*"
            };

            EventData[] events =
            {
                CreateEvent(Include.Insert(entity)), 
                CreateEvent(Include.Replace(entity)), 
            };

            Assert.DoesNotThrow(() =>
                Stream.Write(stream, events),
                    "Insert of entity with Etag=* is simply ignored by Azure");

            var stored = RetrieveTestEntity(EntityRowKey);
            Assert.That(stored, Is.Not.Null);
        }

        [Test]
        public void When_inserting_entity_with_unconditional_Etag_and_entity_already_exists()
        {
            var entity = new TestEntity(EntityRowKey)
            {
                Data = "911",
                ETag = "*"
            };

            InsertTestEntity(entity);

            Assert.Throws<IncludedOperationConflictException>(() =>
                Stream.Write(stream, CreateEvent(Include.Insert(entity))),
                    "Insert of entity with Etag=* does not behave like InsertOrReplace");
        }

        /*           Rules for  operation chaining
                     -----------------------------------------------------------------------------------
                         --->    |    Insert    |    Replace    |    Delete   |  Upmerge   |  InsReplace
                     -----------------------------------------------------------------------------------          
                     Insert      |      ERR     |    Insert     |     NULL    |    ERR     |    ERR    
                     -----------------------------------------------------------------------------------
                     Replace     |      ERR     |    Replace    |    Delete   |    ERR     |    ERR 
                     -----------------------------------------------------------------------------------
                     Delete      |    Replace   |      ERR      |     ERR     |    ERR     |    ERR
                     -----------------------------------------------------------------------------------
                     NULL        |    Insert    |      ERR      |     ERR     |  Upmerge   | InsReplace
                     -----------------------------------------------------------------------------------
                     Upmerge     |      ERR     |      ERR      |     ERR     |  Upmerge   |    ERR
                     ----------------------------------------------------------------------------------- 
                     InsReplace  |      ERR     |      ERR      |     ERR     |    ERR     | InsReplace
                     ----------------------------------------------------------------------------------- 
                     
             Upmerge - Insert-Or-Merge
             InsReplace - Insert-Or-Replace
             
             */


        /********* Insert followed by XXX ************/

        [Test]
        public void When_Insert_followed_by_Insert()
        {
            var entity = new TestEntity(EntityRowKey);

            var events = new[]
            {
                CreateEvent(Include.Insert(entity)),
                CreateEvent(Include.Insert(entity))
            };

            var exception = Assert.Throws<InvalidOperationException>(() => 
                Stream.Write(stream, events));

            Assert.That(exception,
                Has.Message.ContainsSubstring("cannot be followed by"));
        }
                
        [Test]
        public void When_Insert_followed_by_Replace()
        {
            var entity = new TestEntity(EntityRowKey);

            var events = new[]
            {
                CreateEvent(Include.Insert(entity)),
                CreateEvent(Include.Replace(entity))
            };

            Stream.Write(stream, events);

            var stored = RetrieveTestEntity(EntityRowKey);
            Assert.That(stored, Is.Not.Null);
        }
                
        [Test]
        public void When_Insert_followed_by_Delete()
        {
            var entity = new TestEntity(EntityRowKey);

            var events = new[]
            {
                CreateEvent(Include.Insert(entity)),
                CreateEvent(Include.Delete(entity))
            };

            Stream.Write(stream, events);

            var stored = RetrieveTestEntity(EntityRowKey);
            Assert.That(stored, Is.Null, 
                "NULL since Insert interdifused with Delete");
        }

        /********* Replace followed by XXX ************/

        [Test]
        public void When_Replace_followed_by_Insert()
        {
            var entity = new TestEntity(EntityRowKey);

            var events = new[]
            {
                CreateEvent(Include.Replace(entity)),
                CreateEvent(Include.Insert(entity))
            };

            var exception = Assert.Throws<InvalidOperationException>(() =>
                Stream.Write(stream, events));

            Assert.That(exception,
                Has.Message.ContainsSubstring("cannot be followed by"));
        }

        [Test]
        public void When_Replace_followed_by_Replace()
        {
            var entity = new TestEntity(EntityRowKey);
            InsertTestEntity(entity);

            entity.Data = "zzz";
            var events = new[]
            {
                CreateEvent(Include.Replace(entity)),
                CreateEvent(Include.Replace(entity))
            };

            Stream.Write(stream, events);

            var stored = RetrieveTestEntity(EntityRowKey);
            Assert.That(stored.Data, Is.EqualTo("zzz"));
        }
        
        [Test]
        public void When_Replace_followed_by_Delete()
        {
            var entity = new TestEntity(EntityRowKey);
            InsertTestEntity(entity);

            var events = new[]
            {
                CreateEvent(Include.Replace(entity)),
                CreateEvent(Include.Delete(entity))
            };

            Stream.Write(stream, events);

            var stored = RetrieveTestEntity(EntityRowKey);
            Assert.That(stored, Is.Null, "Delete should win");
        }

        /********* Delete followed by XXX ************/

        [Test]
        public void When_Delete_followed_by_Insert()
        {
            //  transition of Delete -> Insert = Replace is safe, 
            //  since you can only get here by either starting from Replace or Delete
            //  which means you either have Etag or * 
            //  that's why there is an asumption that entity exists

            var entity = new TestEntity(EntityRowKey);
            InsertTestEntity(entity);

            entity.Data = "zzz";
            var events = new[]
            {
                CreateEvent(Include.Delete(entity)),
                CreateEvent(Include.Insert(entity))
            };

            Stream.Write(stream, events);
            
            var stored = RetrieveTestEntity(EntityRowKey);
            Assert.That(stored.Data, Is.EqualTo("zzz"));
        }

        [Test]
        public void When_Delete_followed_by_Replace()
        {
            var entity = new TestEntity(EntityRowKey);

            var events = new[]
            {
                CreateEvent(Include.Delete(entity)),
                CreateEvent(Include.Replace(entity))
            };

            var exception = Assert.Throws<InvalidOperationException>(() =>
                Stream.Write(stream, events));

            Assert.That(exception,
                Has.Message.ContainsSubstring("cannot be followed by"));
        }
        
        [Test]
        public void When_Delete_followed_by_Delete()
        {
            var entity = new TestEntity(EntityRowKey);

            var events = new[]
            {
                CreateEvent(Include.Delete(entity)),
                CreateEvent(Include.Delete(entity))
            };

            var exception = Assert.Throws<InvalidOperationException>(() =>
                Stream.Write(stream, events));

            Assert.That(exception,
                Has.Message.ContainsSubstring("cannot be followed by"));
        }

        /********* NULL followed by XXX ************/

        [Test]
        public void When_Null_followed_by_Insert()
        {
            var entity = new TestEntity(EntityRowKey);

            var events = new[]
            {
                CreateEvent(Include.Insert(entity)), // that combination
                CreateEvent(Include.Delete(entity)), //  produces NULL
                
                CreateEvent(Include.Insert(entity))
            };

            Stream.Write(stream, events);

            var stored = RetrieveTestEntity(EntityRowKey);
            Assert.That(stored, Is.Not.Null);
        }        
        
        [Test]
        public void When_Null_followed_by_Replace()
        {
            var entity = new TestEntity(EntityRowKey);

            var events = new[]
            {
                CreateEvent(Include.Insert(entity)), // that combination
                CreateEvent(Include.Delete(entity)), //  produces NULL
                
                CreateEvent(Include.Replace(entity))
            };

            var exception = Assert.Throws<InvalidOperationException>(() =>
                Stream.Write(stream, events));

            Assert.That(exception,
                Has.Message.ContainsSubstring("cannot be applied to NULL"));
        }        
        
        [Test]
        public void When_Null_followed_by_Delete()
        {
            var entity = new TestEntity(EntityRowKey);

            var events = new[]
            {
                CreateEvent(Include.Insert(entity)), // that combination
                CreateEvent(Include.Delete(entity)), //  produces NULL
                
                CreateEvent(Include.Delete(entity))
            };

            var exception = Assert.Throws<InvalidOperationException>(() =>
                Stream.Write(stream, events));

            Assert.That(exception,
                Has.Message.ContainsSubstring("cannot be applied to NULL"));
        }

        /********* Insert-Or-Merge or Insert-Or-Replace followed by XXX ************/
        [TestCaseSource(nameof(GetThrowingOperationsForInsertOrMergeOrReplace))]
        public void ThrowOnPrecedingInsertOrMergeOrReplaceWithAnything(Include first, Include second)
        {
            var events = new[]
            {
                CreateEvent(first),
                CreateEvent(second),
            };

            Assert.Throws<InvalidOperationException>(() =>
                    Stream.Write(stream, events));
        }

        [Test]
        public void When_Null_followed_by_Insert_Or_Merge()
        {
            var entity = new TestEntity(EntityRowKey);

            var events = new[]
            {
                CreateEvent(Include.Insert(entity)), // that combination
                CreateEvent(Include.Delete(entity)), //  produces NULL
                
                CreateEvent(Include.InsertOrMerge(entity))
            };

            Stream.Write(stream, events);

            var stored = RetrieveTestEntity(EntityRowKey);
            Assert.That(stored, Is.Not.Null);
        }

        [Test]
        public void When_Null_followed_by_Insert_Or_Replace()
        {
            var entity = new TestEntity(EntityRowKey);

            var events = new[]
            {
                CreateEvent(Include.Insert(entity)), // that combination
                CreateEvent(Include.Delete(entity)), //  produces NULL
                
                CreateEvent(Include.InsertOrMerge(entity))
            };

            Stream.Write(stream, events);

            var stored = RetrieveTestEntity(EntityRowKey);
            Assert.That(stored, Is.Not.Null);
        }

        [Test]
        public void When_Insert_Or_Replace_followed_by_Insert_Or_Replace()
        {
            var entity = new TestEntity(EntityRowKey);
            InsertTestEntity(entity);

            entity.Data = "zzz";
            var events = new[]
            {
                CreateEvent(Include.InsertOrReplace(entity)),
                CreateEvent(Include.InsertOrReplace(entity))
            };

            Stream.Write(stream, events);

            var stored = RetrieveTestEntity(EntityRowKey);
            Assert.That(stored.Data, Is.EqualTo("zzz"));
        }

        [Test]
        public void When_Insert_Or_Merge_followed_by_Insert_Or_Merge()
        {
            var entity = new ExtendedTestEntity(EntityRowKey)
            {
                Data = "zzz"
            };

            InsertTestEntity(entity);

            entity = new ExtendedTestEntity(EntityRowKey)
            {
                AdditionalData = "zzz",
            };

            var events = new[]
            {
                CreateEvent(Include.InsertOrMerge(entity)),
                CreateEvent(Include.InsertOrMerge(entity))
            };

            Stream.Write(stream, events);

            var stored = RetrieveEntity<ExtendedTestEntity>(EntityRowKey);
            Assert.That(stored.Data, Is.EqualTo("zzz"));
            Assert.That(stored.AdditionalData, Is.EqualTo("zzz"));
        }

        public static IEnumerable<ITestCaseData> GetThrowingOperationsForInsertOrMergeOrReplace()
        {
            var entity = new TestEntity(EntityRowKey);
            var firstIncludeProducers = new Func<ITableEntity, Include>[] {Include.Insert, Include.Delete, Include.Replace};
            var secondIncludeProducers = new Func<ITableEntity, Include>[] { Include.InsertOrMerge, Include.InsertOrReplace };

            foreach (var first in firstIncludeProducers)
            {
                foreach (var second in secondIncludeProducers)
                {
                    yield return
                        new TestCaseData(first(entity), second(entity)).SetName(
                            $"When_{first.Method.Name}_followed_by_{second.Method.Name}");
                }
            }
        }

        void InsertTestEntity(ITableEntity entity)
        {
            entity.PartitionKey = partition.PartitionKey;
            table.Execute(TableOperation.Insert(entity));
        }

        TestEntity RetrieveTestEntity(string rowKey)
        {
            return RetrieveEntity<TestEntity>(rowKey);
        }

        TEntity RetrieveEntity<TEntity>(string rowKey)
            where TEntity : TableEntity, new()
        {
            return table.CreateQuery<TEntity>()
                        .Where(x =>
                               x.PartitionKey == partition.PartitionKey
                               && x.RowKey == rowKey)
                        .ToList()
                        .SingleOrDefault();
        }

        static EventData CreateEvent(params Include[] includes)
        {
            return new EventData(EventId.None, EventProperties.None, EventIncludes.From(includes));
        }

        public class TestEntity : TableEntity
        {
            public TestEntity()
            {}

            public TestEntity(string rowKey)
            {
                RowKey = rowKey;
                Data = DateTime.UtcNow.ToString();
            }

            public string Data { get; set; }            
        }

        public class ExtendedTestEntity : TableEntity
        {
            public ExtendedTestEntity()
            {
            }

            public ExtendedTestEntity(string rowKey)
            {
                RowKey = rowKey;
            }

            public string Data { get; set; }
            public string AdditionalData { get; set; }
        }
    }
}