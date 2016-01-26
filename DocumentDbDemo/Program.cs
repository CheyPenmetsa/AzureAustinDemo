using DocumentDbDemo.Models;
using Microsoft.Azure.Documents.Linq;
using Microsoft.Azure.Documents.Partitioning;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Threading.Tasks;
using Db = Microsoft.Azure.Documents;

namespace DocumentDbDemo
{
    public class Program
    {

        #region Private members

        private static Db.Client.DocumentClient _documentDbClient;

        private static Db.Database _documentDatabase;
        private const string DatabaseStore = "DemoDocumentStore";

        private static Db.DocumentCollection _customerCollection;
        private const string CustomerCollectionName = "CustomerCollection";

        private static Db.DocumentCollection _partitionCollection1;
        private const string PartitionCollectionName1 = "Collection.A-M";

        private static Db.DocumentCollection _partitionCollection2;
        private const string PartitionCollectionName2 = "Collection.N-Z";

        private static RangePartitionResolver<string> _rangeResolver;

        #endregion

        static void Main(string[] args)
        {
            try
            {
                _documentDbClient = new Db.Client.DocumentClient(
                    new Uri(ConfigurationManager.AppSettings["DocumentDbUri"]),
                    ConfigurationManager.AppSettings["DocumentDbKey"]);

                _documentDatabase = _documentDbClient.CreateDatabaseQuery()
                    .Where(db => db.Id == DatabaseStore).AsEnumerable().FirstOrDefault();

                //Create database and collection if not present
                CreateDatabaseIfNotExistsAsync().Wait();

                //Create Seed Data
                CreateDocumentsAsync(_customerCollection.SelfLink).Wait();

                //Query inserted customers
                QueryAllDocuments(_customerCollection.SelfLink);

                //Query subdocuments(orders)
                QueryWithSubdocuments(_customerCollection.SelfLink);

                //Query all customer first and last name with order number with amount greate than $300.00
                QueryWithTwoJoinsAndFilter(_customerCollection.SelfLink);

                //Create partitions
                InitializeRangeResolverAsync().Wait();

                //Run queries agianst different partitions and see where the documents are stored
                QueryPartitionAsync().Wait();

                //Update and delete the document
                UpdateAndDeleteDocumentAsync().Wait();

                //Cleanup
                CleanUpAsync().Wait();
            }
            catch (Db.DocumentClientException de)
            {
                var baseException = de.GetBaseException();
                Console.WriteLine("{0} error occurred: {1}, Message: {2}", de.StatusCode, de.Message,
                    baseException.Message);
            }
            catch (Exception e)
            {
                var baseException = e.GetBaseException();
                Console.WriteLine("Error: {0}, Message: {1}", e.Message, baseException.Message);
            }
            finally
            {
                Console.WriteLine("End of demo, press any key to exit.");
                Console.ReadKey();
            }
        }
        
        #region Private Members

        private static void Assert(string message, bool condition)
        {
            if (!condition)
            {
                throw new ApplicationException(message);
            }
        }

        private static async Task CreateDatabaseIfNotExistsAsync()
        {
            #region Db

            if (_documentDatabase == null)
            {
                _documentDatabase = await _documentDbClient.CreateDatabaseAsync(new Db.Database
                {
                    Id = DatabaseStore
                });
            }

            #endregion

            #region Customer Collection

            if (_customerCollection == null)
            {
                // dbs/MyDatabaseId/colls/MyCollectionId/docs/MyDocumentId              
                _customerCollection =
                    _documentDbClient.CreateDocumentCollectionQuery("dbs/" + _documentDatabase.Id)
                        .Where(c => c.Id == CustomerCollectionName)
                        .AsEnumerable()
                        .FirstOrDefault();

                // ReSharper disable once ConvertIfStatementToNullCoalescingExpression
                if (_customerCollection == null)
                {
                    _customerCollection =
                        await _documentDbClient.CreateDocumentCollectionAsync("dbs/" + _documentDatabase.Id,
                            new Db.DocumentCollection
                            {
                                Id = CustomerCollectionName,
                                IndexingPolicy =
                                {
                                    Automatic = true,
                                    IndexingMode = Db.IndexingMode.Lazy
                                }
                            }, new Db.Client.RequestOptions
                            {
                                OfferType = "S1"
                            });
                }
            }

            #endregion
        }

        private static async Task CreateDocumentsAsync(string collectionLink)
        {
            var customerJohnSmith = new Customer
            {
                CustomerId = "1",
                FirstName = "John",
                LastName = "Smith",
                Orders = new Order[]
                {
                    new Order
                    {
                        OrderNumber = "Order_1",
                        OrderAmount = (decimal) 299.00,
                        ShippingAddress = new Address
                        {
                            AddressLine1 = "123 Main street",
                            AddressLine2 = null,
                            City = "Austin",
                            State = "Texas",
                            Zipcode = "78728"
                        },
                        Products = new Product[]
                        {
                            new Product
                            {
                                ProductId = 12,
                                Description = "Laptop",
                                Price = (decimal) 299.00,
                                Quantity = 1
                            },
                        }
                    },
                }
            };

            await _documentDbClient.CreateDocumentAsync(collectionLink, customerJohnSmith);

            var customerJasonSmith = new Customer
            {
                CustomerId = "2",
                FirstName = "Jason",
                LastName = "Smith",
                Orders = new Order[]
                {
                    new Order
                    {
                        OrderNumber = "Order_2",
                        OrderAmount = (decimal) 400.00,
                        ShippingAddress = new Address
                        {
                            AddressLine1 = "123 Bourbon street",
                            AddressLine2 = null,
                            City = "Dallas",
                            State = "Texas",
                            Zipcode = "78758"
                        },
                        Products = new Product[]
                        {
                            new Product
                            {
                                ProductId = 123,
                                Description = "Monitor",
                                Price = (decimal) 100.00,
                                Quantity = 1
                            },
                            new Product
                            {
                                ProductId = 345,
                                Description = "SSD hard drive",
                                Price = (decimal) 300.00,
                                Quantity = 1
                            },
                        }
                    },
                    new Order
                    {
                        OrderNumber = "Order_3",
                        OrderAmount = (decimal) 1000.00,
                        ShippingAddress = new Address
                        {
                            AddressLine1 = "123 Bourbon street",
                            AddressLine2 = null,
                            City = "Dallas",
                            State = "Texas",
                            Zipcode = "78758"
                        },
                        Products = new Product[]
                        {
                            new Product
                            {
                                ProductId = 567,
                                Description = "Surface 2.0",
                                Price = (decimal) 1000.00,
                                Quantity = 1
                            }
                        }
                    }
                }
            };

            await _documentDbClient.CreateDocumentAsync(collectionLink, customerJasonSmith);

            var customerBillButler = new Customer
            {
                CustomerId = "3",
                FirstName = "Bill",
                LastName = "Butler",
                Orders = new Order[]
                {
                    new Order
                    {
                        OrderNumber = "Order_4",
                        OrderAmount = (decimal) 1200.00,
                        ShippingAddress = new Address
                        {
                            AddressLine1 = "123 Sam smith street",
                            AddressLine2 = null,
                            City = "Austin",
                            State = "Texas",
                            Zipcode = "78714"
                        },
                        Products = new Product[]
                        {
                            new Product
                            {
                                ProductId = 892,
                                Description = "Monitor 1",
                                Price = (decimal) 300.00,
                                Quantity = 2
                            },
                            new Product
                            {
                                ProductId = 983,
                                Description = "SSD hard drive 1",
                                Price = (decimal) 300.00,
                                Quantity = 2
                            },
                        }
                    }
                }
            };

            await _documentDbClient.CreateDocumentAsync(collectionLink, customerBillButler);

        }

        private static void QueryAllDocuments(string collectionLink)
        {
            // LINQ Query
            var customers =
                from c in _documentDbClient.CreateDocumentQuery<Customer>(collectionLink)
                select c;

            Assert("Expected three customers", customers.ToList().Count == 3);

            // LINQ Lambda
            customers = _documentDbClient.CreateDocumentQuery<Customer>(collectionLink);
            Assert("Expected two customers", customers.ToList().Count == 3);

            // SQL
            customers = _documentDbClient.CreateDocumentQuery<Customer>(collectionLink, "SELECT * FROM Customers");
            Assert("Expected two customers", customers.ToList().Count == 3);
        }

        private static void QueryWithSubdocuments(string collectionLink)
        {
            // SQL
            var orderSqlQuery = _documentDbClient.CreateDocumentQuery<Order>(collectionLink,
                "SELECT c " +
                "FROM c IN f.Orders").ToList();

            foreach (var order in orderSqlQuery)
            {
                Console.WriteLine(JsonConvert.SerializeObject(order));
            }

            // LINQ Query
            var orderLinqQuery = _documentDbClient.CreateDocumentQuery<Customer>(collectionLink)
                     .SelectMany(customer => customer.Orders
                     .Select(c => c));

            foreach (var order in orderLinqQuery)
            {
                Console.WriteLine(JsonConvert.SerializeObject(order));
            }
        }

        private static void QueryWithTwoJoinsAndFilter(string collectionLink)
        {
            /*
            No straight way to check the values other than get per order amounts

            Select * from root r
            WHERE r.orders[0].orderamount >= 300

             Select * from root r
            WHERE r.orders[1].orderamount >= 300
            */

            var customerOrders = _documentDbClient.CreateDocumentQuery<dynamic>(collectionLink,
                    "SELECT c.fname as firstName, c.lname as lastName, o.ord_num as orderNumber" +
                    "FROM Customers c " +
                    "JOIN o IN c.Orders" +
                    "WHERE o.ord_amt > 300");

            foreach (var item in customerOrders)
            {
                Console.WriteLine(item);
            }

            // LINQ
            customerOrders = _documentDbClient.CreateDocumentQuery<Customer>(collectionLink)
                .SelectMany(customer => customer.Orders
                .Where(ord => ord.OrderAmount >= 300)
                .Select(ord => new
                {
                    firstName = customer.FirstName,
                    lastName = customer.LastName,
                    orderNumber = ord.OrderNumber
                }));

            foreach (var item in customerOrders)
            {
                Console.WriteLine(item);
            }
        }

        private static async Task InitializeRangeResolverAsync()
        {
            #region Partition Collection 1

            if (_partitionCollection1 == null)
            {
                // dbs/MyDatabaseId/colls/MyCollectionId/docs/MyDocumentId              
                _partitionCollection1 =
                    _documentDbClient.CreateDocumentCollectionQuery("dbs/" + _documentDatabase.Id)
                        .Where(c => c.Id == PartitionCollectionName1)
                        .AsEnumerable()
                        .FirstOrDefault();

                // ReSharper disable once ConvertIfStatementToNullCoalescingExpression
                if (_partitionCollection1 == null)
                {
                    _partitionCollection1 =
                        await _documentDbClient.CreateDocumentCollectionAsync("dbs/" + _documentDatabase.Id,
                            new Db.DocumentCollection
                            {
                                Id = PartitionCollectionName1,
                                IndexingPolicy =
                                {
                                    Automatic = true,
                                    IndexingMode = Db.IndexingMode.Lazy
                                }
                            }, new Db.Client.RequestOptions
                            {
                                OfferType = "S1"
                            });
                }
            }

            #endregion

            #region Partition Collection 2

            if (_partitionCollection2 == null)
            {
                // dbs/MyDatabaseId/colls/MyCollectionId/docs/MyDocumentId              
                _partitionCollection2 =
                    _documentDbClient.CreateDocumentCollectionQuery("dbs/" + _documentDatabase.Id)
                        .Where(c => c.Id == PartitionCollectionName2)
                        .AsEnumerable()
                        .FirstOrDefault();

                // ReSharper disable once ConvertIfStatementToNullCoalescingExpression
                if (_partitionCollection2 == null)
                {
                    _partitionCollection2 =
                        await _documentDbClient.CreateDocumentCollectionAsync("dbs/" + _documentDatabase.Id,
                            new Db.DocumentCollection
                            {
                                Id = PartitionCollectionName2,
                                IndexingPolicy =
                                {
                                    Automatic = true,
                                    IndexingMode = Db.IndexingMode.Lazy
                                }
                            }, new Db.Client.RequestOptions
                            {
                                OfferType = "S1"
                            });
                }
            }

            #endregion

            // Initialize a partition resolver that assigns users (A-M) -> collection1, and (N-Z) -> collection2
            // and register with DocumentClient. 
            // Note: \uffff is the largest UTF8 value, so M\ufff includes all strings that start with M.
            _rangeResolver = new RangePartitionResolver<string>(
                "UserId",
                new Dictionary<Range<string>, string>()
                {
                    { new Range<string>("A", "M\uffff"), _partitionCollection1.SelfLink },
                    { new Range<string>("N", "Z\uffff"), _partitionCollection2.SelfLink },
                });

            _documentDbClient.PartitionResolvers[_documentDatabase.SelfLink] = _rangeResolver;
        }

        private static async Task QueryPartitionAsync()
        {
            // Create some documents. Note that creates use the database's self link instead of a specific collection's self link. 
            // The hash resolver will compute the hash of UserId in order to route the create to either of the collections.
            Db.Document johnDocument = await _documentDbClient.CreateDocumentAsync(
                _documentDatabase.SelfLink, new UserProfile("J1", "@John"));
            Db.Document ryanDocument = await _documentDbClient.CreateDocumentAsync(_documentDatabase.SelfLink, 
                new UserProfile("U4", "@Ryan"));

            var johnProfile = (UserProfile)(dynamic)johnDocument;

            // Query for John's document by ID. We can use the PartitionResolver to restrict the query to just the partition containing @John
            // Again the query uses the database self link, and relies on the hash resolver to route the appropriate collection.
            var query = _documentDbClient.CreateDocumentQuery<UserProfile>(_documentDatabase.SelfLink, null, _rangeResolver.GetPartitionKey(johnProfile))
                .Where(u => u.UserName == "@John");
            johnProfile = query.AsEnumerable().FirstOrDefault();

            // Find the collections where a document exists in. It's uncommon to do this, but can be useful if for example to execute a 
            // stored procedure against a specific set of partitions.
            var collectionLinks = _rangeResolver.ResolveForRead(_rangeResolver.GetPartitionKey(johnProfile)).ToList();

            foreach (var collection in collectionLinks)
            {
                Console.Write("John present in :" + collection);
            }

            var ryanProfile = (UserProfile) (dynamic) ryanDocument;
            collectionLinks = _rangeResolver.ResolveForRead(_rangeResolver.GetPartitionKey(ryanProfile)).ToList();
            foreach (var collection in collectionLinks)
            {
                Console.Write("Ryan present in :" + collection);
            }
        }

        private static async Task UpdateAndDeleteDocumentAsync()
        {
            var johnSmithCustomer = _documentDbClient
                .CreateDocumentQuery<Customer>(_customerCollection.SelfLink)
                .FirstOrDefault(f => f.FirstName == "John" && f.LastName== "Smith");

            if (johnSmithCustomer != null)
            {
                johnSmithCustomer.LastName = "Staton";
            }

            await _documentDbClient.ReplaceDocumentAsync(_customerCollection.SelfLink, 
                johnSmithCustomer);

            johnSmithCustomer = _documentDbClient
               .CreateDocumentQuery<Customer>(_customerCollection.SelfLink)
               .FirstOrDefault(f => f.FirstName == "John" && f.LastName == "Staton");

            //await _documentDbClient.DeleteDocumentAsync(_customerCollection.SelfLink,
            //    johnSmithCustomer);
        }

        private static async Task CleanUpAsync()
        {
            await _documentDbClient.DeleteDatabaseAsync(_documentDatabase.SelfLink);
        }

        #endregion

    }
}
