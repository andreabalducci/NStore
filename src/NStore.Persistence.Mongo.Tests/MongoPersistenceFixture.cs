using System;
using System.IO;
using System.Reflection;
using System.Threading;
using Microsoft.Extensions.Configuration;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver;
using NStore.Core.Persistence;
using NStore.Persistence.Mongo;
using Xunit;

#if MAP_DOMAIN
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Options;
using MongoDB.Bson.Serialization.Serializers;
#endif

[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace NStore.Persistence.Tests
{
    public partial class BasePersistenceTest
    {
        private const string MongoConnectionEnvVar = "NSTORE_MONGODB";
        private const string MongoPerfEnabledEnvVar = "NSTORE_MONGO_BATCH_PERF";
        private static readonly string[] PerfMongoConnectionConfigKeys =
        {
            "NStore:Mongo:Performance:ConnectionString",
            "NStore:Mongo:Performance:AtlasConnectionString"
        };

        private static readonly string[] MongoConnectionConfigKeys =
        {
            "NStore:Mongo:ConnectionString"
        };

        protected static readonly Lazy<IConfigurationRoot> TestConfiguration =
            new Lazy<IConfigurationRoot>(BuildConfiguration);

        protected string _mongoConnectionString;
        protected IMongoPersistence _mongoPersistence;
        private MongoPersistenceOptions _options;
        private const string TestSuitePrefix = "Mongo";

        static BasePersistenceTest()
        {
            // https://github.com/mongodb/mongo-csharp-driver/releases/tag/v2.19.0 
            var objectSerializer = new ObjectSerializer(type => ObjectSerializer.AllAllowedTypes(type));
            BsonSerializer.RegisterSerializer(objectSerializer);
        }

#if MAP_DOMAIN
        static BasePersistenceTest()
        {
            // enable support for dots in key names
            BsonClassMap.RegisterClassMap<Changeset>(map =>
            {
                map.AutoMap();
                map.MapProperty(x => x.Headers).SetSerializer(
                    new DictionaryInterfaceImplementerSerializer<
                        Dictionary<String, Object>
                    >(DictionaryRepresentation.ArrayOfArrays)
                );
            });
        }
#endif
        protected internal IPersistence Create(bool dropOnInit)
        {
            _mongoConnectionString = GetPartitionsConnectionString();
            _options = GetMongoPersistenceOptions();
            if (dropOnInit)
            {
                _options.DropOnInit = true;
            }
            _mongoPersistence = CreatePersistence(_options);

            _mongoPersistence.InitAsync(CancellationToken.None).Wait();

            return _mongoPersistence;
        }

        protected virtual internal MongoPersistenceOptions GetMongoPersistenceOptions()
        {
            return new MongoPersistenceOptions
            {
                PartitionsConnectionString = _mongoConnectionString,
                UseLocalSequence = true,
                PartitionsCollectionName = "partitions_" + GetType().Name + "_" + _testRunId,
                SequenceCollectionName = "seq_" + _testRunId
            };
        }

        private static string GetPartitionsConnectionString()
        {
            var mongo = Environment.GetEnvironmentVariable(MongoConnectionEnvVar);
            if (!string.IsNullOrWhiteSpace(mongo))
            {
                return mongo;
            }

            var config = TestConfiguration.Value;
            if (IsEnabled(Environment.GetEnvironmentVariable(MongoPerfEnabledEnvVar)))
            {
                var perfMongo = ReadFirstConfiguredValue(config, PerfMongoConnectionConfigKeys);
                if (!string.IsNullOrWhiteSpace(perfMongo))
                {
                    return perfMongo;
                }
            }

            mongo = ReadFirstConfiguredValue(config, MongoConnectionConfigKeys);
            if (!string.IsNullOrWhiteSpace(mongo))
            {
                return mongo;
            }

            throw new TestMisconfiguredException(
                $"Mongo connection string not set. Configure {MongoConnectionEnvVar} or appsettings/user-secrets keys: {string.Join(", ", PerfMongoConnectionConfigKeys)} or {string.Join(", ", MongoConnectionConfigKeys)}.");
        }

        private static string ReadFirstConfiguredValue(IConfiguration config, string[] keys)
        {
            for (var i = 0; i < keys.Length; i++)
            {
                var value = config[keys[i]];
                if (!string.IsNullOrWhiteSpace(value))
                {
                    return value;
                }
            }

            return null;
        }

        private static bool IsEnabled(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return false;
            }

            return value == "1" ||
                   value.Equals("true", StringComparison.OrdinalIgnoreCase) ||
                   value.Equals("yes", StringComparison.OrdinalIgnoreCase);
        }

        private static string FindAppSettingsBasePath()
        {
            var current = new DirectoryInfo(AppContext.BaseDirectory);
            while (current != null)
            {
                var appsettingsPath = Path.Combine(current.FullName, "appsettings.json");
                if (File.Exists(appsettingsPath))
                {
                    return current.FullName;
                }

                current = current.Parent;
            }

            return AppContext.BaseDirectory;
        }

        private static IConfigurationRoot BuildConfiguration()
        {
            var basePath = FindAppSettingsBasePath();
            return new ConfigurationBuilder()
                .SetBasePath(basePath)
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: false)
                .AddJsonFile("appsettings.Local.json", optional: true, reloadOnChange: false)
                .AddUserSecrets(typeof(BasePersistenceTest).Assembly, optional: true)
                .Build();
        }

        protected static IConfigurationRoot GetTestConfiguration()
        {
            return TestConfiguration.Value;
        }

        protected IMongoCollection<TChunk> GetCollection<TChunk>()
        {
            var fieldInfo = _mongoPersistence.GetType()
                .GetField("_chunks", BindingFlags.NonPublic | BindingFlags.Instance);

            var collection = (IMongoCollection<TChunk>)fieldInfo.GetValue(_mongoPersistence);
            return collection;
        }

        protected virtual IMongoPersistence CreatePersistence(MongoPersistenceOptions options)
        {
            return new MongoPersistence(options);
        }

        protected void Clear(IPersistence persistence, bool drop)
        {
            // nothing to do
        }
    }
}
