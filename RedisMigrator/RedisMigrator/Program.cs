using System;
using System.Configuration;
using System.Linq;
using StackExchange.Redis;

namespace RedisMigrator
{
    public class Program
    {
        static readonly ConfigurationDto ConfigurationDto = new ConfigurationDto();

        static void Main(string[] args)
        {
            GetConfiguration(args);
            var sourcedb = GetSourceDatabase();
            var targetdb = GetTargetDatabase();

            var keys = sourcedb.ScriptEvaluate("local karr=redis.call('KEYS', '*'); return karr;");

            if (!keys.IsNull)
            {
                if (keys.GetType().Name.Contains("ArrayRedisResult"))
                {
                    var results = (RedisResult[])keys;
                    Console.WriteLine("Keys Count : {0}",results.Count());
                    foreach (var redisResult in results)
                    {
                        var redisKey = (RedisKey)redisResult;
                        var redisValue = sourcedb.StringGet(redisKey);
                        var ttl = sourcedb.KeyTimeToLive(redisKey);
                        targetdb.StringSet(redisKey, redisValue, ttl);

                        Console.WriteLine("Keys Copied : {0}", redisKey);
                    }
                    Console.WriteLine("Copy Finished : {0}", DateTime.Now);
                }
            }
            Console.ReadLine();
        }

        private static readonly Lazy<ConnectionMultiplexer> LazyConnectionSource = new Lazy<ConnectionMultiplexer>(() =>
        {
            var connection = LoadConfiguration(true);
            return connection;
        });

        private static readonly Lazy<ConnectionMultiplexer> LazyConnectionTarget = new Lazy<ConnectionMultiplexer>(() =>
        {
            var connection = LoadConfiguration(false);
            return connection;
        });

        private static ConnectionMultiplexer RedisConnectionSource
        {
            get { return LazyConnectionSource.Value; }
        }

        private static ConnectionMultiplexer RedisConnectionTarget
        {
            get { return LazyConnectionTarget.Value; }
        }

        private static IDatabase GetSourceDatabase()
        {
            var database = RedisConnectionSource.GetDatabase(ConfigurationDto.SourceRedisDb);
            return database;
        }

        private static IDatabase GetTargetDatabase()
        {
            var database = RedisConnectionTarget.GetDatabase(ConfigurationDto.TargetRedisDb);
            return database;
        }

        private static void GetConfiguration(string[] args)
        {
            if (args.Any())
            {
                ConfigurationDto.SourceRedisHost = args[0];
                ConfigurationDto.TargetRedisHost = args[1];
                ConfigurationDto.SourceRedisPort = int.Parse(args[2]);
                ConfigurationDto.TargetRedisPort = int.Parse(args[3]);
                ConfigurationDto.SourceRedisDb = int.Parse(args[4]);
                ConfigurationDto.TargetRedisDb = int.Parse(args[5]);
            }
            else
            {
                ConfigurationDto.SourceRedisHost = ConfigurationManager.AppSettings["SourceRedisHost"];
                ConfigurationDto.TargetRedisHost = ConfigurationManager.AppSettings["TargetRedisHost"];
                ConfigurationDto.SourceRedisPort = int.Parse(ConfigurationManager.AppSettings["SourceRedisPort"]);
                ConfigurationDto.TargetRedisPort = int.Parse(ConfigurationManager.AppSettings["TargetRedisPort"]);
                ConfigurationDto.SourceRedisDb = int.Parse(ConfigurationManager.AppSettings["SourceRedisDb"]);
                ConfigurationDto.TargetRedisDb = int.Parse(ConfigurationManager.AppSettings["TargetRedisDb"]);
            }
        }

        private static ConnectionMultiplexer LoadConfiguration(bool isSource)
        {
            var options = new ConfigurationOptions
            {
                ClientName = "RedisMigrator" + (isSource ? "Source" : "Target"),
                ResolveDns = true,
                ConnectTimeout = 30000,
                AbortOnConnectFail = false // Important for shared usage
            };

            if (isSource)
            {
                options.EndPoints.Add(ConfigurationDto.SourceRedisHost, ConfigurationDto.SourceRedisPort);
            }
            else
            {
                options.EndPoints.Add(ConfigurationDto.TargetRedisHost, ConfigurationDto.TargetRedisPort);
            }

            var redisConnection = ConnectionMultiplexer.Connect(options);
            return redisConnection;
        }
    }

    internal class ConfigurationDto
    {
        public string SourceRedisHost { get; set; }
        public string TargetRedisHost { get; set; }

        public int SourceRedisPort { get; set; }
        public int TargetRedisPort { get; set; }

        public int SourceRedisDb { get; set; }
        public int TargetRedisDb { get; set; }
    }
}
