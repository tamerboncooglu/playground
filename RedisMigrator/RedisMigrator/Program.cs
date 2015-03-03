using System;
using System.Configuration;
using System.Linq;
using CommandLine;
using CommandLine.Text;
using StackExchange.Redis;

namespace RedisMigrator
{
    public class Program
    {
        static readonly Configuration Configuration = new Configuration();

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
            var database = RedisConnectionSource.GetDatabase(Configuration.SourceRedisDb);
            return database;
        }

        private static IDatabase GetTargetDatabase()
        {
            var database = RedisConnectionTarget.GetDatabase(Configuration.TargetRedisDb);
            return database;
        }

        private static void GetConfiguration(string[] args)
        {
            if (args.Any())
            {
                var options = new Options();
                if (Parser.Default.ParseArguments(args, options))
                {
                    Configuration.SourceRedisHost = options.SourceRedisHost;
                    Configuration.TargetRedisHost = options.TargetRedisHost;
                    Configuration.SourceRedisPort = options.SourceRedisPort;
                    Configuration.TargetRedisPort = options.TargetRedisPort;
                    Configuration.SourceRedisDb = options.SourceRedisDb;
                    Configuration.TargetRedisDb = options.TargetRedisDb;
                }
            }
            else
            {
                Configuration.SourceRedisHost = ConfigurationManager.AppSettings["SourceRedisHost"];
                Configuration.TargetRedisHost = ConfigurationManager.AppSettings["TargetRedisHost"];
                Configuration.SourceRedisPort = int.Parse(ConfigurationManager.AppSettings["SourceRedisPort"]);
                Configuration.TargetRedisPort = int.Parse(ConfigurationManager.AppSettings["TargetRedisPort"]);
                Configuration.SourceRedisDb = int.Parse(ConfigurationManager.AppSettings["SourceRedisDb"]);
                Configuration.TargetRedisDb = int.Parse(ConfigurationManager.AppSettings["TargetRedisDb"]);
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
                options.EndPoints.Add(Configuration.SourceRedisHost, Configuration.SourceRedisPort);
            }
            else
            {
                options.EndPoints.Add(Configuration.TargetRedisHost, Configuration.TargetRedisPort);
            }

            var redisConnection = ConnectionMultiplexer.Connect(options);
            return redisConnection;
        }
    }

    internal class Configuration
    {
        public string SourceRedisHost { get; set; }
        public string TargetRedisHost { get; set; }

        public int SourceRedisPort { get; set; }
        public int TargetRedisPort { get; set; }

        public int SourceRedisDb { get; set; }
        public int TargetRedisDb { get; set; }
    }

    class Options
    {
        [Option('s', "source", Required = true, HelpText = "Source Redis Host")]
        public string SourceRedisHost { get; set; }

        [Option('t', "target", Required = true, HelpText = "Target Redis Host")]
        public string TargetRedisHost { get; set; }

        [Option("sp", HelpText = "Source Redis Port", DefaultValue = 6379)]
        public int SourceRedisPort { get; set; }

        [Option("tp", HelpText = "Target Redis Port", DefaultValue = 6379)]
        public int TargetRedisPort { get; set; }

        [Option("sd", HelpText = "Source Redis Db Id")]
        public int SourceRedisDb { get; set; }

        [Option("td", HelpText = "Target Redis Db Id")]
        public int TargetRedisDb { get; set; }

        [ParserState]
        public IParserState LastParserState { get; set; }

        [HelpOption]
        public string GetUsage()
        {
            return HelpText.AutoBuild(this,
              (HelpText current) => HelpText.DefaultParsingErrorsHandler(this, current));
        }
    }
}
