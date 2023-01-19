
using System.Data;
using System.IO.Compression;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Snowflake.Data.Client;

namespace SnowpipeIngest
{

    public class SnowpipeLogger
    {
        private static SnowpipeSettings settings = SnowpipeSettings.ReadSnowpipeSettings();
        private static StreamWriter? _currentWriter;
        private static DateTime? _memoryBufferExpiration;
        private static DateTime? _diskBufferExpiration;
        private static long _currentRecordCount = 0;
        private static long _currentFileSize = 0;
        private static readonly HttpClient client = new();


        static SnowpipeLogger()
        {
            client.DefaultRequestHeaders.Accept.Clear();
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        }

        static void Main(string[] args)
        {
            DateTime stopTime = DateTime.Now.AddMinutes(10);
            int i = 0;
            while (DateTime.Now < stopTime)
            {
                var data = new Dictionary<string, string>
                {
                    { "test", String.Format("{0}", i) },
                    { "test2", String.Format("{0}", i) }
                };
                string jsonString = JsonSerializer.Serialize(data);
                SnowpipeLogger.LogJson(jsonString);
                Console.WriteLine("Logged: " + jsonString);
                i++;
                Task.Delay(1000).Wait();
            }
            SnowpipeLogger.FlushAll();

        }

        public static void LogObjectAsJson(object obj)
        {
            string jsonString = JsonSerializer.Serialize(obj);
            LogJson(jsonString);
        }

        public static void LogJson(string jsonMessage)
        {
            if (jsonMessage == null)
                throw new Exception("JSON message is null");
            if (_currentWriter == null)
                _currentWriter = OpenWriter(settings.CurrentLogFilename);
            if (_memoryBufferExpiration == null)
                _memoryBufferExpiration = DateTime.Now.AddMilliseconds(settings.MemoryBufferFlushTime);
            if (_diskBufferExpiration == null)
                _diskBufferExpiration = DateTime.Now.AddSeconds(settings.BufferFlushTime);

            lock (_currentWriter)
            {
                _currentWriter.WriteAsync(jsonMessage + "\n");
                _currentRecordCount++;

                // Periodically flush the writer to disk and obtain the file size
                if (_memoryBufferExpiration <= DateTime.Now)
                    FlushMemory();


                // Check if the size, record count, or time limit has been exceeded
                if (_currentRecordCount >= settings.BufferCountRecords ||
                    _currentFileSize >= settings.BufferSizeBytes ||
                    _diskBufferExpiration <= DateTime.Now)
                    FlushDisk();

            }

        }

        public static void FlushAll()
        {
            if (_currentWriter != null)
            {
                lock (_currentWriter)
                {
                    FlushMemory();
                    FlushDisk();
                }
            }
        }

        private static void FlushMemory()
        {
            if (settings != null && _currentWriter != null)
            {
                _currentWriter.Flush();
                _currentFileSize = new FileInfo(settings.CurrentLogFilename).Length;
                _memoryBufferExpiration = DateTime.Now.AddMilliseconds(settings.MemoryBufferFlushTime);
            }
        }

        private static void FlushDisk()
        {
            if (settings != null && _currentWriter != null)
            {
                string tmpFilename = Path.GetRandomFileName() + ".gz";
                // Release all resources for file
                // The writer will be recreated the next time a message is logged
                _currentWriter.Dispose();
                _currentWriter = null;

                // Rename file to temporary name
                File.Move(settings.CurrentLogFilename, tmpFilename);

                // Reset Counters
                _currentRecordCount = 0;
                _memoryBufferExpiration = DateTime.Now.AddMilliseconds(settings.MemoryBufferFlushTime);
                _diskBufferExpiration = DateTime.Now.AddSeconds(settings.BufferFlushTime);

                PutFile(tmpFilename);
                File.Delete(tmpFilename);

            }
        }

        private static StreamWriter OpenWriter(string filename)
        {
            FileStream fs = File.OpenWrite(filename);
            GZipStream gZipStream = new GZipStream(fs, CompressionMode.Compress);
            StreamWriter sw = new StreamWriter(new GZipStream(fs, CompressionMode.Compress));
            return sw;
        }

        private static void PutFile(string filename)
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = String.Format(
                    "account={0};authenticator={1};user={2};private_key_file={3};private_key_pwd={4};db={5};schema={6}",
                    settings.Account, "snowflake_jwt", settings.User, settings.PrivateKeyFilename, settings.PrivateKeyPassphrase, settings.DatabaseName, settings.SchemaName);

                conn.Open();
                Console.WriteLine("DB Connection Established");

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = $"PUT file://{filename} @{settings.StageName} OVERWRITE = TRUE AUTO_COMPRESS=FALSE SOURCE_COMPRESSION=gzip";
                IDataReader reader = cmd.ExecuteReader();

                reader.Read();
                //columns = source, target, source_size, target_size
                string uploadedFile = reader.GetString(1);
                long uploadedSize = reader.GetInt64(3);
                Console.WriteLine($"File Uploaded: path={uploadedFile}, size={uploadedSize}");

                conn.Close();
                Console.WriteLine("DB Connection Closed");

                CallFileNotification(uploadedFile, uploadedSize);
            }

        }

        async private static void CallFileNotification(string uploadedFile, long uploadedSize)
        {
            // Create the request body
            // serialize into json string
            string jsonString = JsonSerializer.Serialize(new SnowpipePayload()
            {
                files = new List<SnowPipeFile>{
                    new SnowPipeFile {
                        path = uploadedFile,
                        size = uploadedSize
                    }
                }
            });
            string requestId = Guid.NewGuid().ToString();
            Uri snowpipeUri = new Uri(String.Format(
                "https://{0}.snowflakecomputing.com/v1/data/pipes/{1}.{2}.{3}/insertFiles?requestId={4}",
                settings.Account, settings.DatabaseName, settings.SchemaName, settings.PipeName, requestId));

            StringContent httpContent = new(jsonString, Encoding.UTF8, new MediaTypeHeaderValue("application/json"));

            // Create an authorization token from our private key
            string jwtToken = SnowflakeJWT.GenerateSnowflakeJwtToken(
                settings.Account, settings.User, settings.PrivateKeyFilename, settings.PrivateKeyPassphrase);
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", jwtToken);

            // Call the API
            HttpResponseMessage response = await client.PostAsync(snowpipeUri, httpContent);

            // Throw an exception if the response is an HTTP error
            response.EnsureSuccessStatusCode();

            Console.WriteLine("Rest API called successfully");
        }

        private class SnowPipeFile
        {
            public string? path { get; set; }
            public long size { get; set; }
        }

        private class SnowpipePayload
        {
            public List<SnowPipeFile>? files { get; set; }
        }

    }

}