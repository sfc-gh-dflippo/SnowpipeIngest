using System;
using System.Text.Json;

namespace SnowpipeIngest
{
    public class SnowpipeSettings
    {
        // Number of records buffered before ingesting to Snowflake.
        // The default value is 1,000,000 records.
        public double BufferCountRecords { get; set; } = 1000000;

        // Number of seconds between buffer flushes to the internal stage.
        // The default value is 120 seconds.
        public double BufferFlushTime { get; set; } = 120;

        // Cumulative size in bytes of records buffered before they are ingested in Snowflake as data files.
        // The default value for this is 100,000,000 (100 MB).
        public double BufferSizeBytes { get; set; } = 100000000;

        // Number of milliseconds between disk buffer flushes to the local files.
        // The default value is 10000 milliseconds.
        public double MemoryBufferFlushTime { get; set; } = 10000;

        public string CurrentLogFilename { get; set; } = "SnowpipeMessages.tmp.gz";

#pragma warning disable CS8618 // We want an exception if any of these fields are missing
        public string Account { get; set; }
        public string User { get; set; }
        public string PrivateKeyFilename { get; set; }
        public string PrivateKeyPassphrase { get; set; }
        public string DatabaseName { get; set; }
        public string SchemaName { get; set; }
        public string PipeName { get; set; }
        public string StageName { get; set; }
#pragma warning restore CS8618

        public static SnowpipeSettings ReadSnowpipeSettings()
        {
            string jsonText = File.ReadAllText("SnowpipeSettings.json");
            if (jsonText == null)
                throw new Exception("Could not read SnowpipeSettings.json");

            SnowpipeSettings? settings = JsonSerializer.Deserialize<SnowpipeSettings>(json: jsonText);
            if (settings == null)
                throw new Exception("Settings could not be read");

            return settings;
        }

        public void WriteSnowpipeSettings()
        {
            var options = new JsonSerializerOptions { WriteIndented = true };
            string jsonString = JsonSerializer.Serialize(this, options);
            File.WriteAllText("SnowpipeConfig.json", jsonString);
        }
    }
}

