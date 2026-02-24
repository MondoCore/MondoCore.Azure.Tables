using System;
using System.Text.Json;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;

using MondoCore.Common;

namespace MondoCore.TestHelpers
{
    public static class TestConfiguration
    {
        public static Configuration Load()
        { 
            var path = Assembly.GetCallingAssembly().Location.SubstringBefore("\\bin");
            var json = File.ReadAllText(Path.Combine(path, "localhost.json"));

            return JsonSerializer.Deserialize<Configuration>(json)!;
        }
    }

    public class Configuration
    {
        public string? ConnectionString          { get; set; }
    }
}
