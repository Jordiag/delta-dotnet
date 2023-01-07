using System.Text.Json;

namespace Delta.Common
{
    internal class JsonSerialiser
    {
        protected JsonSerialiser()
        {
        }

        public static T? Deserialise<T>(string line, string fileName, DeltaOptions deltaOptions)
        {
            try
            {
                T? action = JsonSerializer.Deserialize<T>(line, GetJsonSerializerOptions(deltaOptions));
                return action;
            }
            catch(ArgumentNullException ex)
            {
                throw new DeltaException($"Failed to deserialise this line: {line} from this file {fileName}", ex);
            }
            catch(NotSupportedException ex)
            {
                throw new DeltaException($"Failed to deserialise this line: {line} from this file {fileName}", ex);
            }
            catch(JsonException ex)
            {
                throw new DeltaException($"Failed to deserialise this line: {line} from this file {fileName}", ex);
            }
        }

        private static JsonSerializerOptions GetJsonSerializerOptions(DeltaOptions deltaOptions)
        {
            var jsonOptions = new JsonSerializerOptions()
            {
                PropertyNameCaseInsensitive = deltaOptions.DeserialiseCaseInsensitive,
                WriteIndented = true
            };
            jsonOptions.Converters.Add(new DictionaryConverter());

            return jsonOptions;
        }
    }
}
