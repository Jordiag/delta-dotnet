using System.Text.Json;
using System.Text.Json.Serialization;

namespace Delta.Common
{
    /// <summary>
    /// 
    /// </summary>
    public class DictionaryConverter : JsonConverter<IDictionary<string, string>> 
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="typeToConvert"></param>
        /// <returns></returns>
        public override bool CanConvert(Type typeToConvert)
        {
            return typeToConvert == typeof(Dictionary<string, string>);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="typeToConvert"></param>
        /// <param name="options"></param>
        /// <returns></returns>
        public override Dictionary<string, string?> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if(reader.TokenType != JsonTokenType.StartArray && reader.TokenType != JsonTokenType.StartObject)
            {
                throw new JsonException($"JsonTokenType was of type {reader.TokenType}, only objects are supported");
            }

            var dictionary = new Dictionary<string, string?>();
            while(reader.Read())
            {
                if(reader.TokenType == JsonTokenType.EndArray)
                {
                    return dictionary;
                }

                JsonTokenType tokenType = reader.TokenType;

                if(tokenType != JsonTokenType.PropertyName)
                {
                    return tokenType == JsonTokenType.EndObject ? dictionary : throw new JsonException("JsonTokenType was not PropertyName");
                }

                string? propertyName = reader.GetString();

                if(string.IsNullOrWhiteSpace(propertyName))
                {
                    throw new JsonException("Failed to get property name");
                }

                reader.Read();

                dictionary.Add(propertyName!, reader.GetString());
            }

            return dictionary;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="writer"></param>
        /// <param name="value"></param>
        /// <param name="options"></param>
        public override void Write(Utf8JsonWriter writer, IDictionary<string, string?> value, JsonSerializerOptions options)
        {
            //Step 1 - Convert dictionary to a dictionary with string key
            var dictionary = new Dictionary<string, string?>(value.Count);

            foreach(KeyValuePair<string, string?> kvp in value)
            {
                string? key = kvp.Key.ToString();
                if(!string.IsNullOrEmpty(key))
                {
                    dictionary.Add(key, kvp.Value?.ToString());
                }

            }
            //Step 2 - Use the built-in serializer, because it can handle dictionaries with string keys
            JsonSerializer.Serialize(writer, dictionary, options);
        }
    }
}
