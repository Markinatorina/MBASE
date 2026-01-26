using System.Text.Json;
using System.Text.Json.Nodes;

namespace BLL.Utils;

/// <summary>
/// Provides JSON Patch (RFC 6902) operations for FHIR resources.
/// </summary>
public static class JsonPatchHelper
{
    /// <summary>
    /// Applies a JSON Patch document to a FHIR resource.
    /// </summary>
    /// <param name="original">The original JSON document.</param>
    /// <param name="patch">The JSON Patch document containing operations.</param>
    /// <returns>The patched JSON string, or null if patching failed.</returns>
    public static string? ApplyJsonPatch(JsonDocument original, JsonDocument patch)
    {
        try
        {
            var originalObj = JsonNode.Parse(original.RootElement.GetRawText());
            if (originalObj is null) return null;

            foreach (var operation in patch.RootElement.EnumerateArray())
            {
                var op = operation.TryGetProperty("op", out var opProp) ? opProp.GetString() : null;
                var path = operation.TryGetProperty("path", out var pathProp) ? pathProp.GetString() : null;

                if (string.IsNullOrEmpty(op) || string.IsNullOrEmpty(path))
                    continue;

                // Convert JSON Pointer path to property path
                var pathParts = path.TrimStart('/').Split('/');

                switch (op.ToLowerInvariant())
                {
                    case "add":
                    case "replace":
                        if (operation.TryGetProperty("value", out var value))
                        {
                            SetValueAtPath(originalObj, pathParts, value);
                        }
                        break;

                    case "remove":
                        RemoveValueAtPath(originalObj, pathParts);
                        break;

                    case "test":
                        // Test operation - verify value matches
                        if (operation.TryGetProperty("value", out var testValue))
                        {
                            if (!TestValueAtPath(originalObj, pathParts, testValue))
                                return null; // Test failed
                        }
                        break;
                }
            }

            return originalObj.ToJsonString();
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    /// Sets a value at the specified JSON path.
    /// </summary>
    public static void SetValueAtPath(JsonNode node, string[] path, JsonElement value)
    {
        var current = node;
        for (int i = 0; i < path.Length - 1; i++)
        {
            if (current is JsonObject obj)
            {
                if (!obj.ContainsKey(path[i]))
                    obj[path[i]] = new JsonObject();
                current = obj[path[i]];
            }
            else if (current is JsonArray arr && int.TryParse(path[i], out var idx))
            {
                current = arr[idx];
            }
        }

        var lastKey = path[^1];
        if (current is JsonObject finalObj)
        {
            finalObj[lastKey] = JsonNode.Parse(value.GetRawText());
        }
        else if (current is JsonArray finalArr && int.TryParse(lastKey, out var arrIdx))
        {
            if (lastKey == "-" || arrIdx >= finalArr.Count)
                finalArr.Add(JsonNode.Parse(value.GetRawText()));
            else
                finalArr[arrIdx] = JsonNode.Parse(value.GetRawText());
        }
    }

    /// <summary>
    /// Removes a value at the specified JSON path.
    /// </summary>
    public static void RemoveValueAtPath(JsonNode node, string[] path)
    {
        var current = node;
        for (int i = 0; i < path.Length - 1; i++)
        {
            if (current is JsonObject obj && obj.ContainsKey(path[i]))
                current = obj[path[i]];
            else if (current is JsonArray arr && int.TryParse(path[i], out var idx))
                current = arr[idx];
            else
                return;
        }

        var lastKey = path[^1];
        if (current is JsonObject finalObj)
            finalObj.Remove(lastKey);
        else if (current is JsonArray finalArr && int.TryParse(lastKey, out var arrIdx))
            finalArr.RemoveAt(arrIdx);
    }

    /// <summary>
    /// Tests if a value at the specified JSON path matches the expected value.
    /// </summary>
    public static bool TestValueAtPath(JsonNode node, string[] path, JsonElement expected)
    {
        var current = node;
        foreach (var part in path)
        {
            if (current is JsonObject obj && obj.ContainsKey(part))
                current = obj[part];
            else if (current is JsonArray arr && int.TryParse(part, out var idx))
                current = arr[idx];
            else
                return false;
        }

        return current?.ToJsonString() == expected.GetRawText();
    }
}
