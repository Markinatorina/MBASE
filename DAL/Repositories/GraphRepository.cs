using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.Process;
using Gremlin.Net.Structure;

namespace DAL.Repositories;

public interface IGraphRepository
{
    Task<Vertex> AddVertexAsync(string label, IDictionary<string, object> properties, CancellationToken ct = default);
    Task<Edge> AddEdgeAsync(string label, string outVertexId, string inVertexId, IDictionary<string, object>? properties = null, CancellationToken ct = default);
    Task<Vertex?> GetVertexByIdAsync(string id, CancellationToken ct = default);
    Task<bool> UpdateVertexPropertiesAsync(string id, IDictionary<string, object> properties, CancellationToken ct = default);
    Task<bool> DeleteVertexAsync(string id, CancellationToken ct = default);
    Task<long> CountVerticesAsync(CancellationToken ct = default);
    Task<Edge?> AddEdgeByPropertyAsync(
        string label,
        string outLabel, string outKey, object outValue,
        string inLabel, string inKey, object inValue,
        IDictionary<string, object>? properties = null,
        CancellationToken ct = default);
    Task<Vertex> UpsertVertexByPropertyAsync(string label, string key, object value, IDictionary<string, object> properties, CancellationToken ct = default);
    Task<string?> AddVertexAndReturnIdAsync(string label, IDictionary<string, object> properties, CancellationToken ct = default);
    Task<string?> UpsertVertexAndReturnIdAsync(string label, string key, object value, IDictionary<string, object> properties, CancellationToken ct = default);
}

internal sealed class GraphRepository : IGraphRepository
{
    private readonly GremlinClient _client;

    public GraphRepository(GremlinClient client)
    {
        _client = client;
    }

    private async Task<IReadOnlyCollection<T>> SubmitAsync<T>(string script, Dictionary<string, object>? bindings, CancellationToken ct)
    {
        var messageBuilder = RequestMessage.Build(Tokens.OpsEval)
            .AddArgument(Tokens.ArgsGremlin, script);

        if (bindings is not null && bindings.Count > 0)
        {
            messageBuilder = messageBuilder.AddArgument(Tokens.ArgsBindings, bindings);
        }

        var message = messageBuilder.Create();
        // GremlinClient does not expose a CT overload in all versions; if available, could pass ct via RequestOptions. Here we ignore ct.
        return await _client.SubmitAsync<T>(message);
    }

    public async Task<Vertex> AddVertexAsync(string label, IDictionary<string, object> properties, CancellationToken ct = default)
    {
        var script = "g.addV(label)";
        var bindings = new Dictionary<string, object> { ["label"] = label };

        foreach (var kv in properties)
        {
            var key = kv.Key;
            var bindingKey = $"p_{key}";
            script += $".property('{key}', {bindingKey})";
            bindings[bindingKey] = kv.Value!;
        }

        var result = await SubmitAsync<Vertex>(script, bindings, ct);
        return result.First();
    }

    public async Task<string?> AddVertexAndReturnIdAsync(string label, IDictionary<string, object> properties, CancellationToken ct = default)
    {
        var script = "g.addV(label)";
        var bindings = new Dictionary<string, object> { ["label"] = label };

        foreach (var kv in properties)
        {
            var key = kv.Key;
            var bindingKey = $"p_{key}";
            script += $".property('{key}', {bindingKey})";
            bindings[bindingKey] = kv.Value!;
        }

        script += ".id()";
        var result = await SubmitAsync<object>(script, bindings, ct);
        return result.FirstOrDefault()?.ToString();
    }

    public async Task<Edge> AddEdgeAsync(string label, string outVertexId, string inVertexId, IDictionary<string, object>? properties = null, CancellationToken ct = default)
    {
        var script = "g.V(outId).addE(label).to(g.V(inId))";
        var bindings = new Dictionary<string, object>
        {
            ["label"] = label,
            ["outId"] = outVertexId,
            ["inId"] = inVertexId
        };

        if (properties != null)
        {
            foreach (var kv in properties)
            {
                var key = kv.Key;
                var bindingKey = $"p_{key}";
                script += $".property('{key}', {bindingKey})";
                bindings[bindingKey] = kv.Value!;
            }
        }

        var result = await SubmitAsync<Edge>(script, bindings, ct);
        return result.First();
    }

    public async Task<Vertex?> GetVertexByIdAsync(string id, CancellationToken ct = default)
    {
        var result = await SubmitAsync<Vertex>("g.V(id)", new Dictionary<string, object> { ["id"] = id }, ct);
        return result.FirstOrDefault();
    }

    public async Task<bool> UpdateVertexPropertiesAsync(string id, IDictionary<string, object> properties, CancellationToken ct = default)
    {
        var script = "g.V(id)";
        var bindings = new Dictionary<string, object> { ["id"] = id };
        foreach (var kv in properties)
        {
            var key = kv.Key;
            var bindingKey = $"p_{key}";
            script += $".property('{key}', {bindingKey})";
            bindings[bindingKey] = kv.Value!;
        }
        var result = await SubmitAsync<Vertex>(script, bindings, ct);
        return result.Any();
    }

    public async Task<bool> DeleteVertexAsync(string id, CancellationToken ct = default)
    {
        var result = await SubmitAsync<object>("g.V(id).drop()", new Dictionary<string, object> { ["id"] = id }, ct);
        // JanusGraph returns empty result set for drop; treat as success if no exception
        return true;
    }

    public async Task<long> CountVerticesAsync(CancellationToken ct = default)
    {
        var result = await SubmitAsync<long>("g.V().count()", null, ct);
        return result.FirstOrDefault();
    }

    public async Task<Edge?> AddEdgeByPropertyAsync(
        string label,
        string outLabel, string outKey, object outValue,
        string inLabel, string inKey, object inValue,
        IDictionary<string, object>? properties = null,
        CancellationToken ct = default)
    {
        var script = "g.V().has(outLabel, outKey, outVal).addE(label).to(g.V().has(inLabel, inKey, inVal))";
        var bindings = new Dictionary<string, object>
        {
            ["label"] = label,
            ["outLabel"] = outLabel,
            ["outKey"] = outKey,
            ["outVal"] = outValue,
            ["inLabel"] = inLabel,
            ["inKey"] = inKey,
            ["inVal"] = inValue
        };

        if (properties != null)
        {
            foreach (var kv in properties)
            {
                var key = kv.Key;
                var bindingKey = $"p_{key}";
                script += $".property('{key}', {bindingKey})";
                bindings[bindingKey] = kv.Value!;
            }
        }

        var result = await SubmitAsync<Edge>(script, bindings, ct);
        return result.FirstOrDefault();
    }

    public async Task<Vertex> UpsertVertexByPropertyAsync(string label, string key, object value, IDictionary<string, object> properties, CancellationToken ct = default)
    {
        var script = "g.V().has(lbl, propKey, propVal).fold().coalesce(unfold(), addV(lbl).property(propKey, propVal))";
        var bindings = new Dictionary<string, object>
        {
            ["lbl"] = label,
            ["propKey"] = key,
            ["propVal"] = value
        };

        foreach (var kv in properties)
        {
            var propKey = kv.Key;
            var bindingKey = $"p_{propKey}";
            script += $".property('{propKey}', {bindingKey})";
            bindings[bindingKey] = kv.Value!;
        }

        var result = await SubmitAsync<Vertex>(script, bindings, ct);
        return result.First();
    }

    public async Task<string?> UpsertVertexAndReturnIdAsync(string label, string key, object value, IDictionary<string, object> properties, CancellationToken ct = default)
    {
        var script = "g.V().has(lbl, propKey, propVal).fold().coalesce(unfold(), addV(lbl).property(propKey, propVal))";
        var bindings = new Dictionary<string, object>
        {
            ["lbl"] = label,
            ["propKey"] = key,
            ["propVal"] = value
        };

        foreach (var kv in properties)
        {
            var propKey = kv.Key;
            var bindingKey = $"p_{propKey}";
            script += $".property('{propKey}', {bindingKey})";
            bindings[bindingKey] = kv.Value!;
        }

        script += ".id()";
        var result = await SubmitAsync<object>(script, bindings, ct);
        return result.FirstOrDefault()?.ToString();
    }
}
