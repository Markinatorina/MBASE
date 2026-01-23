using Gremlin.Net.Driver;
using Gremlin.Net.Structure.IO.GraphBinary;
using Microsoft.Extensions.Options;
using DAL.Options;

namespace DAL.Internal;

public interface IGremlinClientFactory
{
    GremlinClient Create();
}

internal sealed class GremlinClientFactory : IGremlinClientFactory
{
    private readonly JanusGraphOptions _options;

    public GremlinClientFactory(IOptions<JanusGraphOptions> options)
    {
        _options = options.Value;
    }

    public GremlinClient Create()
    {
        var connectionPoolSettings = new ConnectionPoolSettings
        {
            MaxInProcessPerConnection = _options.MaxInProcessPerConnection,
            PoolSize = _options.PoolSize,
            ReconnectionAttempts = int.MaxValue,
            ReconnectionBaseDelay = TimeSpan.FromSeconds(1)
        };

        var gremlinServer = new GremlinServer(
            hostname: _options.Host,
            port: _options.Port,
            enableSsl: _options.EnableSsl,
            username: string.IsNullOrWhiteSpace(_options.Username) ? null : _options.Username,
            password: string.IsNullOrWhiteSpace(_options.Password) ? null : _options.Password
        );

        // JanusGraph 1.1 defaults to GraphBinary; use matching serializer to avoid protocol mismatch
        var messageSerializer = new GraphBinaryMessageSerializer();

        // Configure WebSocket to be resilient for local/dev usage
        Action<System.Net.WebSockets.ClientWebSocketOptions> wsConfig = options =>
        {
            options.KeepAliveInterval = TimeSpan.FromSeconds(30);
            if (_options.EnableSsl)
            {
                // For local/self-signed Gremlin Server certs, allow all certificates
                options.RemoteCertificateValidationCallback = (_, __, ___, ____) => true;
            }
        };

        return new GremlinClient(
            gremlinServer,
            messageSerializer,
            connectionPoolSettings: connectionPoolSettings,
            webSocketConfiguration: wsConfig
        );
    }
}
