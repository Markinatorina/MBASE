namespace DAL.Options;

public class JanusGraphOptions
{
    public string Host { get; set; } = "localhost";
    public int Port { get; set; } = 8182;
    public bool EnableSsl { get; set; } = false;
    public string? Username { get; set; }
    public string? Password { get; set; }
    public int PoolSize { get; set; } = 4;
    public int MaxInProcessPerConnection { get; set; } = 32;
    public int MessageSerializerVersion { get; set; } = 3; // Gremlin protocol v3
}
