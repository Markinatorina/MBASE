using Gremlin.Net.Driver;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using DAL.Internal;
using DAL.Options;
using DAL.Repositories;

namespace DAL;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddJanusGraph(this IServiceCollection services, Action<JanusGraphOptions> configure)
    {
        services.Configure(configure);
        services.AddSingleton<IGremlinClientFactory, GremlinClientFactory>();
        services.AddSingleton(sp => sp.GetRequiredService<IGremlinClientFactory>().Create());
        services.AddSingleton<IGremlinClient>(sp => sp.GetRequiredService<GremlinClient>());
        services.AddScoped<IGraphRepository, GraphRepository>();
        return services;
    }
}
