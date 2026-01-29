using MediaStoreApi.Modules.Explore.Endpoints;

namespace MediaStoreApi.Modules.Explore;

public static class ExploreModule
{
    public static IServiceCollection RegisterExploreModule(this IServiceCollection services)
    {
        services.AddScoped<IExploreService, ExploreService>();
        return services;
    }

    public static IEndpointRouteBuilder MapExploreEndpoints(this IEndpointRouteBuilder endpoints)
    {
        endpoints.MapPost("/explore/{path}", PostExplore.Handle);
        endpoints.MapGet("/files/{path}/{matchPattern}", GetFiles.Handle);
           endpoints.MapGet("/test", () => "OK");
        return endpoints;
    }
}