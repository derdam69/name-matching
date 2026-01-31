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
        var group = endpoints.MapGroup("/api/explorer");
        group.MapPost("/open/{path}", PostOpen.Handle);
        group.MapPost("/play/{path}", PostPlay.Handle);
        group.MapPost("/winamp/queue/{path}", PostWinampQueue.Handle);
        // group.MapGet("/files/{path}/{matchPattern}", GetFiles.Handle);
        // group.MapGet("/test", () => "OK");
        return group;
    }
}