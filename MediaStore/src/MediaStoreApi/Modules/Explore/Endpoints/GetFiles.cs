using Microsoft.AspNetCore.Http.HttpResults;

namespace MediaStoreApi.Modules.Explore.Endpoints;

public static class GetFiles
{
    public static Results<Ok<IEnumerable<string>>, NotFound> Handle(string path, string matchPattern, IExploreService exploreService)
    {
        return TypedResults.Ok(exploreService.GetFiles(path, matchPattern));
    }
}