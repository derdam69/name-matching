namespace MediaStoreApi.Modules.Explore.Endpoints;

public static class PostExplore
{
    public static  IResult Handle(string path, IExploreService exploreService)
    {
        var ret = exploreService.Open(path);
        return Results.Ok(ret);
    }   
}