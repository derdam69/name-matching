namespace MediaStoreApi.Modules.Explore.Endpoints;

public static class PostOpen
{
    public static  IResult Handle(string path, IExploreService exploreService)
    {
        var ret = exploreService.Open( "\"" + path) +  "\"";
        return Results.Ok(ret);
    }   
}