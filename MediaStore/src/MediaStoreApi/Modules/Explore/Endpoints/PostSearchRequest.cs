namespace MediaStoreApi.Modules.Explore.Endpoints;

public static class PostSearchRequest
{
    public static  IResult Handle(string query, IExploreService exploreService)
    {
        exploreService.AddSearchRequest(query);
        return Results.Ok();
    }   
}