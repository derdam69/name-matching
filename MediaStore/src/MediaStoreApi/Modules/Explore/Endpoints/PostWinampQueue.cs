namespace MediaStoreApi.Modules.Explore.Endpoints;

public static class PostWinampQueue
{
    public static  IResult Handle(string path)
    {
        WinampController.Enqueue(path);
        return Results.Ok();
    }   
}