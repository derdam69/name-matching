namespace MediaStoreApi.Modules.Explore.Endpoints;

public static class PostPlay
{
    public static  IResult Handle(string path)
    {
        WinampController.Play(path);
        return Results.Ok();
    }   
}