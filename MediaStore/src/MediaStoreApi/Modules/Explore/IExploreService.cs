namespace MediaStoreApi.Modules.Explore;

public interface IExploreService {
    string Open(string path);
    IEnumerable<string> GetFiles(string path, string pattern = "*.*");
    void AddSearchRequest(string query);
}