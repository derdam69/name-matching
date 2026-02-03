
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text.Json.Serialization;

namespace MediaStoreApi.Modules.Explore;

public class ExploreService: IExploreService {
    private ILogger<ExploreService> _logger;

    // Imports the FindWindow function from user32.dll
    [DllImport("user32.dll", SetLastError = true, CharSet = CharSet.Auto)]
    static extern IntPtr FindWindow(string lpClassName, string lpWindowName);

    public ExploreService(ILogger<ExploreService> logger)
    {
        _logger = logger;
    }

    public string Open(string path) {
        // Process.Start("explorer.exe", path);
        _logger.LogInformation($"Open {path}");
        
        var process = new Process();

        var startInfo = new ProcessStartInfo();

        startInfo.FileName = "explorer.exe";
        startInfo.Arguments = path;
        startInfo.WindowStyle = ProcessWindowStyle.Normal;
       
        var procStarted = Process.Start(startInfo);
        
        return "Process Id: " + procStarted?.Id;
    }

    private delegate bool EnumThreadDelegate(IntPtr hWnd, IntPtr lParam);

    public IEnumerable<string> GetFiles(string path, string pattern = "*.*")
    {
        var files = Directory.GetFiles(path, pattern,
            new EnumerationOptions() { RecurseSubdirectories = true, AttributesToSkip = FileAttributes.System });
        return files;
    }

    public void AddSearchRequest(string query)
    { 
        var path = Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData) + "/MediaStore/";
        _logger.LogInformation(path);
        System.IO.File.AppendAllText(path+"search-requests.txt", $"{query}\n");
        _logger.LogInformation($"Add search request '{query}'");
    }
}