using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text.Json.Serialization;

namespace MediaStoreApi.Modules.Explore;

public class ExploreService: IExploreService {

    // Imports the FindWindow function from user32.dll
    [DllImport("user32.dll", SetLastError = true, CharSet = CharSet.Auto)]
    static extern IntPtr FindWindow(string lpClassName, string lpWindowName);

    public string Open(string path) {
        // Process.Start("explorer.exe", path);
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
}