using System.Runtime.InteropServices;

public static class WinampController
{
    private static string winampLocation = @"C:\Program Files (x86)\Winamp\winamp.exe";

    // Default template for Now Playing plugin
    // https://github.com/Aldaviva/WinampNowPlayingToFile?tab=readme-ov-file
    // {{#if Artist}}{{Artist}} – {{/if}}{{Title}}{{#if Album}} – {{Album}}{{/if}}

    // {{Filename}} | {{Elapsed:hh\:mm\:ss}} | {{Length:hh\:mm\:ss}}
   
    // Constants
    private const string WinampClassName = "Winamp v1.x";
    private const int WM_COMMAND = 0x111;
    
    // Other common IDs for reference
    public const int IPC_PREVTRACK = 40044;
    public const int IPC_NEXTTRACK = 40048;
    public const int IPC_STOP = 40048; 

    // P/Invoke Declarations
    [DllImport("user32.dll", SetLastError = true, CharSet = CharSet.Auto)]
    static extern IntPtr FindWindow(string lpClassName, string lpWindowName);

    [DllImport("User32.dll")]
    public static extern Int32 SendMessage(IntPtr hWnd, int Msg, IntPtr wParam, IntPtr lParam);

    public static void ClearPlaylist()
    {
        IntPtr winampHandle = FindWindow(WinampClassName, null);

        if (winampHandle != IntPtr.Zero)
        {
            // Send the WM_COMMAND message with the Clear Playlist ID (40214)
            SendMessage(winampHandle, WM_COMMAND, new IntPtr(IPC_NEXTTRACK), IntPtr.Zero);
            Console.WriteLine("Playlist cleared successfully (using command 40214).");
        }
        else
        {
            Console.WriteLine("Winamp window not found. Make sure Winamp is running.");
        }
    }

    
    public static void Enqueue(string path)
    {
       System.Diagnostics.Process.Start(winampLocation, $"/ADD \"{path}\"");
    }

     public static void Play(string path)
    {
       System.Diagnostics.Process.Start(winampLocation, $"\"{path}\"");
    }
}