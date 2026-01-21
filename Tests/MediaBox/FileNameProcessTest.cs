
using System.Net.NetworkInformation;
using System.Collections.Immutable;
using System.Collections.ObjectModel;
using System.Reflection.Emit;
using Newtonsoft.Json;
using System.Linq;
using System.Text;
using System.Globalization;
using Nest;

public class FileNameProcessTest
{
    /*
    */
    [Theory]
    [InlineData(@"d:\{MBVlabel=CDA}\Various Artists\4AD - Lonely is an Eyesore\06-Cocteau Twins - Crushed.wav", @"\Various Artists\4AD - Lonely is an Eyesore\06-Cocteau Twins - Crushed.wav")]
    [InlineData(@"d:\{MBVlabel=JBox@1}\0202101043\Various Artists\4AD Misc (Napster)\Felt - Primitive painters (featuring Liz Fraser from Cocteau.mp3", @"\Various Artists\4AD Misc (Napster)\Felt - Primitive painters (featuring Liz Fraser from Cocteau.mp3")]
    [InlineData(@"d:\{MBVlabel=JBox@2}\0202101043\Foo\Various Artists\4AD Misc (Napster)\Felt - Primitive painters (featuring Liz Fraser from Cocteau.mp3", @"\Various Artists\4AD Misc (Napster)\Felt - Primitive painters (featuring Liz Fraser from Cocteau.mp3")]
    [InlineData(@"d:\{MBVlabel=JBox@1}\MBX20030311009\Depeche Mode\101 (CD 2)\09-Master and servant.MP3", @"\Depeche Mode\101 (CD 2)\09-Master and servant.MP3")]
    [InlineData(@"d:\{MBVLabel=DVD}\David Lynch\Thumbs.db", @"\David Lynch\Thumbs.db")]
    [InlineData(@"d:\{MBVLabel=Downloading}\OMD - Telegraph (Beat Club live 1983).mpg",@"\OMD - Telegraph (Beat Club live 1983).mpg")]
    [InlineData(@"d:\{MBVLabel=ToBurn@1}\200410\Brian Wilson\Smile (Napster)\01 - Our Prayer - Gee.mp3",@"\Brian Wilson\Smile (Napster)\01 - Our Prayer - Gee.mp3")]
    public void evaluate_mediabox_tag(string input, string expected)
    {
        var evaluatedInput = EvaluateMediaBoxPath(input);
        Assert.Equal(expected, evaluatedInput);
    }

    [Fact]
    public void generate_treeemap_from_folder()
    {
        const string rootPath = @"D:\";
        const string outputFile = @"c:\temp\folder-treemap.json";

        try
        {
            var treeStructure = BuildTreeStructure(rootPath);
            var json = JsonConvert.SerializeObject(treeStructure, Formatting.Indented);
            System.IO.File.WriteAllText(outputFile, json);
            Assert.True(System.IO.File.Exists(outputFile), $"Output file {outputFile} was not created");
         var catalogTemplate = $@"
            var collections = [{json}]
        ";
        System.IO.File.WriteAllText(@"c:\temp\catalog\t-flat-grouped-echart.js", catalogTemplate);

       
        }
        catch (Exception ex)
        {
            Assert.True(false, $"Error building tree: {ex.Message}");
        }
    }

    private dynamic BuildTreeStructure(string path)
    {
        try
        {
            var dirInfo = new DirectoryInfo(path);
            var children = new List<dynamic>();
            int fileCount = 0;

            // Enumerate subdirectories
            try
            {
                foreach (var dir in dirInfo.EnumerateDirectories())
                {
                    var subTree = BuildTreeStructure(dir.FullName);
                    children.Add(subTree);
                }
            }
            catch (UnauthorizedAccessException)
            {
                // Skip directories we can't access
            }

            // Enumerate files
            try
            {
                foreach (var file in dirInfo.EnumerateFiles())
                {
                    fileCount++;
                    children.Add(new {name = file.Name, children=new string[0], value = 1});
                }
            }
            catch (UnauthorizedAccessException)
            {
                // Skip if we can't access files
            }

            // Build the node structure for eChart treemap
            var node = new
            {
                name = dirInfo.Name ?? Path.GetPathRoot(path),
                children = children.Count > 0 ? children : (object)null,
                value = fileCount + children.Sum(c => (int?)c.value ?? 0)
            };

            return node;
        }
        catch (Exception ex)
        {
            return new { name = path, value = 0, error = ex.Message };
        }
    }

    [Fact]
    public void process_mediabox_paths_from_file()
    {

          var files = from file in Directory.EnumerateFiles(@"D:\", "", SearchOption.AllDirectories)   
            select new
            {
                File = file,
            };

        System.IO.File.WriteAllText(@"c:\temp\_recdir.json", JsonConvert.SerializeObject(files, Formatting.Indented));
   
        const string outputFile = @"c:\temp\t-flat.txt";
     
        var lines = files.Select(f => f.File);   

        List<MbItem> processedLines = lines.Select(l => new MbItem() {Label=EvaluateMediaBoxPath(l), Path=l}).ToList();

        var sortedProcessedLines = processedLines.ToList().OrderBy(o => o.Label).ToList();
        System.IO.File.WriteAllLines(outputFile, sortedProcessedLines.Select(s => s.Label));

        Assert.True(System.IO.File.Exists(outputFile), $"Output file {outputFile} was not created");
        var catalog = files.Where(f=>f.File.Contains("\\{MB")).Select(s => new 
        {
            Label = EvaluateMediaBoxPath(s.File).Replace("\\"," • "), 
            Search = RemoveDiacritics((s.File).ToLower()), 
            Path = s.File, 
            Folder = Path.GetDirectoryName(s.File)}
        )
        .OrderBy(o => o.Label);
        
        var catalogJson = JsonConvert.SerializeObject(catalog, Formatting.Indented);

        var catalogTemplate = $@"
            var catalog = {catalogJson}
        ";
        System.IO.File.WriteAllText(@"c:\temp\Catalog\mb-catalog.js", catalogTemplate);
    }

    public string RemoveDiacritics(string text) 
    {
        var normalizedString = text.Normalize(NormalizationForm.FormD);
        var stringBuilder = new StringBuilder();

        foreach (var c in normalizedString.EnumerateRunes())
        {
            var unicodeCategory = Rune.GetUnicodeCategory(c);
            if (unicodeCategory != UnicodeCategory.NonSpacingMark)
            {
                stringBuilder.Append(c);
            }
        }

        return stringBuilder.ToString().Normalize(NormalizationForm.FormC);
    }


    [Fact]
    public void process_example_json()
    {
        const string exampleFile = @"c:\temp\t-flat-grouped.json";

        if (!System.IO.File.Exists(exampleFile))
        {
            return;
        }

        var json = System.IO.File.ReadAllText(exampleFile);
        var collections = JsonConvert.DeserializeObject<List<MbCollection>>(json)
        ;

       
        // Generate HTML output
        var htmlBuilder = new System.Text.StringBuilder();
        htmlBuilder.AppendLine("<!DOCTYPE html>");
        htmlBuilder.AppendLine("<html>");
        htmlBuilder.AppendLine("<head>");
        htmlBuilder.AppendLine("<meta charset=\"UTF-8\">");
        htmlBuilder.AppendLine("<title>MediaBox Collections</title>");
        htmlBuilder.AppendLine("<style>");
        htmlBuilder.AppendLine("body { font-family: Arial, sans-serif; margin: 20px; }");
        htmlBuilder.AppendLine("section { margin-bottom: 30px; border: 1px solid #ccc; padding: 15px; }");
        htmlBuilder.AppendLine("h2 { color: #333; border-bottom: 2px solid #0066cc; padding-bottom: 10px; cursor: pointer; user-select: none; }");
        htmlBuilder.AppendLine("h2:hover { background-color: #f0f0f0; }");
        htmlBuilder.AppendLine("h2::before { content: ' '; display: inline-block; margin-right: 5px; transition: transform 0.3s; }");
        htmlBuilder.AppendLine("h2.collapsed::before { transform: rotate(-90deg); }");
        htmlBuilder.AppendLine(".collection-content { max-height: 100000px; overflow: hidden; transition: max-height 0.3s ease, opacity 0.3s ease; opacity: 1; }");
        htmlBuilder.AppendLine(".collection-content.collapsed { max-height: 0; opacity: 0; }");
        htmlBuilder.AppendLine("ul { list-style-type: none; padding: 0; }");
        htmlBuilder.AppendLine("li { margin: 8px 0; }");
        htmlBuilder.AppendLine("a { color: #0066cc; text-decoration: none;");
        htmlBuilder.AppendLine(".item { padding-left: 32px; }");
        htmlBuilder.AppendLine(@".copy { cursor: ""pointer""; }");
        htmlBuilder.AppendLine("a:hover { text-decoration: underline; }");
        htmlBuilder.AppendLine(".path { color: #666; font-size: 0.9em; margin-left: 10px; }");
        htmlBuilder.AppendLine("</style>");
        htmlBuilder.AppendLine("</head>");
        htmlBuilder.AppendLine("<body>");
        htmlBuilder.AppendLine("<script>");
        htmlBuilder.AppendLine("window.addEventListener('DOMContentLoaded', function() {");
        htmlBuilder.AppendLine("  const headings = document.querySelectorAll('h2');");
        htmlBuilder.AppendLine("  headings.forEach(h2 => {");
        htmlBuilder.AppendLine("    h2.classList.add('collapsed');");
        htmlBuilder.AppendLine("    h2.nextElementSibling.classList.add('collapsed');");
        htmlBuilder.AppendLine("  });");
        htmlBuilder.AppendLine("});");
        htmlBuilder.AppendLine("function toggleCollection(element) {");
        htmlBuilder.AppendLine("  const h2 = element;");
        htmlBuilder.AppendLine("  const content = h2.nextElementSibling;");
        htmlBuilder.AppendLine("  h2.classList.toggle('collapsed');");
        htmlBuilder.AppendLine("  content.classList.toggle('collapsed');");
        htmlBuilder.AppendLine("}");
        //  navigator.clipboard.writeText(copyText.value);
        htmlBuilder.AppendLine("function copyToClipboard(sender, value) {");
        htmlBuilder.AppendLine(" navigator.clipboard.writeText(value); ");
        htmlBuilder.AppendLine(" console.log(sender); sender.style.backgroundColor =\"gainsboro\";");
        htmlBuilder.AppendLine(" //alert(value); ");
        htmlBuilder.AppendLine(" setTimeout(() => {sender.style.backgroundColor =\"\"} ,100); ");
        htmlBuilder.AppendLine("}");
        
        htmlBuilder.AppendLine("</script>");

        int collectionIndex = 0;
        foreach (var collection in collections)
        {
            var collectionName = collection.Collection;
            var items = collection.Items;

            htmlBuilder.AppendLine("<section>");
            htmlBuilder.AppendLine($"<h2 onclick=\"toggleCollection(this)\"> {System.Web.HttpUtility.HtmlEncode(collectionName)}</h2>");
            htmlBuilder.AppendLine("<div class=\"collection-content\">");

            htmlBuilder.AppendLine("<ul>");

            var oldItemPath = "";
           
            foreach (var item in items)
            {
                var label = item.Label;
                var path = item.Path;
                var itemPath = System.IO.Path.GetDirectoryName(path);

                if (itemPath != oldItemPath)
                {
                    htmlBuilder.AppendLine($"<div title=\"Copy to clipboard\" class=\"path\" style=\"cursor: pointer; \"  onclick=\"copyToClipboard(this, '{itemPath.Replace("\\","\\\\")}')\">📂 {System.Web.HttpUtility.HtmlEncode(itemPath)}</div>");
                    oldItemPath = itemPath;
                }

                htmlBuilder.AppendLine($"<li>");
                htmlBuilder.AppendLine($"<a  class=\"item\" href=\"{System.Web.HttpUtility.HtmlAttributeEncode(path)}\" target='_blank'>&nbsp;{System.Web.HttpUtility.HtmlEncode(label)}</a>");
                htmlBuilder.AppendLine($"</li>");
            }

            htmlBuilder.AppendLine("</ul>");
            htmlBuilder.AppendLine("</div>");
            htmlBuilder.AppendLine("</section>");
            collectionIndex++;
        }

        htmlBuilder.AppendLine("</body>");
        htmlBuilder.AppendLine("</html>");

        const string outputHtmlFile = @"c:\temp\collections-output.html";
        System.IO.File.WriteAllText(outputHtmlFile, htmlBuilder.ToString());

        Assert.True(System.IO.File.Exists(outputHtmlFile), $"Output file {outputHtmlFile} was not created");
    }




    private string EvaluateMediaBoxPath(string input)
    {
        // Extract the skip count from MBVLabel@X (if present) - case insensitive
        var labelMatch = System.Text.RegularExpressions.Regex.Match(input, @"{MBVLabel=([^@}]*)@?(\d*)}(?i)", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        int skipCount = labelMatch.Success && !string.IsNullOrEmpty(labelMatch.Groups[2].Value) 
            ? int.Parse(labelMatch.Groups[2].Value) 
            : 0;
        
        // Build dynamic regex to skip the required number of folders
        var skipPattern = skipCount > 0 
            ? @"\\(?:[^\\]+\\){" + skipCount + @"}" 
            : @"\\";
        
        var result = System.Text.RegularExpressions.Regex.Replace(
            input, 
            @"^d:\\{MBVLabel=[^}]*}" + skipPattern, 
            @"\",
            System.Text.RegularExpressions.RegexOptions.IgnoreCase);
       
        if (result.StartsWith("\\") && result.Length > 1)
        {
             return result.Substring(1);
        }
        return result;
    }
}

internal class MbCollection
{
    public string Collection { get; set; }
    public IEnumerable<MbItem> Items { get; set; }
}

internal class MbItem
{
    public MbItem()
    {
    }

    public string Label { get; set; }
    public string Path { get; set; }
}