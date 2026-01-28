namespace name_match.Tests.IndexSettings;
using Nest;
using Newtonsoft.Json;
using Helpers;

public class NormalizerTests
{
    const string indexName = $"wc-v2-index-settings-normalizer-test";

    static bool indexCreated = false;
    public NormalizerTests()
    {
        if (indexCreated)
        {
            return;
        }
        IndexHelpers.CreateIndex(indexName);
        indexCreated = true;
    }


    [Theory]
    [InlineData("LOREM", "lorem")]
    [InlineData("François", "francois")]
    [InlineData("ALPHÀ BRAVO", "alpha bravo")]
    [InlineData("LOREM ALPHA BRAVO", "lorem alpha bravo")]
    public void lowercase_asciifolding_normalizer_Test(string input, string expected)
    {
         ElasticClient client = TestHelpers.ClientFactory(indexName);

        var analyzed = client.Indices.Analyze(a => a
             .Index(indexName)
             .Text(input)
             .Normalizer("lowercase_asciifolding_normalizer")
         );

       var tokenList = string.Join(" ", analyzed.Tokens.Select(t => t.Token));
       Assert.Equal(expected, tokenList);
    }

}
