namespace name_match.Tests.IndexSettings;
using Nest;
using Newtonsoft.Json;
using Helpers;

public class AnalyzerTests
{
    const string indexName = $"wc-v2-index-settings-test";

    static bool indexCreated = false;
    public AnalyzerTests()
    {
        if (indexCreated)
        {
            return;
        }
        IndexHelpers.CreateIndex(indexName);
        indexCreated = true;
    }

    [Fact]
    public void It_can_dump_index_analysis()
    {
        ElasticClient client = TestHelpers.ClientFactory(indexName);

        var analyzed = client.Indices.Analyze(a => a
             .Index(indexName)
             .Text("Usrsula VAN DER LAYEN")
             .Analyzer("allnames_pre_token_analyzer_fuzzy")
         );

        File.WriteAllText(@"c:\temp\_analyzed_dump.json", JsonConvert.SerializeObject(analyzed.Tokens, Formatting.Indented));
    }

    [Theory]
    [InlineData("lorem", "lorem")]
    [InlineData("Ursula VAN DER LAYEN", "ursula layen")]
    [InlineData("DA SILVA", "silva")]
    public void allnames_pre_token_analyzer_fuzzy_Must_filter_name_particles(string input, string expectedTokens)
    {
        ElasticClient client = TestHelpers.ClientFactory(indexName);

        var analyzed = client.Indices.Analyze(a => a
             .Index(indexName)
             .Text(input)
             .Analyzer("allnames_pre_token_analyzer_fuzzy")
         );

        var tokenList = string.Join(" ", analyzed.Tokens.Select(t => t.Token));
        Assert.Equal(expectedTokens, tokenList);
    }

    [Theory]
    [InlineData("Online Limited", "online")]
    [InlineData("Online ltd.", "online")]
    [InlineData("Online s.a.", "online")]
    [InlineData("Online sa", "online")]
    [InlineData("Online inc", "online")]
    public void allnames_pre_token_analyzer_fuzzy_Must_filter_legal_suffix_stops(string input, string expectedTokens)
    {
        ElasticClient client = TestHelpers.ClientFactory(indexName);

        var analyzed = client.Indices.Analyze(a => a
             .Index(indexName)
             .Text(input)
             .Analyzer("allnames_pre_token_analyzer_fuzzy")
         );

        var tokenList = string.Join(" ", analyzed.Tokens.Select(t => t.Token));
        Assert.Equal(expectedTokens, tokenList);
    }

    [Theory]
    [InlineData("a", "")]
    [InlineData("ab", "ab")]
    [InlineData("abc", "abc")]
    [InlineData("1", "")]
    [InlineData("12", "12")]
    [InlineData("123", "123")]
    public void allnames_pre_token_analyzer_fuzzy_Must_filter_tokens_with_min_length_2(string input, string expectedTokens)
    {
        ElasticClient client = TestHelpers.ClientFactory(indexName);

        var analyzed = client.Indices.Analyze(a => a
             .Index(indexName)
             .Text(input)
             .Analyzer("allnames_pre_token_analyzer_fuzzy")
         );

        var tokenList = string.Join(" ", analyzed.Tokens.Select(t => t.Token));
        Assert.Equal(expectedTokens, tokenList);
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

    [Theory]
    [InlineData("A.B.C A/C Lorem 43", "abc ac lorem 43")]
    [InlineData("O'Hara", "ohara")]
    [InlineData("O' Timmins", "timmins")]
    public void allnames_pre_token_analyzer_fuzzy_Test(string input, string expected)
    {
        ElasticClient client = TestHelpers.ClientFactory(indexName);

        var analyzed = client.Indices.Analyze(a => a
             .Index(indexName)
             .Text(input)
             .Analyzer("allnames_pre_token_analyzer_fuzzy")
         );

       var tokenList = string.Join(" ", analyzed.Tokens.Select(t => t.Token));
       Assert.Equal(expected, tokenList);
    }

    [Theory]
    [InlineData("A.B.C A/C Lorem 43", "abc ac lorem 43")]
    [InlineData("O'Hara", "ohara")]
    [InlineData("O' Timmins", "o timmins")]
    public void allnames_pre_token_analyzer_Test(string input, string expected)
    {
        ElasticClient client = TestHelpers.ClientFactory(indexName);

        var analyzed = client.Indices.Analyze(a => a
             .Index(indexName)
             .Text(input)
             .Analyzer("allnames_pre_token_analyzer")
         );

       var tokenList = string.Join(" ", analyzed.Tokens.Select(t => t.Token));
       Assert.Equal(expected, tokenList);
    }
}
