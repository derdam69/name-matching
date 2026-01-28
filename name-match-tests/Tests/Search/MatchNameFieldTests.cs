namespace name_match.Tests.Search;
using Nest;
using Helpers;

public class MatchNamesFieldTests
{
    const string indexName = $"wc-v2-match-name-field-test";

    private static void Pause()
    {
        Thread.Sleep(1000);
    }

    static bool samplesLoaded = false;
    public MatchNamesFieldTests()
    { 
        if (samplesLoaded)
        {
            return;
        }
        IndexHelpers.CreateIndex(indexName);
        var client = TestHelpers.ClientFactory(indexName);
        TestHelpers.IndexSampleData(client);
        Pause();
        samplesLoaded = true;
    }

    [Theory]
    [InlineData("ALPHA", "SD-1")]
    [InlineData("ALPHA BRAVO", "SD-2")]
    public void MatchNameFieldExact_must_match_exact_first_hit(string toFind, string mustContainMatch)
    {
        ElasticClient client = TestHelpers.ClientFactory(indexName);
        var searchResponse = client.Search<Record>(s => s
           .Query(
                QueryHelper.MatchNameFieldExact(toFind)
            )
            .Highlight(h =>
              h.Fields(f => f.Field("*")))
        );
        Assert.Contains(searchResponse.Hits, m => m.Id == mustContainMatch);   
        Assert.Equal(1, searchResponse.Total);
    }

    
    [Theory]
    [InlineData("alpha", "SD-1")]
    [InlineData("ALPHA bravo", "SD-2")]
    public void MatchNameFieldExact_must_match_case_insensitive_exact_first_hit(string toFind, string mustContainMatch)
    {
        ElasticClient client = TestHelpers.ClientFactory(indexName);
        var searchResponse = client.Search<Record>(s => s
           .Query(
                QueryHelper.MatchNameFieldExact(toFind)
            )
            .Highlight(h =>
              h.Fields(f => f.Field("*")))
        );
        Assert.Contains(searchResponse.Hits, m => m.Id == mustContainMatch);   
        Assert.Equal(1, searchResponse.Total);
    }

    [Theory]
    [InlineData("LOREM ALPHA", "SD-1")]
    [InlineData("ALPHA BRAVO", "SD-1")]
    [InlineData("LOREM ALPHA BRAVO", "SD-2")]
    public void MatchNameFieldExact_must_not_match_exact(string toFind, string mustNotContainMatch)
    {
        ElasticClient client = TestHelpers.ClientFactory(indexName);
        var searchResponse = client.Search<Record>(s => s
           .Query(
                QueryHelper.MatchNameFieldExact(toFind)
            )
            .Highlight(h =>
              h.Fields(f => f.Field("*")))
        );
        Assert.DoesNotContain(searchResponse.Hits, m => m.Id == mustNotContainMatch);   
    }
}

