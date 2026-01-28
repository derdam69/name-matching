namespace name_match.Tests.Search;
using Nest;
using Helpers;

public class MatchNameSelectorTest
{
    const string indexName = $"wc-v2-match-name-selector-test";

    private static void Pause()
    {
        Thread.Sleep(1000);
    }

    static bool samplesLoaded = false;
    public MatchNameSelectorTest()
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
    [InlineData("HERNNANDEZ A", "SD-11")]
    public void MatchNameSelector_must_match_single_letter(string toFind, string mustBeTopMatch)
    {
        ElasticClient client = TestHelpers.ClientFactory(indexName);
        var searchResponse = client.Search<Record>(s => s
        .Query(
            QueryHelper.MatchNameSelector(toFind)
            )
            .Highlight(h =>
            h.Fields(f => f.Field("*")))
        );
        Assert.True(searchResponse.Hits.First().Id == mustBeTopMatch);
    }

    [Theory]
    [InlineData("PARTNERS 2000 Sarl", "2000")]
    public void MatchNameSelector_must_match_numbers(string toFind, string mmustHighlight)
    {
        ElasticClient client = TestHelpers.ClientFactory(indexName);
        var searchResponse = client.Search<Record>(s => s
        .Query(
            QueryHelper.MatchNameSelector(toFind)
            )
            .Highlight(h =>
            h.Fields(f => f.Field("*")))
        );

        Assert.True(searchResponse.Hits.Any());

        Assert.True(searchResponse.Hits.Any(hit =>
        hit.Highlight != null &&
        hit.Highlight.First().Value.Any(h => h.Contains($"<em>{mmustHighlight}</em>"))),
        $"Hit not found with highlighted '{mmustHighlight}' in allNames.pretoken");
    }


    [Theory]
    [InlineData("DORBES", "SD-37")]
    [InlineData("DERBES Damien", "SD-36")] 
    [InlineData("VAN DERBES Damien", "SD-35")] 
    public void MatchNameSelector_must_match(string toFind, string mustContainMatch)
    {
        ElasticClient client = TestHelpers.ClientFactory(indexName);
        var searchResponse = client.Search<Record>(s => s
        .Query(
            QueryHelper.MatchNameSelector(toFind)
            )
            .Highlight(h =>
            h.Fields(f => f.Field("*")))
        );
        Assert.Contains(searchResponse.Hits, m => m.Id == mustContainMatch);
    }

    [Theory]
    [InlineData("PEREZ DA HERNANDEZ", "DA")]
    [InlineData("PEREZ DE HERNANDEZ", "DE")]
    [InlineData("PEREZ DOS HERNANDEZ", "DOS")]
    public void MatchNameSelector_must_match_name_particle(string toFind, string mustHighlight)
    {
        ElasticClient client = TestHelpers.ClientFactory(indexName);
        var searchResponse = client.Search<Record>(s => s
        .Query(
            QueryHelper.MatchNameSelector(toFind)
            )
            .Highlight(h =>
            h.Fields(f => f.Field("*")))
        );

        Assert.True(searchResponse.Hits.Any());

        Assert.True(searchResponse.Hits.Any(hit =>
        hit.Highlight != null &&
        hit.Highlight.First().Value.Any(h => h.Contains($"<em>{mustHighlight}</em>"))),
        $"Hit not found with highlighted '{mustHighlight}' in allNames.pretoken.");
    }

    [Theory]
    [InlineData("NATIONAL SA", "SA")]
    [InlineData("GROUP ASSET-MANAGEMENT S.A.", "S.A.")]
    public void MatchNameSelector_must_match_company_stop_word(string toFind, string mustHighlight)
    {
        ElasticClient client = TestHelpers.ClientFactory(indexName);
        var searchResponse = client.Search<Record>(s => s
        .Query(
            QueryHelper.MatchNameSelector(toFind)
            )
            .Highlight(h =>
            h.Fields(f => f.Field("*")))
        );

        Assert.True(searchResponse.Hits.Any());

        Assert.True(searchResponse.Hits.Any(hit =>
        hit.Highlight != null &&
        hit.Highlight.First().Value.Any(h => h.Contains($"<em>{mustHighlight}</em>"))),
        $"Hit not found with highlighted '{mustHighlight}' in '{toFind}' in allNames.pretoken");
    }
}

