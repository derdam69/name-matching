namespace name_match.Tests.Search;
using Nest;
using Helpers;

public class MatchNameSelectorFuzzyTest
{
    const string indexName = $"wc-v2-match-name-selector-fuzzy-test";

    private static void Pause()
    {
        Thread.Sleep(1000);
    }

    static bool samplesLoaded = false;
    public MatchNameSelectorFuzzyTest()
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
            .Highlight(h => h.Fields(f => f.Field("*")))
        );
        Assert.True(searchResponse.Hits.First().Id == mustBeTopMatch);
    }

    [Theory]
    [InlineData("PARTNERS 2000 Sarl", "2000")]
    public void MatchNameSelectorFuzzy_must_match_numbers(string toFind, string mustHighlight)
    {
        ElasticClient client = TestHelpers.ClientFactory(indexName);
        var searchResponse = client.Search<Record>(s => s
        .Query(
            QueryHelper.MatchNameSelectorFuzzy(toFind)
            )
            .Highlight(h =>
            h.Fields(f => f.Field("*")))
        );
        TestHelpers.AssertTokenIsHighlighted(searchResponse.Hits, mustHighlight);
    }

    [Theory]
    [InlineData("PARTNERS 2000 Sarl", "2001")]
    public void MatchNameSelectorFuzzy_must_not_mismatch_numbers(string toFind, string mustNotHighlight)
    {
        ElasticClient client = TestHelpers.ClientFactory(indexName);
        var searchResponse = client.Search<Record>(s => s
        .Query(
            QueryHelper.MatchNameSelectorFuzzy(toFind)
            )
            .Highlight(h =>
            h.Fields(f => f.Field("*")))
        );
       TestHelpers.AssertHasHitsAndTokenIsNotHighlighted(searchResponse.Hits, mustNotHighlight);
    }

    [Theory]
    [InlineData("HERNNANDEZ F", "F")]
    [InlineData("HERNNANDEZ F", "A")]
    public void MatchNameSelectorFuzzy_must_not_match_single_letter(string toFind, string mustNotHighlight)
    {
        ElasticClient client = TestHelpers.ClientFactory(indexName);
        var searchResponse = client.Search<Record>(s => s
        .Query(
            QueryHelper.MatchNameSelectorFuzzy(toFind)
            )
            .Highlight(h =>
            h.Fields(f => f.Field("*")))
        );
        TestHelpers.AssertHasHitsAndTokenIsNotHighlighted(searchResponse.Hits, mustNotHighlight);
    }

    [Theory]
    [InlineData("DIRBES", "SD-37")] // fuzzy
    [InlineData("DIRBES", "SD-36")] // fuzzy
    [InlineData("DIRBES", "SD-35")] // fuzzy
    public void MatchNameSelectorFuzzy_must_match_fuzzy(string toFind, string mustContainMatch)
    {
        ElasticClient client = TestHelpers.ClientFactory(indexName);
        var searchResponse = client.Search<Record>(s => s
        .Query(
            QueryHelper.MatchNameSelectorFuzzy(toFind)
            )
            .Highlight(h =>
            h.Fields(f => f.Field("*")))
        );
        Assert.Contains(searchResponse.Hits, m => m.Id == mustContainMatch);
    }

    [Theory]
    [InlineData("PEREZ DA HERNANDEZ", "DA")]
    [InlineData("PEREZ DA HERNANDEZ", "DE")]
    [InlineData("PEREZ DE HERNANDEZ", "DA")]
    [InlineData("PEREZ DE HERNANDEZ", "DE")]
    [InlineData("PEREZ DOS HERNANDEZ", "DOS")]
    public void MatchNameSelectorFuzzy_must_not_match_name_particle(string toFind, string mustNotHighlight)
    {
        ElasticClient client = TestHelpers.ClientFactory(indexName);
        var searchResponse = client.Search<Record>(s => s
        .Query(
            QueryHelper.MatchNameSelectorFuzzy(toFind)
            )
            .Highlight(h =>
            h.Fields(f => f.Field("*")))
        );
        TestHelpers.AssertHasHitsAndTokenIsNotHighlighted(searchResponse.Hits, mustNotHighlight);
    }

    [Theory]
    [InlineData("NATIONAL SA", "SA")]
    [InlineData("NATIONAL SA", "SI")]
    [InlineData("GROUP ASSET-MANAGEMENT S.A.", "S.A.")]
    [InlineData("GROUP ASSET-MANAGEMENT S.A.", "SA")]
    [InlineData("GROUP ASSET-MANAGEMENT SA", "SA")]
    [InlineData("GROUP ASSET-MANAGEMENT SA", "S.A.")]
    public void MatchNameSelectorFuzzy_must_not_match_company_stop_word(string toFind, string mustNotHighlight)
    {
        ElasticClient client = TestHelpers.ClientFactory(indexName);
        var searchResponse = client.Search<Record>(s => s
        .Query(
            QueryHelper.MatchNameSelectorFuzzy(toFind)
            )
            .Highlight(h =>
            h.Fields(f => f.Field("*")))
        );
        TestHelpers.AssertHasHitsAndTokenIsNotHighlighted(searchResponse.Hits, mustNotHighlight);
    }
}

