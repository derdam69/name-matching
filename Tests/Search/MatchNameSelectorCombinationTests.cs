namespace name_match.Tests.Search;
using Nest;
using Newtonsoft.Json;
using Helpers;

public class MatchNameSelectorCombinationTests
{
    const string indexName = $"wc-v2-match-name-selector-combination-test";

    private static void Pause()
    {
        Thread.Sleep(1000);
    }

    static bool samplesLoaded = false;
    public MatchNameSelectorCombinationTests()
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

    private static int totalHits = 0;
    private static int testRun = 0;

    [Theory]
    [InlineData("Carlos HERNANDEZ")]
    [InlineData("Mr. Carlas HERNANDEZ 12 Rue des Tests 1304 Lausanne Switzerland")] // Fuzzy
    [InlineData("Mr. Carlos HERNANDEZ")]
    [InlineData("Carlos HERNANDEZ 12 Rue des Tests 1304 Lausanne Switzerland")]
    [InlineData("Carlos et Sofia HERNANDEZ PEREZ")]
    [InlineData("abc ... eenergy s.o.s.---- Hernandez s.a. common-enterprises! ")]
    [InlineData("MARTIN Jean-Pierre")]
    [InlineData("HERNANDEZ Karlos Lorem Ipsum")] // Fuzzy
    [InlineData("H.ERNNANDEZ S.A. e-energy")]
    [InlineData("1/MAGIC APPEX CAPITAL LIMITED")]
    [InlineData("FOO PILOT SERVICES BAR")]
    [InlineData("HERNANDEZ Carlis")] // Fuzzy
    [InlineData("PEREZ DIS HERNANDEZ")]
    [InlineData("HERNANDES")] // Fuzzy
    [InlineData("CONSULTING CAPITAL MANAGEMENT PLC")]
    [InlineData("CONSULTING CAPITAL PLC")]
    [InlineData("CAPITAL PLC")]
    [InlineData("abc ... eenergy s.o.s. ---- Hernandez s.a. common-enterprises! ")]
    [InlineData("PEREZ DE HERNANDEZ Sofia")]
    [InlineData("alpha foo damien derbes bar baz")]
    [InlineData("alpha")]
    [InlineData("HERNANDEZ")]
    [InlineData("dorbes")]
    [InlineData("ALPHA")]
    // PARTNERS 2000 Sarl
    [InlineData("PARTNERS 2000 Sarl")]

    public void MatchNameSelectorCombination_with_html_report(string toFind)
    {
        ElasticClient client = TestHelpers.ClientFactory(indexName);

        var searchResponse = client.Search<Record>(s => s
           .Query(q => q
                .Bool(b => b
                    .Must(
                        QueryHelper.MatchNameSelectorCombination(toFind)

                    )//.Name("Pretoken loose")
                    .Should(sh =>
                        sh.Bool(b =>
                            b.Should(sh => null
                                // sh.Match(m => m.Field(f => f.Domicile).Query("kkkxxx").Boost(0))
                                // ,QueryHelper.MatchAllNamesOrdered(toFind)
                            )
                        )
                    )
                )
           )
           .Highlight(h =>
              h.Fields(f => f.Field("*"))
           )
        );

        var finalHits = searchResponse.Hits;
        var filteredHits = finalHits;

        // TODO: When any hit has at least a MatchedQuery other than 'AllNamesLoose', 
        // remove hits that have only 'AllNamesLoose' matched.

        var hasStrongMatch = filteredHits.Any(hit => hit.MatchedQueries != null && hit.MatchedQueries.Where(w => !w.Contains("Domicile")).Any(mq => mq != "Pretoken loose"));
        if (hasStrongMatch)
        {
            filteredHits = filteredHits.Where(hit => !(hit.MatchedQueries.Count() == 1 && hit.MatchedQueries.Contains("Pretoken loose"))).ToList();
        }

        var hitsWithScores = filteredHits
            .Select(h => new { Hit = h, Score = (double)(h.Score ?? 0f) })
            .OrderByDescending(x => x.Score)
            .ToList();

        // Compute basic stats
        var scores = hitsWithScores.Select(x => x.Score).ToList();
        var mean = scores.Any() ? scores.Average() : 0.0;
        var stdDev = scores.Any() ? Math.Sqrt(scores.Sum(score => Math.Pow(score - mean, 2)) / scores.Count) : 0.0;

        // Log scores and z-scores (stdDev may be 0 -> yields +/-Infinity)
        foreach (var x in hitsWithScores)
        {
            var z = stdDev > 0 ? (x.Score - mean) / stdDev : double.PositiveInfinity;
            File.AppendAllText(@"c:\temp\_search_stat.txt", $"Score: {x.Score}  Z-score: {z}\n");
        }

        // Hybrid cutoff strategy:
        // 1) Try to find the largest adjacent drop in the descending scores. If that drop
        //    is >= max(minAbsDrop, minRelDrop * maxScore) we treat the score before the drop
        //    as the cutoff.
        // 2) Otherwise fall back to mean + k * stdDev (z-based). If stdDev == 0 use a top-N fallback.
        double cutoff = double.NegativeInfinity;
        if (hitsWithScores.Count > 0)
        {
            double maxScore = hitsWithScores[0].Score;
            // Tunable parameters
            double minRelDrop = 0.10; // require at least 10% of max score as relative drop
            double minAbsDrop = 0.05; // or an absolute drop of 0.05

            double maxDrop = 0.0;
            int maxDropIndex = -1;
            for (int i = 0; i < hitsWithScores.Count - 1; i++)
            {
                double drop = hitsWithScores[i].Score - hitsWithScores[i + 1].Score;
                if (drop > maxDrop)
                {
                    maxDrop = drop;
                    maxDropIndex = i;
                }
            }

            if (maxDropIndex >= 0 && maxDrop >= Math.Max(minAbsDrop, minRelDrop * maxScore))
            {
                // Natural gap found
                cutoff = hitsWithScores[maxDropIndex].Score;
            }
            else
            {
                // Fallback: z-score / mean + k*stdDev
                double k = 0.5; // keep hits >= mean + 0.5 * stdDev
                if (stdDev > 0)
                {
                    cutoff = mean + k * stdDev;
                }
                else
                {
                    // All scores identical (stdDev == 0): keep the top N (configurable)
                    //   //int topN = Math.Min(3, hitsWithScores.Count);
                    //cutoff = hitsWithScores[topN - 1].Score;
                    cutoff = 0;
                }
            }

            // safety: never set cutoff above maxScore
            if (cutoff > maxScore) cutoff = maxScore;
        }

        // Apply cutoff: keep only hits with score >= cutoff
        // var keptHitsWithScores = hitsWithScores.Where(x => x.Score >= cutoff).ToList();
        // filteredHits = keptHitsWithScores.Select(x => x.Hit).ToList();

        var report = new
        {
            Search = toFind,
            Count = filteredHits.Count(),
            searchResponse.MaxScore,
            StdDev = stdDev,
            Mean = mean,
            Cutoff = cutoff,
            Hits = filteredHits.Select(h => new { h.Id, h.Highlight, h.MatchedQueries, h.Score, Keep = h.Score >= cutoff })
        };


        var formattedReport = TestHelpers.FormatHtml(JsonConvert.SerializeObject(report, Formatting.Indented));
        File.WriteAllText(@$"c:\temp\name-match\search-{testRun}.html", formattedReport);

        totalHits += filteredHits.Count();

        File.WriteAllText(@"c:\temp\_search_hits.json", JsonConvert.SerializeObject(totalHits, Formatting.Indented));

        Assert.True(searchResponse.Hits.Any(), "Search returned no hits");

        testRun += 1;
    }

    [Theory]
    [InlineData("VAN DORBES", "DERBES")] // fuzzy 
    [InlineData("VAN DARBES", "VAN")] // fuzzy 
    public void MatchNameSelectorCombination_must_match_fuzzy_name_with_particle(string toFind, string mustHighlight)
    {
        ElasticClient client = TestHelpers.ClientFactory(indexName);
        var searchResponse = client.Search<Record>(s => s
           .Query(q => q
                    .Bool(b => b
                      .Must(
                        QueryHelper.MatchNameSelectorCombination(toFind)
                      )
                    )
            )
            .Highlight(h => h.Fields(f => f.Field("*")))
        );

        TestHelpers.AssertTokenIsHighlighted(searchResponse.Hits, mustHighlight);
    }

    [Theory]
    [InlineData("ALPHA", "SD-1")]
    public void MatchNameSelectorCombination_must_match_single_name_first(string toFind, string mustBeFirstMatchId)
    {
        ElasticClient client = TestHelpers.ClientFactory(indexName);
        var searchResponse = client.Search<Record>(s => s
           .Query(
                QueryHelper.MatchNameSelectorCombination(toFind)
            )
       ); 

       Assert.Equal(mustBeFirstMatchId, searchResponse.Hits.First().Id);
    }
}

