namespace name_match.Tests.Search;
using Nest;
using Newtonsoft.Json;
using Helpers;
using System.Collections.Generic;
using System;
using System.Reflection.Metadata;

public class HighlightTests
{
    const string indexName = $"wc-v2-match-highlight-test";

    private static void Pause()
    {
        Thread.Sleep(1000);
    }

    static bool samplesLoaded = false;
    public HighlightTests()
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
    [InlineData("PERES DE HERNANDEZ", "")] // fuzzy 
    public void Consolidated_highlights_test(string toFind, string mustHighlight)
    {
        ElasticClient client = TestHelpers.ClientFactory(indexName);
        var searchResponse = client.Search<Record>(s => s
           .Query(q => q
                    .Bool(b => b
                      .Should(
                        QueryHelper.MatchNameSelectorCombination(toFind)
                      )
                      .Should(sh => sh
                        .Match(m => m.Field(f => f.AllNamesFuzzy).Query(toFind).Name("AllNamesFuzzy").Operator(Operator.Or)
                .Fuzziness(Fuzziness.EditDistance(1)).FuzzyTranspositions(false).MinimumShouldMatch("2<50% 5<40% 6<25%"))
  
                      )
                    )
                    
            )
           .Highlight(h => 
               h.Fields(f => f
                .Field(n=>n.AllNames)
                .Field(n=>n.AllNamesFuzzy)
               )
           )
        );

        var report = new
        {
            Search = toFind,
            Count = searchResponse.Hits.Count(),
            searchResponse.MaxScore,
            
            Hits = searchResponse.Hits.Select(h => new { h.Id, Highlight = h.Highlight, h.MatchedQueries, h.Score })
        };

        File.WriteAllText(@"c:\temp\_search_highlights.json", JsonConvert.SerializeObject(report, Formatting.Indented));
    }

    private string  ExtractHighlights(IReadOnlyDictionary<string, IReadOnlyCollection<string>> highlight)
    {
        var hlColl = new List<string>();
        foreach(var k in highlight)
        {
            hlColl.Add(k.Value.First());
        }

        if (hlColl.Count == 1)
        {
            return hlColl.First();
        }

        var hlTokens = new List<string[]>();
         foreach(var k in highlight)
        {
            hlTokens.Add(k.Value.First().Split(" "));
        }

        var target = hlTokens.First();
        int tokenIndex = 0;
        foreach(var token in target)
        {
            if (!token.IsHighlighted())
            {
                foreach(var hlRow in hlTokens)
                {
                    if (hlRow[tokenIndex].IsHighlighted())
                    {
                       target[tokenIndex] = hlRow[tokenIndex];         
                    }
                }
            } 
            tokenIndex++;
        }

        return string.Join(" ", target);
    }
}

