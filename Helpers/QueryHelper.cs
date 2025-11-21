using Nest;

namespace name_match.Helpers
{
    public static class QueryHelper
    {
        public static Func<QueryContainerDescriptor<Record>, QueryContainer> MatchNameSelectorFuzzy(string toFind)
        {
            return b => b.Match(m => m.Field(f => f.AllNames.Suffix("pretoken_fuzzy")).Query(toFind).Name("Pretoken loose").Operator(Operator.Or)
                .Fuzziness(Fuzziness.EditDistance(1)).FuzzyTranspositions(false).MinimumShouldMatch("2<50% 5<40% 6<25%"));
        }

        public static Func<QueryContainerDescriptor<Record>, QueryContainer> MatchNameSelector(string toFind)
        {
            return b => b.Match(m => m.Field(f => f.AllNames.Suffix("pretoken")).Query(toFind).Name("Pretoken loose").Operator(Operator.Or)
                .MinimumShouldMatch("2<50% 5<40% 6<25%"));
        }

        public static Func<QueryContainerDescriptor<Record>, QueryContainer> MatchNameFieldExact(string toFind)
        {
            return b => b.Term(t => t.Field(f => f.Name.Suffix("keyword")).Value(toFind));
        }

        public static Func<QueryContainerDescriptor<Record>, QueryContainer> MatchAllNamesOrdered(string toFind)
        {
            return b => b.Intervals(iv => iv
                    .Field(f => f.AllNames.Suffix("pretoken"))
                    .Match(mm => mm
                            .Query(toFind)
                            .MaxGaps(0)
                            .Ordered(true)
                            .Analyzer("allnames_pre_token_analyzer")
                    )
                    .Name("allNames_intervals_ordered")
                    .Boost(50)
            );
        }

        public static Func<QueryContainerDescriptor<Record>, QueryContainer> MatchNameSelectorCombination(string toFind)
        {
            return mu => mu.Bool(
                    b => b.Should(
                       QueryHelper.MatchNameFieldExact(toFind)
                      ,QueryHelper.MatchNameSelectorFuzzy(toFind)
                      ,QueryHelper.MatchNameSelector(toFind)
                    )
                    .Name("Pretoken loose")
            );
        }

       
        /// <summary>
        /// Analyze the provided text using the specified analyzer on the given index and return the tokens.
        /// </summary>
        public static IEnumerable<string> GetAnalyzedTokens(ElasticClient client, string indexName, string analyzerName, string text)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (string.IsNullOrWhiteSpace(analyzerName)) throw new ArgumentException("analyzerName is required", nameof(analyzerName));

            var resp = client.Indices.Analyze(a => a.Index(indexName).Analyzer(analyzerName).Text(text));
            if (resp == null || !resp.IsValid || resp.Tokens == null) return Enumerable.Empty<string>();

            return resp.Tokens
                .Select(t => t.Token)
                .Where(t => !string.IsNullOrWhiteSpace(t))
                .Select(t => t.ToLowerInvariant())
                .ToList();
        }
    }
}
