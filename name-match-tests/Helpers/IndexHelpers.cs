using Nest;

namespace name_match.Helpers
{
    public static class IndexHelpers
    {
        private static ElasticClient? _client;

       

        public static void CreateIndex(string indexName)
        {
            var client = TestHelpers.ClientFactory(indexName);

            if (client.Indices.Exists(indexName).Exists)
            {
                var delResp = client.Indices.Delete(indexName);
                if (!delResp.Acknowledged)
                    throw new Exception("Index delete failed: " + delResp.ServerError?.Error?.Reason);
            }

            if (!client.Indices.Exists(indexName).Exists)
            {
                var createIndexResponse = client.Indices.Create(indexName, c => c
                    .Settings(s => s
                        .Analysis(a => a
                            .Normalizers(n => n
                                .Custom("lowercase_asciifolding_normalizer", c => c
                                    .Filters("lowercase", "asciifolding")
                                )
                            )
                            .TokenFilters(tf => tf
                                .PatternReplace("remove_non_alphanum", pr => pr
                                    .Pattern("[^\\p{L}\\p{Nd}]+")
                                    .Replacement("")
                                )
                                .Stop("legal_suffix_stop", s2 => s2
                                    .StopWords("ltd", "limited", "gmbh", "sarl", "sa", "inc", "corp", "corporation", "llc")
                                )
                                .Stop("person_name_prefix", s2 => s2
                                    .StopWords("dos", "da", "de", "le", "la", "al", "el", "bin", "ben", "van", "von", "der")
                                )
                                .Length("min2Tokens", f => f.Min(2))
                            )
                            .CharFilters(cf => cf
                                .PatternReplace("pre_token_remove_non_alphanum_keep_space", pr => pr
                                    .Pattern("[^\\p{L}\\p{Nd} ]+")
                                    .Replacement("")
                                )
                            )
                            .Analyzers(an => an
                                .Custom("allnames_analyzer", ca => ca
                                    .Tokenizer("standard")
                                    .Filters("lowercase", "remove_non_alphanum")
                                )
                                .Custom("allnames_pre_token_analyzer", ca => ca
                                    .CharFilters("pre_token_remove_non_alphanum_keep_space")
                                    .Tokenizer("standard")
                                    .Filters("lowercase", "asciifolding")
                                )
                                .Custom("allnames_no_suffix_analyzer", ca => ca
                                    .Tokenizer("standard")
                                    .Filters("lowercase", "remove_non_alphanum", "legal_suffix_stop")
                                )
                                .Custom("allnames_pre_token_analyzer_fuzzy", ca => ca
                                    .CharFilters("pre_token_remove_non_alphanum_keep_space")
                                    .Tokenizer("standard")
                                    .Filters("min2Tokens","lowercase", "asciifolding", "person_name_prefix", "legal_suffix_stop")
                                )
                            )
                        )
                        .Setting("index.similarity.no_tf.type", "BM25")
                        .Setting("index.similarity.no_tf.k1", "0")
                        .Setting("index.similarity.no_tf.b", "0")
                    )
                    .Map<Record>(m => m
                        .AutoMap()
                        .Properties(ps => ps
                            .Text(t => t
                                .Name(n => n.Name)
                                .Similarity("no_tf")
                                .TermVector(TermVectorOption.WithPositionsOffsets)
                                .Fields(f => f
                                        .Keyword(k => k
                                        .Name("keyword")
                                        .Normalizer("lowercase_asciifolding_normalizer")
                                        .Similarity("no_tf")
                                    )
                                )
                            )
                            .Text(t => t
                                .Name(n => n.AllNames)
                                .Analyzer("allnames_analyzer")
                                .Similarity("no_tf")
                                .TermVector(TermVectorOption.WithPositionsOffsets)
                                .Fields(f => f
                                    .Text(tt => tt
                                        .Name("pretoken")
                                        .Analyzer("allnames_pre_token_analyzer")
                                        .Similarity("no_tf")
                                        .TermVector(TermVectorOption.WithPositionsOffsets)
                                    )
                                    .Text(tt => tt
                                        .Name("no_suffix")
                                        .Analyzer("allnames_no_suffix_analyzer")
                                        .Similarity("no_tf")
                                        .TermVector(TermVectorOption.WithPositionsOffsets)
                                    )
                                    .Text(tt => tt
                                        .Name("pretoken_fuzzy")
                                        .Analyzer("allnames_pre_token_analyzer_fuzzy")
                                        .Similarity("no_tf")
                                        .TermVector(TermVectorOption.WithPositionsOffsets)
                                    )
                                )
                            )
                            .Text(t => t
                                .Name(n => n.AllNamesFuzzy)
                                
                                .Analyzer("allnames_pre_token_analyzer_fuzzy")
                                        .Similarity("no_tf")

                                .TermVector(TermVectorOption.WithPositionsOffsets)
                               
                            )
                           
                        )
                    )
                );

                if (!createIndexResponse.IsValid)
                    throw new Exception("Index creation failed: " + createIndexResponse.ServerError?.Error?.Reason);
            }
        }

    }
}
