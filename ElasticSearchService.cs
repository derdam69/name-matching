using Nest;

namespace ElasticsearchIntegrationTests
{
    public class ElasticsearchService
    {
        private readonly ElasticClient _client;

        public static string INDEX_NATURAL_PERSON = "wc_np";
        public static string INDEX_LEGAL_ENTITY = "wc_le";

        public ElasticsearchService(Uri elasticsearchUri, bool deleteIndex)
        {
            var settings = new ConnectionSettings(elasticsearchUri)
                .DisableDirectStreaming();

            _client = new ElasticClient(settings);

            if (deleteIndex) {
                _client.Indices.Delete(INDEX_NATURAL_PERSON);
                _client.Indices.Delete(INDEX_LEGAL_ENTITY);
            }
        }

        public ISearchResponse<T> Search<T>(Func<SearchDescriptor<T>, ISearchRequest> searchSelector) where T : class
        {
            var searchResponse = _client.Search(searchSelector);
            if (!searchResponse.IsValid)
            {
                throw new Exception($"Search failed: {searchResponse.DebugInformation}");
            }

            return searchResponse;
        }

        public ISearchResponse<Record> SearchNaturalPerson(Record doc)
        {
            var indexName = INDEX_NATURAL_PERSON;
           
            var searchResponse = _client.Search<Record>(s => s
                 .Index(indexName)
                 .Query(q => q
                     .Bool(b => b
                        .Must(m => m
                            .Bool(b => b
                                .Should(
                                  
                                    bs => bs.MatchPhrase(m => m.Field(f => f.Title).Query(doc.Title).Boost(2.1).Name("match phrase")),
                                    bs => bs.Match(m => m.Field(f => f.Title).Query(doc.Title).Boost(2).Name("match exact").MinimumShouldMatch("3<90%")),
                                    bs => bs.Match(m => m.Field(f => f.Identifications).Query(doc.Identifications).Boost(2).Name("match identification")),
                                    bs => bs.Match(m => m.Field(f => f.Title).Query(doc.Title).Fuzziness(Fuzziness.Auto).Name("match fuzzy").MinimumShouldMatch("3<90%"))
                                   // bs => bs.Match(m => m.Field(f => f.RecordType).Query(doc.RecordType))
                                )
                           )
                        )
                        .Should(
                            bs => bs.Match(m => m.Field(f => f.Dob).Query(doc.Dob).Name("match dob").Boost(20)),
                            bs => bs.Match(m => m.Field(f => f.Citizenships).Query(doc.Citizenships).Operator(Operator.Or).Name("match citizenships").Boost(12)),
                            bs => bs.Match(m => m.Field(f => f.Locations).Query(doc.Locations).Operator(Operator.Or).Name("match location").Boost(10)),
                            bs => bs.Match(m => m.Field(f => f.RelatedTo).Query(doc.RelatedTo).Fuzziness(Fuzziness.Auto).Name("match related").MinimumShouldMatch("3<90%"))                     
                        )
                    )
                 ).Highlight(h => h.Fields(f => f.Field("*")))
             );

            return searchResponse;
        }


        public ISearchResponse<Record> AllLegalEntities(string indexName)
        {
              var searchResponse = _client.Search<Record>(s => s
                 .Index(indexName)
                 .Query(q => q
                     .Bool(b => b
                        .Must(m => m
                            .Bool(b => b
                                .Should(
                                       bs => bs.MatchAll()
                                  
                                )
                           )
                        )
                        
                    )
                 ).Highlight(h => h.Fields(f => f.Field("*")))
             );

            return searchResponse;
        }

         public ISearchResponse<Record> SearchLegalEntity(Record doc)
        {
            var indexName = INDEX_LEGAL_ENTITY;

          
            var searchResponse = _client.Search<Record>(s => s
                 .Index(indexName)
                 .Query(q => q
                     .Bool(b => b
                        .Must(m => m
                            .Bool(b => b
                                .Should(
                                    bs => bs.CommonTerms(m => m.Field(f => f.Title).Query(doc.Title).CutoffFrequency(0.001).Name("common terms"))
                                  //  bs => bs.MatchPhrase(m => m.Field(f => f.Title).Query(doc.Title).Boost(2.1).Name("match phrase")),
                                  //  bs => bs.Match(m => m.Field(f => f.Title).Query(doc.Title).Boost(2).Name("match exact")),
                                  //  bs => bs.Match(m => m.Field(f => f.Identifications).Query(doc.Identifications).Boost(2).Name("match identification")),
                                  //  bs => bs.Match(m => m.Field(f => f.Title).Query(doc.Title).Fuzziness(Fuzziness.Auto).Name("match fuzzy"))
                                   // bs => bs.Match(m => m.Field(f => f.RecordType).Query(doc.RecordType))
                                )
                           )
                        )
                        .Should(
                            bs => bs.Match(m => m.Field(f => f.Locations).Query(doc.Locations).Operator(Operator.Or).Name("match location").Boost(9)),
                            bs => bs.Match(m => m.Field(f => f.RelatedTo).Query(doc.RelatedTo).Fuzziness(Fuzziness.Auto).Name("match related").MinimumShouldMatch("3<90%"))                     
                        )
                    )
                 ).Highlight(h => h.Fields(f => f.Field("*")))
             );

            return searchResponse;
        }


        public void IndexDocuments(IEnumerable<Record> documents, string indexName)
        {
             var indexResponse = _client.IndexMany<Record>(documents, indexName);
            if (!indexResponse.IsValid)
            {
                throw new Exception($"Failed to index document: {indexResponse.DebugInformation}");
            }
             _client.Indices.Refresh(indexName);
        }

       
    }
}
