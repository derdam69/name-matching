using System;
using Nest;
using Elasticsearch.Net;
using Newtonsoft.Json.Bson;

namespace ElasticsearchIntegrationTests
{
    public class ElasticsearchService
    {
        private readonly ElasticClient _client;

        public ElasticsearchService(Uri elasticsearchUri, bool deleteIndex)
        {

            var settings = new ConnectionSettings(elasticsearchUri)
              
                .DisableDirectStreaming()
                ;

            _client = new ElasticClient(settings);

            if (deleteIndex) {
                _client.Indices.Delete("wc");
                _client.Indices.Delete("wc_le");
            }
           
        }

        public void IndexDocument<T>(T document) where T : class
        {
            var indexResponse = _client.IndexDocument<T>(document);
            if (!indexResponse.IsValid)
            {
                throw new Exception($"Failed to index document: {indexResponse.DebugInformation}");
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

        public ISearchResponse<Doc> SearchTest(Doc doc, string? indexName = "wc")
        {
            var searchResponse = _client.Search<Doc>(s => s
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
                                  
                                )
                           )
                        )
                        .Should(
                            bs => bs.Match(m => m.Field(f => f.Dob).Query(doc.Dob).Name("match dob").Boost(20)),
                            bs => bs.Match(m => m.Field(f => f.Citizenships).Query(doc.Citizenships).Operator(Operator.Or).Name("match citizenships").Boost(10)),
                            bs => bs.Match(m => m.Field(f => f.Locations).Query(doc.Locations).Operator(Operator.Or).Name("match location").Boost(9)),
                            bs => bs.Match(m => m.Field(f => f.RelatedTo).Query(doc.RelatedTo).Fuzziness(Fuzziness.Auto).Name("match related").MinimumShouldMatch("3<90%"))

                      
                        )
                    )
                 ).Highlight(h => h.Fields(f => f.Field("*")))
             );
            return searchResponse;
        }

       

        public ISearchResponse<Doc> SearchTest_2(string query, string dob)
        {
            var searchResponse = _client.Search<Doc>(s => s
                 .Index("wc")
                 .Query(q => (q
                       .MatchPhrase(m => m.Field(f => f.Title).Query(query).Boost(2.1).Name("match phrase")) || q
                       .Match(m => m.Field(f => f.Title).Query(query).Boost(2).Name("match exact").MinimumShouldMatch("4<90%")) || q
                       .Match(m => m.Field(f => f.Title).Query(query).Fuzziness(Fuzziness.Auto).Name("match fuzzy").MinimumShouldMatch("4<90%"))
                      )
                 ).Highlight(h => h.Fields(f => f.Field("*")))
             );
            return searchResponse;
        }

        public void IndexDocuments(IEnumerable<Doc> documents, string indexName)
        {
             var indexResponse = _client.IndexMany<Doc>(documents, indexName);
            if (!indexResponse.IsValid)
            {
                throw new Exception($"Failed to index document: {indexResponse.DebugInformation}");
            }
        }
    }
}
