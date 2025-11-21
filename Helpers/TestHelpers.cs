using Nest;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace name_match.Helpers
{
    public static class TestHelpers
    {
        private static ElasticClient? _client;

        public static ElasticClient ClientFactory(string indexName)
        {
            if (_client == null)
            {
                // var settings = new ConnectionSettings(new Uri("http://elk01-int-gva:9200")).DefaultIndex(indexName);
                var settings = new ConnectionSettings(new Uri("http://localhost:9200")).DefaultIndex(indexName);
                _client = new ElasticClient(settings);
            }
            return _client;
        }

        public static void IndexSampleData(ElasticClient client)
        {
            // Use a fixed seed so tests are deterministic
            var records = new List<Record>
            {
            new Record
            {
            Id = "SD-1",
            Name = "ALPHA",
            FirstName = "",
            FurtherInformation = "ID: 123456789, Passport: P12345678, SSN: 987-65-4320",
            Domicile = "ES"
            },
             new Record
            {
            Id = "SD-2",
            Name = "ALPHA BRAVO",
            FirstName = "",
            FurtherInformation = "ID: 123456789, Passport: P12345678, SSN: 987-65-4320",
            Domicile = "ES"
            },
             new Record
            {
            Id = "SD-3",
            Name = "ALPHA BRAVO CHARLIE",
            FirstName = "",
            FurtherInformation = "ID: 123456789, Passport: P12345678, SSN: 987-65-4320",
            Domicile = "CH"
            },

             new Record
            {
            Id = "SD-4",
            Name = "ALPHA BRAVO CHARLIE DELTA",
            FirstName = "",
            FurtherInformation = "ID: 123456789, Passport: P12345678, SSN: 987-65-4320",
            Domicile = "CH"
            },
            new Record
            {
            Id = "SD-5",
            Name = "LOPEZ FIGUERA",
            FirstName = "Jose Antonio",
            FurtherInformation = "ID: 123456789, Passport: P12345678, SSN: 987-65-4320",
            Domicile = "ES"
            },
            new Record
            {
            Id = "SD-6",
            Name = "GARCIA MARTINEZ", //
            FirstName = "Elena Maria",//
            FurtherInformation = "ID: 987654321, Passport: P87654321, SSN: 123-45-6789",
            Locations = "SPAIN, SWITZERLAND",
            Domicile = "CH"
            },
            new Record
            {
            Id = "SD-7",
            Name = "MARTINEZ GARCIA", //
            FirstName = "Elena Maria",//
            FurtherInformation = "ID: 987654321, Passport: A87654321, SSN: 123-45-6789",
            Domicile = "AR"
            }, new Record
            {
            Id = "SD-8",
            Name = "GARCIA MARTINEZ", //
            FirstName = "Rosa Maria",//
            FurtherInformation = "ID: 987654321, Passport: F87654321, SSN: 123-45-6789",
            Domicile = "MX"
            },
            new Record
            {
            Id = "SD-9",
            Name = "HERNANDEZ PEREZ", // CrazyFuzzy
            FirstName = "Carlos Alberto",
            FurtherInformation = "HERNANDEZ Carlos ID: 456789123, Passport: KZYFZY23456789, SSN: 234-56-7890",
            BirthDate = "13/01/1969",
            Domicile = "CL"
            },
            new Record
            {
            Id = "SD-10",
            Name = "HERNANDEZ", // CrazyFuzzy location only
            FirstName = "Carlos",
            FurtherInformation = "ID: 456779123, Passport: XZYFZY23456789, SSN: 734-46-9890",
            BirthDate = "02/01/1978",
            Locations = "SWITZERLAND",
            Domicile = "CH"
            },
             new Record
            {
            Id = "SD-11",
            Name = "HERNNANDEZ A", // CrazyFuzzy location only
            FirstName = "Carla",
            FurtherInformation = "ID: 456779123, Passport: XZYFZY23456789, SSN: 734-46-9890",
            BirthDate = "02/01/1978",
            Locations = "SWITZERLAND",
            Domicile = "CH"
            },
            new Record
            {
            Id = "SD-12",
            Name = "HERNNANDEZ B", // CrazyFuzzy location only
            FirstName = "Karla",
            FurtherInformation = "ID: 456779123, Passport: XZYFZY23456789, SSN: 734-46-9890",
            BirthDate = "02/01/1978",
            Locations = "SWITZERLAND",
            Domicile = "CH"
            },
            new Record
            {
            Id = "SD-13",
            Name = "HERNNANDEZ C", // CrazyFuzzy location only
            FirstName = "Carma",
            FurtherInformation = "ID: 456779123, Passport: XZYFZY23456789, SSN: 734-46-9890",
            BirthDate = "02/01/1978",
            Locations = "SWITZERLAND",
            Domicile = "CH"
            },
            new Record
            {
            Id = "SD-14",
            Name = "HERNNANDEZ", // CrazyFuzzy location only
            FirstName = "Karma",
            FurtherInformation = "ID: 456779123, Passport: XZYFZY23456789, SSN: 734-46-9890",
            BirthDate = "02/01/1978",
            Locations = "SWITZERLAND",
            Domicile = "CH"
            },
            new Record
            {
            Id = "SD-15",
            Name = "HERNNANDEZ", // CrazyFuzzy location only
            FirstName = "Parla",
            FurtherInformation = "ID: 456779123, Passport: XZYFZY23456789, SSN: 734-46-9890",
            BirthDate = "02/01/1978",
            Locations = "SWITZERLAND",
            Domicile = "CH"
            },
            new Record
            {
            Id = "SD-16",
            Name = "HERNANDEZ", // CrazyFuzzy location only
            FirstName = "Foo Carlos",
            FurtherInformation = "ID: 456779123, Passport: XZYFZY23456789, SSN: 734-46-9890",
            BirthDate = "02/01/1978",
            Locations = "SWITZERLAND",
            Domicile = "US"
            },
             new Record
            {
            Id = "SD-17",
            Name = "FOO HERNANDEZ", // CrazyFuzzy location only
            FirstName = "Carlos",
            FurtherInformation = "ID: 456779123, Passport: XZYFZY23456789, SSN: 734-46-7890",
            BirthDate = "02/01/1978",
            Locations = "SWITZERLAND",
            Domicile = "US"
            },
            new Record
            {
            Id = "SD-18",
            Name = "MARTINEZ GARCIA",//
            FirstName = "Elena Lucia",//
            FurtherInformation = "ID: 321654987, Passport: P34567890, SSN: 345-67-8901",
            Domicile = "ES"
            },
            new Record
            {
            Id = "SD-19",
            Name = "RODRIGUEZ LOPEZ",
            FirstName = "Luis Fernando",
            FurtherInformation = "ID: 654321789, Passport: P45678901, SSN: 456-78-9012",
            Domicile = "MX"
            },
            new Record
            {
            Id = "SD-20",
            Name = "PEREZ HERNANDEZ",
            FirstName = "Sofia Isabel",
            FurtherInformation = "ID: 789123456, Passport: P56789012, SSN: 567-89-0123",
            Domicile = "PE"
            },
             new Record
            {
            Id = "SD-21",
            Name = "PEREZ DA HERNANDEZ",
            FirstName = "Sofia Isabel",
            FurtherInformation = "ID: 789123456, Passport: P56789012, SSN: 567-89-0123",
            Domicile = "PE"
            },
            new Record
            {
            Id = "SD-22",
            Name = "PEREZ DE HERNANDEZ",
            FirstName = "Sofia Isabel",
            FurtherInformation = "ID: 789123456, Passport: P56789012, SSN: 567-89-0123",
            Domicile = "PE"
            },
            new Record
            {
            Id = "SD-23",
            Name = "PEREZ DOS HERNANDEZ",
            FirstName = "Sofia Isabel",
            FurtherInformation = "ID: 789123456, Passport: P56789012, SSN: 567-89-0123",
            Domicile = "PE"
            },
            new Record
            {
            Id = "SD-24",
            Name = "SANCHEZ GARCIA",
            FirstName = "Diego Armando",
            FurtherInformation = "ID: 159753486, Passport: P67890123, SSN: 678-90-1234",
            Domicile = "AR"
            },
            new Record
            {
            Id = "SD-25",
            Name = "MORALES RIVERA",
            FirstName = "Valentina Sofia",
            FurtherInformation = "ID: 753159486, Passport: P78901234, SSN: 789-01-2345",
            Domicile = "CO"
            },
            new Record
            {
            Id = "SD-26",
            Name = "CASTRO MARTINEZ",
            FirstName = "Javier Alejandro",
            FurtherInformation = "ID: 951753486, Passport: P89012345, SSN: 890-12-3456",
            Domicile = "UY"
            },
            new Record
            {
            Id = "SD-27",
            Name = "MORENO LOPEZ",
            FirstName = "Camila Andrea",
            FurtherInformation = "ID: 852963741, Passport: P90123456, SSN: 901-23-4567",
            Domicile = "BR"
            }
            ,
            new Record
            {
            Id = "SD-28",
            Name = "H.ERNANDEZ S.A. e-energy",
            FirstName = "",
            FurtherInformation = "",
            Domicile = "CH"
            }
             ,
            new Record
            {
            Id = "SD-29",
            Name = "MARTIN Jean-Pierre",
            FirstName = "",
            FurtherInformation = "",
            Domicile = "FR"
            }
            ,
            new Record
            {
            Id = "SD-30",
            Name = "FOO MARTIN Jean-Pierre | MARTIN Jean-Pierre | MARTIN Jean-Pierre",
            FirstName = "",
            FurtherInformation = "",
            Domicile = "FR"
            }
            ,
            new Record
            {
            Id = "SD-31",
            Name = "MARTIN Pierre-Jean | Pierre Jean MARTIN",
            FirstName = "",
            FurtherInformation = "",
            Domicile = "BE"
            }

             ,
            new Record
            {
            Id = "SD-32",
            Name = "Jean-Pierre MARTIN | Jean Pierre MARTIN",
            FirstName = "",
            FurtherInformation = "",
            Domicile = "FR"
            }
             ,
            new Record
            {
            Id = "SD-33",
            Name = "VAN DER LAYEN",
            FirstName = "Ursula",
            FurtherInformation = "",
            Domicile = "FR"
            }
            ,
            new Record
            {
            Id = "SD-34",
            Name = "LUYEN",
            FirstName = "Ursuli",
            FurtherInformation = "",
            Domicile = "FR"
            }
            ,
            new Record
            {
            Id = "SD-35",
            Name = "VAN DERBES",
            FirstName = "Damien",
            FurtherInformation = "",
            Domicile = "BE"
            }

             ,
            new Record
            {
            Id = "SD-36",
            Name = "DERBES",
            FirstName = "Damien",
            FurtherInformation = "Oooh!!",
            Domicile = "CH"
            },
            new Record
            {
            Id = "SD-37",
            Name = "DORBES",
            FurtherInformation = "Oooh!!",
            Domicile = "DE"
            }
        };

            // Index the built-in sample records first
            var indexManyResp = client.IndexMany(records);
            Assert.True(indexManyResp.IsValid, "IndexMany failed: " + indexManyResp.ServerError?.Error?.Reason);

            // Try to load additional records from legal_entities_1000.json and index them
            var candidates = new[] {
            Path.Combine(Directory.GetCurrentDirectory(), "legal_entities_1000.json"),
            Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "legal_entities_1000.json"),
            Path.Combine(AppContext.BaseDirectory, "legal_entities_1000.json")
            };

            var jsonPath = candidates.FirstOrDefault(File.Exists);
            if (!string.IsNullOrEmpty(jsonPath))
            {
                try
                {
                    var json = File.ReadAllText(jsonPath);
                    var arr = JArray.Parse(json);
                    var extras = new List<Record>();
                    var startId = 1;
                    foreach (var o in arr)
                    {
                        var name = (string?)o["Name"] ?? string.Empty;
                        var domicile = (string?)o["Domicile"] ?? string.Empty;
                        var finfo = (string?)o["FurtherInformation"] ?? string.Empty;

                        extras.Add(new Record
                        {
                            Id = $"LE-{startId++}",
                            Name = name,
                            FirstName = string.Empty,
                            FurtherInformation = finfo,
                            Domicile = domicile
                        });
                    }

                    if (extras.Any())
                    {
                        var idxResp2 = client.IndexMany(extras);
                        // don't fail the test if indexing external sample entries fail, but log to file for debugging
                        if (!idxResp2.IsValid)
                            File.AppendAllText(Path.Combine(Directory.GetCurrentDirectory(), "_index_errors.log"), JsonConvert.SerializeObject(idxResp2.ServerError, Formatting.Indented));
                    }
                }
                catch (Exception ex)
                {
                    File.AppendAllText(Path.Combine(Directory.GetCurrentDirectory(), "_index_errors.log"), ex.ToString());
                }
            }

            System.Threading.Thread.Sleep(5000);
        }

        public static void AssertHasHitsAndTokenIsNotHighlighted(IReadOnlyCollection<IHit<Record>> hits,  string mustNotHighlight)
        {
            Assert.True(hits.Any(), "No hits");

            Assert.False(hits.Any(hit =>
                    hit.Highlight != null &&
                    JsonConvert.SerializeObject(hit.Highlight).Contains($"<em>{mustNotHighlight}</em>")),
                $"Unexpected token '{mustNotHighlight}' found in highlights'");
        }

        public static void AssertTokenIsHighlighted(IReadOnlyCollection<IHit<Record>> hits, string mustHighlight)
        {
            Assert.True(hits.Any(hit =>
                    hit.Highlight != null &&
                    JsonConvert.SerializeObject(hit.Highlight).Contains($"<em>{mustHighlight}</em>")),
                $"Expected token '{mustHighlight}' not found in highlights'");
        }

        public static string FormatHtml(string input)
        {
            string template = @"
             <!DOCTYPE html>
                <html>
                <head>
                    <style>
                        em {background-color: powderblue};
                    </style>
                    </head>
                    <body>
                        <pre>" + input + @"</pre>
                    </body>
                </html>
            ";

            return template;
        }
    }
}
