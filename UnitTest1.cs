using System.Numerics;
using System.Linq;
using System.Diagnostics.SymbolStore;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Text;
using Elasticsearch.Net.Specification.SecurityApi;
using Nest;
using Newtonsoft.Json;

namespace ElasticsearchIntegrationTests;

public class UnitTest1
{
      IEnumerable<Doc> GetNaturalPersons()
      {
            var ret = new List<Doc>();
            ret.Add(new Doc() { Id = "1", Title = "Ipsum Larem", Dob = "20010112" });
            ret.Add(new Doc() { Id = "2", Title = "Larem Ipsum alias Lorem Ipsum" });
            ret.Add(new Doc() { Id = "3", Title = "alpha John AND Doe beta LTD." });
            ret.Add(new Doc() { Id = "4", Title = "Ipsum Lorem", Dob = "20020101" });
            ret.Add(new Doc() { Id = "5", Title = "Ipsum John Lorem Doe" });
            ret.Add(new Doc() { Id = "6", Title = "Heinz Muller", Identifications = "CHE12435687,RU2023230203", Citizenships = "CH,RU", RelatedTo = "Putin Vladmir,Kim Jong Uhn" });
            ret.Add(new Doc() { Id = "7", Title = "Cornelia Liur Ritter Meyer", Identifications = "CHE12345678" });
            ret.Add(new Doc() { Id = "8", Title = "Dos Santos Sanchez Maria" });
            ret.Add(new Doc() { Id = "9", Title = "Dos Santos Sanchez Luis" });
            ret.Add(new Doc() { Id = "10", Title = "Dos Santos Sanchez Jose" });
            ret.Add(new Doc() { Id = "11", Title = "Dos Santos Sanchez Rosa", Dob = "20210304", Citizenships = "PT,CH", Identifications = "PT12345678", Locations = "FR,SP" });
            ret.Add(new Doc() { Id = "12", Title = "Dos Santos Sanchez maria Rosa", Dob = "20210203", Citizenships = "CH,BR", Locations = "IT,SP" });
                 ret.Add(new Doc() { Id = "13", Title = "Jack Hormell" });
     
            return ret;
      }

      IEnumerable<Doc> GetLegalEntities()
      {
            string[] usCompanyNames = new string[]
            {
            "Walmart Inc.",
            "Amazon.com Inc.",
            "Exxon Mobil Corp.",
            "Berkshire Hathaway Inc.",
            "Apple Inc.",
            "UnitedHealth Group Inc.",
            "McKesson Corp.",
            "CVS Health Corp.",
            "AmerisourceBergen Corp.",
            "AT&T Inc.",
            "Cigna Corp.",
            "Ford Motor Co.",
            "General Motors Co.",
            "Costco Wholesale Corp.",
            "Alphabet Inc.",
            "Cardinal Health Inc.",
            "JPMorgan Chase & Co.",
            "Verizon Communications Inc.",
            "Chevron Corp.",
            "Kroger Co.",
            "General Electric Co.",
            "Walgreens Boots Alliance Inc.",
            "Fannie Mae",
            "Phillips 66",
            "Valero Energy Corp.",
            "Bank of America Corp.",
            "Microsoft Corp.",
            "Home Depot Inc.",
            "Boeing Co.",
            "Wells Fargo & Co.",
            "Citigroup Inc.",
            "IBM",
            "Anthem Inc.",
            "Procter & Gamble Co.",
            "Freddie Mac",
            "Comcast Corp.",
            "PepsiCo Inc.",
            "Johnson & Johnson",
            "State Farm Insurance Cos.",
            "United Technologies Corp.",
            "Archer Daniels Midland Co.",
            "Prudential Financial Inc.",
            "MetLife Inc.",
            "Centene Corp.",
            "Marathon Petroleum Corp.",
            "Walt Disney Co.",
            "Pfizer Inc.",
            "Intel Corp.",
            "Caterpillar Inc.",
            "Energy Transfer LP",
            "Sysco Corp.",
            "American International Group Inc.",
            "Cisco Systems Inc.",
            "Goldman Sachs Group Inc.",
            "HCA Healthcare Inc.",
            "Morgan Stanley",
            "DowDuPont Inc.",
            "Delta Air Lines Inc.",
            "Tyson Foods Inc.",
            "Merck & Co. Inc.",
            "United Continental Holdings Inc.",
            "FedEx Corp.",
            "Honeywell International Inc.",
            "Oracle Corp.",
            "Allstate Corp.",
            "Tech Data Corp.",
            "TIAA",
            "Liberty Mutual Holding Co. Inc.",
            "Facebook Inc.",
            "Coca-Cola Co.",
            "American Express Co.",
            "DuPont de Nemours Inc.",
            "Nationwide",
            "General Dynamics Corp.",
            "Raytheon Co.",
            "AbbVie Inc.",
            "Best Buy Co. Inc.",
            "Lowe's Cos. Inc.",
            "Target Corp.",
            "Johnson Controls International plc",
            "INTL FCStone Inc.",
            "Aflac Inc.",
            "Humana Inc.",
            "Gilead Sciences Inc.",
            "Southwest Airlines Co.",
            "United Parcel Service Inc.",
            "Lear Corp.",
            "Molina Healthcare Inc.",
            "Lennar Corp.",
            "Progressive Corp.",
            "Union Pacific Corp.",
            "Paccar Inc.",
            "Twenty-First Century Fox Inc.",
            "CBS Corp.",
            "ConocoPhillips",
            "World Fuel Services Corp.",
            "Deere & Co.",
            "American Airlines Group Inc.",
            "XPO Logistics Inc.",
            "Nucor Corp.",
            "Dollar General Corp.",
            "Penske Automotive Group Inc.",
            "Alcoa Corp.",
            "Kohl's Corp.",
            "AECOM",
            "Navistar International Corp.",
            "PBF Energy Inc.",
            "C.H. Robinson Worldwide Inc.",
            "PulteGroup Inc.",
            "VF Corp.",
            "MGM Resorts International",
            "AES Corp.",
            "Western Digital Corp.",
            "Cognizant Technology Solutions Corp.",
            "Ally Financial Inc.",
            "Newell Brands Inc.",
            "Hertz Global Holdings Inc.",
            "Coty Inc.",
            "NVR Inc.",
            "Owens & Minor Inc.",
            "Celanese Corp.",
            "Crown Holdings Inc.",
            "Weyerhaeuser Co.",
            "Parker-Hannifin Corp.",
            "Hormel Foods Corp.",
            "Celanese Corp.",
            "Crown Holdings Inc.",
            "Weyerhaeuser Co.",
            "Parker-Hannifin Corp.",
            "Hormel Foods Corp.",
            "Celanese Corp.",
            "Crown Holdings Inc.",
            "Weyerhaeuser Co.",
            "Parker-Hannifin Corp.",
            "Hormel Foods Corp."
            };

            List<Doc> ret = new List<Doc>();
            int i = 1;
            foreach (var c in usCompanyNames)
            {
                  ret.Add(new Doc() { Title = c, Id = $"LE{i}" });
            }
            return ret;
      }

      IEnumerable<Doc> GeLegalEntities()
      {
            var ret = new List<Doc>();
            ret.Add(new Doc() { Id = "LE1", Title = "Big Corp Inc." });
            ret.Add(new Doc() { Id = "LE2", Title = "Apple" });
            ret.Add(new Doc() { Id = "LE3", Title = "Big Apple Inc." });
            ret.Add(new Doc() { Id = "LE4", Title = "Star Business Ltd." });
            return ret;
      }

      static ElasticsearchService service;

      string file = @"c:\temp\test.json";

      static object lockObj = new object();
      public UnitTest1()
      {
            lock (lockObj)
            {


                  if (service == null)
                  {
                        service = new ElasticsearchService(new Uri("http://localhost:9200"), true);
                        service.IndexDocuments(GetNaturalPersons(), "wc");
                        service.IndexDocuments(GetLegalEntities(), "wc_le");

                        Thread.Sleep(500);
                  }

            }

      }

      [Theory()]

      [InlineData("11", "Sanchez Rosa", null, null, null, "FR")]
      [InlineData("12", "Sanchez Rosa", null, null, null, "IT")]
      public void LocationsTest(string target, string names, string dob, string citizenships, string identification, string location)
      {
            var query = service.SearchTest(new Doc() { Title = names, Dob = dob, Citizenships = citizenships, Identifications = identification, Locations = location });
            Assert.Equal(target, query.Hits.First().Id);
      }

      [Theory()]
      [InlineData("6", "Heinz Muller", null, "RU", null, null)]
      [InlineData("6", "Heinz Muller", null, "CH", null, null)]
      [InlineData("12", "Sanchez Rosa", null, "BR", null, null)]
      [InlineData("11", "Sanchez Rosa", null, "PT", null, null)]
      [InlineData("11", "Sanchez Rosa", null, "CH", null, null)]
      public void CitizenshipTest(string target, string names, string dob, string citizenships, string identification, string location)
      {
            var query = service.SearchTest(new Doc() { Title = names, Dob = dob, Citizenships = citizenships, Identifications = identification, Locations = location });
            Assert.Equal(target, query.Hits.First().Id);
      }


      [Theory()]
      [InlineData("6", null, null, null, "CHE12435687", null)]
      [InlineData("6", null, null, null, "RU2023230203", null)]
      [InlineData("7", null, null, null, "CHE12345678", null)]
      [InlineData("11", null, null, null, "PT12345678", null)]
      public void IdentificationTest(string target, string names, string dob, string citizenships, string identification, string location)
      {
            var query = service.SearchTest(new Doc() { Title = names, Dob = dob, Citizenships = citizenships, Identifications = identification, Locations = location });
            Assert.Equal(target, query.Hits.First().Id);
      }


      [Theory()]
      [InlineData("1", "Ipsum Larem", "20010112", null, null, null)]
      [InlineData("4", "Ipsum Lorem", "20020101", null, null, null)]
      [InlineData("11", "rosa sanchez", "20210304", null, null, null)]
      [InlineData("12", "rosa sanchez", "20210203", null, null, null)]
      public void DobTest(string target, string names, string dob, string citizenships, string identification, string location)
      {
            var query = service.SearchTest(new Doc() { Title = names, Dob = dob, Citizenships = citizenships, Identifications = identification, Locations = location });
            Assert.Equal(target, query.Hits.First().Id);
      }


      [Theory()]
      [InlineData("11", "sanchoz resa", null, null, null, null)]
      [InlineData("12", "sanchaz marie resa", null, null, null, null)]
      [InlineData("7", "coreneliu ratter", null, null, null, null)]
      [InlineData("3", "johm and boe limited", null, null, null, null)]
      [InlineData("6", "heimz mueller", null, null, null, null)]
      public void NamesFuzzyTest(string target, string names, string dob, string citizenships, string identification, string location)
      {
            var query = service.SearchTest(new Doc() { Title = names, Dob = dob, Citizenships = citizenships, Identifications = identification, Locations = location });
            Assert.Equal(target, query.Hits.First().Id);
      }

      [Theory()]
      [InlineData("11", "sanchez rosa", null, null, null, null)]
      [InlineData("12", "sanchez maria rosa", null, null, null, null)]
      [InlineData("7", "corenelia ritter", null, null, null, null)]
      [InlineData("3", "john and doe limited", null, null, null, null)]
      [InlineData("6", "heinz muller", null, null, null, null)]
      public void NamesTest(string target, string names, string dob, string citizenships, string identification, string location)
      {
            var query = service.SearchTest(new Doc() { Title = names, Dob = dob, Citizenships = citizenships, Identifications = identification, Locations = location });
            Assert.Equal(target, query.Hits.First().Id);
      }

      [Theory]
      [InlineData("xx", "Hormel Foods Corp.", null, null, null, null)]
      public void Legal_Entity_NameTest(string target, string names, string dob, string citizenships, string identification, string location)
      {
            var query = service.SearchTest(new Doc() { Title = names, Dob = dob, Citizenships = citizenships, Identifications = identification, Locations = location });
            Assert.NotNull(query.Hits.Single());
      }


}

public class Doc
{
      public string Id { get; set; }

      public string Title { get; set; }

      public string Dob { get; set; }

      public string Citizenships { get; set; }

      public string Locations { get; set; }

      public string Identifications { get; set; }

      public string RelatedTo { get; set; }
}