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
    
    static ElasticsearchService service;

   string file = @"c:\temp\test.json";

   static object lockObj = new object();
    public UnitTest1()
    {
      lock (lockObj)
      {
        
        
          if (service == null) {
                service = new ElasticsearchService(new Uri("http://localhost:9200"), true);
                
                service.IndexDocument<Doc>(new Doc() {Id="1", Title="Ipsum Larem", Dob="20010112"});
                service.IndexDocument<Doc>(new Doc() {Id="2", Title="Larem Ipsum alias Lorem Ipsum"});
                service.IndexDocument<Doc>(new Doc() {Id="3", Title="alpha John AND Doe beta LTD."});
                service.IndexDocument<Doc>(new Doc() {Id="4", Title="Ipsum Lorem", Dob="20020101"});
                service.IndexDocument<Doc>(new Doc() {Id="5", Title="Ipsum John Lorem Doe"});
                service.IndexDocument<Doc>(new Doc() {Id="6", Title="Heinz Muller", Identifications="CHE12435687,RU2023230203", Citizenships="CH,RU", RelatedTo="Putin Vladmir,Kim Jong Uhn"});
                service.IndexDocument<Doc>(new Doc() {Id="7", Title="Cornelia Liur Ritter Meyer" , Identifications="CHE12345678"});
                service.IndexDocument<Doc>(new Doc() {Id="8", Title="Dos Santos Sanchez Maria"});
                service.IndexDocument<Doc>(new Doc() {Id="9", Title="Dos Santos Sanchez Luis"});
                service.IndexDocument<Doc>(new Doc() {Id="10", Title="Dos Santos Sanchez Jose"});
                service.IndexDocument<Doc>(new Doc() {Id="11", Title="Dos Santos Sanchez Rosa", Dob="20210304", Citizenships="PT,CH", Identifications="PT12345678", Locations="FR,SP"});
                service.IndexDocument<Doc>(new Doc() {Id="12", Title="Dos Santos Sanchez maria Rosa", Dob="20210203", Citizenships="CH,BR", Locations = "IT,SP"});
          

                Thread.Sleep(1000);  
          } 

        }

    }

[Theory()]
 
    [InlineData("11", "Sanchez Rosa", null ,null, null, "FR")]
    [InlineData("12", "Sanchez Rosa", null ,null, null, "IT")]
    public void LocationsTest(string target, string names, string dob, string citizenships, string identification, string location)
    { 
          var query = service.SearchTest(new Doc() { Title=names, Dob=dob, Citizenships=citizenships, Identifications=identification, Locations=location});
          Assert.Equal(target, query.Hits.First().Id);
    }
    
    [Theory()]
    [InlineData("6", "Heinz Muller", null ,"RU", null, null)]
    [InlineData("6", "Heinz Muller", null ,"CH", null, null)]
    [InlineData("12", "Sanchez Rosa", null ,"BR", null, null)]
    [InlineData("11", "Sanchez Rosa", null ,"PT", null, null)]
    [InlineData("11", "Sanchez Rosa", null ,"CH", null, null)]
    public void CitizenshipTest(string target, string names, string dob, string citizenships, string identification, string location)
    { 
          var query = service.SearchTest(new Doc() { Title=names, Dob=dob, Citizenships=citizenships, Identifications=identification, Locations=location});
          Assert.Equal(target, query.Hits.First().Id);
    }


    [Theory()]
    [InlineData("6", null, null ,null, "CHE12435687",null)]
    [InlineData("6", null, null ,null, "RU2023230203",null)]
    [InlineData("7", null, null ,null, "CHE12345678",null)]
    [InlineData("11", null, null ,null, "PT12345678",null)]
    public void IdentificationTest(string target, string names, string dob, string citizenships, string identification, string location)
    { 
          var query = service.SearchTest(new Doc() { Title=names, Dob=dob, Citizenships=citizenships, Identifications=identification, Locations=location});
          Assert.Equal(target, query.Hits.First().Id);
    }


    [Theory()]
    [InlineData("1", "Ipsum Larem", "20010112",null,null,null)]
    [InlineData("4", "Ipsum Lorem", "20020101",null,null,null)] 
    [InlineData("11", "rosa sanchez", "20210304",null,null,null)] 
    [InlineData("12", "rosa sanchez", "20210203",null,null,null)] 
    public void DobTest(string target, string names, string dob, string citizenships, string identification, string location)
    { 
          var query = service.SearchTest(new Doc() { Title=names, Dob=dob, Citizenships=citizenships, Identifications=identification, Locations=location});
          Assert.Equal(target, query.Hits.First().Id);
    }


    [Theory()]
    [InlineData("11", "sanchoz resa", null,null,null,null)]
    [InlineData("12", "sanchaz marie resa", null,null,null,null)]
    [InlineData("7", "coreneliu ratter", null,null,null,null)]
    [InlineData("3", "johm and boe limited", null,null,null,null)]
    [InlineData("6", "heimz mueller", null,null,null,null)]
    public void NamesFuzzyTest(string target, string names, string dob, string citizenships, string identification, string location)
    { 
          var query = service.SearchTest(new Doc() { Title=names, Dob=dob, Citizenships=citizenships, Identifications=identification, Locations=location});
          Assert.Equal(target, query.Hits.First().Id);
    }

    [Theory()]
    [InlineData("11", "sanchez rosa", null,null,null,null)]
    [InlineData("12", "sanchez maria rosa", null,null,null,null)]
    [InlineData("7", "corenelia ritter", null,null,null,null)]
    [InlineData("3", "john and doe limited", null,null,null,null)]
    [InlineData("6", "heinz muller", null,null,null,null)]
    public void NamesTest(string target, string names, string dob, string citizenships, string identification, string location)
    {
          var query = service.SearchTest(new Doc() { Title=names, Dob=dob, Citizenships=citizenships, Identifications=identification, Locations=location});
          Assert.Equal(target, query.Hits.First().Id);
    }

   

}

public class Doc {
    public string Id { get; set;}

    public string Title { get; set;}

    public string Dob { get; set;}

    public string Citizenships { get; set;}

    public string Locations  { get; set;}

    public string Identifications { get; set;}

    public string RelatedTo { get; set; }
}