
Launch ElasticSearch:
 docker run -d --name elasticsearch --net somenetwork -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" elasticsearch:7.17.28

Launch tests and generate HTML report:
 powershell -ExecutionPolicy Bypass -File .\scripts\Run-TestsAndReport.ps1


 dir /s /b /a:-d d:\ >c:\temp\t.txt