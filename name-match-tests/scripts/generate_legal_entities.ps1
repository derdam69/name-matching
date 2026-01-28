param(
    [int]$Count = 1000,
    [string]$Out = "$PSScriptRoot\..\legal_entities_1000.json"
)

$legalTokens = @('LTD','LIMITED','CORP','LLC','PLC','S.A.','SA','SARL','GmbH','Sarl','Inc','Pty Ltd','BV','NV')
$assetTokens = @('ASSET','ASSETS','ASSET MANAGEMENT','ASSET-MANAGEMENT','ASSETMGMT','ASSET & MANAGEMENT','MANAGEMENT','MANAGERS')
$prefixes = @('GLOBAL','NATIONAL','INTERNATIONAL','PRIME','APEX','SUMMIT','HORIZON','PILOT','CENTRAL','UNION','HIGHTOWER','RIVER','OCEAN','PARK','CITY','METRO')
$middles = @('INVESTMENT','CAPITAL','HOLDINGS','PARTNERS','ENTERPRISES','GROUP','VENTURES','ADVISORS','CONSULTING','SOLUTIONS','TECH','RESOURCES')
$companies = @()

# generate diversified domiciles (ISO-2)
$domiciles = @('US','GB','CH','DE','FR','NL','BE','LU','ES','IT','SE','NO','DK','AU','CA','SG','HK','JP','CN','BR')

# Helper: pick an element
function pick($arr){ return $arr[(Get-Random -Minimum 0 -Maximum $arr.Count)] }

for($i=0; $i -lt $Count; $i++){
    # majority contain ASSET or MANAGEMENT tokens
    $useAsset = (Get-Random -Minimum 0 -Maximum 100) -lt 75 # 75% include asset/management

    $nameParts = @()

    # 40% start with a prefix
    if((Get-Random -Minimum 0 -Maximum 100) -lt 40){ $nameParts += pick($prefixes) }

    # middle core
    $core = pick($middles)

    # sometimes add a company base word
    if((Get-Random -Minimum 0 -Maximum 100) -lt 60){
        $core += " " + (pick(@('GROUP','HOLDINGS','CAPITAL','PARTNERS','SERVICES','ADVISORS')))
    }

    $nameParts += $core

    if($useAsset){
        # include asset or management token somewhere
        $asset = pick($assetTokens)
        if((Get-Random -Minimum 0 -Maximum 100) -lt 60){
            # append
            $nameParts += $asset
        } else {
            # insert near start
            $nameParts = @($nameParts[0], $asset) + $nameParts[1..($nameParts.Count-1)]
        }
    }

    # append a common legal token
    $nameParts += pick($legalTokens)

    # sometimes add an identifier number
    if((Get-Random -Minimum 0 -Maximum 100) -lt 10){ $nameParts += ("No. " + (Get-Random -Minimum 1 -Maximum 9999)) }

    $companyName = ($nameParts -join ' ')

    $record = [PSCustomObject]@{
        Name = $companyName
        Domicile = pick($domiciles)
        FurtherInformation = ''
    }
    $companies += $record
}

$companies | ConvertTo-Json -Depth 4 | Out-File -FilePath $Out -Encoding UTF8
Write-Output "Wrote $($companies.Count) records to $Out"
