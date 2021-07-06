config = {
    "SourceDBConnection":{"url":"192.10.15.191","port":"1433","userName":"Kockpit","password":"Tcpl@2050","databaseName":"TEAMLIVE","dbtable":"information_schema.tables"},
    
    
    "DbEntities" : 
    [
        {"Name" : "Team Computers Pvt Ltd$", "Location" : "DB1E2", "DatabaseName" : "TEAMLIVE","ActiveInactive":"Active","Year":2019,"Month":4},
        {"Name" : "kockpit", "Location" : "DB1E1", "DatabaseName" : "TEAMLIVE","ActiveInactive":"Active","Year":2019,"Month":4},
    ],
    "Dimensions" : 
    [
        {"DBEntity":"DB1E1","Active Dimension":['CUSTOMER','BRANCH','TARGETPROD','OT BRANCH','SUBBU','SBU','PRODUCT']}
    ],
    "TablesToIngest": 
    [
        
        {"Table": "Customer","TableType": "Master","TransactionColumn":"","Key": ["No_"],"CheckOn": "timestamp","Columns":["No_","Name","DBName","EntityName","Chain Name","Country_Region Code","Post Code","Blocked","Service Zone Code","City","State Code","Sector","NBFC","Address","Address2","Salesperson Code","Chain Name"]}
        
        
    ],
    "TablesToRename":
    [
        #{"Table": "Customer","Old_Columns" :["No_", "Name", "Address", "Address2", "City", "ChainName", "SalespersonCode", "Country_RegionCode", "Blocked", "PostCode", "ServiceZoneCode", "StateCode", "Sector", "NBFC"],"New_Columns":["Link Customer","Customer Name"," "," ","Customer City","Customer Group Name"," ","Country Region Code", " ", "Customer Post Code" ,"Service Zone Code","Customer State Code"," "," ","DB","Entity","Link Customer Key"]},
        {"Table": "Customer","Columns" :[{"No_":"Link Customer", "Name":"Customer Name", "City":"Customer City", "ChainName":"Customer Group Name", "Country_RegionCode":"Country Region Code","PostCode":"Customer Post Code","StateCode":"Customer State Code"}]}
    ]
}

