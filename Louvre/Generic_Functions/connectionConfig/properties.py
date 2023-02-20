# Databricks notebook source
LouvreConfig = {

  "dataSets": {
  "logSparkMetrics": "false", 
  "logEventsToDatabase": "false",  
  "description": "Here we define configuration of storage acoount we use for reading different files ",
  "louvre_db": {
            "dbDatabase": "shell-31-eun-sqdb-olvqkjpcizccwsddjlmn",
            "dbServer": "shell-31-eun-sq-wwkbjcerekhqpalxtrxg.database.windows.net",
            "dbUser": "louvre_prod",
            "kv_scope": "l_kv_scope",
            "kv_key": "louvreSQL",  
            "dbJdbcPort": "1433",
            "dbJdbcExtraOptions": "encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
            
    },
  "TC_db": {
            "dbDatabase": "shell-31-eun-sqdw-wjqmtzzfkjdstbptqise_prd",
            "dbServer": "shell-31-eun-sq-ajzwyckhunxrpsdqtobe.database.windows.net",
            "dbUser": "tc_dbmaint",
            "kv_scope": "l_kv_scope",
            "kv_key": "TC-prod",
            "dbJdbcPort": "1433",
            "dbJdbcExtraOptions": "encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
            
    },
  # need to update with our SPN with secret
    "Louvre_ADLS":{
      "Kv_scope": "l_kv_scope",
      "kv_key": "LouvreSecretKey",
      "Client_id": "8048f069-287f-49c1-819e-990ecef57e05",
      "Tenant_Id": "db1e96a8-a3da-442a-930b-235cac24cd5c",
      "Storage_Account": "adl://shell31eunadls1nbnbsmmjd.azuredatalakestore.net",
      "Mount_Path": "/mnt/ADLS/"
            },
   "l_ADLS_Loc": {
      "logSparkMetrics": "false", 
      "logEventsToDatabase": "false",  
      "description": "Here we define configuration of storage acoount we use for reading different files ",
      "Louvre_Loc": {
                "land": "/RAW/W00744-LOUVRE_PREPROD_LAND/UAT/",
                "raw": "/PROJECT/P01364-LOUVRE_PREPROD_RAW/UAT/",
                "unharm": "/PROJECT/P01365-LOUVRE_PREPROD_UNHARM/UAT/",  
    },
       "RSO_Loc": {
               "land": "/RAW/W00324-REVOLUTION_SALES_ORDER_PROD_LAND/PROD/",
               "raw": "/PROJECT/P00580-REVOLUTION_SALES_ORDER_PROD_RAW/PROD/",
               "unharm": "/PROJECT/P00581-REVOLUTION_SALES_ORDER_PROD_UNHARM/PROD/", 
       },
        "NextGen_Loc": {
               "MSEG": "/PROJECT/P00036-GSAP_MATERIAL_MOVEMENT_PROD_UNHARM/PROD/",
               "MSEGO2": "/PROJECT/P00036-GSAP_MATERIAL_MOVEMENT_PROD_UNHARM/PROD/",
               "EKKO": "/PROJECT/P00040-GSAP_PURCHASE_AGREEMENT_PROD_UNHARM/PROD/",
    }
  }
}
}

# COMMAND ----------


