-- Vamos começar ingerindo nossos arquivos brutos do nosso Volume UC
-- ==========================================================================
-- == Carregar incrementalmente RIDES RAW de CSV                        ==
-- ==========================================================================
CREATE OR REFRESH STREAMING TABLE RIDES_RAW 
COMMENT "Dados brutos de corridas transmitidos de arquivos CSV."
AS SELECT _metadata.file_name as file_name, * FROM 
  STREAM READ_FILES("/Volumes/${catalog}/${schema}/raw_data/rides/*.csv", FORMAT => "csv");


-- ==========================================================================
-- == Carregar incrementalmente LOG DE MANUTENÇÃO RAW de CSV             ==
-- ==========================================================================
CREATE OR REFRESH STREAMING TABLE MAINTENANCE_LOGS_RAW
COMMENT "Logs de manutenção brutos transmitidos de arquivos CSV."
AS SELECT _metadata.file_name as file_name, * FROM 
  STREAM READ_FILES("/Volumes/${catalog}/${schema}/raw_data/maintenance_logs/*.csv", FORMAT => "csv", MULTILINE => TRUE);


-- ==========================================================================
-- == Carregar incrementalmente WEATHER RAW de JSON                      ==
-- ==========================================================================
CREATE OR REFRESH STREAMING TABLE WEATHER_RAW
COMMENT "Dados brutos de clima transmitidos de arquivos JSON."
AS SELECT _metadata.file_name as file_name, * FROM 
  STREAM READ_FILES("/Volumes/${catalog}/${schema}/raw_data/weather/*.json", FORMAT => "json");

-- ==========================================================================
-- == Carregar incrementalmente CUSTOMER CDC RAW de PARQUET              ==
-- ==========================================================================
CREATE OR REFRESH STREAMING TABLE CUSTOMERS_CDC_RAW
COMMENT "Dados brutos de CDC de clientes transmitidos de arquivos Parquet para processamento Auto CDC."
AS SELECT _metadata.file_name as file_name, * FROM 
  STREAM READ_FILES("/Volumes/${catalog}/${schema}/raw_data/customers_cdc/*.parquet", FORMAT => "parquet");

-- Em seguida, vamos limpar nossos dados para a camada silver em 02-silver.sql