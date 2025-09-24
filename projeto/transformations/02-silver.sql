-- Vamos começar a limpar nossos dados bronze/raw em novas tabelas de streaming.

-- ==========================================================================
-- == TABELA DE STREAMING: rides                                               ==
-- ==========================================================================
CREATE OR REFRESH STREAMING TABLE rides (
  -- As tabelas de streaming podem inferir o esquema da consulta, porém você pode especificá‑lo explicitamente e incluir descrições de colunas ao definir a tabela.
  ride_date DATE COMMENT "A data do passeio de bicicleta.",
  ride_id STRING COMMENT "Identificador único para o passeio.",
  start_time TIMESTAMP COMMENT "Timestamp quando o passeio começou.",
  end_time TIMESTAMP COMMENT "Timestamp quando o passeio terminou.",
  start_station_id STRING COMMENT "Identificador da estação onde o passeio começou.",
  end_station_id STRING COMMENT "Identificador da estação onde o passeio terminou.",
  bike_id STRING COMMENT "Identificador da bicicleta usada no passeio.",
  user_type STRING COMMENT "Tipo de usuário (ex.: membro, casual).",
  ride_revenue DECIMAL(19,4) COMMENT "Receita calculada para o passeio com base na duração e no tipo de usuário.",
  CONSTRAINT invalid_ride_duration EXPECT(DATEDIFF(MINUTE, start_time, end_time) > 0) ON VIOLATION DROP ROW
)
COMMENT "Tabela de streaming contendo dados de passeios processados de compartilhamento de bicicletas."
AS SELECT
  DATE(start_time) AS ride_date, ride_id, start_time, end_time, start_station_id, end_station_id, bike_id, user_type,
  -- Calcular a receita do passeio tomando a duração do passeio em horas fracionárias e multiplicando pela tarifa.
  -- Tarifas: membros pagam 10 dólares por hora e não membros pagam 15 dólares por hora.
  CASE WHEN user_type = "member"
    THEN (DATEDIFF(MINUTE, start_time, end_time) / 60.0) * 10.0
    ELSE (DATEDIFF(MINUTE, start_time, end_time) / 60.0) * 15.0
  END :: DECIMAL(19,4) AS ride_revenue
FROM STREAM (rides_raw);


-- ==========================================================================
-- == TABELA DE STREAMING: maintenance_logs                                    ==
-- ==========================================================================
CREATE OR REFRESH STREAMING TABLE maintenance_logs (
  maintenance_date DATE COMMENT "Data em que a manutenção foi reportada.",
  maintenance_id STRING COMMENT "Identificador único para a entrada de log de manutenção.",
  bike_id STRING COMMENT "Identificador da bicicleta que passou por manutenção.",
  reported_time TIMESTAMP COMMENT "Timestamp quando o problema de manutenção foi reportado.",
  resolved_time DATE COMMENT "Data em que o problema de manutenção foi resolvido.",
  issue_description STRING COMMENT "Descrição textual do problema de manutenção.",
  issue_type STRING COMMENT "Tipo do problema de manutenção classificado por IA (ex.: freios, pneus).",
  -- Podemos adicionar restrições às tabelas para filtrar dados ruins, registrá‑los ou até falhar o pipeline quando os dados chegam.
  -- Descartar linhas com descrições de problema ausentes
  CONSTRAINT no_maintenance_description EXPECT(issue_description IS NOT NULL) ON VIOLATION DROP ROW,
  -- Registrar linhas com descrições curtas de problema, mas não descartá‑las
  CONSTRAINT short_maintenance_description EXPECT(LEN(issue_description) > 10)
)
COMMENT "Tabela de streaming contendo logs de manutenção processados para bicicletas, incluindo tipos de problema classificados por IA."
AS SELECT
  DATE(reported_time) AS maintenance_date, maintenance_id, bike_id, reported_time, resolved_time, issue_description,
  -- Usar AI_CLASSIFY para classificar problemas em categorias específicas usando a descrição. Consulte o notebook de exploração para mais detalhes sobre como esta função funciona.
  AI_CLASSIFY(issue_description, ARRAY("brakes", "chains_pedals", "tires", "other")) AS issue_type
FROM STREAM (maintenance_logs_raw);


-- ==========================================================================
-- == TABELA DE STREAMING: weather                                             ==
-- ==========================================================================
CREATE OR REFRESH STREAMING TABLE weather (
  weather_date DATE COMMENT "Data para a qual os dados meteorológicos são registrados.",
  temperature DOUBLE COMMENT "Temperatura em Fahrenheit.",
  rainfall DOUBLE COMMENT "Precipitação em polegadas.",
  wind_speed DOUBLE COMMENT "Velocidade do vento em milhas por hora."
)
COMMENT "Tabela de streaming contendo dados meteorológicos processados, convertidos para unidades padrão."
AS SELECT
  DATE(FROM_UNIXTIME(`timestamp`)) AS weather_date,
  temperature_f AS temperature,
  rainfall_in AS rainfall,
  wind_speed_mph AS wind_speed
FROM STREAM (weather_raw);


-- ==========================================================================
-- == AUTO CDC: customers (Tabela Tipo SCD 2)                               ==
-- ==========================================================================
CREATE OR REFRESH STREAMING TABLE customers (
  customer_id STRING COMMENT "Identificador único para o cliente",
  user_type STRING COMMENT "Tipo de usuário: membro ou não‑membro",
  registration_date STRING COMMENT "Data em que o cliente se registrou no serviço",
  email STRING COMMENT "Endereço de email do cliente",
  phone STRING COMMENT "Número de telefone do cliente",
  age_group STRING COMMENT "Faixa etária do cliente (18-25, 26-35, 36-45, 46-55, 55+)",
  membership_tier STRING COMMENT "Nível de associação para membros (básico, premium, empresarial)",
  preferred_payment STRING COMMENT "Método de pagamento preferido (cartão_de_crédito, pagamento_móvel, dinheiro)",
  home_station_id STRING COMMENT "Identificador da estação preferida ou de origem",
  is_active BOOLEAN COMMENT "Se a conta do cliente está atualmente ativa"
)
COMMENT "Dados de clientes com rastreamento Tipo SCD 2 para manter histórico completo de alterações";

CREATE FLOW customers_cdc_flow AS AUTO CDC INTO
  customers
FROM
  STREAM(customers_cdc_raw)
KEYS
  (customer_id)  -- Chave(s) primária(s) para identificar registros únicos
APPLY AS DELETE WHEN
  operation = "DELETE"  -- Condição para aplicar exclusões com base na coluna operation
SEQUENCE BY
  to_timestamp(event_timestamp, 'MM-dd-yyyy HH:mm:ss')  -- Coluna para ordenar mudanças cronologicamente
COLUMNS * EXCEPT
  (operation, event_timestamp, _rescued_data, file_name)  -- Incluir todas as colunas exceto metadados CDC e dados resgatados
STORED AS
  SCD TYPE 2;  -- Manter versões históricas dos registros com timestamps de início/fim

-- Em seguida, vamos construir algumas agregações para nosso dashboard em 03-gold.sql.