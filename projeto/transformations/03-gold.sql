-- Vamos criar algumas agregações para nosso dashboard

-- ==========================================================================
-- == MATERIALIZED VIEW: bikes                                             ==
-- ==========================================================================
CREATE OR REFRESH MATERIALIZED VIEW bikes (
  ride_date DATE COMMENT "A data dos passeios e da verificação de manutenção associada.",
  bike_id STRING COMMENT "Identificador da bicicleta.",
  total_rides LONG COMMENT "Número total de passeios únicos para a bicicleta na data especificada.",
  total_revenue DECIMAL(19,4) COMMENT "Receita total gerada pela bicicleta na data especificada.",
  start_station_id STRING COMMENT "O primeiro ID de estação de onde a bicicleta iniciou um passeio na data especificada.",
  end_station_id STRING COMMENT "O último ID de estação onde a bicicleta terminou um passeio na data especificada.",
  requires_maintenance BOOLEAN COMMENT "Flag booleana indicando se a bicicleta teve um registro de manutenção na data especificada."
)
COMMENT "Métricas diárias agregadas por bicicleta, incluindo total de passeios, receita, primeira estação de partida, última estação de chegada e um indicador se houve manutenção reportada para a bicicleta no dia."
AS SELECT
  r.ride_date, 
  r.bike_id, 
  COUNT(DISTINCT r.ride_id) AS total_rides, 
  SUM(r.ride_revenue) :: DECIMAL(19,4) AS total_revenue,
  MIN_BY(r.start_station_id, r.start_time) AS start_station_id, 
  MAX_BY(r.end_station_id, r.end_time) AS end_station_id,
  CASE WHEN ml.maintenance_id IS NOT NULL THEN TRUE ELSE FALSE END AS requires_maintenance
FROM rides r
LEFT JOIN maintenance_logs ml ON r.bike_id = ml.bike_id AND ml.maintenance_date = r.ride_date
GROUP BY ALL;


-- ==========================================================================
-- == MATERIALIZED VIEW: stations                                          ==
-- ==========================================================================
CREATE OR REFRESH MATERIALIZED VIEW stations (
  ride_date DATE COMMENT "A data para a qual as métricas da estação são agregadas.",
  station_id STRING COMMENT "Identificador da estação.",
  total_rides_as_origin LONG COMMENT "Número total de passeios que se originaram nesta estação na data especificada.",
  total_rides_as_destination LONG COMMENT "Número total de passeios que terminaram nesta estação na data especificada.",
  end_of_day_inventory LONG COMMENT "Número de bicicletas localizadas nesta estação ao final da data especificada, com base na última estação de chegada do passeio.",
  total_revenue_as_origin DECIMAL(19,4) COMMENT "Receita total gerada por passeios que se originaram nesta estação na data especificada.",
  total_revenue_as_destination DECIMAL(19,4) COMMENT "Receita total gerada por passeios que terminaram nesta estação na data especificada."
)
COMMENT "Métricas diárias por estação, incluindo passeios originados, passeios terminados, inventário de bicicletas ao final do dia e receita gerada por passeios iniciados ou finalizados na estação."
AS WITH starts AS (
  SELECT ride_date, start_station_id AS station_id, SUM(ride_revenue) AS total_revenue, COUNT(*) AS total_rides FROM rides GROUP BY ALL
), ends AS (
  SELECT ride_date, end_station_id AS station_id, SUM(ride_revenue) AS total_revenue, COUNT(*) AS total_rides FROM rides GROUP BY ALL
), inventory AS (
  SELECT ride_date, end_station_id AS station_id, COUNT(*) AS total_bikes FROM bikes GROUP BY ALL
)
SELECT
  COALESCE(s.ride_date, e.ride_date) AS ride_date,
  COALESCE(s.station_id, e.station_id) AS station_id,
  COALESCE(s.total_rides, 0) AS total_rides_as_origin,
  COALESCE(e.total_rides, 0) AS total_rides_as_destination,
  COALESCE(i.total_bikes, 0) AS end_of_day_inventory,
  COALESCE(s.total_revenue, 0) :: DECIMAL(19,4) AS total_revenue_as_origin,
  COALESCE(e.total_revenue, 0) :: DECIMAL(19,4) AS total_revenue_as_destination
FROM starts s
FULL OUTER JOIN ends e ON s.station_id = e.station_id AND s.ride_date = e.ride_date
LEFT JOIN inventory i ON COALESCE(s.station_id, e.station_id) = i.station_id AND COALESCE(s.ride_date, e.ride_date) = i.ride_date;


-- ==========================================================================
-- == MATERIALIZED VIEW: maintenance_events                                ==
-- ==========================================================================
CREATE OR REFRESH MATERIALIZED VIEW maintenance_events (
  maintenance_id STRING COMMENT "Identificador único para o registro de manutenção.",
  days_to_resolve INT COMMENT "Número de dias necessários para resolver o problema de manutenção, do momento do reporte até a resolução.",
  revenue_lost DECIMAL(19,4) COMMENT "Receita típica de passeio por bicicleta durante o período de manutenção (ml.reported_time até ml.resolved_time). Pode servir como proxy para receita potencial perdida."
)
COMMENT "Métricas por registro de manutenção, incluindo número de dias para resolução e uma estimativa da receita típica por bicicleta durante o tempo de inatividade."
AS SELECT
  ml.maintenance_id, 
  DATEDIFF(ml.resolved_time, ml.reported_time) AS days_to_resolve,
  (SUM(r.ride_revenue) / COUNT(DISTINCT r.bike_id)) :: DECIMAL(19,4) AS revenue_lost
FROM maintenance_logs ml
INNER JOIN rides r ON r.start_time > ml.reported_time AND r.start_time < ml.resolved_time
GROUP BY ALL;