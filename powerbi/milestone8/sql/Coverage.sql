WITH days AS (SELECT explode(sequence(DATE '2026-11-02', DATE '2026-11-15', INTERVAL 1 DAY)) AS delivery_date),
          sources AS (SELECT 'rt_lmp' AS source,288 AS expected_slots UNION ALL
            SELECT 'rt_totals',288 UNION ALL SELECT 'da_lmp',24 UNION ALL SELECT 'generator',24),
          observed AS (
            SELECT delivery_date,'rt_lmp' AS source,count(*) AS observation_rows,
              count(DISTINCT struct(delivery_hour,interval)) AS observed_slots,
              count(DISTINCT pricing_location) AS reported_entities
              FROM ieso_dev.gold.fact_rt_lmp_5m WHERE delivery_date BETWEEN DATE '2026-11-02' AND DATE '2026-11-15' AND lmp IS NOT NULL GROUP BY delivery_date
            UNION ALL
            SELECT delivery_date,'rt_totals',count(*),
              count(DISTINCT struct(delivery_hour,interval)),cast(1 AS BIGINT)
              FROM ieso_dev.gold.fact_ontario_demand_5m WHERE delivery_date BETWEEN DATE '2026-11-02' AND DATE '2026-11-15' AND ontario_demand IS NOT NULL GROUP BY delivery_date
            UNION ALL
            SELECT delivery_date,'da_lmp',count(*),count(DISTINCT delivery_hour),count(DISTINCT pricing_location)
              FROM ieso_dev.gold.fact_da_lmp_hourly WHERE delivery_date BETWEEN DATE '2026-11-02' AND DATE '2026-11-15' AND lmp IS NOT NULL GROUP BY delivery_date
            UNION ALL
            SELECT delivery_date,'generator',count(*),count(DISTINCT delivery_hour),count(DISTINCT generator_name)
              FROM ieso_dev.gold.fact_generator_hourly WHERE delivery_date BETWEEN DATE '2026-11-02' AND DATE '2026-11-15' GROUP BY delivery_date)
          SELECT d.delivery_date,s.source,coalesce(o.observation_rows,0) AS observation_rows,
            coalesce(o.observed_slots,0) AS observed_slots,s.expected_slots,
            s.expected_slots-coalesce(o.observed_slots,0) AS missing_slots,
            coalesce(o.reported_entities,0) AS reported_entities,
            CASE WHEN s.source IN ('rt_lmp','da_lmp') THEN 'Any-location time coverage; not all-location completeness'
                 WHEN s.source='generator' THEN 'Reported facility-hour coverage; measurements may still be missing'
                 ELSE 'Non-null Ontario demand interval coverage' END AS coverage_note
          FROM days d CROSS JOIN sources s LEFT JOIN observed o
          ON d.delivery_date=o.delivery_date AND s.source=o.source ORDER BY d.delivery_date,s.source;
