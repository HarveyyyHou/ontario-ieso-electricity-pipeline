SELECT delivery_date,delivery_hour,generator_name,fuel_type,
          output_mw,capability_mw,forecast_mw,available_capacity_mw
          FROM ieso_dev.gold.fact_generator_hourly WHERE delivery_date BETWEEN DATE '2026-11-02' AND DATE '2026-11-15'
          ORDER BY delivery_date,delivery_hour,generator_name;
