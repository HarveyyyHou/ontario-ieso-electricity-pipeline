SELECT delivery_date,delivery_hour,reported_facilities,
          facilities_with_output,reported_output_mw,demand_intervals,
          CASE WHEN demand_intervals=12 THEN ontario_demand_mw END AS demand_mw
          FROM ieso_dev.gold.mart_generation_demand_hourly WHERE delivery_date BETWEEN DATE '2026-11-02' AND DATE '2026-11-15'
          ORDER BY delivery_date,delivery_hour;
