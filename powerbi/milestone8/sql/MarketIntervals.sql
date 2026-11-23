SELECT delivery_date, delivery_hour, interval,
          interval_start_utc, interval_end_utc, observed_lmp_locations,
          avg_location_lmp_cad_per_mwh AS price_cad_mwh,
          min_location_lmp_cad_per_mwh AS min_price_cad_mwh,
          max_location_lmp_cad_per_mwh AS max_price_cad_mwh,
          ontario_demand_mw AS demand_mw
          FROM ieso_dev.gold.mart_market_overview WHERE delivery_date BETWEEN DATE '2026-11-02' AND DATE '2026-11-15'
          ORDER BY delivery_date,delivery_hour,interval;
