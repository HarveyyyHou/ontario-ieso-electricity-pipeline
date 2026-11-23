SELECT delivery_date, delivery_hour, pricing_location,
          da_lmp_cad_per_mwh AS day_ahead_cad_mwh,
          CASE WHEN rt_intervals=12 THEN observed_rt_mean_cad_per_mwh END AS rt_mean_cad_mwh,
          rt_minus_da_cad_per_mwh AS spread_cad_mwh, rt_intervals
          FROM ieso_dev.gold.mart_hourly_price_spread
          WHERE delivery_date BETWEEN DATE '2026-11-02' AND DATE '2026-11-15' AND pricing_location='CHOOSE_FROM_COVERAGE_OUTPUT' ORDER BY delivery_date,delivery_hour;
