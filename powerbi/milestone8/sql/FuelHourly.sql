SELECT delivery_date,delivery_hour,fuel_type,reported_facilities,
          facilities_with_output,reported_output_mw AS output_mw,
          reported_capability_mw AS capability_mw,reported_forecast_mw AS forecast_mw,
          reported_available_capacity_mw AS available_capacity_mw
          FROM ieso_dev.gold.mart_generation_fuel_hourly WHERE delivery_date BETWEEN DATE '2026-11-02' AND DATE '2026-11-15'
          ORDER BY delivery_date,delivery_hour,fuel_type;
