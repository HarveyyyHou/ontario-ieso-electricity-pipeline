WITH days AS (SELECT explode(sequence(DATE '2026-11-02', DATE '2026-11-15', INTERVAL 1 DAY)) AS delivery_date)
          SELECT delivery_date, cast(date_trunc('week',delivery_date) AS DATE) AS week_start,
          date_add(cast(date_trunc('week',delivery_date) AS DATE),6) AS week_end,
          pmod(dayofweek(delivery_date)+5,7)+1 AS weekday_order,
          date_format(delivery_date,'EEEE') AS weekday FROM days;
