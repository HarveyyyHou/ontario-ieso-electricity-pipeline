WITH inv AS (SELECT from_json(cast(content AS STRING),
      'plan STRUCT<start_date:STRING,end_date:STRING,data_start_date:STRING,delivery_hour:INT,
       tasks:ARRAY<STRUCT<key:STRING,source:STRING,day:STRING,hour:INT>>>,
       reports ARRAY<STRUCT<key:STRING,source:STRING,day:STRING,hour:INT,expected_count:BIGINT,
       record:STRUCT<RowKey:STRING>>>') AS inventory
      FROM read_files('abfss://raw@iesodev001.dfs.core.windows.net/ieso/ops/m7/d99f4e0459d28079c2680094/inventory.json', format => 'binaryFile')), ranked AS (
          SELECT *,row_number() OVER (PARTITION BY batch_id ORDER BY completed_at DESC) AS n FROM (SELECT * FROM ieso_dev.ops.m7_runs WHERE batch_id='d99f4e0459d28079c2680094')),
          r AS (SELECT * FROM ranked WHERE n=1),
          sources AS (SELECT r.batch_id,explode(from_json(get_json_object(r.metrics,'$.real_time'),
            'MAP<STRING,STRUCT<reconcile:STRUCT<reports:ARRAY<STRUCT<report_id:STRING,
      expected:BIGINT,accepted_unique:BIGINT,rejected:BIGINT,valid_duplicate_deliveries:BIGINT,
      accepted_without_kafka_delivery:BIGINT>>,archive_recovered_reports:ARRAY<STRING>>>>')) AS (source,detail) FROM r),
          stats AS (SELECT batch_id,source,detail.reconcile.archive_recovered_reports AS recovered,
            explode(detail.reconcile.reports) AS report FROM sources),
          hourly AS (SELECT explode(from_json(get_json_object(metrics,'$.hourly'),'ARRAY<STRUCT<delivery_date:STRING,facts:STRUCT<da_lmp:STRUCT<rows:BIGINT>,
      generator:STRUCT<rows:BIGINT>>>>')) AS item FROM r),
          wanted AS (SELECT explode(inventory.reports) AS item FROM inv)
          SELECT 'd99f4e0459d28079c2680094' AS batch_id,w.item.source,cast(w.item.day AS DATE) AS delivery_date,
            w.item.hour AS delivery_hour,coalesce(w.item.record.RowKey,w.item.key) AS report_id,
            w.item.expected_count AS expected_observations,
            CASE WHEN w.item.source='da_lmp' THEN h.item.facts.da_lmp.rows
                 WHEN w.item.source='generator' THEN h.item.facts.generator.rows
                 ELSE s.report.accepted_unique END AS accepted_unique,
            s.report.rejected,s.report.valid_duplicate_deliveries AS repeated_deliveries,
            CASE WHEN w.item.source NOT IN ('rt_lmp','rt_totals') THEN 'Not applicable to batch route'
              WHEN s.report.report_id IS NULL THEN 'Unknown'
              WHEN array_contains(s.recovered,s.report.report_id) THEN 'Yes' ELSE 'No' END AS archive_recovered,
            s.report.accepted_without_kafka_delivery AS without_kafka_delivery
          FROM wanted w LEFT JOIN stats s ON w.item.source=s.source AND w.item.record.RowKey=s.report.report_id
          LEFT JOIN hourly h ON w.item.day=h.item.delivery_date
          ORDER BY delivery_date,delivery_hour,source;
