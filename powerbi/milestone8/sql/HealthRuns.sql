WITH inv AS (SELECT from_json(cast(content AS STRING),
      'plan STRUCT<start_date:STRING,end_date:STRING,data_start_date:STRING,delivery_hour:INT,
       tasks:ARRAY<STRUCT<key:STRING,source:STRING,day:STRING,hour:INT>>>,
       reports ARRAY<STRUCT<key:STRING,source:STRING,day:STRING,hour:INT,expected_count:BIGINT,
       record:STRUCT<RowKey:STRING>>>') AS inventory
      FROM read_files('abfss://raw@iesodev001.dfs.core.windows.net/ieso/ops/m7/d99f4e0459d28079c2680094/inventory.json', format => 'binaryFile')), ranked AS (
          SELECT *,row_number() OVER (PARTITION BY batch_id ORDER BY completed_at DESC) AS n FROM (SELECT * FROM ieso_dev.ops.m7_runs WHERE batch_id='d99f4e0459d28079c2680094')),
          r AS (SELECT * FROM ranked WHERE n=1)
          SELECT r.batch_id,r.job_run_id,r.adf_run_id,r.revision,r.completed_at AS spark_completed_utc,
            cast(get_json_object(r.metrics,'$.runtime_seconds') AS DOUBLE) AS spark_seconds,
            get_json_object(r.metrics,'$.passed') AS spark_passed,
            cast(i.inventory.plan.start_date AS DATE) AS requested_start,
            cast(i.inventory.plan.end_date AS DATE) AS requested_end,
            cast(i.inventory.plan.data_start_date AS DATE) AS data_start,
            i.inventory.plan.delivery_hour AS selected_delivery_hour,
            size(i.inventory.plan.tasks) AS expected_reports,size(i.inventory.reports) AS received_reports,
            'Unknown - check ADF/control record' AS final_batch_status,
            cast(NULL AS TIMESTAMP) AS final_batch_completed_utc,
            'Sealed inventory and successful Spark records only; not a complete attempt/failure history' AS scope_note
          FROM r CROSS JOIN inv i;
