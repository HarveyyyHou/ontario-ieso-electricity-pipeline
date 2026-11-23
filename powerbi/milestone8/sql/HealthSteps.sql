SELECT batch_id,stage,revision,completed_at AS completed_utc
          FROM ieso_dev.ops.m7_steps WHERE batch_id='d99f4e0459d28079c2680094' ORDER BY completed_at,stage;
