-- ==============================================================================
-- 04_ops_tables.sql: Pipeline audit logging and Data Quality tracking in Ops schema
-- ==============================================================================

CREATE TABLE IF NOT EXISTS ops.pipeline_run_log (
    run_id UUID PRIMARY KEY,
    stage VARCHAR(50) NOT NULL,
    trade_date DATE NOT NULL,
    status VARCHAR(20) NOT NULL, -- 'SUCCESS', 'FAILED', 'SKIPPED'
    rows_in INT,
    rows_out INT,
    error TEXT,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    finished_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pipeline_run_log_date ON ops.pipeline_run_log(trade_date);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_log_stage ON ops.pipeline_run_log(stage);

CREATE TABLE IF NOT EXISTS ops.dq_results (
    result_id SERIAL PRIMARY KEY,
    run_id UUID,
    stage VARCHAR(50) NOT NULL,
    check_name VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL, -- 'PASS', 'FAIL', 'WARN'
    details TEXT,
    checked_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dq_results_run_id ON ops.dq_results(run_id);
