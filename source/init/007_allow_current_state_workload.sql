ALTER TABLE simulator.workload_runs
    DROP CONSTRAINT IF EXISTS workload_runs_workload_type_check;

ALTER TABLE simulator.workload_runs
    ADD CONSTRAINT workload_runs_workload_type_check
    CHECK (workload_type IN ('snapshot', 'changes', 'current-state'));
