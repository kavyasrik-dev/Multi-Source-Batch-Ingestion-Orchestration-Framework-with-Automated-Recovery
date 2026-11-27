ALTER TABLE validation_result
    DROP CONSTRAINT validation_result_status_check;

ALTER TABLE validation_result
    ADD CONSTRAINT validation_result_status_check
    CHECK (status IN ('passed', 'warning', 'failed', 'error', 'skipped'));
