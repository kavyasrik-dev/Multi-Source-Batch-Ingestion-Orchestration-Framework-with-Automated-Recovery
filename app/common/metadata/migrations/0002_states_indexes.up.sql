CREATE FUNCTION touch_updated_at() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END;
$$;

CREATE FUNCTION enforce_run_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.status <> 'created' THEN
            RAISE EXCEPTION 'Runs must start in created state' USING ERRCODE = '23514';
        END IF;
    ELSIF NEW.status IS DISTINCT FROM OLD.status THEN
        IF NOT ((OLD.status = 'created' AND NEW.status IN ('running', 'cancelled'))
            OR (OLD.status = 'running' AND NEW.status IN ('succeeded', 'failed', 'cancelled'))
            OR (OLD.status = 'failed' AND NEW.status IN ('running', 'cancelled'))) THEN
            RAISE EXCEPTION 'Invalid run transition: % -> %', OLD.status, NEW.status
                USING ERRCODE = '23514';
        END IF;
        IF NEW.status = 'running' THEN
            NEW.started_at := COALESCE(OLD.started_at, clock_timestamp());
            NEW.finished_at := NULL;
        ELSE
            NEW.finished_at := clock_timestamp();
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION enforce_batch_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.status <> 'registered' THEN
            RAISE EXCEPTION 'Batches must start registered' USING ERRCODE = '23514';
        END IF;
    ELSIF NEW.status IS DISTINCT FROM OLD.status AND NOT
        ((OLD.status = 'registered' AND NEW.status IN ('staged', 'rejected'))
        OR (OLD.status = 'staged' AND NEW.status IN ('curated', 'rejected'))) THEN
        RAISE EXCEPTION 'Invalid batch transition' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION enforce_recovery_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.status <> 'pending' THEN
            RAISE EXCEPTION 'Recovery must start pending' USING ERRCODE = '23514';
        END IF;
    ELSIF NEW.status IS DISTINCT FROM OLD.status AND NOT
        ((OLD.status = 'pending' AND NEW.status = 'running')
        OR (OLD.status = 'running' AND NEW.status IN ('succeeded', 'failed'))) THEN
        RAISE EXCEPTION 'Invalid recovery transition' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER run_transition BEFORE INSERT OR UPDATE ON ingestion_run
    FOR EACH ROW EXECUTE FUNCTION enforce_run_transition();
CREATE TRIGGER batch_transition BEFORE INSERT OR UPDATE ON batch_manifest
    FOR EACH ROW EXECUTE FUNCTION enforce_batch_transition();
CREATE TRIGGER recovery_transition BEFORE INSERT OR UPDATE ON recovery_event
    FOR EACH ROW EXECUTE FUNCTION enforce_recovery_transition();
CREATE TRIGGER source_touch BEFORE UPDATE ON source_config
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
CREATE TRIGGER run_touch BEFORE UPDATE ON ingestion_run
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
CREATE TRIGGER batch_touch BEFORE UPDATE ON batch_manifest
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
CREATE TRIGGER checkpoint_touch BEFORE UPDATE ON checkpoint
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
CREATE TRIGGER recovery_touch BEFORE UPDATE ON recovery_event
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

CREATE INDEX run_source_created_idx ON ingestion_run(source_id, created_at DESC);
CREATE INDEX run_active_idx ON ingestion_run(status, updated_at)
    WHERE status IN ('created', 'running', 'failed');
CREATE INDEX batch_run_idx ON batch_manifest(run_id);
CREATE INDEX batch_status_idx ON batch_manifest(status, created_at);
CREATE INDEX validation_status_idx ON validation_result(status, created_at);
CREATE INDEX recovery_run_idx ON recovery_event(run_id, created_at DESC);
CREATE INDEX recovery_pending_idx ON recovery_event(status, updated_at)
    WHERE status IN ('pending', 'running');
