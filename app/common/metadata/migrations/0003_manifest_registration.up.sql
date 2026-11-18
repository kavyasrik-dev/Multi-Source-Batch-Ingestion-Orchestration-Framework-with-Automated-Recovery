ALTER TABLE batch_manifest
    ADD COLUMN logical_date date,
    ADD COLUMN content_checksum_sha256 text,
    ADD COLUMN content_bytes bigint,
    ADD COLUMN manifest_uri text,
    ADD COLUMN manifest_checksum_sha256 text,
    ADD CONSTRAINT manifest_content_checksum_format CHECK (
        content_checksum_sha256 IS NULL
        OR content_checksum_sha256 ~ '^[0-9a-f]{64}$'
    ),
    ADD CONSTRAINT manifest_checksum_format CHECK (
        manifest_checksum_sha256 IS NULL
        OR manifest_checksum_sha256 ~ '^[0-9a-f]{64}$'
    ),
    ADD CONSTRAINT manifest_bytes_nonnegative CHECK (
        content_bytes IS NULL OR content_bytes >= 0
    ),
    ADD CONSTRAINT manifest_registration_complete CHECK (
        (logical_date IS NULL
            AND content_checksum_sha256 IS NULL
            AND content_bytes IS NULL
            AND manifest_uri IS NULL
            AND manifest_checksum_sha256 IS NULL)
        OR
        (logical_date IS NOT NULL
            AND content_checksum_sha256 IS NOT NULL
            AND content_bytes IS NOT NULL
            AND manifest_uri IS NOT NULL
            AND btrim(manifest_uri) <> ''
            AND manifest_checksum_sha256 IS NOT NULL)
    ),
    ADD CONSTRAINT batch_manifest_uri_unique UNIQUE (manifest_uri);
