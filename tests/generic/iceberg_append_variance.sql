{% test iceberg_append_variance(model) %}

    {% set model_name = model | string %}
    {% set parts = model_name.rsplit('"', 1) %}
    {% set snapshots = parts[0] + '$snapshots"' %}
    {% set manifests = parts[0] + '$manifests"' %}

    WITH row_count AS (
        SELECT      row_number() OVER (order by committed_at desc) as rn,
                    added_rows_count
        FROM        {{ snapshots }} s
        JOIN        {{ manifests }} m
                    ON s.snapshot_id = m.added_snapshot_id
        ORDER BY    committed_at desc
        LIMIT       2
    ), latest_row_count AS (
        SELECT      added_rows_count AS current,
                    CAST(COALESCE(LEAD(added_rows_count) OVER(ORDER BY rn),added_rows_count) AS DECIMAL(10,2)) as initial
        FROM        row_count
        ORDER BY    rn
        LIMIT       1
    ), percentage_change AS (
        SELECT      (100.00 * (current - initial)) / initial AS change
        FROM        latest_row_count
    )
    SELECT  change,
            n AS generated_record
    FROM    percentage_change
	CROSS JOIN UNNEST(sequence(1, cast(change as int))) AS t(n)

{% endtest %}