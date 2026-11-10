-- =============================================================================
-- GPMorgan Forge: TimescaleDB Hypertable & Continuous Aggregate Bootstrap
-- Portfolio 4 & 5: High-Frequency FX Tick Telemetry
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS timescaledb;

SELECT create_hypertable(
    'fx_liquidity_ticks',
    by_range('tick_time', INTERVAL '1 day'),
    if_not_exists => TRUE
);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_fx_liquidity_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', tick_time) AS bucket_hour,
    corridor_pair,
    FIRST(mid_rate, tick_time)       AS open_rate,
    MAX(mid_rate)                    AS high_rate,
    MIN(mid_rate)                    AS low_rate,
    LAST(mid_rate, tick_time)        AS close_rate,
    SUM(settlement_volume)           AS hourly_volume
FROM fx_liquidity_ticks
GROUP BY bucket_hour, corridor_pair
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'mv_fx_liquidity_hourly',
    start_offset      => INTERVAL '3 hours',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists     => TRUE
);

INSERT INTO fx_liquidity_ticks (
    tick_time, corridor_pair, bid_rate, ask_rate, mid_rate, settlement_volume
) VALUES
    ('2026-11-08 12:05:00+00', 'USD/NGN', 1644.500000, 1646.000000, 1645.250000, 250000.0000),
    ('2026-11-08 12:45:00+00', 'USD/NGN', 1648.000000, 1649.500000, 1648.750000, 500000.0000)
ON CONFLICT (tick_time, corridor_pair) DO NOTHING;
