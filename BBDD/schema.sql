PRAGMA foreign_keys = ON;
 
CREATE TABLE IF NOT EXISTS operators (
    operator_id TEXT PRIMARY KEY,
    full_name TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
 
CREATE TABLE IF NOT EXISTS stations (
    station_id TEXT PRIMARY KEY,
    location_name TEXT NOT NULL,
    zone_name TEXT,
    description TEXT,
    active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
 
CREATE TABLE IF NOT EXISTS station_runtime_state (
    station_id TEXT PRIMARY KEY,
    visible_status TEXT NOT NULL CHECK (
        visible_status IN (
            'AVAILABLE',
            'WATERING',
            'LEAK',
            'OUT_OF_SERVICE',
            'DISCONNECTED'
        )
    ),
    monitor_connected INTEGER NOT NULL DEFAULT 0 CHECK (monitor_connected IN (0, 1)),
    engine_health TEXT NOT NULL DEFAULT 'UNKNOWN' CHECK (
        engine_health IN ('OK', 'KO', 'UNKNOWN')
    ),
    blocked INTEGER NOT NULL DEFAULT 0 CHECK (blocked IN (0, 1)),
    incident_cause TEXT,
    active_session_id TEXT,
    current_operator_id TEXT,
    current_flow_lpm REAL NOT NULL DEFAULT 0,
    current_volume_litres REAL NOT NULL DEFAULT 0,
    last_monitor_heartbeat_at TEXT,
    last_engine_check_at TEXT,
    last_incident_at TEXT,
    last_recovery_at TEXT,
    last_status_change_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (station_id) REFERENCES stations(station_id),
    FOREIGN KEY (current_operator_id) REFERENCES operators(operator_id)
);
 
CREATE TABLE IF NOT EXISTS watering_requests (
    request_id TEXT PRIMARY KEY,
    station_id TEXT NOT NULL,
    operator_id TEXT NOT NULL,
    requested_duration_seconds INTEGER NOT NULL CHECK (requested_duration_seconds > 0),
    source TEXT NOT NULL CHECK (
        source IN ('FO', 'CENTRAL', 'WS_MANUAL')
    ),
    status TEXT NOT NULL CHECK (
        status IN (
            'RECEIVED',
            'AUTHORIZED',
            'DENIED',
            'IN_PROGRESS',
            'FINISHED',
            'CANCELLED',
            'FAILED'
        )
    ),
    denial_code TEXT,
    denial_message TEXT,
    authorized_session_id TEXT,
    requested_at TEXT NOT NULL,
    decided_at TEXT,
    finished_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (station_id) REFERENCES stations(station_id),
    FOREIGN KEY (operator_id) REFERENCES operators(operator_id),
    FOREIGN KEY (authorized_session_id) REFERENCES watering_sessions(session_id)
);
 
CREATE TABLE IF NOT EXISTS watering_sessions (
    session_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL UNIQUE,
    station_id TEXT NOT NULL,
    operator_id TEXT NOT NULL,
    planned_duration_seconds INTEGER NOT NULL CHECK (planned_duration_seconds > 0),
    actual_duration_seconds INTEGER,
    status TEXT NOT NULL CHECK (
        status IN (
            'AUTHORIZED',
            'RUNNING',
            'FINISHED',
            'INTERRUPTED'
        )
    ),
    started_at TEXT,
    finished_at TEXT,
    finish_reason TEXT CHECK (
        finish_reason IN (
            'TIME_LIMIT',
            'MANUAL_STOP',
            'LEAK',
            'BLOCKED',
            'ENGINE_KO',
            'ENGINE_TIMEOUT',
            'CENTRAL_DOWN'
        )
    ),
    final_volume_litres REAL,
    final_average_flow_lpm REAL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (request_id) REFERENCES watering_requests(request_id),
    FOREIGN KEY (station_id) REFERENCES stations(station_id),
    FOREIGN KEY (operator_id) REFERENCES operators(operator_id)
);
 
CREATE TABLE IF NOT EXISTS telemetry_samples (
    telemetry_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    session_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    station_id TEXT NOT NULL,
    operator_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence > 0),
    flow_litres_per_minute REAL NOT NULL CHECK (flow_litres_per_minute >= 0),
    volume_litres REAL NOT NULL CHECK (volume_litres >= 0),
    occurred_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (session_id) REFERENCES watering_sessions(session_id),
    FOREIGN KEY (request_id) REFERENCES watering_requests(request_id),
    FOREIGN KEY (station_id) REFERENCES stations(station_id),
    FOREIGN KEY (operator_id) REFERENCES operators(operator_id)
);
 
CREATE TABLE IF NOT EXISTS watering_results (
    session_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL UNIQUE,
    station_id TEXT NOT NULL,
    operator_id TEXT NOT NULL,
    reason TEXT NOT NULL CHECK (
        reason IN (
            'TIME_LIMIT',
            'MANUAL_STOP',
            'LEAK',
            'BLOCKED',
            'ENGINE_KO',
            'ENGINE_TIMEOUT',
            'CENTRAL_DOWN'
        )
    ),
    duration_seconds INTEGER NOT NULL CHECK (duration_seconds >= 0),
    volume_litres REAL NOT NULL CHECK (volume_litres >= 0),
    finished_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (session_id) REFERENCES watering_sessions(session_id),
    FOREIGN KEY (request_id) REFERENCES watering_requests(request_id),
    FOREIGN KEY (station_id) REFERENCES stations(station_id),
    FOREIGN KEY (operator_id) REFERENCES operators(operator_id)
);
 
CREATE TABLE IF NOT EXISTS station_incidents (
    incident_id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id TEXT,
    station_id TEXT NOT NULL,
    session_id TEXT,
    cause TEXT NOT NULL CHECK (
        cause IN ('ENGINE_KO', 'ENGINE_TIMEOUT', 'LEAK_SENSOR')
    ),
    status TEXT NOT NULL CHECK (
        status IN ('OPEN', 'RESOLVED')
    ),
    detected_at TEXT NOT NULL,
    resolved_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (station_id) REFERENCES stations(station_id),
    FOREIGN KEY (session_id) REFERENCES watering_sessions(session_id)
);
 
CREATE TABLE IF NOT EXISTS station_commands (
    command_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    station_id TEXT NOT NULL,
    operator_id TEXT,
    request_id TEXT,
    session_id TEXT,
    command_type TEXT NOT NULL CHECK (
        command_type IN (
            'START_WATERING',
            'STOP_WATERING',
            'BLOCK_STATION',
            'ACTIVATE_STATION'
        )
    ),
    payload_json TEXT,
    command_status TEXT NOT NULL CHECK (
        command_status IN (
            'PENDING',
            'SENT',
            'ACKNOWLEDGED',
            'FAILED'
        )
    ),
    occurred_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (station_id) REFERENCES stations(station_id),
    FOREIGN KEY (operator_id) REFERENCES operators(operator_id),
    FOREIGN KEY (request_id) REFERENCES watering_requests(request_id),
    FOREIGN KEY (session_id) REFERENCES watering_sessions(session_id)
);
 
CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    source_component TEXT NOT NULL,
    processed_at TEXT NOT NULL
);
 
CREATE TABLE IF NOT EXISTS socket_messages (
    message_id TEXT PRIMARY KEY,
    station_id TEXT,
    direction TEXT NOT NULL CHECK (
        direction IN ('IN', 'OUT')
    ),
    peer_type TEXT NOT NULL CHECK (
        peer_type IN ('MONITOR', 'ENGINE')
    ),
    operation TEXT NOT NULL,
    payload TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    FOREIGN KEY (station_id) REFERENCES stations(station_id)
);
 
CREATE TABLE IF NOT EXISTS operator_events_outbox (
    outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    operator_id TEXT NOT NULL,
    topic_name TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    publish_status TEXT NOT NULL CHECK (
        publish_status IN ('PENDING', 'PUBLISHED', 'FAILED')
    ),
    related_request_id TEXT,
    related_session_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (operator_id) REFERENCES operators(operator_id),
    FOREIGN KEY (related_request_id) REFERENCES watering_requests(request_id),
    FOREIGN KEY (related_session_id) REFERENCES watering_sessions(session_id)
);
 
CREATE INDEX IF NOT EXISTS idx_requests_station
    ON watering_requests(station_id);
 
CREATE INDEX IF NOT EXISTS idx_requests_operator
    ON watering_requests(operator_id);
 
CREATE INDEX IF NOT EXISTS idx_sessions_station
    ON watering_sessions(station_id);
 
CREATE INDEX IF NOT EXISTS idx_sessions_operator
    ON watering_sessions(operator_id);
 
CREATE INDEX IF NOT EXISTS idx_sessions_status
    ON watering_sessions(status);
 
CREATE INDEX IF NOT EXISTS idx_telemetry_session_sequence
    ON telemetry_samples(session_id, sequence);
 
CREATE INDEX IF NOT EXISTS idx_incidents_station_status
    ON station_incidents(station_id, status);
 
CREATE INDEX IF NOT EXISTS idx_commands_station_status
    ON station_commands(station_id, command_status);
 
CREATE INDEX IF NOT EXISTS idx_outbox_operator_status
    ON operator_events_outbox(operator_id, publish_status);
 
INSERT OR IGNORE INTO operators
    (operator_id, full_name, active, created_at, updated_at)
VALUES
    ('OP-01', 'Operario 1', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('OP-02', 'Operario 2', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
 
INSERT OR IGNORE INTO stations
    (station_id, location_name, zone_name, description, active, created_at, updated_at)
VALUES
    ('WS-01', 'Parque El Palmeral', 'Zona Norte', 'Estacion principal sector norte', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('WS-02', 'Jardines de la Diputacion', 'Zona Central', 'Estacion zona central', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('WS-03', 'Plaza de los Luceros', 'Zona Sur', 'Estacion plaza principal', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
 
INSERT OR IGNORE INTO station_runtime_state
    (station_id, visible_status, monitor_connected, engine_health, blocked,
     current_flow_lpm, current_volume_litres, last_status_change_at, updated_at)
VALUES
    ('WS-01', 'DISCONNECTED', 0, 'UNKNOWN', 0, 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('WS-02', 'DISCONNECTED', 0, 'UNKNOWN', 0, 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('WS-03', 'DISCONNECTED', 0, 'UNKNOWN', 0, 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);