import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Instant;
import java.util.UUID;

/*
 * Unica clase que contiene SQL. El resto del sistema (HiloWM, WM_Central...)
 * solo llama a sus metodos.
 *
 * Una sola Connection compartida por todos los hilos: por eso todos los
 * metodos publicos son synchronized (SQLite solo admite un escritor a la vez).
 */
public class DataBaseManager {

    private Connection conexion;

    // ============ 1. INFRAESTRUCTURA ============

    public DataBaseManager(String rutaDb) throws SQLException, IOException {
        conexion = DriverManager.getConnection("jdbc:sqlite:" + rutaDb);   // crea wm.db si no existe

        try (Statement st = conexion.createStatement()) {
            st.execute("PRAGMA foreign_keys = ON");                        // hay que activarlo en cada conexion
        }
        crearEsquema();
        asegurarEstadoInicial();
    }

    // Ejecuta schema.sql (cada sentencia por separado). Se puede repetir en cada arranque
    // porque usa CREATE ... IF NOT EXISTS e INSERT OR IGNORE.
    private void crearEsquema() throws SQLException, IOException {
        String script;
        try (InputStream in = DataBaseManager.class.getResourceAsStream("/schema.sql")) {
            if (in == null) {
                throw new IOException("No se encuentra schema.sql dentro del jar");
            }
            script = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }

        for (String trozo : script.split(";")) {
            String sentencia = trozo.trim();
            if (!sentencia.isEmpty()) {
                try (Statement st = conexion.createStatement()) {
                    st.execute(sentencia);
                }
            }
        }
    }

    // Si alguien anade una estacion a "stations" sin fila de estado, se la crea aqui.
    private void asegurarEstadoInicial() throws SQLException {
        update("INSERT OR IGNORE INTO station_runtime_state "
             + "(station_id, visible_status, monitor_connected, engine_health, blocked, "
             + " current_flow_lpm, current_volume_litres, last_status_change_at, updated_at) "
             + "SELECT station_id, 'DISCONNECTED', 0, 'UNKNOWN', 0, 0, 0, ?, ? FROM stations",
             ahora(), ahora());
    }

    public synchronized void cerrar() {
        try {
            conexion.close();
        } catch (SQLException e) {
            System.out.println("Error BD (cerrar): " + e.getMessage());
        }
    }

    // ============ AUXILIARES PRIVADOS ============

    private String ahora() {
        return Instant.now().toString();
    }

    // Ejecuta un INSERT/UPDATE/DELETE. Los parametros son siempre Strings y null se guarda como NULL.
    private int update(String sql, String... params) throws SQLException {
        try (PreparedStatement ps = conexion.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                if (params[i] == null) {
                    ps.setNull(i + 1, Types.VARCHAR);
                } else {
                    ps.setString(i + 1, params[i]);
                }
            }
            return ps.executeUpdate();
        }
    }

    // Devuelve true si el SELECT devuelve al menos una fila.
    private boolean existe(String sql, String... params) throws SQLException {
        try (PreparedStatement ps = conexion.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                ps.setString(i + 1, params[i]);
            }
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    // Transacciones: todo lo que ocurre entre comenzar() y confirmar() se guarda junto o no se guarda.
    private void comenzar() throws SQLException {
        conexion.setAutoCommit(false);
    }

    private void confirmar() throws SQLException {
        conexion.commit();
        conexion.setAutoCommit(true);
    }

    private void deshacer() {
        try {
            conexion.rollback();
            conexion.setAutoCommit(true);
        } catch (SQLException e) {
            System.out.println("Error BD (rollback): " + e.getMessage());
        }
    }

    // Convierte "" en null (el Monitor manda "" cuando no hay sesion y en la BD debe ir NULL).
    private String vacioANull(String s) {
        if (s == null || s.isEmpty()) {
            return null;
        }
        return s;
    }

    // ============ 2. LECTURAS ============

    public synchronized boolean existeEstacion(String stationId) {
        try {
            return existe("SELECT 1 FROM stations WHERE station_id = ? AND active = 1", stationId);
        } catch (SQLException e) {
            System.out.println("Error BD (existeEstacion): " + e.getMessage());
            return false;
        }
    }

    public synchronized boolean existeOperador(String operatorId) {
        try {
            return existe("SELECT 1 FROM operators WHERE operator_id = ? AND active = 1", operatorId);
        } catch (SQLException e) {
            System.out.println("Error BD (existeOperador): " + e.getMessage());
            return false;
        }
    }

    public synchronized void listarEstaciones() {
        String sql = "SELECT s.station_id, s.location_name, r.visible_status "
                   + "FROM stations s JOIN station_runtime_state r ON r.station_id = s.station_id "
                   + "ORDER BY s.station_id";
        try (PreparedStatement ps = conexion.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                System.out.println(rs.getString("station_id") + " | "
                                 + rs.getString("location_name") + " | "
                                 + rs.getString("visible_status"));
            }
        } catch (SQLException e) {
            System.out.println("Error BD (listarEstaciones): " + e.getMessage());
        }
    }

    // Devuelve el estado visible o null si la estacion no existe.
    public synchronized String obtenerEstadoVisible(String stationId) {
        String sql = "SELECT visible_status FROM station_runtime_state WHERE station_id = ?";
        try (PreparedStatement ps = conexion.prepareStatement(sql)) {
            ps.setString(1, stationId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(1);
                }
                return null;
            }
        } catch (SQLException e) {
            System.out.println("Error BD (obtenerEstadoVisible): " + e.getMessage());
            return null;
        }
    }

    // ============ 3. ESTADO DE LA ESTACION ============

    // Al arrancar Central: ningun Monitor esta conectado todavia. Se conserva el bloqueo.
    public synchronized void marcarTodasDesconectadas() {
        String t = ahora();
        try {
            update("UPDATE station_runtime_state "
                 + "SET monitor_connected = 0, visible_status = 'DISCONNECTED', "
                 + "    engine_health = 'UNKNOWN', last_status_change_at = ?, updated_at = ?",
                 t, t);
        } catch (SQLException e) {
            System.out.println("Error BD (marcarTodasDesconectadas): " + e.getMessage());
        }
    }

    public synchronized void marcarMonitorConectado(String stationId) {
        String t = ahora();
        try {
            comenzar();
            update("UPDATE station_runtime_state "
                 + "SET monitor_connected = 1, last_monitor_heartbeat_at = ?, updated_at = ? "
                 + "WHERE station_id = ?",
                 t, t, stationId);
            recalcularEstadoVisible(stationId);
            confirmar();
        } catch (SQLException e) {
            deshacer();
            System.out.println("Error BD (marcarMonitorConectado): " + e.getMessage());
        }
    }

    // Al perder el Monitor, la salud del Engine pasa a UNKNOWN: al reconectar, el Monitor
    // volvera a notificar su estado (RECOVERY o INCIDENT) porque empieza en UNKNOWN.
    public synchronized void marcarMonitorDesconectado(String stationId) {
        String t = ahora();
        try {
            comenzar();
            update("UPDATE station_runtime_state "
                 + "SET monitor_connected = 0, engine_health = 'UNKNOWN', updated_at = ? "
                 + "WHERE station_id = ?",
                 t, stationId);
            recalcularEstadoVisible(stationId);
            confirmar();
        } catch (SQLException e) {
            deshacer();
            System.out.println("Error BD (marcarMonitorDesconectado): " + e.getMessage());
        }
    }

    // Guardo la salud comunicada por el Monitor y recalculo el estado visible.
    // La fecha indica cuando Central ha recibido el heartbeat.
    public synchronized void registrarHeartbeat(String stationId, String engineHealth) throws SQLException {

        if (!"OK".equals(engineHealth) && !"KO".equals(engineHealth) && !"UNKNOWN".equals(engineHealth)) {
            throw new IllegalArgumentException("Estado del Engine no valido");
        }
        String t = ahora();
        try {
            comenzar();

            int filas = update(
                    "UPDATE station_runtime_state "
                    + "SET last_monitor_heartbeat_at = ?, "
                    + "    engine_health = ?, updated_at = ? "
                    + "WHERE station_id = ?",
                    t, engineHealth, t, stationId);

            if (filas != 1) {
                throw new SQLException("La estacion no tiene estado persistente: " + stationId);
            }

            recalcularEstadoVisible(stationId);
            confirmar();
        } catch (SQLException e) {
            deshacer();
            throw e;
        }
    }



    // Aplica la prioridad de estados.md. Se llama dentro de las transacciones de los demas metodos.
    private void recalcularEstadoVisible(String stationId) throws SQLException {
        String sql = "SELECT monitor_connected, blocked, engine_health, active_session_id, visible_status "
                + "FROM station_runtime_state WHERE station_id = ?";

        int monitor;
        int bloqueada;
        String salud;
        String sesion;
        String actual;

        try (PreparedStatement ps = conexion.prepareStatement(sql)) {
            ps.setString(1, stationId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return;                                   // estacion sin fila de estado
                }
                monitor = rs.getInt("monitor_connected");
                bloqueada = rs.getInt("blocked");
                salud = rs.getString("engine_health");
                sesion = rs.getString("active_session_id");   // null si no hay riego
                actual = rs.getString("visible_status");
            }
        }

        String nuevo;
        if (monitor == 0) {
            nuevo = "DISCONNECTED";
        } else if (bloqueada == 1) {
            nuevo = "OUT_OF_SERVICE";
        } else if (salud.equals("KO")) {
            nuevo = "LEAK";
        } else if (salud.equals("UNKNOWN")) {
            nuevo = "DISCONNECTED";       // Monitor conectado pero el Engine aun no ha confirmado OK
        } else if (sesion != null && existe("SELECT 1 FROM watering_sessions "+ "WHERE session_id = ? AND status = 'RUNNING'",sesion)) {
            nuevo = "WATERING";
        } else {
            nuevo = "AVAILABLE";
        }

        if (!nuevo.equals(actual)) {
            String t = ahora();
            update("UPDATE station_runtime_state "
                 + "SET visible_status = ?, last_status_change_at = ?, updated_at = ? "
                 + "WHERE station_id = ?",
                 nuevo, t, t, stationId);
            System.out.println("BD: " + stationId + " " + actual + " -> " + nuevo);
        }
    }

    // ============ 4. INCIDENCIAS ============

    public synchronized void abrirIncidencia(String stationId, String causa,
                                             String sessionId, String messageId) {
        String t = ahora();
        try {
            comenzar();

            // La sesion solo se guarda si Central la conoce (FK). Si no, NULL.
            String sesion = vacioANull(sessionId);
            if (sesion != null
                && !existe("SELECT 1 FROM watering_sessions WHERE session_id = ?", sesion)) {
                sesion = null;
            }

            // Una sola incidencia abierta por estacion
            if (!existe("SELECT 1 FROM station_incidents WHERE station_id = ? AND status = 'OPEN'",
                        stationId)) {
                update("INSERT INTO station_incidents "
                     + "(message_id, station_id, session_id, cause, status, detected_at, created_at, updated_at) "
                     + "VALUES (?, ?, ?, ?, 'OPEN', ?, ?, ?)",
                     vacioANull(messageId), stationId, sesion, causa, t, t, t);
            }

            update("UPDATE station_runtime_state "
                 + "SET engine_health = 'KO', incident_cause = ?, last_incident_at = ?, updated_at = ? "
                 + "WHERE station_id = ?",
                 causa, t, t, stationId);

            recalcularEstadoVisible(stationId);
            confirmar();
        } catch (SQLException e) {
            deshacer();
            System.out.println("Error BD (abrirIncidencia): " + e.getMessage());
        }
    }

    // Tambien vale si no hay incidencia abierta (primer OK tras arrancar): el primer UPDATE toca 0 filas.
    public synchronized void cerrarIncidencia(String stationId) {
        String t = ahora();
        try {
            comenzar();

            update("UPDATE station_incidents "
                 + "SET status = 'RESOLVED', resolved_at = ?, updated_at = ? "
                 + "WHERE station_id = ? AND status = 'OPEN'",
                 t, t, stationId);

            update("UPDATE station_runtime_state "
                 + "SET engine_health = 'OK', incident_cause = NULL, last_recovery_at = ?, updated_at = ? "
                 + "WHERE station_id = ?",
                 t, t, stationId);

            recalcularEstadoVisible(stationId);
            confirmar();
        } catch (SQLException e) {
            deshacer();
            System.out.println("Error BD (cerrarIncidencia): " + e.getMessage());
        }
    }

    // ============ 5. BLOQUEO (ordenes de Central) ============

    public synchronized void bloquearEstacion(String stationId) {
        cambiarBloqueo(stationId, 1);
    }

    public synchronized void activarEstacion(String stationId) {
        cambiarBloqueo(stationId, 0);
    }

    private void cambiarBloqueo(String stationId, int valor) {
        String t = ahora();
        try {
            comenzar();
            update("UPDATE station_runtime_state SET blocked = " + valor + ", updated_at = ? "
                 + "WHERE station_id = ?",
                 t, stationId);
            recalcularEstadoVisible(stationId);
            confirmar();
        } catch (SQLException e) {
            deshacer();
            System.out.println("Error BD (cambiarBloqueo): " + e.getMessage());
        }
    }

    public record DecisionRiego(boolean autorizada,String sessionId,String codigo,String mensaje) { }

    public synchronized DecisionRiego procesarSolicitud(String requestId,String stationId,String operatorId,int duracion,String requestedAt) throws SQLException {
        if (duracion <= 0) {
            throw new IllegalArgumentException("Duracion no valida");
        }

        try{
            comenzar();

            String sql = "SELECT station_id, operator_id,requested_duration_seconds, authorized_session_id,denial_code, denial_message FROM watering_requests WHERE request_id = ?";

            try(PreparedStatement ps = conexion.prepareStatement(sql)){
                ps.setString(1, requestId);

                try(ResultSet rs = ps.executeQuery()){
                    if(rs.next()){
                        if (!stationId.equals(rs.getString("station_id")) || !operatorId.equals(rs.getString("operator_id")) || duracion != rs.getInt("requested_duration_seconds")) {
                            throw new IllegalArgumentException("requestId repetido con datos diferentes");
                        }

                        String sessionId = rs.getString("authorized_session_id");

                        
                        DecisionRiego decision = new DecisionRiego(sessionId != null,sessionId,rs.getString("denial_code"),
                            sessionId != null
                                    ? "Solicitud ya autorizada"
                                    : rs.getString("denial_message"));
                        confirmar();
                        return decision;
                    }
                }
            }

            if(!existe( "SELECT 1 FROM operators WHERE operator_id = ? AND active = 1",operatorId)){
                confirmar();
                return new DecisionRiego(false, null, "UNKNOWN_OPERATOR","El operario no existe o esta inactivo");
            }

            if (!existe("SELECT 1 FROM stations WHERE station_id = ? AND active = 1",stationId)) {
                confirmar();
                return new DecisionRiego(false, null, "UNKNOWN_STATION","La estacion no existe o esta inactiva");
            }

            String t = ahora();
            String sessionId = UUID.randomUUID().toString();

            int reservadas = update(
                "UPDATE station_runtime_state "
                + "SET active_session_id = ?, current_operator_id = ?, "
                + "current_flow_lpm = 0, current_volume_litres = 0, updated_at = ? "
                + "WHERE station_id = ? "
                + "AND monitor_connected = 1 AND engine_health = 'OK' "
                + "AND blocked = 0 AND visible_status = 'AVAILABLE' "
                + "AND active_session_id IS NULL "
                + "AND NOT EXISTS ("
                + " SELECT 1 FROM station_incidents "
                + " WHERE station_id = ? AND status = 'OPEN')",
                sessionId, operatorId, t, stationId, stationId);
            
            if(reservadas==0){
                update(
                    "INSERT INTO watering_requests "
                    + "(request_id, station_id, operator_id, "
                    + "requested_duration_seconds, source, status, "
                    + "denial_code, denial_message, requested_at, decided_at, "
                    + "created_at, updated_at) "
                    + "VALUES (?, ?, ?, ?, 'FO', 'DENIED', ?, ?, ?, ?, ?, ?)",
                    requestId, stationId, operatorId,
                    String.valueOf(duracion),
                    "STATION_NOT_AVAILABLE",
                    "La estacion no esta disponible o ya esta reservada",
                    requestedAt, t, t, t);

                confirmar();

                return new DecisionRiego(false, null, "STATION_NOT_AVAILABLE","La estacion no esta disponible o ya esta reservada");
            }

            update(
                "INSERT INTO watering_requests "
                + "(request_id, station_id, operator_id, "
                + "requested_duration_seconds, source, status, "
                + "requested_at, decided_at, created_at, updated_at) "
                + "VALUES (?, ?, ?, ?, 'FO', 'AUTHORIZED', ?, ?, ?, ?)",
                requestId, stationId, operatorId,
                String.valueOf(duracion), requestedAt, t, t, t);
            
            update(
                "INSERT INTO watering_sessions "
                + "(session_id, request_id, station_id, operator_id, "
                + "planned_duration_seconds, status, created_at, updated_at) "
                + "VALUES (?, ?, ?, ?, ?, 'AUTHORIZED', ?, ?)",
                sessionId, requestId, stationId, operatorId,
                String.valueOf(duracion), t, t);
            
            update(
                "UPDATE watering_requests SET authorized_session_id = ? "
                + "WHERE request_id = ?",
                sessionId, requestId);

            confirmar();

            return new DecisionRiego(true, sessionId, null,"Solicitud autorizada; pendiente de inicio del Engine");

        }catch(SQLException | IllegalArgumentException e){
            deshacer();
            throw e;
        }
    }

    private String comprobarSesion(
        String sessionId, String requestId,
        String stationId, String operatorId) throws SQLException {

        String sql = "SELECT status FROM watering_sessions "
                + "WHERE session_id = ? AND request_id = ? "
                + "AND station_id = ? AND operator_id = ?";

        try (PreparedStatement ps = conexion.prepareStatement(sql)) {
            ps.setString(1, sessionId);
            ps.setString(2, requestId);
            ps.setString(3, stationId);
            ps.setString(4, operatorId);

            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalArgumentException(
                            "El evento no corresponde con una sesion conocida");
                }
                return rs.getString("status");
            }
        }
    }

    private boolean terminada(String estado) {
        return "FINISHED".equals(estado)
                || "INTERRUPTED".equals(estado);
    }

    public synchronized boolean sesionPendiente(String sessionId)
        throws SQLException {

        return existe(
            "SELECT 1 FROM watering_sessions "
            + "WHERE session_id = ? AND status = 'AUTHORIZED'",
            sessionId);
    }

    public synchronized boolean registrarInicio(
        String sessionId, String requestId,
        String stationId, String operatorId,
        String fecha) throws SQLException {

        try {
            comenzar();

            String estado = comprobarSesion(
                    sessionId, requestId, stationId, operatorId);

            if (terminada(estado)) {
                confirmar();
                return false;
            }

            if (!existe(
                    "SELECT 1 FROM station_runtime_state "
                    + "WHERE station_id = ? AND active_session_id = ?",
                    stationId, sessionId)) {
                throw new IllegalArgumentException("La sesion no esta reservada");
            }

            String t = ahora();

            update(
                    "UPDATE watering_sessions "
                    + "SET status = 'RUNNING', "
                    + "started_at = COALESCE(started_at, ?), updated_at = ? "
                    + "WHERE session_id = ? AND status = 'AUTHORIZED'",
                    fecha, t, sessionId);

            update(
                    "UPDATE watering_requests "
                    + "SET status = 'IN_PROGRESS', updated_at = ? "
                    + "WHERE request_id = ? AND status = 'AUTHORIZED'",
                    t, requestId);

            recalcularEstadoVisible(stationId);
            confirmar();
            return true;

        } catch (SQLException | IllegalArgumentException e) {
            deshacer();
            throw e;
        }
    }

    public synchronized boolean registrarTelemetria(
        String eventId, String sessionId, String requestId,
        String stationId, String operatorId,
        int secuencia, double caudal, double volumen,
        String fecha) throws SQLException {

        if (secuencia <= 0
                || !Double.isFinite(caudal) || caudal < 0
                || !Double.isFinite(volumen) || volumen < 0) {
            throw new IllegalArgumentException("Telemetria no valida");
        }

        try {
            comenzar();

            String estado = comprobarSesion(
                    sessionId, requestId, stationId, operatorId);

            if (terminada(estado)) {
                confirmar();
                return false;
            }

            String t = ahora();

            update(
                    "INSERT INTO telemetry_samples "
                    + "(event_id, session_id, request_id, station_id, operator_id, "
                    + "sequence, flow_litres_per_minute, volume_litres, "
                    + "occurred_at, created_at) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                    + "ON CONFLICT(event_id) DO NOTHING",
                    eventId, sessionId, requestId, stationId, operatorId,
                    String.valueOf(secuencia), String.valueOf(caudal),
                    String.valueOf(volumen), fecha, t);

            update(
                    "UPDATE station_runtime_state "
                    + "SET current_flow_lpm = ?, current_volume_litres = ?, "
                    + "updated_at = ? "
                    + "WHERE station_id = ? AND active_session_id = ? "
                    + "AND ? = (SELECT MAX(sequence) FROM telemetry_samples "
                    + "WHERE session_id = ?)",
                    String.valueOf(caudal), String.valueOf(volumen),
                    t, stationId, sessionId,
                    String.valueOf(secuencia), sessionId);

            confirmar();
            return true;

        } catch (SQLException | IllegalArgumentException e) {
            deshacer();
            throw e;
        }
    }

    public synchronized boolean registrarFin(
        String sessionId, String requestId,
        String stationId, String operatorId,
        String motivo, int duracion, double volumen,
        String fecha) throws SQLException {

        if (duracion < 0
                || !Double.isFinite(volumen) || volumen < 0
                || !java.util.Set.of(
                        "TIME_LIMIT", "MANUAL_STOP", "LEAK", "BLOCKED",
                        "ENGINE_KO", "ENGINE_TIMEOUT", "CENTRAL_DOWN")
                        .contains(motivo)) {
            throw new IllegalArgumentException("Resultado no valido");
        }

        try {
            comenzar();

            String estado = comprobarSesion(
                    sessionId, requestId, stationId, operatorId);

            // Permite reenviar al FO un resultado ya guardado.
            if (existe(
                    "SELECT 1 FROM watering_results WHERE session_id = ?",
                    sessionId)) {
                confirmar();
                return true;
            }

            if (terminada(estado)) {
                confirmar();
                return false;
            }

            String t = ahora();
            boolean normal = "TIME_LIMIT".equals(motivo)
                    || "MANUAL_STOP".equals(motivo);

            update(
                    "INSERT INTO watering_results "
                    + "(session_id, request_id, station_id, operator_id, reason, "
                    + "duration_seconds, volume_litres, finished_at, created_at) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    sessionId, requestId, stationId, operatorId, motivo,
                    String.valueOf(duracion), String.valueOf(volumen), fecha, t);

            update(
                    "UPDATE watering_sessions SET status = ?, "
                    + "actual_duration_seconds = ?, finished_at = ?, "
                    + "finish_reason = ?, final_volume_litres = ?, updated_at = ? "
                    + "WHERE session_id = ?",
                    normal ? "FINISHED" : "INTERRUPTED",
                    String.valueOf(duracion), fecha, motivo,
                    String.valueOf(volumen), t, sessionId);

            update(
                    "UPDATE watering_requests "
                    + "SET status = ?, finished_at = ?, updated_at = ? "
                    + "WHERE request_id = ?",
                    normal ? "FINISHED" : "FAILED", fecha, t, requestId);

            update(
                    "UPDATE station_runtime_state "
                    + "SET active_session_id = NULL, current_operator_id = NULL, "
                    + "current_flow_lpm = 0, current_volume_litres = ?, "
                    + "updated_at = ? "
                    + "WHERE station_id = ? AND active_session_id = ?",
                    String.valueOf(volumen), t, stationId, sessionId);

            recalcularEstadoVisible(stationId);
            confirmar();
            return true;

        } catch (SQLException | IllegalArgumentException e) {
            deshacer();
            throw e;
        }
    }

    public synchronized String listarEstacionesJson() {
        StringBuilder json = new StringBuilder("[");

        String sql = """
            SELECT s.station_id, s.location_name, r.visible_status,
                r.monitor_connected, r.engine_health,
                r.current_flow_lpm, r.current_volume_litres
            FROM stations s
            JOIN station_runtime_state r ON r.station_id = s.station_id
            ORDER BY s.station_id
            """;

        try (PreparedStatement ps = conexion.prepareStatement(sql);
            ResultSet rs = ps.executeQuery()) {

            boolean primero = true;

            while (rs.next()) {
                if (!primero) {
                    json.append(",");
                }
                primero = false;

                json.append("{")
                    .append("\"stationId\":\"").append(rs.getString("station_id")).append("\",")
                    .append("\"location\":\"").append(rs.getString("location_name")).append("\",")
                    .append("\"status\":\"").append(rs.getString("visible_status")).append("\",")
                    .append("\"monitorConnected\":").append(rs.getInt("monitor_connected")).append(",")
                    .append("\"engineHealth\":\"").append(rs.getString("engine_health")).append("\",")
                    .append("\"flow\":").append(rs.getDouble("current_flow_lpm")).append(",")
                    .append("\"volume\":").append(rs.getDouble("current_volume_litres"))
                    .append("}");
            }
        } catch (SQLException e) {
            System.out.println("Error BD listarEstacionesJson: " + e.getMessage());
        }

        return json.append("]").toString();
    }
    
}
