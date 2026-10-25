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
        } else if (sesion != null) {
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
}
