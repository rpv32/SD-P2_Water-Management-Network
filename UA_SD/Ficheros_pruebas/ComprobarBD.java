import java.sql.*;

public class ComprobarBD {

    public static void main(String[] args) {
        if (args.length != 5) {
            System.out.println("DB_CHECK_FAIL: argumentos incorrectos");
            return;
        }

        String stationId = args[0];
        String monitorEsperado = args[1];
        String saludEsperada = args[2];
        String estadoEsperado = args[3];
        String incidenciasAbiertasEsperadas = args[4];

        try {
            Class.forName("org.sqlite.JDBC");

            try (Connection c = DriverManager.getConnection("jdbc:sqlite:wm.db")) {
                int monitor = -1;
                String salud = null;
                String estado = null;
                int incidencias = -1;

                String sqlEstado =
                    "SELECT monitor_connected, engine_health, visible_status "
                  + "FROM station_runtime_state WHERE station_id = ?";

                try (PreparedStatement ps = c.prepareStatement(sqlEstado)) {
                    ps.setString(1, stationId);

                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            monitor = rs.getInt("monitor_connected");
                            salud = rs.getString("engine_health");
                            estado = rs.getString("visible_status");
                        }
                    }
                }

                String sqlIncidencias =
                    "SELECT COUNT(*) FROM station_incidents "
                  + "WHERE station_id = ? AND status = 'OPEN'";

                try (PreparedStatement ps = c.prepareStatement(sqlIncidencias)) {
                    ps.setString(1, stationId);

                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            incidencias = rs.getInt(1);
                        }
                    }
                }

                boolean ok =
                    String.valueOf(monitor).equals(monitorEsperado)
                    && saludEsperada.equals(salud)
                    && estadoEsperado.equals(estado)
                    && String.valueOf(incidencias).equals(incidenciasAbiertasEsperadas);

                System.out.println(
                    "DB_STATE station=" + stationId
                    + " monitor=" + monitor
                    + " health=" + salud
                    + " status=" + estado
                    + " openIncidents=" + incidencias
                );

                if (ok) {
                    System.out.println("DB_CHECK_OK");
                } else {
                    System.out.println(
                        "DB_CHECK_FAIL esperado monitor=" + monitorEsperado
                        + " health=" + saludEsperada
                        + " status=" + estadoEsperado
                        + " openIncidents=" + incidenciasAbiertasEsperadas
                    );
                }
            }

        } catch (Exception e) {
            System.out.println("DB_CHECK_FAIL: " + e.toString());
        }
    }
}