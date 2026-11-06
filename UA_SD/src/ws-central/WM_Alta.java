

public class WM_Alta{
    public static void main(String[] args) {
        if (args.length != 4) {
            System.err.println("Uso: WM_Alta ruta_db station|operator ID nombre");
            System.exit(1);
        }

        DataBaseManager db = null;

        try {
            db = new DataBaseManager(args[0]);

            switch (args[1]) {
                case "station":
                    db.altaEstacion(args[2], args[3]);
                    break;

                case "operator":
                    db.altaOperario(args[2], args[3]);
                    break;

                default:
                    throw new IllegalArgumentException("Tipo no valido: usa station u operator");
            }

            System.out.println("Alta comprobada correctamente: " + args[2]);

        } catch (Exception e) {
            System.err.println("Error en el alta: " + e.getMessage());
            System.exit(1);

        } finally {
            if (db != null) {
                db.cerrar();
            }
        }
    }


}