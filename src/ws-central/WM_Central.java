import java.net.*;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class WM_Central {

	//Estaciones que tiene un Monitor conectado
	//Luego cuando se cree SQLite se guardaran en la base de datos.
	private static Set<String> estacionesConectadas = new HashSet<String>();
	private static DataBaseManager db;

	//Registra una estación como conectada, si ya estaba conectada devuelve false, si no devuelve true
	public static synchronized boolean registrarConexion(String stationId){
		if(estacionesConectadas.contains(stationId)){
			return false;
		}
		estacionesConectadas.add(stationId);
		mostrarEstacionesConectadas();

		return true;
	}

	//Elimina una estación de la lista de estaciones conectadas cuando cierra la conexión
	public static synchronized void cerrarConexion(String stationId){
		estacionesConectadas.remove(stationId);
		mostrarEstacionesConectadas();
	}

	//Muestra por pantalla las estaciones conectadas a un Monitor
	public static synchronized void mostrarEstacionesConectadas(){
		System.out.println("----------------------------------------");
		System.out.println("Estaciones conectadas:");

		if(estacionesConectadas.isEmpty()){
			System.out.println("Ninguna estación conectada");
		}else{
			for(String estacion : estacionesConectadas){
				System.out.println("- " + estacion);
			}
		}
		System.out.println("----------------------------------------");
	}

	private static void menuCentral(){
		Scanner teclado = new Scanner(System.in);

		while(true){
			System.out.println(
				"""
				----CENTRAL----
				1.Listar Estaciones
				2.Bloquear Estaciones
				3.Activar Estacion
				4.Salir
				"""
			);

			if(!teclado.hasNextLine()){
				return;
			}

			String opcion = teclado.nextLine().trim();

			switch(opcion){
				case "1" -> db.listarEstaciones();
				case "2" -> {
					System.out.print("ID de la estacion: ");
                	db.bloquearEstacion(teclado.nextLine().trim());
				}

				case "3" -> {
					System.out.print("ID de la estacion: ");
                	db.activarEstacion(teclado.nextLine().trim());
				}

				case "0" -> {
                System.out.println("Cerrando Central...");
                System.exit(0);
				}

				default -> System.out.println("Opcion no valida, eliga una opcion valida");
			}


		}

	}

	private static void iniciarWeb(int puertoWeb) {
		try {
			HttpServer servidor = HttpServer.create(new InetSocketAddress(puertoWeb), 0);

			servidor.createContext("/", intercambio -> {
				byte[] cuerpo;

				if ("/api/stations".equals(intercambio.getRequestURI().getPath())) {
					cuerpo = db.listarEstacionesJson().getBytes(StandardCharsets.UTF_8);

					intercambio.getResponseHeaders().add(
							"Content-Type", "application/json; charset=utf-8");
				} else {
					cuerpo = html().getBytes(StandardCharsets.UTF_8);

					intercambio.getResponseHeaders().add(
							"Content-Type", "text/html; charset=utf-8");
				}

				intercambio.sendResponseHeaders(200, cuerpo.length);

				try (OutputStream salida = intercambio.getResponseBody()) {
					salida.write(cuerpo);
				}
			});

			servidor.start();
			System.out.println("Web de estado disponible en el puerto " + puertoWeb);

		} catch (Exception e) {
			System.out.println("No se pudo iniciar la web: " + e.getMessage());
		}
	}

	private static String html() {
		return """
			<!doctype html>
			<html lang="es">
			<head>
			<meta charset="utf-8">
			<meta name="viewport" content="width=device-width, initial-scale=1">
			<title>Water Management Network</title>
			<style>
				body { font-family: Arial, sans-serif; margin: 0; background: #f3f6f8; color: #17272f; }
				header { background: #0f4c5c; color: white; padding: 24px; }
				main { padding: 24px; }
				.tarjeta { background: white; border-radius: 10px; padding: 16px; margin-bottom: 12px; box-shadow: 0 2px 6px rgba(0,0,0,.08); }
				.estado { display: inline-block; padding: 4px 10px; border-radius: 999px; font-size: 12px; font-weight: bold; }
				.AVAILABLE { background: #d1f2df; color: #12653a; }
				.WATERING { background: #d6e9ff; color: #114d7a; }
				.LEAK { background: #ffe0e0; color: #8a1c1c; }
				.DISCONNECTED { background: #e8e8e8; color: #555; }
				.OUTOFSERVICE { background: #fff2cc; color: #7a5b00; }
			</style>
			</head>
			<body>
			<header>
				<h1>Water Management Network</h1>
				<p>Estado de las estaciones</p>
			</header>
			<main>
				<div id="estaciones">Cargando...</div>
			</main>
			<script>
				async function cargar() {
				const respuesta = await fetch('/api/stations',{ cache: 'no-store' } );
				const estaciones = await respuesta.json();
				const contenedor = document.getElementById('estaciones');

				contenedor.innerHTML = estaciones.length === 0
					? '<div class="tarjeta">No hay estaciones registradas.</div>'
					: estaciones.map(e => `
					<div class="tarjeta">
						<h2>${e.stationId}</h2>
						<p>${e.location}</p>
						<span class="estado ${e.status}">${e.status}</span>
						<p>Engine: ${e.engineHealth} | Monitor: ${e.monitorConnected ? 'conectado' : 'desconectado'}</p>
						<p>Caudal: ${e.flow} L/min | Volumen: ${e.volume} L</p>
					</div>
					`).join('');
				}

				cargar();
				setInterval(cargar, 1000);
			</script>
			</body>
			</html>
			""";
	}

	/**
	 * @param args
	 */

	public static void main(String[] args) {
		
		/*
		* Descriptores de socket servidor y de socket con el cliente
		*/
		String puerto="";
		String booststrapServer="";
		
		/*
		* Se comprueba que se han pasado los parametros correctos
		*/

		if (args.length < 2) {
				System.out.println("Debe indicar el puerto de escucha de WM_Central y el de boostrapServer de Kafka");
				System.out.println("$ java WM_Central puerto_servidor boostrapServer");
				return;
			}
			puerto = System.getenv().getOrDefault("CENTRAL_PORT", args.length > 0 ? args[0] : "5000");
			booststrapServer = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", args.length > 1 ? args[1] : "localhost:9092");

		/*
		* Se crea el socket servidor y se queda a la espera de conexiones de clientes
		*/
		try
		{

			ServerSocket skServidor = new ServerSocket(Integer.parseInt(puerto));
			String rutaDb = System.getenv().getOrDefault("DB_PATH", "wm.db");
			db = new DataBaseManager(rutaDb);
            db.marcarTodasDesconectadas();
			
			KafkaCentral kafka = new KafkaCentral(booststrapServer, db);
			Runtime.getRuntime().addShutdownHook(new Thread(kafka::detener));
			kafka.start();

			int puertoWeb = Integer.parseInt(System.getenv().getOrDefault("WEB_PORT", "8080"));
			iniciarWeb(puertoWeb);
			
			System.out.println("WM_Central iniciado.");
		    System.out.println("Escuchando el puerto " + puerto);
			System.out.println("BoostrapServer Kafka: " + booststrapServer);
			
			System.out.println("Esperando conexiones de estaciones de riego...");

			Thread hiloMenu = new Thread(() -> WM_Central.menuCentral(), "central-menu");
			hiloMenu.setDaemon(true);
			hiloMenu.start();

			/*
			* Mantenemos la comunicacion con el cliente
			*/	
			for(;;)
			{
				/*
				* Se espera un cliente(Monitor) que quiera conectarse
				*/
				Socket skCliente = skServidor.accept(); // Crea objeto
		        System.out.println("Nueva estación conectada desde: " + skCliente.getInetAddress().getHostAddress());

				//Cada conexion se atiende en un hilo diferente para poder aceptar varias conexiones a la vez
		        Thread hilo = new HiloWM(skCliente, 5000, db);
		        hilo.start();
			}
		}
		catch(NumberFormatException e)
		{
			System.out.println("Error: el puerto debe de ser un numero entero");
			
		} catch (BindException e){
			System.out.println("Error: el puerto " + puerto + " ya esta en uso");
		}
		catch(Exception e){
			System.out.println("Error: " + e.toString());
		}
	}

}
