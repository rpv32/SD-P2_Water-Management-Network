import java.net.*;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.UUID;
import java.time.Instant;

public class WM_Central {

	//Estaciones que tiene un Monitor conectado
	//Luego cuando se cree SQLite se guardaran en la base de datos.
	private static final ObjectMapper json = new ObjectMapper();
	private static Set<String> estacionesConectadas = new HashSet<String>();
	private static DataBaseManager db;
	private static KafkaCentral kafka;

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
				4.Iniciar Riego
				0.Salir
				"""
			);

			if(!teclado.hasNextLine()){
				return;
			}

			String opcion = teclado.nextLine().trim();

			switch(opcion){
				case "1" -> db.listarEstaciones();
				case "2" -> {
					System.out.print("ID de estacion o * para todas: ");
    				cambiarEstadoDesdeMenu(teclado.nextLine().trim(), true);
				}

				case "3" -> {
					System.out.print("ID de estacion o * para todas: ");
    				cambiarEstadoDesdeMenu(teclado.nextLine().trim(), false);
				}

				case "4" -> {
					try{
						System.out.print("ID de estacion o * para todas: ");
        				String objetivo = teclado.nextLine().trim();
						System.out.print("ID de operario registrado: ");
        				String operador = teclado.nextLine().trim();
						System.out.print("Duracion en segundos: ");
        				int duracion = Integer.parseInt(teclado.nextLine().trim());
						
						for (String id : estacionesObjetivo(objetivo)) {
            				kafka.solicitarRiego(id, operador, duracion);
        				}
						
					}catch(Exception e){
						System.err.println("No se pudo solicitar el riego: "+e.getMessage());
					}
				}

				case "0" -> {
                System.out.println("Cerrando Central...");
                System.exit(0);
				}

				default -> System.out.println("Opcion no valida, eliga una opcion valida");
			}


		}

	}

	private static void iniciarWeb(int puertoWeb, KafkaCentral kafka) {
		try {
			HttpServer servidor = HttpServer.create(new InetSocketAddress(puertoWeb), 0);

					com.fasterxml.jackson.databind.ObjectMapper mapper =
				new com.fasterxml.jackson.databind.ObjectMapper();

		instalarAltas(servidor);

        servidor.createContext("/api/action", intercambio -> {
			int codigo;
			var respuesta = mapper.createObjectNode();

			try {
				if (!"/api/action".equals(
						intercambio.getRequestURI().getPath())) {

					codigo = 404;
					respuesta.put("message", "Ruta no encontrada");

				} else if (!"POST".equals(intercambio.getRequestMethod())) {
					codigo = 405;
					intercambio.getResponseHeaders().set("Allow", "POST");
					respuesta.put("message", "Debe utilizar POST");

				} else {
					String origen = intercambio.getRequestHeaders()
							.getFirst("Origin");

					String host = intercambio.getRequestHeaders()
							.getFirst("Host");

					// Bloquea peticiones de navegadores desde otros origenes.
					if (origen != null
							&& !origen.equals("http://" + host)
							&& !origen.equals("https://" + host)) {

						codigo = 403;
						respuesta.put("message", "Origen no permitido");

					} else {
						byte[] datos;

						try (var entrada = intercambio.getRequestBody()) {
							datos = entrada.readNBytes(8193);
						}

						if (datos.length > 8192) {
							codigo = 413;
							respuesta.put("message", "Peticion demasiado grande");

						} else {
							var peticion = mapper.readTree(datos);

							if (peticion == null || !peticion.isObject()) {
								throw new IllegalArgumentException(
										"Se esperaba un objeto JSON");
							}

							String accion = peticion.path("action").asText();
							String estacion = peticion.path("stationId").asText();
							String operario = peticion.path("operatorId").asText();

							var tiempo = peticion.path("durationSeconds");
							int duracion = 0;

							if ("START".equals(accion)) {
								if (!tiempo.isIntegralNumber()
										|| !tiempo.canConvertToInt()) {
									throw new IllegalArgumentException(
											"La duracion debe ser un entero");
								}

								duracion = tiempo.intValue();
							}

							String mensaje = kafka.ejecutarAccionWeb(
									accion, estacion, operario, duracion);

							codigo = 200;
							respuesta.put("message", mensaje);
						}
					}
				}

			} catch (com.fasterxml.jackson.core.JsonProcessingException
					| IllegalArgumentException e) {

				codigo = 400;
				respuesta.put("message", "Peticion no valida: " + e.getMessage());

			} catch (Exception e) {
				codigo = 503;
				respuesta.put(
						"message",
						"No se pudo confirmar la accion. "
						+ "Comprueba el estado antes de repetirla.");

				System.err.println("WEB: " + e.getMessage());
			}

			byte[] cuerpo = mapper.writeValueAsBytes(respuesta);

			intercambio.getResponseHeaders().set(
					"Content-Type", "application/json; charset=utf-8");

			intercambio.getResponseHeaders().set(
					"Cache-Control", "no-store");

			intercambio.sendResponseHeaders(codigo, cuerpo.length);

			try (OutputStream salida = intercambio.getResponseBody()) {
				salida.write(cuerpo);
			}
		});
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
			servidor.setExecutor(java.util.concurrent.Executors.newFixedThreadPool(4));
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
				body {
					font-family: Arial, sans-serif;
					margin: 0;
					background: #f3f6f8;
					color: #17272f;
				}

				header {
					background: #0f4c5c;
					color: white;
					padding: 24px;
				}

				main { padding: 24px; }

				.tarjeta, .controles {
					background: white;
					border-radius: 10px;
					padding: 16px;
					margin-bottom: 16px;
					box-shadow: 0 2px 6px rgba(0,0,0,.08);
				}

				.estado {
					display: inline-block;
					padding: 5px 12px;
					border-radius: 999px;
					font-size: 13px;
					font-weight: bold;
				}

				.AVAILABLE { background: #d1f2df; color: #12653a; }

				.WATERING {
					background: #d1f2df;
					color: #12653a;
					animation: regando 1s infinite alternate;
				}

				@keyframes regando {
					from { opacity: 1; }
					to { opacity: .45; }
				}

				.LEAK { background: #ffe0e0; color: #8a1c1c; }
				.DISCONNECTED { background: #e8e8e8; color: #555; }
				.OUT_OF_SERVICE { background: #ffe0b2; color: #7a3e00; }

				label {
					display: inline-block;
					margin: 8px 16px 8px 0;
				}

				input {
					padding: 8px;
					border: 1px solid #bbc9cf;
					border-radius: 6px;
					width: 110px;
				}

				button {
					border: 0;
					border-radius: 6px;
					padding: 10px 14px;
					margin: 5px 5px 0 0;
					cursor: pointer;
					background: #0f4c5c;
					color: white;
				}

				button.bloquear { background: #a64b00; }
				button.activar { background: #187346; }

				button:disabled {
					opacity: .5;
					cursor: not-allowed;
				}

				#mensaje {
					white-space: pre-wrap;
					padding: 12px;
					border-radius: 6px;
					background: #e7eef2;
				}

				#conexion {
					font-size: 14px;
					margin-top: 12px;
				}
			</style>
			</head>

			<body>
			<header>
				<h1>Water Management Network</h1>
				<p>Monitorizacion y control de las estaciones</p>
			</header>

			<main>
				<section class="controles">
					<h2>Control de Central</h2>

					<label>
						Operario:
						<input id="operario" value="OP-01">
					</label>

					<label>
						Duracion (s):
						<input id="duracion" type="number"
							min="1" step="1" value="30">
					</label>

					<p>Estos datos se utilizan para iniciar los riegos.</p>

					<div>
						<button data-action="START" data-station="*">
							Iniciar en todas
						</button>

						<button class="bloquear"
								data-action="BLOCK" data-station="*">
							Bloquear todas
						</button>

						<button class="activar"
								data-action="ACTIVATE" data-station="*">
							Activar todas
						</button>
					</div>

					<p id="mensaje" role="status">Sin acciones pendientes.</p>
					<p id="conexion">Conectando con Central...</p>
				</section>

				<div id="estaciones">Cargando...</div>
			</main>

			<script>
				let enviando = false;
				let cargando = false;

				const nombres = {
					AVAILABLE: 'Disponible',
					WATERING: 'Regando',
					LEAK: 'Fuga / Averiada',
					OUT_OF_SERVICE: 'Fuera de servicio',
					DISCONNECTED: 'Desconectada'
				};

				function escapar(valor) {
					const elemento = document.createElement('span');
					elemento.textContent = String(valor ?? '');
					return elemento.innerHTML
						.replaceAll('"', '&quot;')
						.replaceAll("'", '&#39;');
				}

				function numero(valor) {
					const n = Number(valor);
					return Number.isFinite(n) ? n.toFixed(2) : '0.00';
				}

				async function cargar() {
					if (cargando) return;
					cargando = true;

					try {
						const respuesta = await fetch('/api/stations', {
							cache: 'no-store'
						});

						if (!respuesta.ok) {
							throw new Error('HTTP ' + respuesta.status);
						}

						const estaciones = await respuesta.json();
						const contenedor = document.getElementById('estaciones');

						contenedor.innerHTML = estaciones.length === 0
							? '<div class="tarjeta">No hay estaciones registradas.</div>'
							: estaciones.map(e => {
								const estado = Object.hasOwn(nombres, e.status)
									? e.status : 'DISCONNECTED';

								return `
									<section class="tarjeta">
										<h2>${escapar(e.stationId)}</h2>
										<p>${escapar(e.location)}</p>

										<span class="estado ${estado}">
											${nombres[estado]}
										</span>

										<p>
											Engine: ${escapar(e.engineHealth)} |
											Monitor: ${e.monitorConnected
												? 'conectado' : 'desconectado'}
										</p>

										<p>Operario: ${escapar(e.operatorId || '—')}</p>

										<p>
											Caudal: ${numero(e.flow)} L/min |
											Volumen: ${numero(e.volume)} L
										</p>

										<button data-action="START"
												data-station="${escapar(e.stationId)}"
												${enviando || estado !== 'AVAILABLE'
													? 'disabled' : ''}>
											Iniciar riego
										</button>

										<button class="bloquear"
												data-action="BLOCK"
												data-station="${escapar(e.stationId)}"
												${enviando ? 'disabled' : ''}>
											Bloquear
										</button>

										<button class="activar"
												data-action="ACTIVATE"
												data-station="${escapar(e.stationId)}"
												${enviando ? 'disabled' : ''}>
											Activar
										</button>
									</section>
								`;
							}).join('');

						document.getElementById('conexion').textContent =
							'Estado actualizado: ' + new Date().toLocaleTimeString();

					} catch (error) {
						document.getElementById('conexion').textContent =
							'Sin conexion con Central: ' + error.message;

					} finally {
						cargando = false;
					}
				}

				async function ejecutar(accion, estacion) {
					if (enviando) return;

					const operario =
						document.getElementById('operario').value.trim();

					const duracion =
						Number(document.getElementById('duracion').value);

					const mensaje = document.getElementById('mensaje');

					if (accion === 'START'
						&& (!operario || !Number.isInteger(duracion)
							|| duracion <= 0)) {

						mensaje.textContent =
							'Indica un operario y una duracion entera positiva.';
						return;
					}

					const destino = estacion === '*'
						? 'todas las estaciones' : estacion;

					if (!confirm('Ejecutar ' + accion + ' sobre ' + destino + '?')) {
						return;
					}

					enviando = true;
					document.querySelectorAll('button[data-action]')
						.forEach(b => b.disabled = true);

					mensaje.textContent = 'Enviando accion...';

					try {
						const respuesta = await fetch('/api/action', {
							method: 'POST',
							headers: {
								'Content-Type': 'application/json'
							},
							body: JSON.stringify({
								action: accion,
								stationId: estacion,
								operatorId: operario,
								durationSeconds: duracion
							})
						});

						const resultado = await respuesta.json();

						if (!respuesta.ok) {
							throw new Error(resultado.message || 'Error HTTP');
						}

						mensaje.textContent = resultado.message;

					} catch (error) {
						mensaje.textContent =
							'No se pudo confirmar la accion: ' + error.message
							+ '. Comprueba el estado antes de repetirla.';

					} finally {
						enviando = false;

						document.querySelectorAll(
							'.controles button[data-action]'
						).forEach(b => b.disabled = false);

						await cargar();
					}
				}

				document.addEventListener('click', evento => {
					const boton = evento.target.closest('button[data-action]');

					if (boton && !boton.disabled) {
						ejecutar(boton.dataset.action, boton.dataset.station);
					}
				});

				cargar();
				setInterval(cargar, 1000);
			</script>
			</body>
			</html>
			""";
	}

	private static java.util.List<String> estacionesObjetivo(String objetivo) throws Exception {

		if (!"*".equals(objetivo)) {
			if (!db.existeEstacion(objetivo)) {
				throw new IllegalArgumentException("La estacion no existe: " + objetivo);
			}
			return java.util.List.of(objetivo);
		}

		var estaciones = new com.fasterxml.jackson.databind.ObjectMapper().readTree(db.listarEstacionesJson());

		java.util.List<String> ids = new java.util.ArrayList<>();

		for (var estacion : estaciones) {
			String id = estacion.path("stationId").asText();

			if (db.existeEstacion(id)) {
				ids.add(id);
			}
		}
		return ids;
	}
	
	private static void cambiarEstadoDesdeMenu(String objetivo, boolean bloquear) {

		try {
			for (String id : estacionesObjetivo(objetivo)) {
				try {
					kafka.enviarOrden(id,bloquear? "BLOCK_STATION": "ACTIVATE_STATION");

					if (bloquear) {
						db.bloquearEstacion(id);
					} else {
						db.activarEstacion(id);
					}

				} catch (Exception e) {
					System.err.println("No se pudo cambiar " + id + ": "+ e.getMessage());
				}
			}
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
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
			
			kafka = new KafkaCentral(booststrapServer, db);
			Runtime.getRuntime().addShutdownHook(new Thread(kafka::detener));
			kafka.start();

			int puertoWeb = Integer.parseInt(System.getenv().getOrDefault("WEB_PORT", "8080"));
			iniciarWeb(puertoWeb, kafka);
			
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


    // Formularios y consultas comparten la misma lectura de pares clave=valor.
    private static java.util.Map<String, String> parametros(String texto) {
        var resultado = new java.util.HashMap<String, String>();
        if (texto != null) for (String par : texto.split("&")) {
            String[] c = par.split("=", 2);
            if (c.length == 2) resultado.put(c[0],
                java.net.URLDecoder.decode(c[1], StandardCharsets.UTF_8));
        }
        return resultado;
    }

    private static void instalarAltas(HttpServer servidor) {
        servidor.createContext("/api/registro", x -> {
            int estado = 200;
            String respuesta;
            try {
                if (!"/api/registro".equals(x.getRequestURI().getPath())) {
                    estado = 404; respuesta = "Ruta no encontrada";
                } else if (!"GET".equals(x.getRequestMethod())) {
                    estado = 405; respuesta = "Usa GET";
                    x.getResponseHeaders().set("Allow", "GET");
                } else {
                    var p = parametros(x.getRequestURI().getRawQuery());
                    String id = p.getOrDefault("id", "");
                    if (!id.matches("[A-Za-z0-9_-]{1,64}"))
                        throw new IllegalArgumentException("ID no valido");
                    respuesta = Boolean.toString(db.registroActivo(p.getOrDefault("tipo", ""), id));
                }
            } catch (IllegalArgumentException e) {
                estado = 400; respuesta = "Parametros no validos";
            } catch (Exception e) {
                estado = 503; respuesta = "No se pudo consultar el registro";
                System.err.println("Registro: " + e.getMessage());
            }
            byte[] bytes = respuesta.getBytes(StandardCharsets.UTF_8);
            x.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
            x.getResponseHeaders().set("Cache-Control", "no-store");
            x.sendResponseHeaders(estado, bytes.length);
            try (var salida = x.getResponseBody()) { salida.write(bytes); }
        });

        // Sin contrasena, como el resto del panel actual. Es una ruta publica.
        servidor.createContext("/altas", x -> {
            int estado = 200;
            String respuesta;
            String tipoContenido = "text/html; charset=utf-8";
            try {
                if (!"/altas".equals(x.getRequestURI().getPath())) {
                    estado = 404; respuesta = "Ruta no encontrada";
                } else if ("GET".equals(x.getRequestMethod())) {
                    respuesta = """
                    <!doctype html><html lang="es"><meta charset="utf-8"><title>Altas</title>
                    <style>body{font:16px system-ui;max-width:600px;margin:40px auto;padding:15px}
                    label{display:block;margin:15px 0}input,select,button{padding:8px}</style>
                    <h1>Alta de estaciones y operarios</h1>
                    <form method="post" action="/altas">
					<label>Acción
						<select name="accion">
							<option value="alta">Dar de alta</option>
							<option value="baja">Dar de baja</option>
						</select>
					</label>
                    <label>Tipo <select name="tipo"><option value="station">Estacion</option>
                    <option value="operator">Operario</option></select></label>
                    <label>ID <input name="id" required maxlength="64" pattern="[A-Za-z0-9_-]+"></label>
                    <label>Ubicacion / nombre <input name="nombre" maxlength="200"></label>
                    <button>Aplicar</button></form>
                    <p id="resultado"></p><a href="/">Volver al panel</a>
                    <script>if(location.search==='?ok')document.getElementById('resultado').textContent=
                    'Operación completada correctamente.';</script></html>
                    """;
                } else if ("POST".equals(x.getRequestMethod())) {
                    String origen = x.getRequestHeaders().getFirst("Origin");
                    String host = x.getRequestHeaders().getFirst("Host");
                    if (origen != null && !origen.equals("https://" + host)
                            && !origen.equals("http://" + host)) {
                        estado = 403; respuesta = "Origen no permitido";
                    } else {
                        byte[] bytes = x.getRequestBody().readNBytes(8193);
                        if (bytes.length > 8192) throw new IllegalArgumentException("Formulario demasiado grande");
                        var p = parametros(new String(bytes, StandardCharsets.UTF_8));
                        String tipo = p.getOrDefault("tipo", "");
                        String accion = p.getOrDefault("accion", "alta");
						String id = p.getOrDefault("id", "").trim();
						String nombre = p.getOrDefault("nombre", "").trim();

						if (!id.matches("[A-Za-z0-9_-]{1,64}")) {
							throw new IllegalArgumentException("ID no valido");
						}

						if ("baja".equals(accion)) {

							db.darDeBaja(tipo, id);

						} else if ("alta".equals(accion)) {

							if (nombre.isBlank() || nombre.length() > 200) {
								throw new IllegalArgumentException(
										"Introduce una ubicacion o nombre valido");
							}

							if ("station".equals(tipo)) {
								db.altaEstacion(id, nombre);
							} else if ("operator".equals(tipo)) {
								db.altaOperario(id, nombre);
							} else {
								throw new IllegalArgumentException("Tipo no valido");
							}

							if (!db.registroActivo(tipo, id)) {
								throw new IllegalArgumentException("El ID ya existe pero esta inactivo. " + "Dar de alta no lo reactiva");
							}
						}
                        x.getResponseHeaders().set("Location", "/altas?ok");
                        x.sendResponseHeaders(303, -1); x.close(); 
						return;
                    }
                } else {
                    estado = 405; respuesta = "Usa GET o POST";
                    x.getResponseHeaders().set("Allow", "GET, POST");
                }
            } catch (IllegalArgumentException e) {
                estado = 400; respuesta = e.getMessage() + ". Vuelve atras para corregir el formulario.";
                tipoContenido = "text/plain; charset=utf-8";
            } catch (Exception e) {
                estado = 503; respuesta = "No se pudo confirmar el alta. Comprueba el registro antes de repetir.";
                tipoContenido = "text/plain; charset=utf-8";
                System.err.println("Alta: " + e.getMessage());
            }
            byte[] bytes = respuesta.getBytes(StandardCharsets.UTF_8);
            x.getResponseHeaders().set("Content-Type", tipoContenido);
            x.getResponseHeaders().set("Cache-Control", "no-store");
            x.getResponseHeaders().set("X-Content-Type-Options", "nosniff");
            x.sendResponseHeaders(estado, bytes.length);
            try (var salida = x.getResponseBody()) { salida.write(bytes); }
        });
    }
}