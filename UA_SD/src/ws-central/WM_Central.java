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
import java.sql.SQLException;

public class WM_Central {

	//Estaciones que tiene un Monitor conectado
	//Luego cuando se cree SQLite se guardaran en la base de datos.
	private static final ObjectMapper json = new ObjectMapper();
	private static Set<String> estacionesConectadas = new HashSet<String>();
	private static DataBaseManager db;
	private static KafkaCentral kafka;
	private static final java.util.ArrayDeque<String> logsWeb = new java.util.ArrayDeque<>();

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
			//Obtener logs de Central
			servidor.createContext("/api/logs", intercambio -> {
				if (!"/api/logs".equals(
						intercambio.getRequestURI().getPath())) {

					intercambio.sendResponseHeaders(404, -1);
					intercambio.close();
					return;
				}

				if (!"GET".equals(intercambio.getRequestMethod())) {
					intercambio.getResponseHeaders().set("Allow", "GET");
					intercambio.sendResponseHeaders(405, -1);
					intercambio.close();
					return;
				}

				String texto;

				synchronized (logsWeb) {
					texto = String.join("\n", logsWeb);
				}

				byte[] cuerpo = texto.getBytes(StandardCharsets.UTF_8);

				intercambio.getResponseHeaders().set(
						"Content-Type", "text/plain; charset=utf-8");

				intercambio.getResponseHeaders().set(
						"Cache-Control", "no-store");

				intercambio.getResponseHeaders().set(
						"X-Content-Type-Options", "nosniff");

				intercambio.sendResponseHeaders(200, cuerpo.length);

				try (var salida = intercambio.getResponseBody()) {
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
		return 
private static String htmlWeb() {
    return """
        <!doctype html>
        <html lang="es">
        <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>Water Management Network</title>

        <style>
            * {
                box-sizing: border-box;
            }

            body {
                font-family: Arial, sans-serif;
                margin: 0;
                background: #101923;
                color: #e8f0f5;
            }

            header {
                background: linear-gradient(135deg, #172b3a, #203d50);
                color: white;
                padding: 25px 30px;
                border-bottom: 1px solid #304858;
            }

            header h1 {
                margin: 0;
                font-size: 25px;
                letter-spacing: .5px;
            }

            header p {
                margin: 8px 0 0;
                color: #a8bdca;
                font-size: 14px;
            }

            main {
                max-width: 1250px;
                margin: auto;
                padding: 25px;
            }

            h2 {
                margin: 0 0 18px;
                font-size: 19px;
                color: #edf5fa;
            }

            .tarjeta, .controles {
                background: #192633;
                border: 1px solid #2b3d4b;
                border-radius: 14px;
                padding: 20px;
                margin-bottom: 18px;
                box-shadow: 0 5px 18px rgba(0,0,0,.16);
            }

            .controles {
                border-top: 3px solid #36a9c8;
            }

            .cabecera-seccion {
                display: flex;
                justify-content: space-between;
                align-items: center;
                gap: 12px;
                flex-wrap: wrap;
            }

            .etiqueta-seccion {
                color: #6fc7e2;
                font-size: 11px;
                font-weight: bold;
                letter-spacing: 1px;
                background: #203b4b;
                padding: 6px 10px;
                border-radius: 20px;
            }

            label {
                display: inline-flex;
                flex-direction: column;
                gap: 7px;
                margin: 5px 18px 12px 0;
                color: #a9bdc9;
                font-size: 13px;
                font-weight: bold;
            }

            input {
                width: 180px;
                max-width: 100%;
                padding: 11px 12px;
                border: 1px solid #3a5262;
                border-radius: 8px;
                background: #101923;
                color: #edf5fa;
                font-size: 14px;
                outline: none;
            }

            input:focus {
                border-color: #36a9c8;
                box-shadow: 0 0 0 2px rgba(54,169,200,.15);
            }

            .ayuda {
                color: #91a6b4;
                font-size: 13px;
                margin: 3px 0 16px;
            }

            button {
                border: 0;
                border-radius: 8px;
                padding: 10px 15px;
                margin: 5px 5px 0 0;
                cursor: pointer;
                background: #167b59;
                color: white;
                font-size: 13px;
                font-weight: bold;
                transition: filter .2s, transform .2s;
            }

            button:hover:not(:disabled) {
                filter: brightness(1.15);
                transform: translateY(-1px);
            }

            button.bloquear {
                background: #a64b28;
            }

            button.activar {
                background: #28658c;
            }

            button:disabled {
                opacity: .4;
                cursor: not-allowed;
                transform: none;
            }

            .acciones-globales {
                display: flex;
                flex-wrap: wrap;
                gap: 7px;
                margin-bottom: 17px;
            }

            .acciones-globales button {
                padding: 12px 17px;
            }

            #mensaje {
                white-space: pre-wrap;
                padding: 12px 14px;
                border-radius: 8px;
                background: #101923;
                border: 1px solid #304554;
                color: #43d49a;
                font-size: 13px;
                margin: 14px 0 8px;
            }

            #conexion {
                font-size: 12px;
                color: #91a6b4;
                margin: 8px 0 0;
            }

            .estaciones-titulo {
                display: flex;
                justify-content: space-between;
                align-items: center;
                flex-wrap: wrap;
                gap: 10px;
                margin: 25px 0 15px;
            }

            .estaciones-titulo h2 {
                margin: 0;
            }

            .actualizacion {
                color: #91a6b4;
                font-size: 11px;
                letter-spacing: .5px;
            }

            #estaciones {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
                gap: 16px;
            }

            #estaciones .tarjeta {
                margin: 0;
                min-width: 0;
            }

            #estaciones .tarjeta h2 {
                margin-bottom: 7px;
                font-size: 20px;
            }

            #estaciones .tarjeta p {
                color: #a9bdc9;
                font-size: 13px;
                line-height: 1.5;
                overflow-wrap: anywhere;
            }

            .estado {
                display: inline-block;
                padding: 6px 12px;
                border-radius: 999px;
                font-size: 11px;
                font-weight: bold;
                letter-spacing: .4px;
                margin: 8px 0;
            }

            /* Estación disponible: verde */
            .AVAILABLE {
                background: #d1f2df;
                color: #12653a;
            }

            /* Estación regando: verde intermitente */
            .WATERING {
                background: #d1f2df;
                color: #12653a;
                animation: regando 1s infinite alternate;
            }

            @keyframes regando {
                from { opacity: 1; }
                to { opacity: .45; }
            }

            /* Fuga o avería: rojo */
            .LEAK {
                background: #ffe0e0;
                color: #8a1c1c;
            }

            /* Desconectada: gris */
            .DISCONNECTED {
                background: #e1e5e8;
                color: #505b63;
            }

            /* Fuera de servicio: naranja */
            .OUT_OF_SERVICE {
                background: #ffe0b2;
                color: #7a3e00;
            }

            .datos-estacion {
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 10px;
                margin: 15px 0;
            }

            .dato {
                background: #101923;
                border: 1px solid #293d4b;
                border-radius: 9px;
                padding: 11px;
                min-width: 0;
            }

            .dato-titulo {
                display: block;
                color: #8fa6b5;
                font-size: 10px;
                font-weight: bold;
                letter-spacing: .7px;
                margin-bottom: 7px;
            }

            .dato-valor {
                color: #edf5fa;
                font-size: 18px;
                font-weight: bold;
                overflow-wrap: anywhere;
            }

            .unidad {
                color: #91a6b4;
                font-size: 11px;
                font-weight: normal;
            }

            .acciones-estacion {
                display: flex;
                flex-wrap: wrap;
                gap: 5px;
                margin-top: 12px;
            }

            .acciones-estacion button {
                flex: 1;
                min-width: 90px;
            }

            .logs-cabecera {
                display: flex;
                justify-content: space-between;
                align-items: center;
                flex-wrap: wrap;
                gap: 10px;
            }

            #estadoLogs {
                color: #91a6b4;
                font-size: 12px;
                margin: 0 0 12px;
            }

            #logsCentral {
                height: 280px;
                overflow-y: auto;
                overflow-x: hidden;
                white-space: pre-wrap;
                overflow-wrap: anywhere;
                background: #0b1219;
                color: #dce9ef;
                border: 1px solid #2b414f;
                border-radius: 10px;
                padding: 16px;
                margin: 0;
                font: 13px/1.6 Consolas, monospace;
            }

            @media (max-width: 600px) {
                header {
                    padding: 22px 18px;
                }

                header h1 {
                    font-size: 21px;
                }

                main {
                    padding: 15px;
                }

                .tarjeta, .controles {
                    padding: 16px;
                }

                label {
                    display: flex;
                    margin-right: 0;
                }

                input {
                    width: 100%;
                }

                .acciones-globales {
                    flex-direction: column;
                }

                .acciones-globales button {
                    width: 100%;
                    margin-right: 0;
                }

                #estaciones {
                    grid-template-columns: 1fr;
                }

                #logsCentral {
                    height: 240px;
                    font-size: 12px;
                }
            }
        </style>
        </head>

        <body>

        <header>
            <h1>Water Management Network</h1>
            <p>Monitorización y control de las estaciones</p>
        </header>

        <main>

            <section class="controles">
                <div class="cabecera-seccion">
                    <h2>Control de Central</h2>
                    <span class="etiqueta-seccion">CONTROL GLOBAL</span>
                </div>

                <label>
                    Operario
                    <input id="operario" value="OP-01">
                </label>

                <label>
                    Duración (segundos)
                    <input id="duracion" type="number"
                           min="1" step="1" value="30">
                </label>

                <p class="ayuda">
                    Estos datos se utilizan para iniciar los riegos.
                </p>

                <div class="acciones-globales">
                    <button data-action="START" data-station="*">
                        ▶ Iniciar en todas
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

                <p id="mensaje" role="status">
                    Sin acciones pendientes.
                </p>

                <p id="conexion">Conectando con Central...</p>
            </section>

            <div class="estaciones-titulo">
                <h2>Estaciones de la red</h2>
                <span class="actualizacion">
                    ACTUALIZACIÓN AUTOMÁTICA
                </span>
            </div>

            <div id="estaciones">Cargando estaciones...</div>

            <section class="tarjeta">
                <div class="logs-cabecera">
                    <h2>Actividad de Central</h2>
                    <span class="etiqueta-seccion">REGISTRO</span>
                </div>

                <p id="estadoLogs">
                    Conectando con el registro...
                </p>

                <pre id="logsCentral">Esperando mensajes...</pre>
            </section>

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
                    const contenedor =
                        document.getElementById('estaciones');

                    contenedor.innerHTML = estaciones.length === 0
                        ? '<div class="tarjeta">No hay estaciones registradas.</div>'
                        : estaciones.map(e => {

                            const estado = Object.hasOwn(nombres, e.status)
                                ? e.status
                                : 'DISCONNECTED';

                            return `
                                <section class="tarjeta">

                                    <div class="cabecera-seccion">
                                        <h2>${escapar(e.stationId)}</h2>

                                        <span class="estado ${estado}">
                                            ${nombres[estado]}
                                        </span>
                                    </div>

                                    <p>${escapar(e.location)}</p>

                                    <div class="datos-estacion">
                                        <div class="dato">
                                            <span class="dato-titulo">
                                                CAUDAL
                                            </span>
                                            <span class="dato-valor">
                                                ${numero(e.flow)}
                                                <span class="unidad">L/min</span>
                                            </span>
                                        </div>

                                        <div class="dato">
                                            <span class="dato-titulo">
                                                VOLUMEN
                                            </span>
                                            <span class="dato-valor">
                                                ${numero(e.volume)}
                                                <span class="unidad">L</span>
                                            </span>
                                        </div>
                                    </div>

                                    <p>
                                        Motor: ${escapar(e.engineHealth)} |
                                        Monitor: ${e.monitorConnected
                                            ? 'conectado'
                                            : 'desconectado'}
                                    </p>

                                    <p>
                                        Operario:
                                        ${escapar(e.operatorId || '—')}
                                    </p>

                                    <div class="acciones-estacion">

                                        <button
                                            data-action="START"
                                            data-station="${escapar(e.stationId)}"
                                            ${enviando || estado !== 'AVAILABLE'
                                                ? 'disabled'
                                                : ''}>
                                            ▶ Iniciar riego
                                        </button>

                                        <button
                                            class="bloquear"
                                            data-action="BLOCK"
                                            data-station="${escapar(e.stationId)}"
                                            ${enviando ? 'disabled' : ''}>
                                            Bloquear
                                        </button>

                                        <button
                                            class="activar"
                                            data-action="ACTIVATE"
                                            data-station="${escapar(e.stationId)}"
                                            ${enviando ? 'disabled' : ''}>
                                            Activar
                                        </button>

                                    </div>
                                </section>
                            `;
                        }).join('');

                    document.getElementById('conexion').textContent =
                        'Estado actualizado: ' +
                        new Date().toLocaleTimeString();

                } catch (error) {
                    document.getElementById('conexion').textContent =
                        'Sin conexión con Central: ' + error.message;

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

                const mensaje =
                    document.getElementById('mensaje');

                if (accion === 'START'
                    && (!operario
                        || !Number.isInteger(duracion)
                        || duracion <= 0)) {

                    mensaje.textContent =
                        'Indica un operario y una duración entera positiva.';

                    return;
                }

                const destino = estacion === '*'
                    ? 'todas las estaciones'
                    : estacion;

                if (!confirm(
                    '¿Ejecutar ' + accion + ' sobre ' + destino + '?'
                )) {
                    return;
                }

                enviando = true;

                document.querySelectorAll('button[data-action]')
                    .forEach(b => b.disabled = true);

                mensaje.textContent = 'Enviando acción...';

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
                        throw new Error(
                            resultado.message || 'Error HTTP'
                        );
                    }

                    mensaje.textContent = resultado.message;

                } catch (error) {
                    mensaje.textContent =
                        'No se pudo confirmar la acción: ' +
                        error.message +
                        '. Comprueba el estado antes de repetirla.';

                } finally {
                    enviando = false;

                    document.querySelectorAll(
                        '.controles button[data-action]'
                    ).forEach(b => b.disabled = false);

                    await cargar();
                }
            }

            document.addEventListener('click', evento => {
                const boton = evento.target.closest(
                    'button[data-action]'
                );

                if (boton && !boton.disabled) {
                    ejecutar(
                        boton.dataset.action,
                        boton.dataset.station
                    );
                }
            });

            cargar();
            setInterval(cargar, 1000);

            async function actualizarLogs() {
                const ventana =
                    document.getElementById('logsCentral');

                const estado =
                    document.getElementById('estadoLogs');

                const seguir = ventana.scrollHeight
                    - ventana.scrollTop
                    - ventana.clientHeight < 40;

                const posicion = ventana.scrollTop;

                try {
                    const respuesta = await fetch('/api/logs', {
                        cache: 'no-store'
                    });

                    if (!respuesta.ok) {
                        throw new Error('HTTP ' + respuesta.status);
                    }

                    const texto = await respuesta.text();

                    const contenido =
                        texto || 'Sin mensajes todavía.';

                    if (ventana.textContent !== contenido) {
                        ventana.textContent = contenido;

                        ventana.scrollTop = seguir
                            ? ventana.scrollHeight
                            : posicion;
                    }

                    estado.textContent =
                        'Últimas 300 líneas · Actualización automática';

                } catch {
                    estado.textContent =
                        'No se pudo actualizar. Se conservan los mensajes anteriores.';
                }

                setTimeout(actualizarLogs, 1500);
            }

            actualizarLogs();
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

	private static java.io.PrintStream copiarLogs(
        java.io.PrintStream original) {

		return new java.io.PrintStream(
				new java.io.OutputStream() {

					private final java.io.ByteArrayOutputStream linea =
							new java.io.ByteArrayOutputStream();

					private boolean truncada;

					@Override
					public synchronized void write(int dato) {
						// La consola original sigue funcionando.
						original.write(dato);

						if (dato == '\n') {
							String texto = linea.toString(
									StandardCharsets.UTF_8).stripTrailing();

							linea.reset();

							if (truncada) {
								texto += " [linea truncada]";
								truncada = false;
							}

							// No llenar la ventana con heartbeats rutinarios.
							if (!texto.isBlank()
									&& !texto.contains("MONITOR_HEARTBEAT")
									&& !texto.startsWith("SRV: Heartbeat de ")) {

								String hora = java.time.LocalTime.now()
										.withNano(0).toString();

								synchronized (logsWeb) {
									if (logsWeb.size() >= 300) {
										logsWeb.removeFirst();
									}

									logsWeb.addLast(
											"[" + hora + "] " + texto);
								}
							}

						} else if (linea.size() < 8192) {
							linea.write(dato);

						} else {
							truncada = true;
						}
					}

					@Override
					public void flush() {
						original.flush();
					}
				},
				true,
				StandardCharsets.UTF_8);
	}

	public static void notificarIncidencia( String stationId, String causa) throws Exception {

		if (kafka == null) {
			throw new IllegalStateException(
					"El servicio Kafka de Central no esta iniciado");
		}

		kafka.notificarIncidencia(stationId, causa);
	}

	/**
	 * @param args
	 */

	public static void main(String[] args) {
		//Activo los logs
		System.setOut(copiarLogs(System.out));
		System.setErr(copiarLogs(System.err));

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
							<!DOCTYPE html>
							<html lang="es">
							<head>
								<meta charset="UTF-8">
								<meta name="viewport" content="width=device-width, initial-scale=1.0">
								<meta name="theme-color" content="#101923">
								<title>Gestión | Altas y Bajas</title>

								<style>
									:root {
										--bg: #101923;
										--panel: #192633;
										--panel2: #203140;
										--border: #304453;
										--text: #edf4f8;
										--muted: #9aafbd;
										--green: #27c989;
										--blue: #399cff;
										--orange: #f0a34a;
										--red: #e66c65;
									}

									* {
										box-sizing: border-box;
									}

									body {
										margin: 0;
										padding: 30px 18px;
										min-height: 100vh;
										background:
											radial-gradient(ellipse at top, #20394b 0%, transparent 55%),
											var(--bg);
										color: var(--text);
										font: 15px system-ui, sans-serif;
									}

									.container {
										max-width: 950px;
										margin: auto;
									}

									header {
										display: flex;
										align-items: center;
										gap: 16px;
										margin-bottom: 30px;
										padding-bottom: 22px;
										border-bottom: 1px solid var(--border);
									}

									.logo {
										width: 55px;
										height: 55px;
										display: grid;
										place-items: center;
										border-radius: 16px;
										background: linear-gradient(135deg, #27c989, #168eaa);
										font-size: 27px;
										flex-shrink: 0;
									}

									h1 {
										font-size: 27px;
										margin: 0;
										letter-spacing: -0.5px;
									}

									h2 {
										font-size: 18px;
										margin: 0;
									}

									.subtitle {
										color: var(--muted);
										margin-top: 4px;
										font-size: 13px;
									}

									.status {
										margin-left: auto;
										display: flex;
										align-items: center;
										gap: 8px;
										color: var(--green);
										font-size: 12px;
										font-weight: 600;
										white-space: nowrap;
									}

									.dot {
										width: 9px;
										height: 9px;
										background: var(--green);
										border-radius: 50%;
										box-shadow: 0 0 12px var(--green);
									}

									.card {
										background: var(--panel);
										border: 1px solid var(--border);
										border-radius: 16px;
										padding: 24px;
										margin-bottom: 18px;
										box-shadow: 0 8px 25px #0002;
									}

									.card-header {
										display: flex;
										align-items: center;
										gap: 12px;
										margin-bottom: 20px;
									}

									.icon {
										width: 43px;
										height: 43px;
										display: grid;
										place-items: center;
										border-radius: 12px;
										background: var(--panel2);
										font-size: 21px;
										flex-shrink: 0;
									}

									.description {
										color: var(--muted);
										font-size: 12px;
										margin-top: 4px;
									}

									label {
										display: block;
										color: var(--muted);
										font-size: 12px;
										margin: 15px 0 7px;
									}

									input, select {
										width: 100%;
										padding: 12px 14px;
										border-radius: 9px;
										border: 1px solid var(--border);
										background: #101c27;
										color: var(--text);
										font: 14px system-ui;
										outline: none;
										transition: .2s;
									}

									input:focus, select:focus {
										border-color: var(--blue);
										box-shadow: 0 0 0 3px #399cff20;
									}

									input::placeholder {
										color: #627987;
									}

									select {
										cursor: pointer;
									}

									button {
										padding: 12px 17px;
										border: 0;
										border-radius: 9px;
										color: white;
										font-weight: 650;
										font-size: 14px;
										cursor: pointer;
										transition: .2s;
									}

									button:hover:not(:disabled) {
										filter: brightness(1.12);
										transform: translateY(-1px);
									}

									button:active:not(:disabled) {
										transform: scale(.98);
									}

									button:disabled {
										opacity: .55;
										cursor: wait;
									}

									.btn-green {
										background: linear-gradient(135deg, #27c989, #159c70);
										width: 100%;
										margin-top: 20px;
									}

									.btn-blue {
										background: linear-gradient(135deg, #398fff, #2471d2);
									}

									.btn-back {
										display: inline-block;
										padding: 11px 16px;
										border: 1px solid var(--border);
										border-radius: 9px;
										color: var(--muted);
										text-decoration: none;
										font-size: 13px;
										transition: .2s;
									}

									.btn-back:hover {
										background: var(--panel2);
										color: white;
									}

									.notice {
										display: flex;
										gap: 11px;
										padding: 13px 15px;
										border-radius: 10px;
										background: #203140;
										border: 1px solid var(--border);
										color: var(--muted);
										font-size: 12px;
										line-height: 1.6;
										margin-bottom: 18px;
									}

									.notice-icon {
										font-size: 18px;
										flex-shrink: 0;
									}

									.toolbar {
										display: flex;
										align-items: center;
										justify-content: space-between;
										gap: 15px;
										flex-wrap: wrap;
										margin-bottom: 16px;
									}

									#estadoRegistros {
										color: var(--muted);
										font-size: 12px;
										margin: 14px 0;
										min-height: 18px;
									}

									.tabla-registros {
										overflow-x: auto;
										width: 100%;
										border: 1px solid var(--border);
										border-radius: 11px;
										background: #101c27;
									}

									.tabla-registros table {
										width: 100%;
										border-collapse: collapse;
										min-width: 550px;
									}

									.tabla-registros th,
									.tabla-registros td {
										padding: 14px 15px;
										border-bottom: 1px solid var(--border);
										text-align: left;
										overflow-wrap: anywhere;
										font-size: 13px;
									}

									.tabla-registros th {
										background: #203140;
										color: #b5c7d2;
										font-size: 11px;
										text-transform: uppercase;
										letter-spacing: .5px;
									}

									.tabla-registros tbody tr:last-child td {
										border-bottom: none;
									}

									.tabla-registros tbody tr:hover {
										background: #192b39;
									}

									.registro-activo {
										color: var(--green);
										font-weight: 650;
									}

									.registro-inactivo {
										color: var(--red);
										font-weight: 650;
									}

									.estado-vacio {
										color: var(--muted);
										text-align: center !important;
										padding: 28px !important;
									}

									#resultado {
										font-size: 13px;
										margin: 15px 0;
									}

									.resultado-ok {
										color: var(--green);
									}

									.resultado-error {
										color: var(--red);
									}

									footer {
										text-align: center;
										color: #657c8b;
										font-size: 11px;
										padding: 12px 0 5px;
									}

									@media (max-width: 650px) {
										body {
											padding: 20px 13px;
										}

										h1 {
											font-size: 22px;
										}

										.status {
											font-size: 10px;
										}

										.card {
											padding: 18px;
										}

										.toolbar {
											align-items: stretch;
											flex-direction: column;
										}

										.toolbar button {
											width: 100%;
										}
									}
								</style>
							</head>

							<body>

							<div class="container">

								<header>
									<div class="logo">⚙️</div>

									<div>
										<h1>Gestión de registros</h1>
										<div class="subtitle">
											Alta y baja de estaciones y operarios
										</div>
									</div>

									<div class="status">
										<span class="dot"></span>
										CENTRAL
									</div>
								</header>

								<!-- FORMULARIO DE ALTAS Y BAJAS -->
								<section class="card">

									<div class="card-header">
										<div class="icon">📝</div>
										<div>
											<h2>Gestión administrativa</h2>
											<div class="description">
												Registrar o deshabilitar estaciones y operarios
											</div>
										</div>
									</div>

									<form method="post" action="/altas">

										<label for="accion">Acción</label>

										<select name="accion" id="accion">
											<option value="alta">Dar de alta</option>
											<option value="baja">Dar de baja</option>
										</select>

										<label for="tipo">Tipo de registro</label>

										<select name="tipo" id="tipo">
											<option value="station">Estación</option>
											<option value="operator">Operario</option>
										</select>

										<label for="id">Identificador (ID)</label>

										<input
											id="id"
											name="id"
											placeholder="Ej. WS-01"
											required
											maxlength="64"
											pattern="[A-Za-z0-9_-]+"
											autocomplete="off">

										<label for="nombre">Ubicación / nombre</label>

										<input
											id="nombre"
											name="nombre"
											placeholder="Ej. Invernadero norte"
											maxlength="200">

										<button class="btn-green">
											✓ Aplicar operación
										</button>

									</form>

									<p id="resultado" role="status"></p>

								</section>

								<!-- LISTADO DE REGISTROS -->
								<section class="card">

									<div class="card-header">
										<div class="icon">📋</div>
										<div>
											<h2>Estaciones y operarios registrados</h2>
											<div class="description">
												Consulta del estado administrativo en Central
											</div>
										</div>
									</div>

									<div class="notice">
										<span class="notice-icon">ℹ️</span>
										<span>
											<strong>Información:</strong>
											Activo indica que el ID está habilitado en Central,
											no que el proceso esté conectado.
										</span>
									</div>

									<div class="toolbar">

										<div>
											<div style="font-weight:600;font-size:14px">
												Registros de Central
											</div>
											<div class="description">
												Consulta y actualización de datos
											</div>
										</div>

										<button
											type="button"
											class="btn-blue"
											id="botonRegistros"
											onclick="cargarRegistros()">
											↻ Listar / actualizar registros
										</button>

									</div>

									<p id="estadoRegistros" role="status" aria-live="polite">
										Pulsa el botón para consultar los registros.
									</p>

									<div class="tabla-registros">

										<table>
											<thead>
												<tr>
													<th>Tipo</th>
													<th>ID</th>
													<th>Ubicación / nombre</th>
													<th>Alta administrativa</th>
												</tr>
											</thead>

											<tbody id="filasRegistros">
												<tr>
													<td colspan="4" class="estado-vacio">
														📂 Sin consultar.
													</td>
												</tr>
											</tbody>
										</table>

									</div>

								</section>

								<a href="/" class="btn-back">
									← Volver al panel de operario
								</a>

								<footer>
									GESTIÓN ADMINISTRATIVA · SISTEMA DE CONTROL DE RIEGO
								</footer>

							</div>

							<script>

								// Mensaje después de una operación completada.
								if (location.search === '?ok') {
									const resultado = document.getElementById('resultado');

									resultado.textContent =
										'✓ Operación completada correctamente.';

									resultado.className = 'resultado-ok';
								}

								async function cargarRegistros() {

									const boton =
										document.getElementById('botonRegistros');

									const estado =
										document.getElementById('estadoRegistros');

									const filas =
										document.getElementById('filasRegistros');

									boton.disabled = true;

									estado.textContent = 'Consultando registros...';

									try {

										const respuesta = await fetch('/api/registros', {
											cache: 'no-store'
										});

										if (!respuesta.ok) {
											throw new Error('HTTP ' + respuesta.status);
										}

										const registros = await respuesta.json();

										if (!Array.isArray(registros)) {
											throw new Error('Respuesta no válida');
										}

										const contenido =
											document.createDocumentFragment();

										for (const registro of registros) {

											const fila = document.createElement('tr');

											for (const valor of [
												registro.tipo,
												registro.id,
												registro.nombre
											]) {

												const celda =
													document.createElement('td');

												celda.textContent = valor ?? '';

												fila.appendChild(celda);
											}

											const activo =
												document.createElement('td');

											activo.textContent = registro.activo
												? '● Activo'
												: '● Inactivo / dado de baja';

											activo.className = registro.activo
												? 'registro-activo'
												: 'registro-inactivo';

											fila.appendChild(activo);

											contenido.appendChild(fila);
										}

										if (registros.length === 0) {

											const fila = document.createElement('tr');

											const celda = document.createElement('td');

											celda.colSpan = 4;
											celda.className = 'estado-vacio';
											celda.textContent = 'No hay registros.';

											fila.appendChild(celda);
											contenido.appendChild(fila);
										}

										// Sustituye las filas anteriores; no las duplica.
										filas.replaceChildren(contenido);

										estado.textContent =
											'✓ Actualizado: ' +
											new Date().toLocaleTimeString();

									} catch (error) {

										estado.textContent =
											'No se pudo actualizar. Los datos mostrados ' +
											'pueden estar desactualizados.';

										console.warn(
											'Error consultando registros:', error
										);

									} finally {

										boton.disabled = false;

									}
								}

							</script>

							</body>
							</html>
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
								throw new IllegalArgumentException("No se pudo confirmar el alta o la reactivacion");
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

		servidor.createContext("/api/registros", x -> {

			if (!"/api/registros".equals(x.getRequestURI().getPath())) {
				x.sendResponseHeaders(404, -1);
				x.close();
				return;
			}

			if (!"GET".equals(x.getRequestMethod())) {
				x.getResponseHeaders().set("Allow", "GET");
				x.sendResponseHeaders(405, -1);
				x.close();
				return;
			}

			int estado = 200;
			String respuesta;

			try {
				respuesta = db.listarRegistrosJson();

			} catch (SQLException e) {
				estado = 503;
				respuesta = "{\"message\":\"No se pudieron consultar los registros\"}";

				System.err.println(
						"Error al listar registros: " + e.getMessage());
			}

			byte[] bytes = respuesta.getBytes(StandardCharsets.UTF_8);

			x.getResponseHeaders().set(
					"Content-Type", "application/json; charset=utf-8");

			x.getResponseHeaders().set(
					"Cache-Control", "no-store");

			x.getResponseHeaders().set(
					"X-Content-Type-Options", "nosniff");

			x.sendResponseHeaders(estado, bytes.length);

			try (var salida = x.getResponseBody()) {
				salida.write(bytes);
			}
		});

    }
}