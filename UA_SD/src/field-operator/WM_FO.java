import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class WM_FO {

    // Todos los operarios publican sus solicitudes en este topic.
    private static final String TOPIC_SOLICITUDES = "wm.watering.requests";

    // Identificador del operario y topic donde recibe sus respuestas.
    private final String operatorId;
    private final String topicEventos;

    // Jackson permite construir e interpretar los mensajes JSON.
    private final ObjectMapper json = new ObjectMapper();
    private com.sun.net.httpserver.HttpServer web;
    private final java.util.concurrent.ExecutorService executorWeb =
        java.util.concurrent.Executors.newFixedThreadPool(4);
    private volatile String mensajeWeb = "Sin acciones pendientes";
    private final java.util.ArrayDeque<String> eventosWeb = new java.util.ArrayDeque<>();
    private final java.util.concurrent.atomic.AtomicBoolean ficheroEnCurso =
        new java.util.concurrent.atomic.AtomicBoolean();

    private final KafkaProducer<String, String> productor;
    private final KafkaConsumer<String, String> consumidor;

    private static final java.nio.file.Path CARPETA_ARCHIVOS = java.nio.file.Path.of("archivos_fo");

    private final java.util.Map<String, String> solicitudesPendientes = new java.util.concurrent.ConcurrentHashMap<>();

    private final java.util.Map<String, Instant> tiemposPendientes = new java.util.concurrent.ConcurrentHashMap<>();
    /*
     * El hilo principal espera esta señal antes de mostrar el menu.
     * Empieza en 1 y pasa a 0 cuando el receptor esta preparado o falla.
     */
    private final CountDownLatch inicioReceptor = new CountDownLatch(1);

    /*
     * Estos atributos se comparten entre el hilo del menu y el receptor.
     * volatile permite que los cambios sean visibles entre ambos hilos.
     */
    private volatile boolean ejecutando = true;
    private volatile String errorReceptor;

    private Thread hiloReceptor;
    private final java.util.Map<String,java.util.concurrent.CompletableFuture<String>> finalesServicio = new java.util.concurrent.ConcurrentHashMap<>();

    public WM_FO(String bootstrapServers, String operatorId) {
        validarOperario(operatorId);
        this.operatorId = operatorId;
        this.topicEventos = "wm.fo." + operatorId + ".events";
        try {
            var archivo = CARPETA_ARCHIVOS.resolve("web-eventos-" + operatorId + ".json");
            if (java.nio.file.Files.exists(archivo)) {
                var guardados = json.readTree(java.nio.file.Files.readString(archivo));
                if (!guardados.isArray()) throw new java.io.IOException("Historial no valido");
                for (var evento : guardados) {
                    if (eventosWeb.size() >= 100) eventosWeb.removeFirst();
                    eventosWeb.addLast(evento.asText());
                }
            }
        } catch (java.io.IOException e) {
            throw new IllegalStateException("No se pudo recuperar el historial del FO", e);
        }
        asegurarTopicOperario(bootstrapServers, operatorId);

        // Configuracion del productor: envia claves y valores de tipo String.
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrapServers);
        p.put("key.serializer", StringSerializer.class.getName());
        p.put("value.serializer", StringSerializer.class.getName());

        // Esperamos confirmacion de las replicas que participan en el envio.
        p.put("acks", "all");

        // Evita duplicados provocados por reintentos internos del productor.
        // No sustituye el control de requestId que debe realizar Central.
        p.put("enable.idempotence", "true");

        // Limites de espera para las operaciones de publicacion.
        p.put("max.block.ms", "10000");
        p.put("delivery.timeout.ms", "15000");
        p.put("request.timeout.ms", "10000");

        productor = new KafkaProducer<>(p);

        // Configuracion del consumidor: recibe claves y valores como String.
        Properties c = new Properties();
        c.put("bootstrap.servers", bootstrapServers);
        c.put("group.id", "wm-fo-" + operatorId);
        c.put("key.deserializer", StringDeserializer.class.getName());
        c.put("value.deserializer", StringDeserializer.class.getName());

        // Confirmamos los mensajes despues de procesarlos, no automaticamente.
        c.put("enable.auto.commit", "false");

        /*
         * Si el grupo no tiene una posicion guardada valida, empieza desde
         * los mensajes mas antiguos que Kafka conserve en el topic.
         */
        c.put("auto.offset.reset", "earliest");

        // El topic del operario debe estar creado previamente.
        c.put("allow.auto.create.topics", "false");

        consumidor = new KafkaConsumer<>(c);
    }

    private void consultarEstaciones() throws Exception {
        String requestId = UUID.randomUUID().toString();

        ObjectNode consulta = json.createObjectNode();

        consulta.put("schemaVersion", 1);
        consulta.put("eventId", UUID.randomUUID().toString());
        consulta.put("eventType", "STATIONS_QUERY");
        consulta.put("occurredAt", Instant.now().toString());
        consulta.put("requestId", requestId);
        consulta.put("operatorId", operatorId);

        productor.send(new ProducerRecord<>("wm.fo.queries",operatorId,json.writeValueAsString(consulta))).get();

        registrarMensaje("Consulta enviada. Esperando estaciones disponibles...");
    }

    /*
     * Este metodo se ejecuta en un hilo independiente.
     * Asi podemos recibir eventos mientras el usuario utiliza el menu.
     */
    private void recibirEventos(){
        try {
            consumidor.subscribe(List.of(topicEventos));

            while (ejecutando) {
                // Espera mensajes durante un maximo de 500 milisegundos.
                var registros = consumidor.poll(Duration.ofMillis(500));

                // Avisamos cuando Kafka ya ha asignado particiones al consumidor.
                if (!consumidor.assignment().isEmpty()) {
                    inicioReceptor.countDown();
                }

                // Procesamos todos los mensajes recibidos en esta lectura.
                for (ConsumerRecord<String, String> registro : registros) {
                    JsonNode evento = json.readTree(registro.value());
                    String requestId = evento.path("requestId").asText();
                    if(!requestId.isEmpty()) {
                        solicitudesPendientes.remove(requestId);
                        tiemposPendientes.remove(requestId);

                        try {
                            java.nio.file.Files.deleteIfExists(CARPETA_ARCHIVOS.resolve(requestId + ".json"));

                        } catch (java.io.IOException e) {
                            System.err.println("No se pudo retirar el archivo de la solicitud: "+ e.getMessage());
                        }
                    }
                    mostrarEvento(registro.value());
                    String tipo = evento.path("eventType").asText();

                    if ("WATERING_DENIED".equals(tipo) || "WATERING_FINISHED".equals(tipo)) {

                        var fin = finalesServicio.get(requestId);

                        if (fin != null) {
                            fin.complete(tipo);
                        }
                    }
                }

                // Guardamos la posicion despues de procesar todo el lote.
                if (!registros.isEmpty()) {
                    consumidor.commitSync();
                }
            }

        } catch (WakeupException e) {
            /*
             * wakeup se utiliza al cerrar para interrumpir la espera.
             * Solo lo mostramos como error si no estabamos cerrando.
             */
            if (ejecutando) {
                errorReceptor = e.toString();
                System.err.println("Error del receptor: " + errorReceptor);
            }

        } catch (Exception e) {
            errorReceptor = e.toString();
            System.err.println("Receptor Kafka detenido: " + errorReceptor);

        } finally {
            ejecutando = false;

            // Si falla al arrancar, liberamos tambien al hilo principal.
            inicioReceptor.countDown();

            // El consumidor se cierra desde el mismo hilo que lo utiliza.
            consumidor.close();
        }
    }

    /*
     * Interpreta el JSON y comprueba que el mensaje pertenece a este operario.
     */
    private void mostrarEvento(String contenido) throws Exception {
        JsonNode evento = json.readTree(contenido);

        if (evento == null || !evento.isObject()) {
            throw new IllegalArgumentException(
                    "El evento no es un objeto JSON");
        }

        String destinatario = evento.path("operatorId").asText();

        if (!operatorId.equals(destinatario)) {
            throw new IllegalArgumentException(
                    "Evento recibido con operatorId incorrecto");
        }

        System.out.println();
        synchronized (eventosWeb) {
            if (eventosWeb.size() >= 100) eventosWeb.removeFirst();
            eventosWeb.addLast(json.writerWithDefaultPrettyPrinter().writeValueAsString(evento));
            // Guardar antes de confirmar offsets permite recuperar el resultado tras reiniciar FO.
            java.nio.file.Files.createDirectories(CARPETA_ARCHIVOS);
            var destino = CARPETA_ARCHIVOS.resolve("web-eventos-" + operatorId + ".json");
            var temporal = CARPETA_ARCHIVOS.resolve("web-eventos-" + operatorId + ".tmp");
            java.nio.file.Files.writeString(temporal, json.writeValueAsString(eventosWeb));
            java.nio.file.Files.move(temporal, destino, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        }
        System.out.println("=== EVENTO PARA " + operatorId + " ===");
        System.out.println(
                json.writerWithDefaultPrettyPrinter()
                        .writeValueAsString(evento));
        System.out.println("================================");
    }

    /*
     * Pide los datos del servicio y publica una solicitud de riego.
     * Publicarla no significa que Central haya autorizado el servicio.
     */
    private void solicitarRiego(Scanner teclado) throws Exception {
        System.out.print("ID de la estacion: ");
        String stationId = teclado.nextLine().trim();

        if (stationId.isEmpty()) {
            System.out.println("La estacion no puede estar vacia.");
            return;
        }

        System.out.print("Duracion en segundos: ");
        int duracion;

        try {
            duracion = Integer.parseInt(teclado.nextLine().trim());
        } catch (NumberFormatException e) {
            System.out.println("La duracion debe ser un numero entero.");
            return;
        }

        if (duracion <= 0) {
            System.out.println("La duracion debe ser mayor que cero.");
            return;
        }

        publicarSolicitud(UUID.randomUUID().toString(),stationId,duracion);
    }

    // Gestiona la entrada del usuario, mientras el otro hilo recibe eventos.
    private void mostrarMenu() {
        Scanner teclado = new Scanner(System.in);

        while (ejecutando) {
            System.out.println();
            System.out.println("=== WM_FO " + operatorId + " ===");
            System.out.println("1. Solicitar riego");
            System.out.println("2. Consultar estaciones disponibles");
            System.out.println("3. Ejecutar servicios desde fichero");
            System.out.println("0. Salir");
            System.out.print("> ");

            // Si se cierra la entrada de consola, terminamos el menu.
            if (!teclado.hasNextLine()) {
                return;
            }

            String opcion = teclado.nextLine().trim();

            // El receptor podria haber fallado mientras el usuario escribia.
            if (!ejecutando) {
                return;
            }

            switch (opcion) {
                case "1":
                    try {
                        solicitarRiego(teclado);
                    } catch (Exception e) {
                        System.err.println("No se pudo confirmar el envio: " + e.getMessage());
                        System.err.println("Esto no significa que Central haya " + "denegado el riego.");
                    }
                    break;

                case "2":
                    try {
                        consultarEstaciones();
                    } catch (Exception e) {
                        System.err.println("No se pudo enviar la consulta: " + e.getMessage());
                    }
                    break;
                case "3":
                    System.out.print("Ruta del fichero: ");

                    try {
                        ejecutarServiciosDesdeFichero(teclado.nextLine().trim());

                    } catch (Exception e) {
                        System.err.println("Fichero detenido: " + e.getMessage());
                        System.err.println("Si se envio una solicitud, puede seguir pendiente. No se envia el siguiente servicio automaticamente.");
                    }
                    break;
                case "0":
                    return;

                default:
                    System.out.println("Opcion no valida.");
            }
        }
    }

    // Arranca el receptor y espera a que este preparado antes de abrir el menu.
    public void ejecutar() throws Exception {
        hiloReceptor = new Thread(this::recibirEventos, "fo-eventos");
        hiloReceptor.start();
        iniciarArchivoPendientes();

        try {
            System.out.println("Conectando con Kafka...");
            System.out.println("Topic de respuestas: " + topicEventos);

            boolean inicioCompletado = inicioReceptor.await(30, TimeUnit.SECONDS);

            if (!inicioCompletado) {
                throw new IllegalStateException("El receptor no se ha preparado en 30 segundos");
            }

            if (!ejecutando) {
                throw new IllegalStateException("No se pudo iniciar el receptor: " + errorReceptor);
            }

            System.out.println("Operario conectado: " + operatorId);
            iniciarWeb();
            System.out.println("FO_READY");
            if (Boolean.parseBoolean(System.getenv().getOrDefault("FO_CONSOLE", "false"))) {
                mostrarMenu();
            } else {
                while (ejecutando) Thread.sleep(500);
            }

        } finally {
            // Cerramos tanto al salir del menu como si ocurre un error.
            cerrar();
        }
    }

    // Solicita el cierre del receptor y libera el productor.
    private void cerrar() throws InterruptedException {
        ejecutando = false;
        if (web != null) web.stop(0);
        executorWeb.shutdownNow();

        // Interrumpe la espera del consumidor para que pueda terminar.
        consumidor.wakeup();

        try {
            // Esperamos a que el receptor termine y cierre su consumidor.
            hiloReceptor.join();
        } finally {
            productor.close(Duration.ofSeconds(5));
        }
    }
    
    private void archivarSolicitud(String requestId, String contenido) {
        try {
            java.nio.file.Files.createDirectories(CARPETA_ARCHIVOS);

            java.nio.file.Files.writeString(
                    CARPETA_ARCHIVOS.resolve(requestId + ".json"),
                    contenido);

            registrarMensaje("Solicitud archivada por falta de respuesta: " + requestId);
        } catch (Exception e) {
            registrarMensaje("No se pudo archivar la solicitud: " + requestId + ": " + e.getMessage());
        }
    }

    private void iniciarArchivoPendientes() {
        Thread hilo = new Thread(() -> {
            while (ejecutando) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    return;
                }

                Instant limite = Instant.now().minusSeconds(4);

                for (var entrada : tiemposPendientes.entrySet()) {
                    if (entrada.getValue().isBefore(limite)) {
                        String requestId = entrada.getKey();

                        String contenido = solicitudesPendientes.remove(requestId);
                        tiemposPendientes.remove(requestId);

                        if (contenido != null) {
                            archivarSolicitud(requestId, contenido);
                        }
                    }
                }
            }
        }, "fo-archivo-pendientes");

        hilo.setDaemon(true);
        hilo.start();
    }

    private void ejecutarServiciosDesdeFichero(String ruta) throws Exception {
        var lineas = java.nio.file.Files.readAllLines(java.nio.file.Path.of(ruta),java.nio.charset.StandardCharsets.UTF_8);

        var estaciones = new java.util.ArrayList<String>();
        var duraciones = new java.util.ArrayList<Integer>();

        for (int i = 0; i < lineas.size(); i++) {
            String linea = lineas.get(i).trim();

            if (linea.isEmpty() || linea.startsWith("#")) {
                continue;
            }

            String[] campos = linea.split(";", -1);

            if (campos.length != 2 || campos[0].isBlank()) {
                throw new IllegalArgumentException("Formato incorrecto en linea " + (i + 1));
            }

            int duracion;

            try {
                duracion = Integer.parseInt(campos[1].trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Duracion incorrecta en linea " + (i + 1));
            }

            if (duracion <= 0) {
                throw new IllegalArgumentException("Duracion no positiva en linea " + (i + 1));
            }

            estaciones.add(campos[0].trim());
            duraciones.add(duracion);
        }

        for (int i = 0; i < estaciones.size() && ejecutando; i++) {
            String requestId = UUID.randomUUID().toString();

            var fin =new java.util.concurrent.CompletableFuture<String>();

            finalesServicio.put(requestId, fin);

            try {
                publicarSolicitud(requestId,estaciones.get(i),duraciones.get(i));

                boolean terminado = false;

                while (ejecutando && !terminado) {
                    try {
                        String resultado = fin.get(1, java.util.concurrent.TimeUnit.SECONDS);

                        registrarMensaje("Servicio del fichero terminado: " + resultado);

                        terminado = true;

                    } catch (java.util.concurrent.TimeoutException e) {
                    }
                }

                if (!terminado) {
                    throw new IllegalStateException("Receptor detenido; se interrumpe el fichero");
                }

            } finally {
                finalesServicio.remove(requestId);
            }

            if (i + 1 < estaciones.size() && ejecutando) {
                registrarMensaje("Esperando 4 segundos para el siguiente servicio...");

                Thread.sleep(4000);
            }
        }

        registrarMensaje("Ejecucion del fichero terminada.");
    }

    private void publicarSolicitud(String requestId,String stationId,int duracion) throws Exception {

        if (stationId.isBlank() || duracion <= 0) {
            throw new IllegalArgumentException("Estacion o duracion no valida");
        }

        ObjectNode solicitud = json.createObjectNode();

        solicitud.put("schemaVersion", 1);
        solicitud.put("eventId", UUID.randomUUID().toString());
        solicitud.put("eventType", "WATERING_REQUESTED");
        solicitud.put("occurredAt", Instant.now().toString());
        solicitud.put("requestId", requestId);
        solicitud.put("stationId", stationId);
        solicitud.put("operatorId", operatorId);
        solicitud.put("durationSeconds", duracion);

        String contenido = json.writeValueAsString(solicitud);

        solicitudesPendientes.put(requestId, contenido);
        tiemposPendientes.put(requestId, Instant.now());

        registrarMensaje("Enviando solicitud " + requestId + " a " + stationId + " durante " + duracion + " segundos.");

        productor.send(new ProducerRecord<>(TOPIC_SOLICITUDES,stationId,contenido)).get();

        registrarMensaje("Solicitud " + requestId + " publicada en Kafka. Pendiente de respuesta de Central.");
    }

    private static void asegurarTopicOperario(String bootstrapServers,String operatorId) {

        Properties propiedades = new Properties();

        propiedades.put("bootstrap.servers", bootstrapServers);
        propiedades.put("request.timeout.ms", "10000");
        propiedades.put("default.api.timeout.ms", "15000");

        String topic = "wm.fo." + operatorId + ".events";

        try (var admin = org.apache.kafka.clients.admin.Admin.create(propiedades)) {

            var nuevoTopic = new org.apache.kafka.clients.admin.NewTopic(topic, 1, (short) 1);

            try {
                admin.createTopics(List.of(nuevoTopic)).all().get(20, TimeUnit.SECONDS);
                System.out.println("Topic creado: " + topic);

            } catch (java.util.concurrent.ExecutionException e) {
                if (!(e.getCause()instanceof org.apache.kafka.common.errors.TopicExistsException)) {
                    throw new IllegalStateException("No se pudo crear el topic del operario",e.getCause());
                }
                System.out.println("Topic ya existente: " + topic);
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IllegalStateException("Creacion del topic interrumpida", e);

        } catch (java.util.concurrent.TimeoutException e) {
            throw new IllegalStateException("Timeout creando el topic del operario", e);
        }
    }

    public static void main(String[] args) {
        // Ejemplo: java WM_FO localhost:9092 OP-01
        if (args.length != 2 || args[0].isBlank() || args[1].isBlank()) {
            System.out.println("Uso: java WM_FO bootstrapServers operatorId");
            return;
        }

        // Limitamos los caracteres porque el ID forma parte del topic.
        if (!args[1].matches("[A-Za-z0-9_-]+")) {
            System.out.println("Identificador de operario no valido.");
            return;
        }

        try {
            WM_FO operario = new WM_FO(args[0], args[1]);
            operario.ejecutar();
        } catch (Exception e) {
            System.err.println("No se pudo ejecutar WM_FO: " + e.getMessage());
            System.exit(1);
        }
    }

    private static void validarOperario(String id) {
        String central = System.getenv("CENTRAL_URL");
        if (central == null || central.isBlank())
            throw new IllegalStateException("Falta CENTRAL_URL (URL de la web de Central)");
        try {
            var cliente = java.net.http.HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5)).build();
            var uri = java.net.URI.create(central.replaceAll("/+$", "")
                + "/api/registro?tipo=operator&id="
                + java.net.URLEncoder.encode(id, java.nio.charset.StandardCharsets.UTF_8));
            var peticion = java.net.http.HttpRequest.newBuilder(uri)
                .timeout(Duration.ofSeconds(8)).GET().build();
            var respuesta = cliente.send(peticion, java.net.http.HttpResponse.BodyHandlers.ofString());
            if (respuesta.statusCode() != 200)
                throw new IllegalStateException("Central no pudo validar el operario (HTTP " + respuesta.statusCode() + ")");
            String activo = respuesta.body().trim();
            if (!activo.equals("true") && !activo.equals("false"))
                throw new IllegalStateException("Respuesta de registro no valida");
            if (activo.equals("false"))
                throw new IllegalStateException("El operario " + id + " no existe o esta inactivo. Registralo en /altas.");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Validacion interrumpida", e);
        } catch (java.io.IOException | IllegalArgumentException e) {
            throw new IllegalStateException("No se pudo conectar con Central para validar el operario", e);
        }
    }

    private void iniciarWeb() throws java.io.IOException {
        int puerto = Integer.parseInt(System.getenv().getOrDefault("FO_WEB_PORT", "8081"));
        web = com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress(puerto), 0);
        web.createContext("/", x -> {
            String ruta = x.getRequestURI().getPath();
            if ("/".equals(ruta) && "POST".equals(x.getRequestMethod())) {
                try {
                    String origen = x.getRequestHeaders().getFirst("Origin");
                    String host = x.getRequestHeaders().getFirst("Host");
                    if (origen != null && !origen.equals("http://" + host) && !origen.equals("https://" + host)) {
                        x.sendResponseHeaders(403, -1); x.close(); return;
                    }
                    if (!ejecutando) throw new IllegalStateException("Receptor detenido");
                    byte[] bytes = x.getRequestBody().readNBytes(8193);
                    if (bytes.length > 8192) throw new IllegalArgumentException("Formulario demasiado grande");
                    var datos = new java.util.HashMap<String, String>();
                    for (String par : new String(bytes, java.nio.charset.StandardCharsets.UTF_8).split("&")) {
                        String[] c = par.split("=", 2);
                        if (c.length == 2) datos.put(c[0], java.net.URLDecoder.decode(c[1], java.nio.charset.StandardCharsets.UTF_8));
                    }
                    switch (datos.getOrDefault("accion", "")) {
                        case "consultar" -> consultarEstaciones();
                        case "regar" -> publicarSolicitud(UUID.randomUUID().toString(),
                            datos.getOrDefault("estacion", "").trim(), Integer.parseInt(datos.getOrDefault("duracion", "0")));
                        case "fichero" -> {
                            var raiz = java.nio.file.Path.of("/servicios").toRealPath();
                            var archivo = java.nio.file.Path.of(datos.getOrDefault("ruta", "")).toRealPath();
                            if (!archivo.startsWith(raiz) || !java.nio.file.Files.isRegularFile(archivo))
                                throw new IllegalArgumentException("El fichero debe estar dentro de /servicios");
                            if (!ficheroEnCurso.compareAndSet(false, true))
                                throw new IllegalArgumentException("Ya se esta ejecutando un fichero");
                            new Thread(() -> {
                                try {
                                    ejecutarServiciosDesdeFichero(archivo.toString());
                                    mensajeWeb = "Fichero terminado";
                                } catch (Exception e) {
                                    mensajeWeb = "Fichero detenido: " + e.getMessage();
                                    registrarMensaje(mensajeWeb);
                                } finally { ficheroEnCurso.set(false); }
                            }, "fo-fichero").start();
                        }
                        default -> throw new IllegalArgumentException("Accion no valida");
                    }
                    mensajeWeb = "Accion enviada. Espera su confirmacion en los eventos.";
                } catch (IllegalArgumentException e) {
                    mensajeWeb = "Datos no validos: " + e.getMessage();
                } catch (Exception e) {
                    mensajeWeb = "No se pudo confirmar la accion. Revisa los eventos antes de repetir.";
                    registrarMensaje(mensajeWeb + " Detalle: " + e.getMessage());
                }
                x.getResponseHeaders().set("Location", "/");
                x.sendResponseHeaders(303, -1); x.close(); return;
            }
            int estado = 200;
            String tipo = "text/plain; charset=utf-8";
            String respuesta;
            if ("/".equals(ruta) && "GET".equals(x.getRequestMethod())) {
                tipo = "text/html; charset=utf-8"; respuesta = htmlWeb();
            } else if ("/eventos".equals(ruta) && "GET".equals(x.getRequestMethod())) {
                synchronized (eventosWeb) {
                    respuesta = "Operario: " + operatorId + "\n" + mensajeWeb + "\n\n"
                        + String.join("\n\n", eventosWeb);
                }
            } else {
                estado = 404; respuesta = "Ruta o metodo no encontrado";
            }
            byte[] bytes = respuesta.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            x.getResponseHeaders().set("Content-Type", tipo);
            x.getResponseHeaders().set("Cache-Control", "no-store");
            x.getResponseHeaders().set("X-Content-Type-Options", "nosniff");
            x.sendResponseHeaders(estado, bytes.length);
            try (var salida = x.getResponseBody()) { salida.write(bytes); }
        });
        web.setExecutor(executorWeb);
        web.start();
        System.out.println("Web FO en puerto " + puerto);
    }

   
private static String htmlWeb() {
    return """
        <!DOCTYPE html>
        <html lang="es">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <meta name="theme-color" content="#101923">
            <title>Operario | Panel de control</title>

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

                .grid {
                    display: grid;
                    grid-template-columns: repeat(2, 1fr);
                    gap: 18px;
                }

                .card {
                    background: var(--panel);
                    border: 1px solid var(--border);
                    border-radius: 16px;
                    padding: 23px;
                    box-shadow: 0 8px 25px #0002;
                }

                .card.full {
                    grid-column: 1 / -1;
                }

                .card-header {
                    display: flex;
                    align-items: center;
                    gap: 12px;
                    margin-bottom: 20px;
                }

                .icon {
                    width: 42px;
                    height: 42px;
                    display: grid;
                    place-items: center;
                    border-radius: 12px;
                    background: var(--panel2);
                    font-size: 21px;
                    flex-shrink: 0;
                }

                h2 {
                    margin: 0;
                    font-size: 17px;
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
                    margin: 14px 0 7px;
                }

                input {
                    width: 100%;
                    padding: 12px 14px;
                    border-radius: 9px;
                    border: 1px solid var(--border);
                    background: #101c27;
                    color: var(--text);
                    font-size: 14px;
                    outline: none;
                    transition: .2s;
                }

                input:focus {
                    border-color: var(--blue);
                    box-shadow: 0 0 0 3px #399cff20;
                }

                input::placeholder {
                    color: #627987;
                }

                button {
                    width: 100%;
                    margin-top: 17px;
                    padding: 12px 16px;
                    border: 0;
                    border-radius: 9px;
                    color: white;
                    font-weight: 650;
                    font-size: 14px;
                    cursor: pointer;
                    transition: .2s;
                }

                button:hover {
                    filter: brightness(1.12);
                    transform: translateY(-1px);
                }

                button:active {
                    transform: scale(.98);
                }

                .btn-blue {
                    background: linear-gradient(135deg, #398fff, #2471d2);
                }

                .btn-green {
                    background: linear-gradient(135deg, #27c989, #159c70);
                }

                .btn-orange {
                    background: linear-gradient(135deg, #f0a34a, #d67b27);
                }

                .terminal-header {
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    margin-bottom: 14px;
                    gap: 10px;
                }

                .terminal-label {
                    display: flex;
                    align-items: center;
                    gap: 12px;
                }

                .terminal-dots {
                    display: flex;
                    gap: 5px;
                    margin-bottom: 10px;
                }

                .terminal-dots span {
                    width: 9px;
                    height: 9px;
                    border-radius: 50%;
                    background: #526574;
                }

                .terminal-dots span:first-child {
                    background: #ff6262;
                }

                .terminal-dots span:nth-child(2) {
                    background: #f4b74f;
                }

                .terminal-dots span:last-child {
                    background: #37ce8a;
                }

                .live {
                    color: var(--green);
                    font-size: 11px;
                    font-weight: 700;
                    white-space: nowrap;
                }

                #eventos {
                    height: 320px;
                    overflow-y: auto;
                    overflow-x: hidden;
                    white-space: pre-wrap;
                    overflow-wrap: anywhere;
                    background: #0b141c;
                    color: #a9d9c3;
                    border: 1px solid #294350;
                    border-radius: 11px;
                    padding: 17px;
                    font: 13px/1.7 Consolas, monospace;
                    scrollbar-width: thin;
                    scrollbar-color: #456174 #0b141c;
                }

                footer {
                    text-align: center;
                    color: #657c8b;
                    font-size: 11px;
                    padding: 25px 0 5px;
                }

                @media (max-width: 650px) {
                    body {
                        padding: 20px 13px;
                    }

                    .grid {
                        grid-template-columns: 1fr;
                    }

                    .card.full {
                        grid-column: auto;
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
                }
            </style>
        </head>

        <body>

        <div class="container">

            <header>
                <div class="logo">⚙️</div>

                <div>
                    <h1>Panel de Operario</h1>
                    <div class="subtitle">
                        Sistema de gestión y control de riego
                    </div>
                </div>

                <div class="status">
                    <span class="dot"></span>
                    SISTEMA ACTIVO
                </div>
            </header>

            <div class="grid">

                <!-- CONSULTAR ESTACIONES -->
                <section class="card">

                    <div class="card-header">
                        <div class="icon">📡</div>
                        <div>
                            <h2>Estaciones</h2>
                            <div class="description">
                                Consultar estaciones disponibles
                            </div>
                        </div>
                    </div>

                    <form method="post">
                        <button
                            class="btn-blue"
                            name="accion"
                            value="consultar">
                            ↻ Consultar estaciones
                        </button>
                    </form>

                </section>

                <!-- SOLICITAR RIEGO -->
                <section class="card">

                    <div class="card-header">
                        <div class="icon">💧</div>
                        <div>
                            <h2>Solicitar riego</h2>
                            <div class="description">
                                Enviar una orden de riego
                            </div>
                        </div>
                    </div>

                    <form method="post">

                        <input type="hidden"
                            name="accion"
                            value="regar">

                        <label for="estacion">
                            Identificador de estación
                        </label>

                        <input
                            id="estacion"
                            name="estacion"
                            placeholder="Ej. WS-01"
                            required
                            pattern="[A-Za-z0-9_-]+">

                        <label for="duracion">
                            Duración del riego (segundos)
                        </label>

                        <input
                            id="duracion"
                            name="duracion"
                            type="number"
                            min="1"
                            step="1"
                            value="20"
                            required>

                        <button class="btn-green">
                            💧 Solicitar riego
                        </button>

                    </form>

                </section>

                <!-- SERVICIOS DESDE FICHERO -->
                <section class="card full">

                    <div class="card-header">
                        <div class="icon">📂</div>
                        <div>
                            <h2>Servicios desde fichero</h2>
                            <div class="description">
                                Ejecutar operaciones definidas en un fichero
                            </div>
                        </div>
                    </div>

                    <form method="post">

                        <input type="hidden"
                            name="accion"
                            value="fichero">

                        <label for="ruta">
                            Ruta del fichero
                        </label>

                        <input
                            id="ruta"
                            name="ruta"
                            placeholder="/servicios/prueba.txt"
                            required>

                        <button class="btn-orange">
                            ▶ Ejecutar servicio
                        </button>

                    </form>

                </section>

                <!-- REGISTRO DE EVENTOS -->
                <section class="card full">

                    <div class="terminal-header">

                        <div class="terminal-label">
                            <div class="icon">📋</div>
                            <div>
                                <h2>Mensajes del operario</h2>
                                <div class="description">
                                    Registro de actividad en tiempo real
                                </div>
                            </div>
                        </div>

                        <div class="live">● EN DIRECTO</div>

                    </div>

                    <div class="terminal-dots">
                        <span></span>
                        <span></span>
                        <span></span>
                    </div>

                    <pre id="eventos">Conectando con el sistema...</pre>

                </section>

            </div>

            <footer>
                PANEL DE OPERARIO · Sistema de control de riego
            </footer>

        </div>

        <script>
            async function actualizar() {

                const ventana = document.getElementById('eventos');

                // Si estaba cerca del final, seguimos los mensajes nuevos.
                const seguir =
                    ventana.scrollHeight -
                    ventana.scrollTop -
                    ventana.clientHeight < 40;

                const posicion = ventana.scrollTop;

                try {

                    const respuesta = await fetch('/eventos', {
                        cache: 'no-store'
                    });

                    if (!respuesta.ok) {
                        throw new Error(
                            'No se pudieron consultar los mensajes'
                        );
                    }

                    const texto = await respuesta.text();

                    if (ventana.textContent !== texto) {

                        ventana.textContent = texto;

                        ventana.scrollTop = seguir
                            ? ventana.scrollHeight
                            : posicion;
                    }

                } catch {

                    // Conservamos los mensajes anteriores si falla una consulta.
                    console.warn(
                        'No se pudo actualizar el registro del operario'
                    );

                }

                setTimeout(actualizar, 1500);
            }

            actualizar();
        </script>

        </body>
        </html>
        """;
    }

    private void registrarMensaje(String mensaje) {
        String linea = "[" + java.time.LocalTime.now()
                .withNano(0) + "] " + mensaje;

        System.out.println(linea);

        synchronized (eventosWeb) {
            if (eventosWeb.size() >= 100) {
                eventosWeb.removeFirst();
            }

            eventosWeb.addLast(linea);
        }
    }
}
