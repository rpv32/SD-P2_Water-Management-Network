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

    private final KafkaProducer<String, String> productor;
    private final KafkaConsumer<String, String> consumidor;

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

    public WM_FO(String bootstrapServers, String operatorId) {
        this.operatorId = operatorId;
        this.topicEventos = "wm.fo." + operatorId + ".events";

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
                    mostrarEvento(registro.value());
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
     * Por ahora mostramos el evento completo para facilitar las pruebas.
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

        // requestId identifica la solicitud durante todo su recorrido.
        String requestId = UUID.randomUUID().toString();

        // Construimos el mensaje siguiendo el protocolo del proyecto.
        ObjectNode solicitud = json.createObjectNode();
        solicitud.put("schemaVersion", 1);
        solicitud.put("eventId", UUID.randomUUID().toString());
        solicitud.put("eventType", "WATERING_REQUESTED");
        solicitud.put("occurredAt", Instant.now().toString());
        solicitud.put("requestId", requestId);
        solicitud.put("stationId", stationId);
        solicitud.put("operatorId", operatorId);
        solicitud.put("durationSeconds", duracion);

        // La clave del mensaje es el identificador de la estacion.
        ProducerRecord<String, String> registro = new ProducerRecord<>(TOPIC_SOLICITUDES, stationId, json.writeValueAsString(solicitud));

        /*
         * Mostramos el ID antes de enviar para poder localizar la solicitud
         * incluso si no conseguimos confirmar su publicacion.
         */
        System.out.println("requestId: " + requestId);

        // get espera a que el envio termine correctamente o produzca un error.
        productor.send(registro).get();

        System.out.println("Solicitud publicada en Kafka.");
        System.out.println("Pendiente de respuesta de Central.");
    }

    // Gestiona la entrada del usuario, mientras el otro hilo recibe eventos.
    private void mostrarMenu() {
        Scanner teclado = new Scanner(System.in);

        while (ejecutando) {
            System.out.println();
            System.out.println("=== WM_FO " + operatorId + " ===");
            System.out.println("1. Solicitar riego");
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

        try {
            System.out.println("Conectando con Kafka...");
            System.out.println("Topic de respuestas: " + topicEventos);

            boolean inicioCompletado = inicioReceptor.await(30, TimeUnit.SECONDS);

            if (!inicioCompletado) {
                System.err.println("El receptor no se ha preparado en 30 segundos.");
                return;
            }

            if (!ejecutando) {
                System.err.println("No se pudo iniciar el receptor: " + errorReceptor);
                return;
            }

            System.out.println("Operario conectado: " + operatorId);
            mostrarMenu();

        } finally {
            // Cerramos tanto al salir del menu como si ocurre un error.
            cerrar();
        }
    }

    // Solicita el cierre del receptor y libera el productor.
    private void cerrar() throws InterruptedException {
        ejecutando = false;

        // Interrumpe la espera del consumidor para que pueda terminar.
        consumidor.wakeup();

        try {
            // Esperamos a que el receptor termine y cierre su consumidor.
            hiloReceptor.join();
        } finally {
            productor.close(Duration.ofSeconds(5));
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
            System.err.println("No se pudo ejecutar WM_FO: " + e);
        }
    }
}