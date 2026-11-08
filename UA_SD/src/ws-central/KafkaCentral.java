import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class KafkaCentral extends Thread {

    private final String bootstrapServers;
    private final DataBaseManager db;
    private final ObjectMapper json = new ObjectMapper();
    private volatile KafkaProducer<String, String> productorActivo;

    private volatile boolean ejecutando = true;
    private volatile KafkaConsumer<String, String> consumidor;

    public KafkaCentral(String bootstrapServers, DataBaseManager db) {
        super("central-kafka");
        this.bootstrapServers = bootstrapServers;
        this.db = db;
    }

    private Properties propiedadesConsumidor() {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrapServers);
        p.put("group.id", "wm-central-requests");
        p.put("key.deserializer", StringDeserializer.class.getName());
        p.put("value.deserializer", StringDeserializer.class.getName());
        p.put("enable.auto.commit", "false");
        p.put("auto.offset.reset", "earliest");
        p.put("allow.auto.create.topics", "false");
        p.put("max.poll.records", "1");
        return p;
    }

    private Properties propiedadesProductor() {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrapServers);
        p.put("key.serializer", StringSerializer.class.getName());
        p.put("value.serializer", StringSerializer.class.getName());
        p.put("acks", "all");
        p.put("enable.idempotence", "true");
        p.put("max.block.ms", "10000");
        p.put("delivery.timeout.ms", "15000");
        p.put("request.timeout.ms", "10000");
        return p;
    }

    private String texto(JsonNode evento, String campo) {
        JsonNode valor = evento.get(campo);

        if (valor == null || !valor.isTextual()
                || valor.asText().isBlank()) {
            throw new IllegalArgumentException(
                    "Campo obligatorio no valido: " + campo);
        }

        return valor.asText();
    }

    private void procesar(
            String contenido,
            KafkaProducer<String, String> productor) throws Exception {

        JsonNode solicitud = json.readTree(contenido);

        if (solicitud == null || !solicitud.isObject()) {
            throw new IllegalArgumentException("Se esperaba un objeto JSON");
        }

        JsonNode version = solicitud.get("schemaVersion");

        if (version == null || !version.isIntegralNumber()
                || version.asInt() != 1) {
            throw new IllegalArgumentException("schemaVersion no valida");
        }

        if (!"WATERING_REQUESTED".equals(texto(solicitud, "eventType"))) {
            throw new IllegalArgumentException("Tipo de evento no valido");
        }

        String eventId = texto(solicitud, "eventId");
        String requestId = texto(solicitud, "requestId");
        String stationId = texto(solicitud, "stationId");
        String operatorId = texto(solicitud, "operatorId");
        String occurredAt = texto(solicitud, "occurredAt");

        UUID.fromString(eventId);
        UUID.fromString(requestId);
        Instant.parse(occurredAt);

        if (!operatorId.matches("[A-Za-z0-9_-]+")) {
            throw new IllegalArgumentException("operatorId no valido");
        }

        JsonNode tiempo = solicitud.get("durationSeconds");

        if (tiempo == null || !tiempo.isIntegralNumber()
                || !tiempo.canConvertToInt() || tiempo.asInt() <= 0) {
            throw new IllegalArgumentException("Duracion no valida");
        }

        System.out.println("KAFKA: solicitud " + requestId
                + " de " + operatorId + " para " + stationId);

        DataBaseManager.DecisionRiego decision = db.procesarSolicitud(
                requestId, stationId, operatorId,
                tiempo.asInt(), occurredAt);

        ObjectNode respuesta = json.createObjectNode();
        respuesta.put("schemaVersion", 1);
        respuesta.put("eventId", UUID.randomUUID().toString());
        respuesta.put("eventType", decision.autorizada()
                ? "WATERING_AUTHORIZED" : "WATERING_DENIED");
        respuesta.put("occurredAt", Instant.now().toString());
        respuesta.put("requestId", requestId);
        respuesta.put("stationId", stationId);
        respuesta.put("operatorId", operatorId);
        respuesta.put("message", decision.mensaje());

        if (decision.autorizada()) {
            respuesta.put("sessionId", decision.sessionId());
        } else {
            respuesta.put("errorCode", decision.codigo());
        }

        String topic = "wm.fo." + operatorId + ".events";

        productor.send(new ProducerRecord<>(
                topic, requestId,
                json.writeValueAsString(respuesta))).get();
        
        if (decision.autorizada()&& db.sesionPendiente(decision.sessionId())) {
            ObjectNode orden = json.createObjectNode();

            orden.put("schemaVersion", 1);
            orden.put("eventId", UUID.randomUUID().toString());
            orden.put("eventType", "START_WATERING");
            orden.put("occurredAt", Instant.now().toString());
            orden.put("requestId", requestId);
            orden.put("sessionId", decision.sessionId());
            orden.put("stationId", stationId);
            orden.put("operatorId", operatorId);
            orden.put("durationSeconds", tiempo.asInt());

            productor.send(new ProducerRecord<>(
                    "wm.station.commands",
                    stationId,
                    json.writeValueAsString(orden))).get();

            System.out.println("KAFKA: START_WATERING enviado a "
                    + stationId + " para " + requestId);
        }

        System.out.println("KAFKA: respuesta "
                + respuesta.path("eventType").asText()
                + " para " + requestId);
    }


     private int entero(JsonNode evento, String campo) {
        JsonNode valor = evento.get(campo);

        if (valor == null || !valor.isIntegralNumber() || !valor.canConvertToInt()) {
            throw new IllegalArgumentException("Entero no valido: " + campo);
        }
        return valor.intValue();
    }

    private double numero(JsonNode evento, String campo) {
        JsonNode valor = evento.get(campo);

        if (valor == null || !valor.isNumber() || !Double.isFinite(valor.doubleValue())) {
            throw new IllegalArgumentException("Numero no valido: " + campo);
        }
        return valor.doubleValue();
    }   

    private void procesarEvento(String topic, String contenido,KafkaProducer<String, String> productor) throws Exception {

        JsonNode e = json.readTree(contenido);

        if (e == null || !e.isObject() || entero(e, "schemaVersion") != 1) {
            throw new IllegalArgumentException("Evento no valido");
        }

        String tipo = texto(e, "eventType");
        String eventId = texto(e, "eventId");
        String sessionId = texto(e, "sessionId");
        String requestId = texto(e, "requestId");
        String stationId = texto(e, "stationId");
        String operatorId = texto(e, "operatorId");
        String fecha = texto(e, "occurredAt");

        UUID.fromString(eventId);
        UUID.fromString(sessionId);
        UUID.fromString(requestId);
        Instant.parse(fecha);

        if (!operatorId.matches("[A-Za-z0-9_-]+")) {
            throw new IllegalArgumentException("Operario no valido");
        }

        String topicEsperado = "WATERING_TELEMETRY".equals(tipo)
                ? "wm.station.telemetry" : "wm.station.results";

        if (!topicEsperado.equals(topic)) {
            throw new IllegalArgumentException("Topic incorrecto");
        }

        boolean reenviar;

        switch (tipo) {
            case "WATERING_STARTED":
                reenviar = db.registrarInicio(
                        sessionId, requestId, stationId, operatorId, fecha);
                break;

            case "WATERING_TELEMETRY":
                reenviar = db.registrarTelemetria(eventId, sessionId, requestId, stationId, operatorId,entero(e, "sequence"),numero(e, "flowLitresPerMinute"),numero(e, "volumeLitres"), fecha);
                break;

            case "WATERING_FINISHED":
                reenviar = db.registrarFin(sessionId, requestId, stationId, operatorId,texto(e, "reason"), entero(e, "durationSeconds"),numero(e, "volumeLitres"), fecha);
                break;

            default:
                throw new IllegalArgumentException("Evento no soportado");
        }

        if (reenviar) {
            productor.send(new ProducerRecord<>("wm.fo." + operatorId + ".events",sessionId,json.writeValueAsString(e))).get();

            System.out.println("KAFKA: " + tipo + " de " + stationId);

            if ("WATERING_TELEMETRY".equals(tipo)) {
                System.out.println("Caudal: " + numero(e, "flowLitresPerMinute") + " L/min; volumen: " + numero(e, "volumeLitres") + " L");
            }
        }
    }

    public void enviarOrden(String stationId, String tipo) throws Exception {
        KafkaProducer<String, String> p = productorActivo;

        if (p == null) {
            throw new IllegalStateException("El servicio Kafka de Central no esta preparado");
        }

        ObjectNode orden = json.createObjectNode();

        orden.put("schemaVersion", 1);
        orden.put("eventId", UUID.randomUUID().toString());
        orden.put("eventType", tipo);
        orden.put("occurredAt", Instant.now().toString());
        orden.put("stationId", stationId);

        p.send(new ProducerRecord<>("wm.station.commands",stationId,json.writeValueAsString(orden))).get();

        System.out.println("KAFKA: " + tipo + " enviado a " + stationId);
    }

    public void solicitarRiego(String stationId, String operatorId, int duracion)throws Exception {

        if (duracion <= 0) {
            throw new IllegalArgumentException("La duracion debe ser mayor que cero");
        }

        KafkaProducer<String, String> p = productorActivo;

        if (p == null) {
            throw new IllegalStateException("El servicio Kafka de Central no esta preparado");
        }

        ObjectNode solicitud = json.createObjectNode();

        solicitud.put("schemaVersion", 1);
        solicitud.put("eventId", UUID.randomUUID().toString());
        solicitud.put("eventType", "WATERING_REQUESTED");
        solicitud.put("occurredAt", Instant.now().toString());
        solicitud.put("requestId", UUID.randomUUID().toString());
        solicitud.put("stationId", stationId);
        solicitud.put("operatorId", operatorId);
        solicitud.put("durationSeconds", duracion);

        p.send(new ProducerRecord<>("wm.watering.requests",stationId,json.writeValueAsString(solicitud))).get();

        System.out.println("Solicitud publicada para " + stationId);
    }

    private void procesarConsulta(
        String contenido,
        KafkaProducer<String, String> productor) throws Exception {

        JsonNode consulta = json.readTree(contenido);

        if (consulta == null || !consulta.isObject()|| entero(consulta, "schemaVersion") != 1|| !"STATIONS_QUERY".equals(texto(consulta, "eventType"))) {
            throw new IllegalArgumentException("Consulta no valida");
        }

        String operatorId = texto(consulta, "operatorId");
        String requestId = texto(consulta, "requestId");

        UUID.fromString(texto(consulta, "eventId"));
        UUID.fromString(requestId);
        Instant.parse(texto(consulta, "occurredAt"));

        if (!operatorId.matches("[A-Za-z0-9_-]+")) {
            throw new IllegalArgumentException("Operario no valido");
        }

        ObjectNode respuesta = json.createObjectNode();

        respuesta.put("schemaVersion", 1);
        respuesta.put("eventId", UUID.randomUUID().toString());
        respuesta.put("occurredAt", Instant.now().toString());
        respuesta.put("requestId", requestId);
        respuesta.put("operatorId", operatorId);

        if (!db.existeOperador(operatorId)) {
            respuesta.put("eventType", "QUERY_DENIED");
            respuesta.put("errorCode", "UNKNOWN_OPERATOR");
            respuesta.put("message", "El operario no existe o esta inactivo");

        } else {
            respuesta.put("eventType", "STATIONS_RESULT");

            var disponibles = respuesta.putArray("stations");
            JsonNode estaciones = json.readTree(db.listarEstacionesJson());

            for (JsonNode estacion : estaciones) {
                if ("AVAILABLE".equals(estacion.path("status").asText())) {
                    disponibles.add(estacion);
                }
            }
        }

        productor.send(new ProducerRecord<>("wm.fo." + operatorId + ".events",requestId,json.writeValueAsString(respuesta))).get();
    }

    public synchronized String ejecutarAccionWeb(
        String accion,
        String stationId,
        String operatorId,
        int duracion) throws Exception {

		if (!java.util.Set.of("START", "BLOCK", "ACTIVATE").contains(accion)) {
			throw new IllegalArgumentException("Accion no valida");
		}

		if (stationId == null || stationId.isBlank()) {
			throw new IllegalArgumentException("Debe indicar una estacion");
		}

		if ("START".equals(accion)) {
			if (operatorId == null
					|| !operatorId.matches("[A-Za-z0-9_-]+")
					|| !db.existeOperador(operatorId)) {
				throw new IllegalArgumentException(
						"Operario inexistente o no valido");
			}

			if (duracion <= 0) {
				throw new IllegalArgumentException(
						"La duracion debe ser mayor que cero");
			}
		}

		// Una estacion concreta o todas las registradas.
		java.util.List<String> destinos = new java.util.ArrayList<>();

		if ("*".equals(stationId)) {
			JsonNode estaciones = json.readTree(db.listarEstacionesJson());

			for (JsonNode estacion : estaciones) {
				String id = estacion.path("stationId").asText();

				if (db.existeEstacion(id)) {
					destinos.add(id);
				}
			}
		} else {
			if (!db.existeEstacion(stationId)) {
				throw new IllegalArgumentException(
						"La estacion no existe o esta inactiva");
			}

			destinos.add(stationId);
		}

		if (destinos.isEmpty()) {
			throw new IllegalArgumentException("No hay estaciones");
		}

		StringBuilder resumen = new StringBuilder();

		// Productor independiente del consumidor Kafka de Central.
		try (KafkaProducer<String, String> productorWeb =
					new KafkaProducer<>(propiedadesProductor())) {

			for (String id : destinos) {
				try {
					if ("START".equals(accion)) {
						ObjectNode solicitud = json.createObjectNode();

						solicitud.put("schemaVersion", 1);
						solicitud.put("eventId", UUID.randomUUID().toString());
						solicitud.put("eventType", "WATERING_REQUESTED");
						solicitud.put("occurredAt", Instant.now().toString());
						solicitud.put("requestId", UUID.randomUUID().toString());
						solicitud.put("stationId", id);
						solicitud.put("operatorId", operatorId);
						solicitud.put("durationSeconds", duracion);

						// Se publica en el flujo normal de solicitudes.
						// Central comprobara disponibilidad y autorizara o denegara.
						productorWeb.send(new ProducerRecord<>(
								"wm.watering.requests",
								id,
								json.writeValueAsString(solicitud)
						)).get();

						resumen.append(id)
								.append(": solicitud enviada; pendiente de autorizacion. ");

					} else {
						ObjectNode orden = json.createObjectNode();

						orden.put("schemaVersion", 1);
						orden.put("eventId", UUID.randomUUID().toString());
						orden.put("eventType",
								"BLOCK".equals(accion)
										? "BLOCK_STATION"
										: "ACTIVATE_STATION");
						orden.put("occurredAt", Instant.now().toString());
						orden.put("stationId", id);

						// Se registra primero la intencion de Central.
						if ("BLOCK".equals(accion)) {
							db.bloquearEstacion(id);
						} else {
							db.activarEstacion(id);
						}

						productorWeb.send(new ProducerRecord<>(
								"wm.station.commands",
								id,
								json.writeValueAsString(orden)
						)).get();

						resumen.append(id)
								.append(": orden enviada. ");
					}

				} catch (Exception e) {
					resumen.append(id)
							.append(": ERROR; resultado no confirmado. ");

					System.err.println(
							"WEB: error ejecutando " + accion
							+ " sobre " + id + ": " + e.getMessage());
				}
			}
		}

		return resumen.toString();
	}

    @Override
    public void run() {
        try (
            KafkaConsumer<String, String> cliente =
                    new KafkaConsumer<>(propiedadesConsumidor());
            KafkaProducer<String, String> productor =
                    new KafkaProducer<>(propiedadesProductor())
        ) {
            consumidor = cliente;
            productorActivo = productor;
            cliente.subscribe(List.of("wm.watering.requests","wm.station.results","wm.station.telemetry","wm.fo.queries"));

            System.out.println("KAFKA: consumidor de solicitudes iniciado");

            while (ejecutando) {
                var registros = cliente.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> registro : registros) {
                    try {
                        if ("wm.watering.requests".equals(registro.topic())) {
                            procesar(registro.value(), productor);
                        }
                        else if("wm.fo.queries".equals(registro.topic())){
                            procesarConsulta(registro.value(), productor);
                        } else {
                            procesarEvento(registro.topic(), registro.value(), productor);
                        }

                    } catch (JsonProcessingException
                            | IllegalArgumentException e) {

                        // Descartamos mensajes que no cumplen el contrato.
                        System.err.println(
                                "KAFKA: mensaje no valido en offset "
                                + registro.offset() + ": " + e.getMessage());
                    }

                    cliente.commitSync(Map.of(
                            new TopicPartition(
                                    registro.topic(), registro.partition()),
                            new OffsetAndMetadata(registro.offset() + 1)));
                }
            }

        } catch (WakeupException e) {
            if (ejecutando) {
                System.err.println("KAFKA: receptor interrumpido: " + e);
            }

        } catch (Exception e) {
            System.err.println("KAFKA: procesamiento detenido: " + e);
            System.err.println(
                    "La solicitud pendiente no se ha confirmado. "
                    + "Revisa el error y reinicia Central.");

        } finally {
            ejecutando = false;
            consumidor = null;
            productorActivo = null;
        }
    }

    public void detener() {
        ejecutando = false;

        KafkaConsumer<String, String> cliente = consumidor;

        if (cliente != null) {
            cliente.wakeup();
        }
    }
}
