import java.lang.Exception;
import java.io.*;
import java.net.*;
import java.util.Scanner;
import java.util.UUID;
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
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import java.util.Map;

public class Engine{

    private String StationId;
    private boolean ValvulaAbierta;
    private boolean Regando;
    private volatile String SessionId;
    private String OperatorId;
    private int DuracionObjetivo;
    private int SegundosRegados;
    private double Caudal;
    private double VolumenAcumulado;
    private boolean Bloqueado;
    private volatile boolean Averiado;
    private boolean Fuga;
    private volatile boolean running;
    private volatile boolean monitorConectado;
    private volatile String RequestId;
    private volatile boolean RiegoKafka;
    private final ObjectMapper json = new ObjectMapper();
    private volatile KafkaProducer<String, String> productor;
    private KafkaConsumer<String, String> consumidor;
    private volatile boolean kafkaEjecutando = true;
    private ObjectNode resultadoPendiente;


    public Engine(){
        ValvulaAbierta = false;
        Regando = false;
        running = true;
        monitorConectado = true;
        DuracionObjetivo = 0;
        SegundosRegados = 0;
        Caudal = 0;
        VolumenAcumulado = 0;
        this.StationId = "";
        this.SessionId = "";
        this.OperatorId = "";
    }

    //Topics
    //docker exec broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker:29092 --create --if-not-exists --topic wm.station.commands --partitions 1 --replication-factor 1
    //docker exec broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker:29092 --create --if-not-exists --topic wm.station.telemetry --partitions 1 --replication-factor 1
    //docker exec broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker:29092 --create --if-not-exists --topic wm.station.results --partitions 1 --replication-factor 1


    //Hilos
    public void HiloMonitor(Socket Monitor){
        try{
            Monitor.setSoTimeout(5000);
        }
        catch (SocketException e)
        {
            monitorConectado = false;
            return;
        }

        while(running){ 
            String Respuesta = leeSocket(Monitor);

            if(Respuesta == null){
                break;
            }
            String[] campos = Respuesta.split("#", -1);
            if(campos.length == 3 && campos[0].equals("HEALTH_CHECK")){
                
                try {
                    escribeSocket(Monitor, "HEALTH_RESULT#" + campos[1] + "#" + estadoSalud() + "#" + sessionActiva());
                } catch (IOException e) {
                    System.out.println("Error al escribir en el socket: " + e.toString());
                    monitorConectado = false;
                    return;
                }
            }
            else{
                String correlationId = "";

                if (campos.length > 1) {
                    correlationId = campos[1];
                }
                try {
                    escribeSocket(Monitor, "ERROR#" + correlationId + "#INVALID_FIELD#Trama invalida");
                } catch (IOException e) {
                    System.out.println("Error al escribir en el socket: " + e.toString());
                    monitorConectado = false;
                    return;
                }
              }
        }    
        if (running) {
            System.out.println("\nConexion con el Monitor perdida. No se admiten nuevos riegos, el servicio ya iniciado puede finalizar");
            monitorConectado = false;
            
        }
    }

    public void HiloSimulacion(){
        while(running){
            try{
                Thread.sleep(1000);
            }
            catch(InterruptedException e){
                return;
           }
           simularSegundo();
           publicarTelemetria();
           ComprobarTiempo();
           EnviarResultado();
        }
    }

    public void HiloKafka(String bootstrapServer){
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrapServer);
        p.put("group.id", "wm-engine-" + StationId);
        p.put("key.deserializer", StringDeserializer.class.getName());
        p.put("value.deserializer", StringDeserializer.class.getName());
        p.put("enable.auto.commit", "false");
        p.put("auto.offset.reset", "latest");
        p.put("allow.auto.create.topics", "false");

        Properties productorProps = new Properties();
        productorProps.put("bootstrap.servers", bootstrapServer);
        productorProps.put("key.serializer",StringSerializer.class.getName());
        productorProps.put("value.serializer",StringSerializer.class.getName());
        productorProps.put("acks", "all");
        productorProps.put("enable.idempotence", "true");
        productorProps.put("max.block.ms", "10000");
        productorProps.put("delivery.timeout.ms", "15000");
        productorProps.put("request.timeout.ms", "10000");


        try(KafkaConsumer<String,String> cliente = new KafkaConsumer<>(p); KafkaProducer<String,String> productorKafka = new KafkaProducer<>(productorProps)){
            consumidor = cliente;
            productor = productorKafka;

            cliente.subscribe(List.of("wm.station.commands"));

            System.out.println("[ENGINE] Kafka conectado. Esperando ordenes.");

            while(kafkaEjecutando && running){
                var registros = cliente.poll(Duration.ofMillis(500));

                for(ConsumerRecord<String,String> registro : registros){
                    try{
                        procesarOrden(registro.value(),productorKafka);
                    }
                    catch(JsonProcessingException | IllegalArgumentException e){
                        System.err.println("[ENGINE] Orden Kafka no valida en offset: " + e.getMessage());
                    }

                    cliente.commitSync(Map.of(new TopicPartition(registro.topic(),registro.partition()), new OffsetAndMetadata(registro.offset() + 1)));
                }
            }
        }catch(WakeupException e){
            if (kafkaEjecutando && running) {
                System.err.println("[ENGINE] Kafka interrumpido: " + e.getMessage());
            }
        }catch(Exception e){
            System.err.println("[ENGINE] Kafka detenido: " + e.getMessage());
        }
        finally{
            kafkaEjecutando = false;
            productor = null;
            consumidor = null;
        }
    }

    private void procesarOrden(String contenido, KafkaProducer<String, String> productorKafka) throws Exception{
        JsonNode orden = json.readTree(contenido);

        if(orden == null || !orden.isObject()){
            throw new IllegalArgumentException("Se esperaba un objeto JSON");
        }

        if(!StationId.equals(orden.path("stationId").asText())){
            return;
        }

        if(!"START_WATERING".equals(orden.path("eventType").asText())){
            throw new IllegalArgumentException("Tipo de orden no soportado");
        }

        String sessionId = orden.path("sessionId").asText();
        String requestId = orden.path("requestId").asText();
        String operatorId = orden.path("operatorId").asText();
        int duracion = orden.path("durationSeconds").asInt();

        if (sessionId.isEmpty() || requestId.isEmpty() || operatorId.isEmpty() || duracion <= 0) {
            throw new IllegalArgumentException("Campos obligatorios vacios");
        }

        boolean iniciado = IniciarRiegoKafka(sessionId, requestId, operatorId, duracion);

        if (!iniciado) {
            System.out.println("[ENGINE] No se pudo iniciar el riego solicitado");
            return;
        }

        ObjectNode inicio = json.createObjectNode();
        inicio.put("schemaVersion", 1);
        inicio.put("eventId", UUID.randomUUID().toString());
        inicio.put("eventType", "WATERING_STARTED");
        inicio.put("occurredAt", Instant.now().toString());
        inicio.put("requestId", requestId);
        inicio.put("sessionId", sessionId);
        inicio.put("stationId", StationId);
        inicio.put("operatorId", operatorId);

        productorKafka.send(new ProducerRecord<>("wm.station.results",StationId,json.writeValueAsString(inicio))).get();

        System.out.println("[ENGINE] WATERING_STARTED publicado en Kafka.");
    }

    public void publicarTelemetria(){
        ObjectNode muestra;
        KafkaProducer<String,String> productorAct = productor;

        if(productorAct == null){
            return;
        }

        synchronized(this){
            if (productor == null || !RiegoKafka || RequestId.isEmpty()) {
                return;
            }

            muestra = json.createObjectNode();

            muestra.put("schemaVersion", 1);
            muestra.put("eventId", UUID.randomUUID().toString());
            muestra.put("eventType", "WATERING_TELEMETRY");
            muestra.put("occurredAt", Instant.now().toString());
            muestra.put("requestId", RequestId);
            muestra.put("sessionId", SessionId);
            muestra.put("stationId", StationId);
            muestra.put("operatorId", OperatorId);
            muestra.put("sequence", SegundosRegados);
            muestra.put("flowLitresPerMinute", Caudal);
            muestra.put("volumeLitres", VolumenAcumulado);
        }
        try{
            productor.send(new ProducerRecord<>("wm.station.telemetry",StationId,json.writeValueAsString(muestra))).get();

            System.out.println("[ENGINE] Telemetria publicada. Volumen: " + String.format("%.2f", VolumenAcumulado) + " L");

        }catch(Exception e){
            System.err.println("[ENGINE] Error al publicar telemetria: "+ e.getMessage());
        }
    }

    private ObjectNode crearResultado(String motivo) {
        ObjectNode resultado = json.createObjectNode();

        resultado.put("schemaVersion", 1);
        resultado.put("eventId", UUID.randomUUID().toString());
        resultado.put("eventType", "WATERING_FINISHED");
        resultado.put("occurredAt", Instant.now().toString());
        resultado.put("requestId", RequestId);
        resultado.put("sessionId", SessionId);
        resultado.put("stationId", StationId);
        resultado.put("operatorId", OperatorId);
        resultado.put("reason", motivo);
        resultado.put("durationSeconds", SegundosRegados);
        resultado.put("volumeLitres", VolumenAcumulado);
        return resultado;
    }

    private void EnviarResultado(){
        ObjectNode resultado;
        KafkaProducer<String, String> ProductorAct = productor;

        synchronized(this){
            resultado = resultadoPendiente;
        }

        if(resultado == null || ProductorAct == null){
            return;
        }

        try{
            ProductorAct.send(new ProducerRecord<>("wm.station.results",StationId,json.writeValueAsString(resultado))).get();
            
            synchronized(this){
                if(resultadoPendiente == resultado){
                    resultadoPendiente = null;
                }
            }
        }
        catch(Exception e){
            System.err.println("[ENGINE] Resultado pendiente de envio:" + e.getMessage());
        }
    }

    private synchronized void mostrarEstado() {
        System.out.println("Estacion: " + StationId);
        System.out.println("Estado: " +
            (Bloqueado ? "FUERA DE SERVICIO" :
            Averiado ? "AVERIA / FUGA" :
            Regando ? "REGANDO" : "DISPONIBLE"));

        System.out.println("Valvula: " +
            (ValvulaAbierta ? "ABIERTA" : "CERRADA"));

        if (Regando) {
            System.out.println("Operario: " + OperatorId);
            System.out.println("Caudal: " + Caudal + " L/min");
            System.out.println("Volumen: " + VolumenAcumulado + " L");
            System.out.println("Tiempo: " + SegundosRegados + "/" + DuracionObjetivo);
        }
    }

    public void Menu(){
        Scanner sc = new Scanner(System.in);

        while (running) {
            System.out.println("\n=== Engine " + StationId + " ===");
            System.out.println("1. Iniciar riego");
            System.out.println("2. Parar riego");
            System.out.println("3. Averia / Reparar");
            System.out.println("4. Mostrar estado");
            System.out.println("0. Salir");
            System.out.print("> ");

            if (!sc.hasNextLine()){
                break;
            }

        switch (sc.nextLine().trim()) {
                    case "1":
                        System.out.print("Duracion en segundos: ");
                        try {
                            int d = Integer.parseInt(sc.nextLine().trim());
                            if (!IniciarRiego("LOCAL", d)) {
                                System.out.println("No se puede iniciar el riego");
                            }
                        } catch (NumberFormatException e) {
                            System.out.println("Duracion no valida");
                        }
                        break;
                    case "2":
                        if (sessionActiva().isEmpty()) System.out.println("No hay riego en curso");
                        else PararRiego("MANUAL_STOP");
                        EnviarResultado();

                        synchronized(this){
                            if(resultadoPendiente != null){
                                System.err.println("[ENGINE] Aviso: queda un resultado sin confirmar en Kafka.");
                            }
                        }
                        break;
                    case "3":
                        setAveria(estadoSalud().equals("OK"));   // alterna OK <-> KO
                        break;
                    case "4":
                        mostrarEstado();
                        break;
                    case "0":
                        running = false;
                        break;
                    default:
                        System.out.println("Opcion no valida");
                }
        }
        PararRiego("MANUAL_STOP");
    }

    public void Iniciar(Socket Monitor, String bootstrapServers){
        new Thread(() -> HiloMonitor(Monitor)).start();
        new Thread(() -> HiloSimulacion()).start();
        new Thread(() -> HiloKafka(bootstrapServers)).start();
        Menu();
    }

    synchronized void simularSegundo() {
        if (!Regando){
            return;
        }                    

        Caudal = 10 + Math.random() * 5;           
        VolumenAcumulado += Caudal / 60.0;          
        SegundosRegados++;
    }

    private synchronized void ComprobarTiempo(){
        if(Regando && SegundosRegados >= DuracionObjetivo){
            PararRiego("TIME_LIMIT");
        }
    }

    public synchronized boolean IniciarRiego(String OperatorId, int Duracion){
        if (Duracion <= 0 || Regando || Bloqueado || Averiado || !monitorConectado || resultadoPendiente != null) {
            return false;
        }

        this.SessionId = UUID.randomUUID().toString();
        this.OperatorId = OperatorId;
        this.DuracionObjetivo = Duracion;
        this.SegundosRegados = 0;
        this.Caudal = 0;
        this.VolumenAcumulado = 0;
        this.ValvulaAbierta = true;
        this.Regando = true;
        this.RequestId = "";
        this.RiegoKafka = false;
        
        System.out.println("[ENGINE] Riego iniciado. Operario " + OperatorId + ", " + Duracion + " s");
        return true;
    }

    public synchronized boolean IniciarRiegoKafka(String sessionId,String requestId,String operatorId,int duracion){
        if(duracion <= 0 || Regando || Bloqueado ||Averiado || !monitorConectado || resultadoPendiente != null){
            return false;
        }

        this.SessionId = sessionId;
        this.RequestId = requestId;
        this.OperatorId = operatorId;
        this.RiegoKafka = true;
        this.DuracionObjetivo = duracion;
        this.SegundosRegados = 0;
        this.Caudal = 0;
        this.VolumenAcumulado = 0;
        this.ValvulaAbierta = true;
        this.Regando = true;

        System.out.println("[ENGINE] Riego Kafka iniciado. Sesion: "+ sessionId);
        return true;
    }

    public synchronized void PararRiego(String Motivo){
        if(Regando){
            this.ValvulaAbierta = false;
            Regando = false;
            Caudal = 0;

            System.out.println("[ENGINE] Riego finalizado (" + Motivo + "). Duracion: "  + SegundosRegados + " s, volumen: " + String.format("%.2f", VolumenAcumulado) + " L");

            if(RiegoKafka && !RequestId.isEmpty()){
                resultadoPendiente = crearResultado(Motivo);
            }

            SessionId = "";
            OperatorId = "";
            RequestId = "";
            RiegoKafka = false;
        }
    }

    public synchronized void setAveria(boolean valor) {
        if (valor == Averiado) return;
        Averiado = valor;
        if (Averiado) {
            System.out.println("[ENGINE] AVERIA: el Engine responde KO");
            PararRiego("LEAK"); 
        } else {
            System.out.println("[ENGINE] Averia resuelta: el Engine responde OK");
        }
    }

    public synchronized void bloquear() {
        PararRiego("BLOCKED");
        Bloqueado = true;
        System.out.println("[ENGINE] Estacion FUERA DE SERVICIO");
    }

    public synchronized void activar() {
        Bloqueado = false;
        System.out.println("[ENGINE] Estacion activada");
    }

    private synchronized String estadoSalud() {
        if (Averiado) {
            return "KO";
        } else {
            return "OK";
        }
    }

    private synchronized String sessionActiva() {
        if (Regando) {
            return SessionId;
        } else {
            return "";
        }
    }

	public String leeSocket (Socket p_sk)
	{
		 try {
            DataInputStream flujo = new DataInputStream(p_sk.getInputStream());
            return flujo.readUTF();
        } catch (EOFException e) {
            return null;
        } catch (Exception e) {
            System.out.println("Error al leer del socket: " + e.toString());
            return null;
        }
	}

	/*
	* Escribe dato en el socket cliente. Devuelve numero de bytes escritos,
	* o -1 si hay error.
	*/
	public void escribeSocket (Socket p_sk, String p_Datos) throws IOException
	{
		OutputStream aux = p_sk.getOutputStream();
        DataOutputStream flujo = new DataOutputStream(aux);
        flujo.writeUTF(p_Datos);
        flujo.flush();
	}


    public static void main(String[] args) {
        if(args.length != 4){
            System.out.println("Debe indicar la direccion del monitor y su puerto");
            return;
        }
        

        Engine Engine = new Engine();

        try{ 
            int PuertoMonitor = Integer.parseInt(args[1]);
            String stationId = args[2];
            String BoostrapServer = args[3];
            Engine.StationId = stationId;

            Socket skSocket = new Socket(args[0], PuertoMonitor);

            String id = UUID.randomUUID().toString();
            Engine.escribeSocket(skSocket, "ENGINE_HELLO#" + id + "#" + stationId);

            String r = Engine.leeSocket(skSocket);
            String[] c;

            if (r == null) {
                c = new String[0];
            } else {
                c = r.split("#", -1);
            }
            if (c.length != 4 || !c[0].equals("ENGINE_HELLO_RESULT") || !c[1].equals(id) || !c[2].equals("OK")) {
                System.out.println("Handshake rechazado por el Monitor: " + r);
                skSocket.close();
                return;
            }

           System.out.println("Engine conectado a la estacion " + Engine.StationId);

            Engine.Iniciar(skSocket, BoostrapServer);
            System.exit(0);

        }catch(ConnectException e){
            System.out.println("No se ha podido establecer conexion con el monitor.");
        } catch (UnknownHostException e) {
            System.out.println("No se ha podido encontrar el host del monitor.");
        } catch (IOException e) {   
            System.out.println("Error de entrada/salida: " + e.getMessage());
        }

    }

}