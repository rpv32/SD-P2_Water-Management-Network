import java.lang.Exception;
import java.io.*;
import java.net.*;
import java.util.Scanner;
import java.util.UUID;

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
            System.out.println("\nConexion con el Monitor perdida. Cerrando Engine.");
            monitorConectado = false;
            synchronized (this){
                if (Regando){
                    PararRiego("MONITOR_DISCONNECTED");
                }
            }
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

    public void Iniciar(Socket Monitor){
        new Thread(() -> HiloMonitor(Monitor)).start();
        new Thread(() -> HiloSimulacion()).start();
        Menu();
    }

    synchronized void simularSegundo() {
        if (!Regando){
            return;
        }                    

        Caudal = 10 + Math.random() * 5;           
        VolumenAcumulado += Caudal / 60.0;          
        SegundosRegados++;

        if (SegundosRegados >= DuracionObjetivo) {
            PararRiego("TIME_LIMIT");
        }
    }

    public synchronized boolean IniciarRiego(String OperatorId, int Duracion){
        if (Duracion <= 0 || Regando || Bloqueado || Averiado || !monitorConectado) {
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

        System.out.println("[ENGINE] Riego iniciado. Operario " + OperatorId + ", " + Duracion + " s");
        return true;
    }

    public synchronized void PararRiego(String Motivo){
        if(Regando){
            this.ValvulaAbierta = false;
            Regando = false;
            Caudal = 0;

            System.out.println("[ENGINE] Riego finalizado (" + Motivo + "). Duracion: "  + SegundosRegados + " s, volumen: " + String.format("%.2f", VolumenAcumulado) + " L");

            SessionId = "";
            OperatorId = "";
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
        if(args.length != 3){
            System.out.println("Debe indicar la direccion del monitor y su puerto");
            return;
        }
        

        Engine Engine = new Engine();

        try{ 
            int PuertoMonitor = Integer.parseInt(args[1]);
            String stationId = args[2];
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

            Engine.Iniciar(skSocket);
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