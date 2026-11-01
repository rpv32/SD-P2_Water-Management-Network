import java.io.*;
import java.net.*;
import java.time.Instant;
import java.util.UUID;

public class WM_WS_M {

	private volatile boolean running = true;
	private volatile String engineHealth = "UNKNOWN";
	private volatile String engineCause = "";
	private volatile String engineSessionId = "";

	/*
	* Lee datos del socket. Supone que se le pasa un buffer con hueco 
	*	suficiente para los datos. Devuelve el numero de bytes leidos o
	* 0 si se cierra fichero o -1 si hay error.
	*/
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
	
	private void HiloEngine(Socket Engine){

		try {
       		Engine.setSoTimeout(5000);
   		} catch (SocketException e) {
       		running = false;
       		return;
   		}

		while(running){
			String messageId = UUID.randomUUID().toString();
			try {
    			escribeSocket(Engine, "HEALTH_CHECK#" + messageId + "#" + Instant.now().toString());
			} catch (IOException e) {
				engineSessionId = "";
    		    engineCause = "ENGINE_TIMEOUT";
	        	engineHealth = "KO";
    			System.out.println("Error al escribir en el socket: " + e.toString());	
    		return;
			}
        	
			
			String Respuesta = leeSocket(Engine);

			if(Respuesta == null){
				engineSessionId = "";
				engineCause = "ENGINE_TIMEOUT";
    			engineHealth = "KO";
    			System.out.println("Engine sin respuesta. Esperando nueva conexion...");
   		 		return;
			}
			else{
				String[] c = Respuesta.split("#",-1);
				if(c.length == 4 && c[0].equals("HEALTH_RESULT") && c[1].equals(messageId) && (c[2].equals("OK") || c[2].equals("KO"))){
					engineSessionId = c[3];
					if (c[2].equals("KO")) {
						engineCause = "ENGINE_KO";
					} else {
						engineCause = "";
					}
					engineHealth = c[2];  
				}
				else{
					System.out.println("Respuesta invalida del Engine: " + Respuesta);
    				engineSessionId = "";
					engineCause = "ENGINE_KO";
    				engineHealth = "KO";
				}
			}
			try {
            	Thread.sleep(1000);
        	} catch (InterruptedException e) {
            	return;
        	}
		}
	}

	private void HiloCentral(Socket Central, String stationId){
		String UltimoNotificado = "UNKNOWN";
		while(running){
			String Actual = engineHealth;
			String causa = engineCause;
			String sesion = engineSessionId;

			if(Actual.equals("KO") && !UltimoNotificado.equals("KO")){
				String trama = "INCIDENT#" + UUID.randomUUID() + "#" + stationId + "#" + causa + "#" + sesion + "#" + Instant.now();
    			try	{
					escribeSocket(Central, trama);
				}
				catch (Exception e)
				{
					System.out.println("Conexion con WM_Central perdida");
					return;
				}
				System.out.println("INCIDENCIA en " + stationId + ": " + causa);
			}
			else if(Actual.equals("OK") && !UltimoNotificado.equals("OK")){
				String trama = "RECOVERY#" + UUID.randomUUID() + "#" + stationId + "#" + Instant.now();
   				try{
					escribeSocket(Central, trama);
				}
				catch (Exception e)
				{
					System.out.println("Conexion con WM_Central perdida");
					return;
				}
				System.out.println("Estacion " + stationId + " recuperada");
			}	
			UltimoNotificado = Actual;

			String trama = "MONITOR_HEARTBEAT#" + UUID.randomUUID() + "#" + stationId + "#" + Actual + "#" + Instant.now();
    		try{
				escribeSocket(Central, trama);
			}
			catch (Exception e)
			{
				System.out.println("Conexion con WM_Central perdida");
				return;
				}

			try {
            	Thread.sleep(1000);
			} catch (InterruptedException e) {
				return;
			}
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// Se crea el objeto cliente para usar leerSocket() y escribeSocket()
		WM_WS_M wm_ws_m = new WM_WS_M();
		
		String puertoEngine = "";
		String hostCentral = "";
		String puertoCentral = "";
		String stationId = "";

		String cadena = "";
		String respuesta = "";
		
		//Compruebo que me han pasado 4 argumentos
		if(args.length < 4) {
			System.out.println ("Debe indicar el puerto del engine, la dirección del servidor, el puerto del servidor y el ID de la estación");
			System.out.println ("$ java WM_WS_M puerto_engine host_central puerto_central station_id");
			return;
		}
		
		puertoEngine = args[0];
		hostCentral = args[1];
		puertoCentral = args[2];
		stationId = args[3];
		
		try {
			//Comprobación de que los puertos sean números
			Integer.parseInt(puertoEngine);
			int puertoCentralInt = Integer.parseInt(puertoCentral);

			//Creo el socket cliente y se conecta con el servidor.

			Socket skCliente = null;
			while(skCliente == null){  
				try{ 
					skCliente = new Socket(hostCentral, puertoCentralInt);
				}
				catch(ConnectException e){
					System.out.println("Central no disponible, reintentando");
					Thread.sleep(1000);
				}
			}

			System.out.println("Monitor de la estacion " + stationId + " iniciado");
			System.out.println("Puerto reservado para el Engine: " + puertoEngine);
			System.out.println("Conectado con WM_Central en " + hostCentral + ":" + puertoCentral);
			
			//ID mensaje de registro

			String messageId = UUID.randomUUID().toString();

			cadena = "REGISTER#" + messageId + "#" + stationId;
			
			//Envio el mensaje de registro al servidor
			wm_ws_m.escribeSocket(skCliente, cadena);
			
			//Recibo la respuesta del servidor
			respuesta = wm_ws_m.leeSocket(skCliente);
			
			if(respuesta == null){
				System.out.println("WM_Central ha cerrado la conexion");
				skCliente.close();
                return;
			}

			//Muestro la respuesta del servidor y cierro conexión
			System.out.println("Respuesta de WM_Central: " + respuesta);

			//Se divide la respuesta para comprobar si Central ha aceptado el registro.
            String[] camposRespuesta = respuesta.split("#", -1);

            if (camposRespuesta.length != 5 || camposRespuesta[0].compareTo("REGISTER_RESULT") != 0 || camposRespuesta[1].compareTo(messageId) != 0 || camposRespuesta[2].compareTo("OK") != 0 || camposRespuesta[3].compareTo("") != 0){
                System.out.println("La estacion no ha sido registrada");
                skCliente.close();
                return;
            }

			System.out.println("Estacion registrada correctamente");

			ServerSocket SocketEngine = new ServerSocket(Integer.parseInt(puertoEngine));
			final Socket socketCentral = skCliente;
			final String id = stationId;
			new Thread(() -> wm_ws_m.HiloCentral(socketCentral, id)).start();
			while(true){
				Socket skEngine = SocketEngine.accept();
				skEngine.setSoTimeout(2000);

				String hello = wm_ws_m.leeSocket(skEngine);
				String[] h;

				if (hello == null) {
					h = new String[0];
				} else {
					h = hello.split("#", -1);
				}

				if (h.length != 3 || !h[0].equals("ENGINE_HELLO")	|| h[1].equals("") || h[2].equals("")){
					String correlationId = "";

					if (h.length > 1)
						correlationId = h[1];

					wm_ws_m.escribeSocket(skEngine,
							"ERROR#" + correlationId
							+ "#INVALID_FIELD#Hello invalido");
					skEngine.close();
					continue;
				}

				if (h[2].compareTo(stationId) != 0)
				{
					wm_ws_m.escribeSocket(skEngine,"ENGINE_HELLO_RESULT#" + h[1] + "#ERROR#UNKNOWN_STATION");
					skEngine.close();
					continue;
				}
				wm_ws_m.escribeSocket(skEngine, "ENGINE_HELLO_RESULT#" + h[1] + "#OK#");
				System.out.println("Engine conectado");
				wm_ws_m.engineHealth = "OK";
				wm_ws_m.engineCause = "";
				wm_ws_m.engineSessionId = "";
				wm_ws_m.HiloEngine(skEngine);
			}

		}
		catch (NumberFormatException e)
        {
            System.out.println("Error: los puertos deben ser numeros enteros");
        }
        catch (ConnectException e)
        {
            System.out.println("Error: no se puede conectar con WM_Central");
        }
		catch(Exception e) {
			System.out.println("Error: " + e.toString());
		}
	}

}
