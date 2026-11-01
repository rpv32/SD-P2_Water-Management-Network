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

	private void mantenerConexionCentral(String hostCentral, int puertoCentral, String stationId){
		while (running) {
			try (Socket central = new Socket()) {
				central.connect(new InetSocketAddress(hostCentral, puertoCentral),3000);

				// Limite de espera para la respuesta al registro.
				central.setSoTimeout(5000);

				String messageId = UUID.randomUUID().toString();

				escribeSocket(central, "REGISTER#" + messageId + "#" + stationId);

				String respuesta = leeSocket(central);

				if (respuesta == null) {
					throw new IOException("Central no ha respondido al registro");
				}

				System.out.println("Respuesta de WM_Central: " + respuesta);

				String[] campos = respuesta.split("#", -1);

				if (campos.length != 5 || !campos[0].equals("REGISTER_RESULT") || !campos[1].equals(messageId) || !campos[2].equals("OK") || !campos[3].isEmpty()) {
					throw new IOException("Registro no aceptado: " + respuesta);
				}

				System.out.println("Estacion registrada correctamente en WM_Central");

				// Se queda aqui hasta perder Central.
				// Al regresar, el bucle vuelve a conectar y registrar.
				HiloCentral(central, stationId);

			} catch (IOException e) {
				System.out.println("WM_Central no disponible: " + e.getMessage());
			}

			if (!running) {
				return;
			}

			System.out.println("Reintentando conexion con WM_Central en 2 segundos...");

			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		WM_WS_M wm_ws_m = new WM_WS_M();

		if (args.length < 4) {
			System.out.println(
					"Debe indicar el puerto del engine, la direccion "
					+ "del servidor, el puerto del servidor y el ID "
					+ "de la estacion");

			System.out.println(
					"$ java WM_WS_M puerto_engine host_central "
					+ "puerto_central station_id");
			return;
		}

		final String hostCentral = args[1];
		final String stationId = args[3];

		try {
			int puertoEngine = Integer.parseInt(args[0]);
			final int puertoCentral = Integer.parseInt(args[2]);

			try (ServerSocket SocketEngine = new ServerSocket(puertoEngine)) {

				System.out.println("Monitor de la estacion " + stationId + " iniciado");

				System.out.println("Puerto reservado para el Engine: " + puertoEngine);

				new Thread(() -> wm_ws_m.mantenerConexionCentral(hostCentral, puertoCentral, stationId),"monitor-central").start();

				while(true){
					try(Socket skEngine = SocketEngine.accept()) {
						skEngine.setSoTimeout(2000);

						String hello = wm_ws_m.leeSocket(skEngine);

						String[] h = hello == null ? new String[0] : hello.split("#", -1);

						if(h.length != 3 || !h[0].equals("ENGINE_HELLO") || h[1].isEmpty() || h[2].isEmpty()) {
							String correlationId = "";

							if (h.length > 1) {
								correlationId = h[1];
							}

							wm_ws_m.escribeSocket(skEngine, "ERROR#" + correlationId + "#INVALID_FIELD#Hello invalido");
							continue;
						}

						if(!h[2].equals(stationId)) {
							wm_ws_m.escribeSocket(skEngine, "ENGINE_HELLO_RESULT#" + h[1] + "#ERROR#UNKNOWN_STATION");
							continue;
						}

						wm_ws_m.escribeSocket(skEngine, "ENGINE_HELLO_RESULT#" + h[1] + "#OK#");

						System.out.println("Engine conectado");

						// La salud real la determina HEALTH_RESULT.
						wm_ws_m.engineHealth = "UNKNOWN";
						wm_ws_m.engineCause = "";
						wm_ws_m.engineSessionId = "";

						wm_ws_m.HiloEngine(skEngine);

					} catch (IOException e) {
						wm_ws_m.engineCause = "ENGINE_TIMEOUT";
						wm_ws_m.engineHealth = "KO";

						System.out.println("Error en la conexion con Engine: " + e.getMessage());
					}
				}
			}

		} catch (NumberFormatException e) {
			System.out.println("Error: los puertos deben ser numeros enteros");
		} catch (Exception e) {
			System.out.println("Error: " + e);
		}
	}

}
