import java.io.*;
import java.net.*;
import java.time.Instant;
import java.util.UUID;

public class WM_WS_M {

	/*
	* Lee datos del socket. Supone que se le pasa un buffer con hueco 
	*	suficiente para los datos. Devuelve el numero de bytes leidos o
	* 0 si se cierra fichero o -1 si hay error.
	*/
	public String leeSocket (Socket p_sk, String p_Datos)
	{
		try
		{
			InputStream aux = p_sk.getInputStream();
			DataInputStream flujo = new DataInputStream( aux );
			p_Datos = flujo.readUTF();
		}
		catch (Exception e)
		{
			System.out.println("Error: " + e.toString());
		}
      return p_Datos;
	}

	/*
	* Escribe dato en el socket cliente. Devuelve numero de bytes escritos,
	* o -1 si hay error.
	*/
	public void escribeSocket (Socket p_sk, String p_Datos)
	{
		try
		{
			OutputStream aux = p_sk.getOutputStream();
			DataOutputStream flujo= new DataOutputStream( aux );
			flujo.writeUTF(p_Datos);      
		}
		catch (Exception e)
		{
			System.out.println("Error: " + e.toString());
		}
		return;
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

			Socket skCliente = new Socket(hostCentral, puertoCentralInt);

			System.out.println("Monitor de la estacion " + stationId + " iniciado");
			System.out.println("Puerto reservado para el Engine: " + puertoEngine);
			System.out.println("Conectado con WM_Central en " + hostCentral + ":" + puertoCentral);
			
			//ID mensaje de registro

			String messageId = UUID.randomUUID().toString();

			cadena = "REGISTER#" + messageId + "#" + stationId;
			
			//Envio el mensaje de registro al servidor
			wm_ws_m.escribeSocket(skCliente, cadena);
			
			//Recibo la respuesta del servidor
			respuesta = wm_ws_m.leeSocket(skCliente, respuesta);
			
			if(respuesta == null){
				System.out.println("WM_Central ha cerrado la conexion");
				skCliente.close();
                return;
			}

			//Muestro la respuesta del servidor y cierro conexión
			System.out.println("Respuesta de WM_Central: " + respuesta);

			//Se divide la respuesta para comprobar si Central ha aceptado el registro.
            String[] camposRespuesta = respuesta.split("#", -1);

            if (camposRespuesta.length < 3 || camposRespuesta[0].compareTo("REGISTER_RESULT") != 0 || camposRespuesta[2].compareTo("OK") != 0){
                System.out.println("La estacion no ha sido registrada");
                skCliente.close();
                return;
            }

            System.out.println("Estacion registrada correctamente");

            //PROVISIONAL
            //prueba la conexión con Central.
            //Cuando se implemente el Engine sustituiremos UNKNOWN por el resultado real del Heartbeat.
            while (true)
            {
                messageId = UUID.randomUUID().toString();

                cadena = "MONITOR_HEARTBEAT#" + messageId + "#" + stationId + "#UNKNOWN#" + Instant.now().toString(); 
                wm_ws_m.escribeSocket(skCliente, cadena);

                System.out.println("Mensaje de vida enviado a WM_Central");

                Thread.sleep(1000);
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
        catch (InterruptedException e)
        {
            System.out.println("El Monitor ha sido interrumpido");
        }
		catch(Exception e) {
			System.out.println("Error: " + e.toString());
		}
	}

}
