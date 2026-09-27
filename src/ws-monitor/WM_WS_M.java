import java.io.*;
import java.net.*;

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
		
		String host = "";
		String puerto = "";
		String estacion = "";
		String ubicacion = "";
		String cadena = "";
		String respuesta = "";
		
		//Compruebo que me han pasado 4 argumentos
		if (args.length < 4) {
			System.out.println ("Debe indicar la direccion del servidor, el puerto, estación y ubicación");
			System.out.println ("$./Cliente nombre_servidor puerto_servidor estación ubicación");
			System.exit(-1);
		}
		
		host = args[0];
		puerto = args[1];
		estacion = args[2];
		ubicacion = args[3];
		
		try {
			//Creo el socket cliente y se conecta con el servidor.
			Socket skCliente = new Socket(host, Integer.parseInt(puerto));
			
			cadena = "REGISTRO#" + estacion + "#" + ubicacion;
			
			//Envio el mensaje de registro al servidor
			wm_ws_m.escribeSocket(skCliente, cadena);
			
			//Recibo la respuesta del servidor
			respuesta = wm_ws_m.leeSocket(skCliente, respuesta);
			
			//Muestro la respuesta del servidor y cierro conexión
			System.out.println("Respuesta del servidor: " + respuesta);
			skCliente.close();
			
			System.out.println("Conexion cerrada.");
		}catch(Exception e) {
			System.out.println("Error: " + e.toString());
		}
	}

}
