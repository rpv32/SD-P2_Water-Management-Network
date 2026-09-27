import java.lang.Exception;
import java.net.Socket;
import java.io.*;

public class HiloWM extends Thread {

	private Socket skCliente;
	
	public HiloWM(Socket p_cliente)
	{
		this.skCliente = p_cliente;
	}
	
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
			p_Datos = new String();
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
	
	//Recibe la trama enviada por una estación y la valida
	public String realizarRegistro(String p_Cadena) {
		//Divido la trama
		String[] registro = p_Cadena.split("#");
		String respuesta = "";
		
		System.out.println("SRV: El mensaje recibido es: " + registro[0]);
		
		//Compruebo que la trama sea válida
		if ((registro.length == 3) && (registro[0].compareTo("REGISTRO") == 0) && 
			(registro[1].compareTo("") != 0 && registro[2].compareTo("") != 0)) {
			
			//Muestro la estación y su ubicación
			System.out.println("SRV: ID de la estación: " + registro[1]);
			System.out.println("SRV: Ubicación de la estación: " + registro[2]);
			
			//Respondo positivamente al registro
			respuesta = "STATUS#OK#Estacion registrada correctamente";
		}else {
			respuesta = "STATUS#ERROR#Estacion no registrada";
		}
		
		return respuesta;
	}
	
    public void run() {
    	String respuesta = "";
		String Cadena="";
		
        try {
        		//Leo el mensaje enviado por el cliente
				Cadena = this.leeSocket (skCliente, Cadena);
				
				//Se valida el mensaje
				respuesta = this.realizarRegistro(Cadena);
				
				//Se envia la respuesta al cliente
				this.escribeSocket(skCliente, respuesta);		
				
				//Cierro la conexión
				skCliente.close();
			//System.exit(0); No se debe poner esta sentencia, porque en ese caso el primer cliente que cierra rompe el socket 
			//				  y desconecta a todos
				System.out.println("SRV: Conexión cerrada.");
        }
        catch (Exception e) {
          System.out.println("Error: " + e.toString());
        }
      }
}
