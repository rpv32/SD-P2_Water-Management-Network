import java.net.*;

public class WM_Central {

	/**
	 * @param args
	 */

	public static void main(String[] args) {
		
		/*
		* Descriptores de socket servidor y de socket con el cliente
		*/
		String puerto="";
		String booststrapServer="";
		
		/*
		* Se comprueba que se han pasado los parametros correctos
		*/

		if (args.length < 2) {
				System.out.println("Debe indicar el puerto de escucha de WM_Central y el de boostrapServer de Kafka");
				System.out.println("$ java WM_Central puerto_servidor boostrapServer");
				return;
			}
			puerto = args[0];
			booststrapServer = args[1];

		/*
		* Se crea el socket servidor y se queda a la espera de conexiones de clientes
		*/
		try
		{
			ServerSocket skServidor = new ServerSocket(Integer.parseInt(puerto));
			System.out.println("WM_Central iniciado.");
		    System.out.println("Escuchando el puerto " + puerto);
			System.out.println("BoostrapServer Kafka: " + booststrapServer);
			
			System.out.println("Esperando conexiones de estaciones de riego...");

			/*
			* Mantenemos la comunicacion con el cliente
			*/	
			for(;;)
			{
				/*
				* Se espera un cliente(Monitor) que quiera conectarse
				*/
				Socket skCliente = skServidor.accept(); // Crea objeto
		        System.out.println("Nueva estación conectada desde: " + skCliente.getInetAddress().getHostAddress());

				//Cada conexion se atiende en un hilo diferente para poder aceptar varias conexiones a la vez
		        Thread hilo = new HiloWM(skCliente);
		        hilo.start();
			}
		}
		catch(NumberFormatException e)
		{
			System.out.println("Error: el puerto debe de ser un numero entero");
			
		} catch (BindException e){
			System.out.println("Error: el puerto " + puerto + " ya esta en uso");
		}
		catch(Exception e){
			System.out.println("Error: " + e.toString());
		}
	}

}
