import java.net.*;

public class WM_Central {

	/**
	 * @param args
	 */
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		/*
		* Descriptores de socket servidor y de socket con el cliente
		*/
		String puerto="";

		try
		{
			
			if (args.length < 1) {
				System.out.println("Debe indicar el puerto de escucha del servidor WM_Central");
				System.out.println("$./Servidor puerto_servidor");
				System.exit (1);
			}
			puerto = args[0];
			ServerSocket skServidor = new ServerSocket(Integer.parseInt(puerto));
		    System.out.println("Escucho el puerto " + puerto);
	
			/*
			* Mantenemos la comunicacion con el cliente
			*/	
			for(;;)
			{
				/*
				* Se espera un cliente que quiera conectarse
				*/
				Socket skCliente = skServidor.accept(); // Crea objeto
		        System.out.println("Nueva estación conectada.");

		        Thread t = new HiloWM(skCliente);
		        t.start();
			}
		}
		catch(Exception e)
		{
			System.out.println("Error: " + e.toString());
			
		}
	}

}
