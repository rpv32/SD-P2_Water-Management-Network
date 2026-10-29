import java.net.*;
import java.util.HashSet;
import java.util.Set;

public class WM_Central {

	//Estaciones que tiene un Monitor conectado
	//Luego cuando se cree SQLite se guardaran en la base de datos.
	private static Set<String> estacionesConectadas = new HashSet<String>();
	private static DataBaseManager db;

	//Registra una estación como conectada, si ya estaba conectada devuelve false, si no devuelve true
	public static synchronized boolean registrarConexion(String stationId){
		if(estacionesConectadas.contains(stationId)){
			return false;
		}
		estacionesConectadas.add(stationId);
		mostrarEstacionesConectadas();

		return true;
	}

	//Elimina una estación de la lista de estaciones conectadas cuando cierra la conexión
	public static synchronized void cerrarConexion(String stationId){
		estacionesConectadas.remove(stationId);
		mostrarEstacionesConectadas();
	}

	//Muestra por pantalla las estaciones conectadas a un Monitor
	public static synchronized void mostrarEstacionesConectadas(){
		System.out.println("----------------------------------------");
		System.out.println("Estaciones conectadas:");

		if(estacionesConectadas.isEmpty()){
			System.out.println("Ninguna estación conectada");
		}else{
			for(String estacion : estacionesConectadas){
				System.out.println("- " + estacion);
			}
		}
		System.out.println("----------------------------------------");
	}

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
			db = new DataBaseManager("wm.db");
            db.marcarTodasDesconectadas();
			

			KafkaCentral kafka = new KafkaCentral(booststrapServer, db);
			Runtime.getRuntime().addShutdownHook(new Thread(kafka::detener));
			kafka.start();
			
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
		        Thread hilo = new HiloWM(skCliente, 5000, db);
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
