import java.lang.Exception;
import java.net.Socket;
import java.io.*;

public class HiloWM extends Thread {

	private Socket skCliente;
	
	private String stationId;
	private boolean registrada;

	public HiloWM(Socket p_cliente)
	{
		this.skCliente = p_cliente;
		this.stationId = "";
		this.registrada = false;
	}
	
	/*
	* Lee datos del socket. Supone que se le pasa un buffer con hueco 
	*	suficiente para los datos. Devuelve el numero de bytes leidos o
	* 	null si hay error.
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
		catch(EOFException e) 
		{
			p_Datos = null;
		}
		catch (Exception e)
		{
			System.out.println("Error al leer del socket: " + e.toString());
			p_Datos = null;
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
			flujo.flush();
		}
		catch (Exception e)
		{
			System.out.println("Error al escribir en el socket: " + e.toString());
		}
		return;
	}
	
	

	//Recibe la trama enviada por una estación y la valida
	//Formato de la trama: REGISTER_RESULT#correlationId#OK|ERROR#errorCode#message

	public String realizarRegistro(String p_Cadena){
		//Divido la trama
		String[] registro = p_Cadena.split("#", -1);
		String respuesta = "";
		
		//Compruebo que la trama sea válida
		if((registro.length == 3) && (registro[0].compareTo("REGISTER") == 0) && 
			(registro[1].compareTo("") != 0 && registro[2].compareTo("") != 0)){
				
				//Compruebo que la estación no esté ya conectada
				if(WM_Central.registrarConexion(registro[2])){
					//Registro correcto
					this.stationId = registro[2];
					this.registrada = true;

					System.out.println("SRV: ID de la estación " + stationId);
					respuesta = "REGISTER_RESULT#" + registro[1] + "#OK##Estación registrada correctamente";
				}else{
					//La estación ya estaba conectada
					respuesta = "REGISTER_RESULT#" + registro[1] + "#ERROR#ALREADY_CONNECTED#La estación ya tiene un Monitor conectado";
				}

		}else{
			String correlationId = "";

			if(registro.length > 1){
				correlationId = registro[1];
			}
			respuesta = "ERROR#" + correlationId + "#INVALID_FIELD#Registro no válido";
		}
		
		return respuesta;
	}
	
	//Recibe la trama enviada por una estación y la valida
	public String crearError(String[] p_Campos,String p_Codigo,String p_Mensaje){
        String correlationId = "";

        if (p_Campos.length > 1){
            correlationId = p_Campos[1];
        }

        return "ERROR#"+ correlationId + "#" + p_Codigo + "#" + p_Mensaje;
    }

	/*
     * Procesa los mensajes recibidos desde el Monitor.
     */
    public String procesarMensaje(String p_Cadena){
        String[] campos = p_Cadena.split("#", -1);
        String respuesta = "";

		//Si el mensaje está vacío
        if(campos.length == 0 || campos[0].compareTo("") == 0){
            return "ERROR##MALFORMED_MESSAGE#Mensaje no valido";
        }

        System.out.println("SRV: El mensaje recibido es: " + campos[0]);

        //Mensaje de registro.
        if(campos[0].compareTo("REGISTER") == 0){
			//Si está registrado, no se puede registrar de nuevo.
            if(registrada){
                String correlationId = "";

                if(campos.length > 1){
                    correlationId = campos[1];
                }
                
                respuesta = "ERROR#" + correlationId + "#ALREADY_REGISTERED#La conexion ya esta registrada";
            }else{
                respuesta = realizarRegistro(p_Cadena);
            }
        }else if(campos[0].compareTo("MONITOR_HEARTBEAT") == 0){
            //Mensaje de vida del Monitor.
            //MONITOR_HEARTBEAT#messageId#stationId#engineHealth#occurredAt
            if(campos.length != 5){
				//Si mensaje no tiene campos correctos
                respuesta = crearError(campos,"BAD_FIELD_COUNT","Numero incorrecto de campos");

            }else if(!registrada){
				//Si el Monitor no está registrado
                respuesta = crearError(campos,"NOT_REGISTERED","El Monitor debe registrarse primero");

            }else if(campos[2].compareTo(stationId) != 0){
				//Si el ID del heartbeat no corresponde con el de la estación registrada
                respuesta = crearError(campos,"UNKNOWN_STATION","La estacion no corresponde con esta conexion");

            }else if(campos[1].compareTo("") == 0 || campos[3].compareTo("") == 0 || campos[4].compareTo("") == 0){
				//Si algún campo del mensaje está vacío
                respuesta = crearError(campos,"INVALID_FIELD","El mensaje contiene campos vacios");

            }else if(campos[3].compareTo("OK") != 0 && campos[3].compareTo("KO") != 0 && campos[3].compareTo("UNKNOWN") != 0){
				//Si el estado del Engine no es válido
                respuesta = crearError(campos,"INVALID_FIELD","El estado del Engine no es valido");

            }else{
                System.out.println("SRV: Heartbeat de " + stationId + ". Estado del Engine: " + campos[3]);
                //Heartbeat correcto no necesita respuesta.
                respuesta = "";
            }
        } else if(campos[0].compareTo("INCIDENT") == 0){
            //Aviso de incidencia.
            //INCIDENT#messageId#stationId#cause#sessionId#occurredAt
            if (campos.length != 6){
				//Si mensaje no tiene campos correctos
                respuesta = crearError(campos, "BAD_FIELD_COUNT", "Numero incorrecto de campos");

            }else if(!registrada){
				//Si el Monitor no está registrado
                respuesta = crearError(campos, "NOT_REGISTERED", "El Monitor debe registrarse primero");

            }else if(campos[2].compareTo(stationId) != 0){
				//Si el ID de la incidencia no corresponde con el de la estación registrada
                respuesta = crearError(campos, "UNKNOWN_STATION", "La estacion no corresponde con esta conexion");

            }else if(campos[1].compareTo("") == 0 || campos[3].compareTo("") == 0 || campos[5].compareTo("") == 0){
				//Si algún campo del mensaje está vacío
                respuesta = crearError(campos, "INVALID_FIELD", "La incidencia contiene campos vacios");

            } else{
                System.out.println("SRV: Incidencia en " + stationId + ". Causa: " + campos[3]);
                respuesta = "";
            }
        }else if(campos[0].compareTo("RECOVERY") == 0){
            //Aviso de recuperación.
            //RECOVERY#messageId#stationId#occurredAt
            if(campos.length != 4){
				//Si mensaje no tiene campos correctos
                respuesta = crearError(campos, "BAD_FIELD_COUNT", "Numero incorrecto de campos");

            }else if(!registrada){
				//Si el Monitor no está registrado
                respuesta = crearError(campos, "NOT_REGISTERED", "El Monitor debe registrarse primero");

            }else if(campos[2].compareTo(stationId) != 0){
				//Si el ID de la recuperación no corresponde con el de la estación registrada
                respuesta = crearError(campos, "UNKNOWN_STATION", "La estacion no corresponde con esta conexion");

            }else if(campos[1].compareTo("") == 0 || campos[3].compareTo("") == 0){
				//Si algún campo del mensaje está vacío
                respuesta = crearError(campos, "INVALID_FIELD", "La recuperacion contiene campos vacios");

            }else{
                System.out.println("SRV: La estación " + stationId + " vuelve a estar operativa");
                respuesta = "";
            }
        }else{
            //Operación no reconocida.
            respuesta = crearError(campos, "UNKNOWN_OPERATION", "Operacion desconocida");
        }

        return respuesta;
    }

    public void run() {
    	String respuesta = "";
		String Cadena="";
		/*
    	* Se leen mensajes hasta que el Monitor cierra la conexión.
        */
        try {
			while(true){
				//Leo el mensaje enviado por el cliente
				Cadena = this.leeSocket (skCliente, Cadena);

				if(Cadena == null){
					break;
				}

				//Se valida el mensaje
				respuesta = this.procesarMensaje(Cadena);

				//Se envia la respuesta al cliente si no está vacía
				if(respuesta.compareTo("") != 0){
					this.escribeSocket(skCliente, respuesta);
				}
			}
        } catch (Exception e) {
        	System.out.println("Error: " + e.toString());
        } finally{
			//Si la conexión se había registrado se elimina de la lista de estaciones conectadas
			if(registrada){
				WM_Central.cerrarConexion(stationId);
				System.out.println("SRV: Conexión cerrada con la estación " + stationId);
			}else{
				System.out.println("SRV: Conexión cerrada sin estación registrada");
			}

			//Se intenta cerrar siempre el socket con el cliente si se ha producido error como si no
			try {
				skCliente.close();
			} catch (Exception e) {
				System.out.println("Error al cerrar socket: " + e.toString());
			}
		}
    }
}
