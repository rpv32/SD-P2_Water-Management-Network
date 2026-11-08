# Water Management Network — Ejecución

Usamos dos PCs:

- Un PC para las estaciones: Monitor y Engine de cada WS.
- Otro PC para los operarios: una instancia de FO por operario.

Los comandos están preparados para PowerShell y se ejecutan desde la carpeta `UA_SD`, donde están `pom.xml` e `instancias.ps1`.

## 1. Qué necesitamos

Antes de empezar tenemos que tener:

- Docker Desktop abierto y funcionando con contenedores Linux.
- PowerShell.
- El proyecto descargado en los dos PCs.
- Central y Kafka funcionando en Railway.

Podemos comprobar que tenemos Docker con:

```powershell
docker version
docker compose version
```

## 2. Preparar la imagen

Desde `UA_SD` construimos la imagen:

```powershell
docker build -f .\docker\dockerfile -t watermanagement:practica-v1 .
```

El Dockerfile compila el código con Maven y genera el JAR.

Comprobamos que la imagen existe:

```powershell
docker image inspect watermanagement:practica-v1 --format '{{.Id}}'
```

### Copiar la imagen al otro PC

En el PC donde la hemos construido:

```powershell
docker save -o watermanagement.tar watermanagement:practica-v1
```

Copiamos `watermanagement.tar` al otro PC y la cargamos:

```powershell
docker load -i .\watermanagement.tar
```

En ambos PCs usamos el mismo proyecto y la misma imagen. Si no copiamos todo el proyecto al segundo equipo, para ejecutar necesitamos al menos esta estructura y la imagen cargada:

```text
UA_SD/
├── instancias.ps1
└── kafka/
    ├── compose.instancias.yml
    └── servicios/
        ├── riegos.txt
        └── riegos-invalido.txt
```

## 3. Comprobar Central y los IDs

Abrimos el panel de Central:

```powershell
Start-Process "https://central-production-8c7f.up.railway.app/"
```

Antes de arrancar un módulo, su ID tiene que estar dado de alta y activo. Esto lo comprobamos en:

```powershell
Start-Process "https://central-production-8c7f.up.railway.app/altas"
```

En esa página pulsamos «Listar / actualizar registros» para consultar los IDs y su estado y podemos crear, reactivar o darlo de baja .

Con la base de datos inicial tenemos:

| Tipo | IDs |
|---|---|
| Estaciones | `WS-01`, `WS-02`, `WS-03`, `WS-04`, `WS-05` |
| Operarios | `OP-01`, `OP-02` |

## 4. Inicializar el script en los dos PCs

Desde `UA_SD`, cargamos el script:

```powershell
. .\instancias.ps1
```
Después inicializamos la configuración:

```powershell
Inicializar-WM
```

Si abrimos otra terminal, repetimos estos dos pasos. Si cambiaran las direcciones de Railway, las actualizamos en -UrlCentral -HostCentral -PuertoCentral y -Kafka.

No ejecutamos `kafka/compose.yml` para este escenario ya que lo usamos para probar con Central y Kafka locales. Para conectarnos con Railway usamos `instancias.ps1` y `kafka/compose.instancias.yml`.

## 5. Arrancar las estaciones

En el PC de estaciones ejecutamos:

```powershell
Nueva-WS -Id WS-01
Nueva-WS -Id WS-02
```

Cada comando crea el Monitor y después lanza su Engine cuando Central ha aceptado al Monitor.

Comprobamos los contenedores:

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}"
```

Para ver la salida de una estación:

```powershell
docker logs --tail 40 wm-monitor-ws-01
docker logs --tail 40 wm-engine-ws-01
```

En Central esperamos a que las estaciones aparezcan disponibles antes de solicitar riegos.

## 6. Arrancar los operarios

En el PC de operarios, después de inicializar el script, ejecutamos:

```powershell
Nuevo-FO -Id OP-01 -PuertoWeb 8090
Nuevo-FO -Id OP-02 -PuertoWeb 8091
```

Abrimos las dos aplicaciones:

```powershell
Start-Process "http://localhost:8090"
Start-Process "http://localhost:8091"
```

Cada operario necesita un ID propio y un puerto web libre. Estas direcciones se abren en el mismo PC donde hemos arrancado los FO.

Desde la web podemos consultar las estaciones disponibles, solicitar un riego y ver los logs.

## 7. Ejecutar servicios desde fichero

Los archivos están en:

```text
kafka/servicios/
```

El formato es una estación y una duración en segundos por línea:

```text
WS-01;10
WS-02;15
```

En la web del FO, dentro de «Servicios desde fichero», introducimos:

```text
/servicios/riegos.txt
```

Pulsamos «Ejecutar». El FO espera a que concluya cada servicio y después espera cuatro segundos antes de solicitar el siguiente.

## 8. Arrancar más instancias

Primero registramos el nuevo ID en `/altas`. Por ejemplo:

```text
Estación: WS-10
Ubicación: Jardín de prueba

Operario: OP-10
Nombre: Operario de prueba
```

En el PC de estaciones:

```powershell
Nueva-WS -Id WS-10
```

En el PC de operarios:

```powershell
Nuevo-FO -Id OP-10 -PuertoWeb 8092
Start-Process "http://localhost:8092"
```

No tenemos que editar el Compose ni reconstruir la imagen para añadir otro ID.

Si el ID no existe o está inactivo, el módulo lo rechaza y el script elimina el contenedor rechazado.

No usamos IDs que solo se diferencien por mayúsculas y minúsculas, porque los nombres de los contenedores se generan en minúsculas.

## 9. Abrir el menú de una estación

Para acceder al menú del Engine:

```powershell
docker attach wm-engine-ws-01
```

Desde ahí podemos solicitar un riego, detenerlo, simular una avería o repararla y consultar su estado.

Para salir de la conexión a la terminal sin detener el contenedor pulsamos `Ctrl+P` y después `Ctrl+Q`.

## 10. Parar y volver a arrancar

Los comandos `Nueva-WS` y `Nuevo-FO` se utilizan para crear contenedores. Si ya existen, no los ejecutamos otra vez con el mismo ID.

### Parar los módulos

En el PC de estaciones y operarios usamos el mismo comando:

```powershell
docker stop wm-engine-ws-01 wm-engine-ws-02
docker stop wm-monitor-ws-01 wm-monitor-ws-02
```

```powershell
docker stop wm-fo-op-01 wm-fo-op-02
```

### Volver a arrancarlos

En el PC de estaciones arrancamos primero los Monitores:

```powershell
docker start wm-monitor-ws-01 wm-monitor-ws-02
```

Comprobamos en sus logs que Central ha aceptado el registro y después arrancamos los Engine:

```powershell
docker start wm-engine-ws-01 wm-engine-ws-02
```

En el PC de operarios:

```powershell
docker start wm-fo-op-01 wm-fo-op-02
```

Para revisar los contenedores, incluidos los que están detenidos:

```powershell
docker ps -a --format "table {{.Names}}\t{{.Status}}"
```