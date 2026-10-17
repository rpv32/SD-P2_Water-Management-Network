# Arquitectura de Water Management

El sistema permite:

- Registrar estaciones
- Monitorizar su estado
- Solicitar y autorizar riegos
- Simular caudal y volumen
- Detectar fugas
- Bloquear y reactivar estaciones
- Consultar la información desde una aplicación de operarios

## 1. Componentes

### WM_Central

Núcleo del sistema. Gestiona las estaciones, consulta SQLite,
procesa solicitudes de riego, publica y consume eventos Kafka y muestra
el estado global de la red.

### WM_WS_M

Se registra en WM_Central, supervisa Engine y notifica problemas de salud.

### WM_WS_E

Es el Engine de una estación. Simula la electroválvula, el caudalímetro,
el riego, el volumen acumulado y los fallos de la estación.

### WM_FO

Aplicación utilizada por los operarios para solicitar y supervisar los riegos.

### SQLite

Base de datos utilizada por WM_Central para almacenar estaciones,
operarios, sesiones de riego e incidencias.

### Apache Kafka

Sistema de streaming de eventos utilizado para desacoplar las solicitudes,
las respuestas, la telemetría, las incidencias y las órdenes de riego.

## 2. Comunicaciones

### Sockets TCP

Se utilizan para las comunicaciones directas:

- WM_WS_M con WM_Central.
- WM_WS_M con WM_WS_E.

### Kafka

Se utiliza para transmitir eventos:

- Solicitudes de riego.
- Autorizaciones y denegaciones.
- Telemetría durante riego.
- Órdenes de inicio, parada, bloqueo y activación.
- Resultados de los servicios.
- Avisos al operario sobre incidencias de su riego.

## 3. Flujo de registro

1. WM_WS_M se conecta a WM_Central.
2. WM_WS_M envía el identificador de la estación.
3. WM_Central consulta SQLite.
4. Si la estación existe, recupera su ubicación y acepta la conexión.
5. Si no existe, rechaza el registro.
6. La estación queda `AVAILABLE` cuando el Monitor confirma que el Engine responde correctamente.

## 4. Flujo de riego

1. WM_FO solicita un riego.
2. La solicitud se publica en Kafka.
3. WM_Central recibe la solicitud.
4. WM_Central comprueba el estado de la estación.
5. Si está disponible, autoriza el riego.
6. La orden llega a la estación.
7. WM_WS_E inicia la simulación del riego.
8. La estación envía caudal y volumen periódicamente.
9. WM_Central actualiza SQLite y el panel.
10. El riego termina por tiempo, orden manual o incidencia.
11. Se envía un resumen al operario.

## 5. Estados de una estación

- `AVAILABLE`: DISPONIBLE.
- `WATERING`: REGANDO.
- `LEAK`: FUGA.
- `OUT_OF_SERVICE`: FUERA_SERVICIO.
- `DISCONNECTED`: DESCONECTADA.

## 6. Despliegue previsto

- WM_Central, SQLite y Kafka se desplegarán en Railway.
- WM_WS_M y WM_WS_E se ejecutarán en los equipos del laboratorio.
- WM_FO se ejecutará en los equipos del laboratorio.
- Los módulos se desplegarán mediante Docker
