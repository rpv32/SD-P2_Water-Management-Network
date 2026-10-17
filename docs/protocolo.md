# Protocolo de comunicación

## 1. Reparto de comunicaciones

Usaremos cada tecnología para una función distinta:

| Comunicación | Tecnología | Motivo |
|---|---|---|
| `WM_WS_M` <--> `WM_Central` | Socket TCP | Registro de la estación, mensajes de vida y aviso de averías. |
| `WM_WS_M` <--> `WM_WS_E` | Socket TCP | Identificación del Engine y comprobación de salud cada segundo. |
| `WM_FO` <--> `WM_Central` | Kafka | Solicitudes de riego, respuestas y consulta de estaciones disponibles. |
| `WM_Central` --> `WM_WS_E` | Kafka | Órdenes de inicio, parada, bloqueo y activación. |
| `WM_WS_E` --> `WM_Central` | Kafka | Telemetría y resultado final del riego. |

Los sockets se utilizan para las comunicaciones directas entre componentes que deben conocerse y comprobar su estado. Kafka se utiliza para las solicitudes y los eventos del sistema, ya que permite desacoplar los módulos.

## 2. Identificadores comunes

Los mensajes utilizarán estos campos cuando sean necesarios:

- `stationId`: identificador de la estación, por ejemplo `WS-01`.
- `operatorId`: identificador del operario, por ejemplo `OP-01`.
- `requestId`: UUID de una solicitud de riego.
- `sessionId`: UUID asignado al riego cuando se autoriza.
- `messageId`: UUID de un mensaje de socket.
- `eventId`: UUID de un evento Kafka.
- `occurredAt`: fecha y hora UTC en formato ISO-8601.

`requestId` permite detectar una solicitud repetida y `sessionId` relaciona todos los mensajes de un mismo riego.

Las unidades serán:

- Duración: segundos.
- Caudal: litros por minuto.
- Volumen: litros.

## 3. Tramas de socket

Los mensajes enviados mediante sockets serán cadenas de texto con los campos
separados por el carácter `#`.

El formato general será:

```text
OPERACION#campo1#campo2#...#campoN
```

Por ejemplo, el Monitor puede solicitar el registro de una estación con:

```text
REGISTER#550e8400-e29b-41d4-a716-446655440000#WS-01
```

Central puede responder:

```text
REGISTER_RESULT#550e8400-e29b-41d4-a716-446655440000#OK##Estación autenticada
```

Para enviar y recibir los mensajes utilizaremos:

- `DataOutputStream.writeUTF()`
- `DataInputStream.readUTF()`

Cada tipo de mensaje tendrá un número concreto de campos. El receptor comprobará:

1. Que la operación sea conocida.
2. Que el número de campos sea correcto.
3. Que los campos obligatorios no estén vacíos.
4. Que los valores numéricos tengan un formato válido.
5. Que los identificadores correspondan con una estación, solicitud o sesión conocida.

Los campos de texto no podrán contener el carácter `#`, porque se utiliza como
separador. Si un texto introducido por el usuario contiene ese carácter, se
rechazará o se sustituirá antes de enviarlo.

En Java se utilizará:

```java
String[] campos = mensaje.split("#", -1);
```

El segundo parámetro permite conservar los campos vacíos situados al final del
mensaje. Esto es necesario porque algunos campos son opcionales.

Cuando un mensaje sea incorrecto, el receptor responderá:

```text
ERROR#correlationId#errorCode#message
```

Por ejemplo:

```text
ERROR#msg-01#BAD_FIELD_COUNT#Número incorrecto de campos
```

Si no se puede obtener el identificador del mensaje recibido, `correlationId`
se enviará vacío:

```text
ERROR##MALFORMED_MESSAGE#Mensaje no válido
```

## 4. Monitor y Central

`WM_WS_M` abre la conexión con `WM_Central` y la mantiene mientras está funcionando.

| Mensaje | Sentido | Campos de `DATA` | Función |
|---|---|---|---|
| `REGISTER` | Monitor → Central | `messageId#stationId` | Solicita la autenticación de una estación ya registrada en SQLite. |
| `REGISTER_RESULT` | Central → Monitor | `correlationId#OK/ERROR#errorCode#message` | Acepta o rechaza el registro. |
| `MONITOR_HEARTBEAT` | Monitor → Central | `messageId#stationId#engineHealth#occurredAt` | Informa de que el Monitor sigue conectado y del último estado del Engine. |
| `INCIDENT` | Monitor → Central | `messageId#stationId#cause#sessionId#occurredAt` | Notifica que el Engine responde `KO` o no responde. |
| `RECOVERY` | Monitor → Central | `messageId#stationId#occurredAt` | Notifica que el Engine vuelve a responder `OK`. |

Ejemplos de `DATA`:

```text
REGISTER#550e8400-e29b-41d4-a716-446655440000#WS-01
REGISTER_RESULT#550e8400-e29b-41d4-a716-446655440000#OK##Estación autenticada
MONITOR_HEARTBEAT#msg-02#WS-01#OK#2026-10-17T12:00:00Z
INCIDENT#msg-03#WS-01#ENGINE_TIMEOUT#session-01#2026-10-17T12:01:00Z
RECOVERY#msg-04#WS-01#2026-10-17T12:02:00Z
```

### Registro y autenticación

Para mantener el diseño sencillo, las estaciones deberán existir previamente en SQLite. Si el `stationId` no está registrado, Central responderá con `UNKNOWN_STATION` y el Monitor no podrá prestar servicio.

La ubicación se obtiene de SQLite, por lo que no es necesario enviarla en cada conexión. Esto evita que una estación pueda cambiarla al registrarse.

Aunque la descripción general del enunciado menciona el envío del identificador y la ubicación, en nuestro diseño la ubicación se registra previamente en
SQLite. El Monitor envía únicamente su identificador y Central recupera la ubicación asociada. De esta forma, una estación no puede modificar su ubicación
durante la autenticación.

### Detección de desconexión

El Monitor enviará un `MONITOR_HEARTBEAT` periódicamente. El intervalo y el tiempo máximo de espera serán configurables. Si Central supera ese tiempo sin recibir mensajes, marcará la estación como `DISCONNECTED`.

## 5. Monitor y Engine

`WM_WS_M` crea un servidor socket y `WM_WS_E` se conecta a él. El Monitor envía una comprobación de salud cada segundo, tal como indica el enunciado.

| Mensaje | Sentido | Campos de `DATA` | Función |
|---|---|---|---|
| `ENGINE_HELLO` | Engine → Monitor | `messageId#stationId` | Identifica el Engine que intenta conectarse. |
| `ENGINE_HELLO_RESULT` | Monitor → Engine | `correlationId#OK/ERROR#errorCode` | Acepta o rechaza al Engine. |
| `HEALTH_CHECK` | Monitor → Engine | `messageId#occurredAt` | Pregunta si el Engine está funcionando. |
| `HEALTH_RESULT` | Engine → Monitor | `correlationId#OK/KO#sessionId` | Devuelve su estado y la sesión activa, si existe. |

Ejemplos:

```text
ENGINE_HELLO#msg-10#WS-01
ENGINE_HELLO_RESULT#msg-10#OK#
HEALTH_CHECK#msg-11#2026-10-17T12:00:00Z
HEALTH_RESULT#msg-11#OK#session-01
```

Si el Engine responde `KO` o no responde antes del timeout, el Monitor envía un `INCIDENT` a Central. Cuando vuelve a responder `OK`, envía `RECOVERY`.

Las órdenes de riego no se enviarán por este socket. El Engine las recibirá mediante Kafka. Así se mantiene separada la comprobación de salud de los eventos de negocio.

## 6. Eventos Kafka

Los valores de Kafka se enviarán en JSON UTF-8. Todos los eventos incluirán:

```json
{
  "schemaVersion": 1,
  "eventId": "uuid",
  "eventType": "TIPO_DEL_EVENTO",
  "occurredAt": "2026-10-17T12:00:00Z"
}
```

### Topics

| Topic | Productor | Consumidor | Clave | Uso |
|---|---|---|---|---|
| `wm.watering.requests` | `WM_FO` | `WM_Central` | `stationId` | Solicitudes de riego. |
| `wm.fo.queries` | `WM_FO` | `WM_Central` | `operatorId` | Consulta de estaciones disponibles. |
| `wm.station.commands` | `WM_Central` | `WM_WS_E` | `stationId` | Inicio, parada, bloqueo y activación. |
| `wm.station.telemetry` | `WM_WS_E` | `WM_Central` | `stationId` | Caudal y volumen durante el riego. |
| `wm.station.results` | `WM_WS_E` | `WM_Central` | `stationId` | Confirmación de órdenes y resumen final. |
| `wm.fo.<operatorId>.events` | `WM_Central` | `WM_FO` indicado | `requestId` o `sessionId` | Respuestas y eventos dirigidos a un operario concreto. |

Para esta práctica se usará un topic de eventos por operario. De esta forma, cada `WM_FO` consume solamente sus mensajes. Como el número de operarios de la simulación es pequeño, esta solución es sencilla de implementar y explicar.

### Solicitud de riego

```json
{
  "schemaVersion": 1,
  "eventId": "event-01",
  "eventType": "WATERING_REQUESTED",
  "occurredAt": "2026-10-17T12:00:00Z",
  "requestId": "request-01",
  "stationId": "WS-01",
  "operatorId": "OP-01",
  "durationSeconds": 120
}
```

Central comprueba que el operario existe y que la estación está `AVAILABLE`. Después publica `WATERING_AUTHORIZED` o `WATERING_DENIED` en el topic del operario.

### Orden para la estación

```json
{
  "schemaVersion": 1,
  "eventId": "event-02",
  "eventType": "START_WATERING",
  "occurredAt": "2026-10-17T12:00:01Z",
  "requestId": "request-01",
  "sessionId": "session-01",
  "stationId": "WS-01",
  "operatorId": "OP-01",
  "durationSeconds": 120
}
```

Otros valores posibles de `eventType` serán:

- `STOP_WATERING`
- `BLOCK_STATION`
- `ACTIVATE_STATION`

### Telemetría

```json
{
  "schemaVersion": 1,
  "eventId": "event-03",
  "eventType": "WATERING_TELEMETRY",
  "occurredAt": "2026-10-17T12:00:02Z",
  "stationId": "WS-01",
  "operatorId": "OP-01",
  "requestId": "request-01",
  "sessionId": "session-01",
  "sequence": 1,
  "flowLitresPerMinute": 12.5,
  "volumeLitres": 0.21
}
```

El Engine enviará telemetría cada segundo mientras esté regando. Central actualizará su panel y reenviará los datos al topic del operario que inició ese riego.

### Resultado final

Al terminar, el Engine publicará `WATERING_FINISHED` con:

- `sessionId`
- `stationId`
- `operatorId`
- `reason`
- `durationSeconds`
- `volumeLitres`

Los motivos de finalización serán, como mínimo:

- `TIME_LIMIT`
- `MANUAL_STOP`
- `LEAK`
- `BLOCKED`

## 7. Grupos de consumidores

- Central usará un grupo propio para solicitudes, consultas, telemetría y resultados.
- Cada `WM_FO` usará un grupo basado en su identificador, por ejemplo `wm-fo-OP-01`.
- Los Engine no deben compartir un grupo si eso puede hacer que una orden para una estación sea recibida únicamente por otra. Durante la implementación se decidirá entre asignar particiones o filtrar por `stationId`, y se probará expresamente con dos estaciones.

Este último punto queda pendiente de concretar en la issue de Kafka, porque depende de cómo se creen las particiones y los consumidores Java.

## 8. Duplicados y errores

Kafka puede volver a entregar un evento. Por tanto:

- Central guardará los `requestId` ya procesados.
- Una solicitud repetida no creará otro riego, sino que devolverá la respuesta anterior.
- Los resultados finales se guardarán una sola vez por `sessionId`.
- Los consumidores confirmarán el offset después de procesar correctamente el evento.

Errores principales:

| Código | Significado |
|---|---|
| `MALFORMED_MESSAGE` | El mensaje recibido no se puede interpretar. |
| `UNKNOWN_OPERATION` | La operación indicada no existe. |
| `BAD_FIELD_COUNT` | Faltan campos en el mensaje. |
| `INVALID_FIELD` | Un campo no tiene un valor válido. |
| `UNKNOWN_STATION` | La estación no está registrada. |
| `UNKNOWN_OPERATOR` | El operario no está registrado. |
| `STATION_NOT_AVAILABLE` | La estación no puede empezar otro riego. |
| `DUPLICATE_REQUEST` | La solicitud ya se había procesado. |
| `ENGINE_KO` | El Engine ha informado de un fallo. |
| `ENGINE_TIMEOUT` | El Engine no ha respondido a tiempo. |
| `UNKNOWN_SESSION` | No existe la sesión indicada. |
| `PERSISTENCE_ERROR` | No se ha podido guardar la operación en SQLite. |
| `KAFKA_UNAVAILABLE` | No se puede conectar con Kafka. |

## 9. Recuperación

Para no perder las operaciones importantes:

- Las solicitudes tendrán `requestId` y las sesiones `sessionId`.
- Central guardará en SQLite las solicitudes, sesiones e incidencias antes de darlas por procesadas.
- Si un FO se desconecta, el riego continuará y al volver podrá consultar o recibir el resultado.
- Si Central se reinicia, recuperará de SQLite las estaciones y sesiones conocidas.
- La telemetría pendiente y los reintentos se concretarán en las tareas de Kafka y resiliencia.

No se implementará de momento una solución compleja de outbox/inbox. Si durante las pruebas se comprueba que es necesaria para cumplir la rúbrica, se añadirá de forma documentada.

## 10. Decisiones tomadas

1. Los mensajes de socket utilizarán campos separados por `#` y se enviarán mediante `readUTF()` y `writeUTF()`.
2. Central solo aceptará estaciones y operarios previamente registrados en SQLite.
3. El Monitor informará a Central mediante socket.
4. El Monitor comprobará al Engine mediante socket cada segundo.
5. Las solicitudes, órdenes, telemetría y resultados se enviarán mediante Kafka.
6. Se usará un topic de respuesta por operario para que no reciba mensajes de otros.
7. Los tiempos de heartbeat y timeout serán configurables.

## 11. Decisiones pendientes

1. Valor exacto del timeout Monitor-Central.
2. Valor exacto del timeout Monitor-Engine.
3. Número de particiones de cada topic.
4. Forma definitiva de repartir órdenes Kafka entre varios Engine.
5. Política de retención y reintentos.
6. Mecanismo de almacenamiento temporal si Kafka no está disponible.
7. Valorar si se añade STX/ETX/LRC como mejora técnica.
