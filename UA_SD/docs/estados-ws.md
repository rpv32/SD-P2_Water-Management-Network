# Estados de una Watering Station

## 1. Estados

| Estado | Texto mostrado | Color | Significado |
|---|---|---|---|
| `AVAILABLE` | Disponible | Verde | El Monitor y el Engine funcionan, la estación no está bloqueada y no hay ningún riego activo. |
| `WATERING` | Regando | Verde parpadeante o similar | Hay un riego autorizado y el Engine ha confirmado que está regando. |
| `LEAK` | Fuga / Averiada | Rojo | El Engine responde `KO`, no responde o se ha detectado una fuga. |
| `OUT_OF_SERVICE` | Fuera de servicio | Naranja | Central ha bloqueado la estación. |
| `DISCONNECTED` | Desconectada | Gris | Central no recibe mensajes de vida del Monitor. |

La causa de una avería se guarda por separado, por ejemplo `ENGINE_KO`, `ENGINE_TIMEOUT` o `LEAK_SENSOR`. Estas causas no son estados nuevos.

## 2. Información necesaria

Central calculará el estado visible a partir de estos datos:

- `monitorConnected`: indica si Central recibe mensajes del Monitor.
- `engineHealth`: puede ser `OK`, `KO` o `UNKNOWN`.
- `blocked`: indica si Central ha bloqueado la estación.
- `activeSessionId`: sesión de riego activa, si existe.
- `incidentCause`: causa de la incidencia actual, si existe.

Cuando Central arranca, carga las estaciones de SQLite y las muestra como `DISCONNECTED` hasta que sus Monitores se conecten.

## 3. Prioridad de estados

Si coinciden varias situaciones, se aplicará este orden:

1. Si el Monitor no está conectado: `DISCONNECTED`.
2. Si está conectado pero Central la ha bloqueado: `OUT_OF_SERVICE`.
3. Si está conectada, no está bloqueada y el Engine está averiado: `LEAK`.
4. Si todo está correcto y existe un riego activo: `WATERING`.
5. Si todo está correcto y no hay riego: `AVAILABLE`.

Por ejemplo, si una estación bloqueada también tiene el Engine en `KO`, seguirá mostrando `OUT_OF_SERVICE`. Al activarla, pasará a `LEAK`, no a `AVAILABLE`, ya que la avería todavía existe.

## 4. Transiciones

| Estado actual | Evento | Estado nuevo | Acción |
|---|---|---|---|
| Inicio de Central | La estación está guardada pero el Monitor no ha conectado | `DISCONNECTED` | Mostrarla en gris y rechazar nuevos riegos. |
| `DISCONNECTED` | Monitor conecta y Engine responde `OK` | `AVAILABLE` | Actualizar la última conexión y permitir solicitudes. |
| `DISCONNECTED` | Monitor conecta y Engine responde `KO` o no responde | `LEAK` | Guardar la incidencia y rechazar solicitudes. |
| `AVAILABLE` | Central autoriza y Engine confirma el inicio | `WATERING` | Abrir la sesión y mostrar operario, caudal y volumen. |
| `AVAILABLE` | Central ordena bloquear | `OUT_OF_SERVICE` | Guardar el bloqueo y rechazar solicitudes. |
| `WATERING` | Termina el tiempo o se solicita una parada manual | `AVAILABLE` | Cerrar la electroválvula y guardar el resumen. |
| `WATERING` | Engine responde `KO`, hay timeout o se detecta fuga | `LEAK` | Parar el riego, guardar la incidencia y avisar a Central y al FO. |
| `WATERING` | Central ordena bloquear | `OUT_OF_SERVICE` | Parar el riego y guardar el resultado con motivo `BLOCKED`. |
| Cualquier estado conectado | Se pierde el Monitor | `DISCONNECTED` | No aceptar nuevos riegos y conservar la sesión y los últimos datos. |
| `LEAK` | Engine se recupera y no existe bloqueo | `AVAILABLE` | Cerrar la incidencia. |
| `OUT_OF_SERVICE` | Central activa y Engine está sano | `AVAILABLE` | Quitar el bloqueo. |
| `OUT_OF_SERVICE` | Central activa pero Engine sigue averiado | `LEAK` | Quitar el bloqueo, pero mantener la avería. |
| `DISCONNECTED` | Central intenta activar sin Monitor | `DISCONNECTED` | Registrar la intención si procede, pero esperar al Monitor. |

## 5. Reglas del riego

- Solo se puede iniciar un riego desde `AVAILABLE`.
- La estación pasa a `WATERING` cuando Central lo autoriza y el Engine confirma que ha comenzado.
- `LEAK`, `OUT_OF_SERVICE` y `DISCONNECTED` rechazan nuevas solicitudes.
- Una fuga durante un riego obliga a detenerlo inmediatamente.
- Un bloqueo durante un riego también obliga a detenerlo.
- Al terminar un riego se guarda su duración, volumen y motivo de finalización.
- Un mensaje repetido no puede crear una segunda sesión ni guardar dos veces el mismo resultado.

## 6. Caídas y recuperación

### Caída del Monitor

Si Central deja de recibir mensajes del Monitor, muestra la estación como `DISCONNECTED` y no admite nuevos riegos.

Si había un riego activo y el Engine sigue funcionando, este puede terminar el servicio que ya tenía autorizado. Cuando el Monitor y Central recuperen la comunicación, se reconciliará el estado y se guardará el resultado. Si también ha caído el Engine, se conserva la última información disponible hasta su recuperación.

### Caída del Engine

El Monitor detecta el fallo porque recibe `KO` o porque vence el timeout. Entonces informa a Central, la estación pasa a `LEAK` y el riego activo se considera interrumpido. El volumen medido hasta ese momento no se borra.

Cuando el Engine vuelve a responder `OK`, el Monitor informa de la recuperación. La estación pasa a `AVAILABLE` solamente si no está bloqueada y no mantiene otra incidencia.

### Caída de Central

Las estaciones que ya estén regando continúan hasta finalizar el servicio. Mientras Central esté caída no se aceptan nuevos riegos. Al recuperarse, Central consulta SQLite y los eventos pendientes para reconstruir el estado.

## 7. Casos que no cambian el estado

Los siguientes mensajes no deben cambiar el estado por sí solos:

- Un heartbeat repetido.
- Un evento Kafka duplicado.
- Una recuperación antigua recibida después de una avería más reciente.
- Una respuesta relacionada con una sesión que ya ha finalizado.

Para detectar mensajes repetidos o antiguos utilizaremos: `messageId`, `eventId`, `requestId`, `sessionId` y la fecha del evento.

## 8. Pruebas previstas

1. Arrancar Central con una estación guardada y el Monitor apagado: aparece `DISCONNECTED`.
2. Conectar Monitor y Engine sanos: pasa a `AVAILABLE`.
3. Solicitar un riego válido: pasa a `WATERING`.
4. Solicitar riego en `LEAK`, `OUT_OF_SERVICE` o `DISCONNECTED`: se deniega.
5. Simular `KO` durante un riego: pasa a `LEAK` y se detiene.
6. Bloquear durante un riego: pasa a `OUT_OF_SERVICE` y se detiene.
7. Finalizar normalmente: vuelve a `AVAILABLE` y guarda un único resumen.
8. Recuperar un Engine averiado: vuelve a `AVAILABLE` si no está bloqueada.
9. Activar una estación bloqueada con Engine averiado: pasa a `LEAK`.
10. Cerrar el Monitor: Central muestra `DISCONNECTED`.
11. Recuperar el Monitor: Central calcula de nuevo el estado real.
12. Repetir mensajes: no se crean sesiones ni resultados duplicados.
