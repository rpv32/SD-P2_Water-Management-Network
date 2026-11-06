@echo off
setlocal EnableExtensions DisableDelayedExpansion

if /i "%~1"=="worker" goto worker

cd /d "%~dp0"

set "JDK=%JAVA_HOME%"
if not exist "%JDK%\bin\javac.exe" (
    echo ERROR: JAVA_HOME debe apuntar a un JDK con java.exe y javac.exe.
    pause
    exit /b 1
)

set "JAVA=%JDK%\bin\java.exe"
set "JAVAC=%JDK%\bin\javac.exe"

if not exist "%JAVA%" exit /b 1

if not exist "%CD%\schema.sql" (
    call :note FALLO: no se encuentra schema.sql en la carpeta del proyecto.
    goto finish
)

if not exist "%CD%\lib\sqlite-jdbc.jar" (
    if not exist "%CD%\lib\*" (
        call :note FALLO: no se encuentra la carpeta lib con sqlite-jdbc.
        goto finish
    )
)

set "RUN=%CD%\logs_pruebas\run_%RANDOM%_%RANDOM%"
mkdir "%RUN%\classes"

set "TRACE=%RUN%\proceso.log"
set "FINAL=%RUN%\log_unificado.txt"

set "CP=%RUN%\classes;%CD%;%CD%\lib\*"

call :note INICIO: compilacion de clases y comprobador de base de datos.

"%JAVA%" -version >> "%TRACE%" 2>&1
"%JAVAC%" -version >> "%TRACE%" 2>&1

"%JAVAC%" -cp "%CD%;%CD%\lib\*" -d "%RUN%\classes" ^
WM_Central.java HiloWM.java WM_WS_M.java Engine.java ComprobarBD.java >> "%TRACE%" 2>&1

if errorlevel 1 (
    call :note FALLO: compilacion. No se arrancan procesos.
    goto finish
)

call :note OK: compilacion correcta.

call :note FASE 1: arranque de Central en puerto 5000.

start "CENTRAL PRUEBA" cmd /d /c ""%~f0" worker CENTRAL"

call :wait "%RUN%\CENTRAL.log" "Escuchando el puerto" 45
if errorlevel 1 (
    call :note FALLO: Central no anuncia escucha.
    goto finish
)

call :note OK: Central escuchando.

call :check_bd WS-01 0 UNKNOWN DISCONNECTED 0 "Estado inicial WS-01 en SQLite"
if errorlevel 1 goto finish

call :note FASE 2: arranque del Monitor para WS-01.

start "MONITOR PRUEBA" cmd /d /c ""%~f0" worker MONITOR"

call :wait "%RUN%\MONITOR.log" "Estacion registrada correctamente" 45
if errorlevel 1 (
    call :note FALLO: el Monitor no confirmo el registro.
    goto finish
)

call :note OK: Monitor registrado.

call :check_bd WS-01 1 UNKNOWN DISCONNECTED 0 "Monitor conectado en SQLite antes de Engine"
if errorlevel 1 goto finish

call :note Observacion sin Engine durante 15 segundos.
timeout /t 15 /nobreak >nul

call :check "%RUN%\CENTRAL.log" "MONITOR_HEARTBEAT" "Heartbeats recibidos por Central"
call :check "%RUN%\CENTRAL.log" "UNKNOWN" "Estado UNKNOWN antes de conectar Engine"

call :note FASE 3: arranque del Engine.

start "ENGINE PRUEBA" cmd /d /c ""%~f0" worker ENGINE"

call :wait "%RUN%\MONITOR.log" "Engine conectado" 45
if errorlevel 1 (
    call :note FALLO: el Engine no completo el handshake.
    goto finish
)

call :note OK: Engine conectado. Esperando estado OK durante 15 segundos.
timeout /t 15 /nobreak >nul

call :check_bd WS-01 1 OK AVAILABLE 0 "Engine OK y estacion disponible en SQLite"
if errorlevel 1 goto finish

call :note FASE 4 MANUAL: en la ventana ENGINE escribe 3 y pulsa Intro para provocar averia.
echo Tras realizar la accion, vuelve aqui y pulsa una tecla.
pause >nul

call :note Esperando incidente durante 20 segundos.
timeout /t 20 /nobreak >nul

call :check "%RUN%\CENTRAL.log" "INCIDENT" "INCIDENT recibido por Central"
call :check_bd WS-01 1 KO LEAK 1 "Incidencia abierta y Engine KO en SQLite"
if errorlevel 1 goto finish

call :note FASE 5 MANUAL: en la ventana ENGINE escribe 3 y pulsa Intro para reparar.
echo Tras realizar la accion, vuelve aqui y pulsa una tecla.
pause >nul

call :note Esperando recovery durante 20 segundos.
timeout /t 20 /nobreak >nul

call :check "%RUN%\CENTRAL.log" "RECOVERY" "RECOVERY recibido por Central"
call :check_bd WS-01 1 OK AVAILABLE 0 "Recovery aplicado en SQLite"
if errorlevel 1 goto finish

call :note FASE 6 MANUAL: cierra SOLO la ventana MONITOR PRUEBA.
echo Espera a que Central detecte el timeout y marque la estacion como desconectada.
echo Cuando hayas cerrado el Monitor, pulsa una tecla.
pause >nul

call :note Esperando desconexion persistente durante 20 segundos.
timeout /t 20 /nobreak >nul

call :check_bd WS-01 0 UNKNOWN DISCONNECTED 0 "Monitor desconectado en SQLite tras cierre"
if errorlevel 1 goto finish

call :note OK: todas las comprobaciones de socket y base de datos han pasado.

goto finish

:finish
call :note FIN: cierre manual para consolidar las trazas.

echo Cierra SOLO las ventanas CENTRAL PRUEBA, MONITOR PRUEBA y ENGINE PRUEBA.
echo Cuando esten cerradas, pulsa una tecla para generar el informe.
pause >nul

copy /y "%TRACE%" "%FINAL%" >nul

for %%M in (CENTRAL MONITOR ENGINE) do (
    >> "%FINAL%" echo.
    >> "%FINAL%" echo ==================== TRAZA %%M ====================
    if exist "%RUN%\%%M.log" type "%RUN%\%%M.log" >> "%FINAL%"
)

echo.
echo Informe generado: "%FINAL%"
pause
exit /b

:note
echo [%date% %time%] %*
>> "%TRACE%" echo [%date% %time%] %*
exit /b

:wait
for /l %%N in (1,1,%~3) do (
    if exist "%~1" (
        findstr /i /l /c:"%~2" "%~1" >nul 2>&1
        if not errorlevel 1 exit /b 0
    )
    timeout /t 1 /nobreak >nul
)
exit /b 1

:check
findstr /i /l /c:"%~2" "%~1" >nul 2>&1
if errorlevel 1 (
    call :note NO OBSERVADO: %~3
) else (
    call :note OBSERVADO: %~3
)
exit /b

:check_bd
"%JAVA%" -cp "%CP%" ComprobarBD %1 %2 %3 %4 %5 >> "%TRACE%" 2>&1

findstr /c:"DB_CHECK_OK" "%TRACE%" >nul 2>&1
if errorlevel 1 (
    call :note FALLO BD: %~6
    exit /b 1
)

call :note OK BD: %~6
exit /b 0

:worker
echo Proceso %~2. Salida guardada en su archivo independiente.
echo Para Engine: escribe la opcion del menu y pulsa Intro.

if /i "%~2"=="CENTRAL" (
    "%JAVA%" -cp "%CP%" WM_Central 5000 localhost:9092 > "%RUN%\CENTRAL.log" 2>&1
)

if /i "%~2"=="MONITOR" (
    "%JAVA%" -cp "%CP%" WM_WS_M 6000 localhost 5000 WS-01 > "%RUN%\MONITOR.log" 2>&1
)

if /i "%~2"=="ENGINE" (
    "%JAVA%" -cp "%CP%" Engine localhost 6000 WS-01 > "%RUN%\ENGINE.log" 2>&1
)

echo Proceso terminado. Consulta su log.
pause
exit /b