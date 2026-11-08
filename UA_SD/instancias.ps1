$ErrorActionPreference = "Stop"
$script:ComposeInstancias = Join-Path $PSScriptRoot "kafka\compose.instancias.yml"

function Inicializar-WM {
    param(
        [string]$Imagen = "watermanagement:practica-v1",
        [string]$ClaseEngine = "WM_WS_E",
        [string]$UrlCentral = "https://central-production-8c7f.up.railway.app",
        [string]$HostCentral = "switchyard.proxy.rlwy.net",
        [int]$PuertoCentral = 16591,
        [string]$Kafka = "centerbeam.proxy.rlwy.net:45491"
    )
    $script:Preparado = $false
    docker image inspect $Imagen --format '{{.Id}}'
    if ($LASTEXITCODE -ne 0) { throw "No existe la imagen $Imagen o Docker no responde." }
    $env:WM_IMAGE = $Imagen
    docker compose -f $script:ComposeInstancias config --quiet
    if ($LASTEXITCODE -ne 0) { throw "Compose no valido." }
    $script:ClaseEngine = $ClaseEngine
    $script:UrlCentral = $UrlCentral.TrimEnd('/')
    $script:HostCentral = $HostCentral
    $script:PuertoCentral = $PuertoCentral
    $script:Kafka = $Kafka
    $script:Preparado = $true
    Write-Host "Preparado: $Imagen"
}

# No consulta IDs: solo observa si el proceso confirma el arranque o termina.
function Esperar-Modulo {
    param([string]$Nombre, [string]$Marca)
    $ErrorActionPreference = "Continue"
    for ($i = 0; $i -lt 60; $i++) {
        $activo = docker inspect --format '{{.State.Running}}' $Nombre
        if ($LASTEXITCODE -ne 0) { throw "No se pudo inspeccionar $Nombre." }
        $logs = (docker logs --tail 150 $Nombre 2>&1 | Out-String)
        if ($activo -ne 'true') { 
            $estacionDenegada = $logs -match 'REGISTRO_DENEGADO:[^\r\n]*#ERROR#UNKNOWN_STATION#'
            $operarioDenegado = $logs -match 'El operario [A-Za-z0-9_-]+ no existe o esta inactivo'

            if ($estacionDenegada -or $operarioDenegado) {
                docker container rm $Nombre | Out-Null

                if ($LASTEXITCODE -ne 0) {
                    throw "Registro rechazado, pero no se pudo eliminar el contenedor $Nombre."
                }

                throw "El ID no existe o esta inactivo en Central. Contenedor $Nombre eliminado. Revisa el registro en /altas antes de volver a intentarlo."
            }

            throw "El modulo $Nombre termino:`n$logs"
        }
        if ($logs -match [regex]::Escape($Marca)) { return }
        Start-Sleep -Seconds 1
    }
    throw "Sin confirmacion de arranque. No implica ID desconocido. Revisa: docker logs $Nombre"
}

function Nueva-WS {
    param([Parameter(Mandatory = $true)][string]$Id)
    if (-not $script:Preparado) { throw "Ejecuta Inicializar-WM primero." }
    if ($Id -cnotmatch '^[A-Za-z0-9_-]{1,64}$') { throw "ID no valido." }
    $monitor = "wm-monitor-$Id".ToLowerInvariant()
    $engine = "wm-engine-$Id".ToLowerInvariant()
    $volumen = "wm-engine-$Id-data".ToLowerInvariant()
    $nombres = @(docker container ls -a --format '{{.Names}}')
    if ($LASTEXITCODE -ne 0) { throw "Docker no responde." }
    if ($nombres -contains $monitor -or $nombres -contains $engine) {
        throw "Ya existe un contenedor para $Id. Revisalo antes de recrear, o utiliza docker start."
    }
    docker compose -f $script:ComposeInstancias run -d --no-deps --name $monitor `
        monitor -cp app.jar WM_WS_M 6000 $script:HostCentral $script:PuertoCentral $Id
    if ($LASTEXITCODE -ne 0) { throw "No se pudo crear $monitor." }
    Esperar-Modulo $monitor 'MONITOR_READY'
    docker compose -f $script:ComposeInstancias run -d --no-deps --name $engine `
        -v "${volumen}:/data" engine `
        "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn" `
        -cp app.jar $script:ClaseEngine $monitor 6000 $Id $script:Kafka
    if ($LASTEXITCODE -ne 0) { throw "No se pudo crear $engine. Revisa el Monitor." }
    Write-Host "Monitor autorizado; Engine lanzado para $Id. Comprueba su estado en Central."
}

function Nuevo-FO {
    param(
        [Parameter(Mandatory = $true)][string]$Id,
        [Parameter(Mandatory = $true)][int]$PuertoWeb
    )
    if (-not $script:Preparado) { throw "Ejecuta Inicializar-WM primero." }
    if ($Id -cnotmatch '^[A-Za-z0-9_-]{1,64}$') { throw "ID no valido." }
    if ($PuertoWeb -lt 1024 -or $PuertoWeb -gt 65535) { throw "Puerto no valido." }
    $contenedor = "wm-fo-$Id".ToLowerInvariant()
    $volumen = "wm-fo-$Id-data".ToLowerInvariant()
    $nombres = @(docker container ls -a --format '{{.Names}}')
    if ($LASTEXITCODE -ne 0) { throw "Docker no responde." }
    if ($nombres -contains $contenedor) { throw "Ya existe $contenedor. Revisalo o usa docker start." }
    docker compose -f $script:ComposeInstancias run -d --no-deps --name $contenedor `
        -e "CENTRAL_URL=$script:UrlCentral" `
        -p "127.0.0.1:${PuertoWeb}:8081" -v "${volumen}:/app/archivos_fo" `
        fo "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn" `
        -cp app.jar WM_FO $script:Kafka $Id
    if ($LASTEXITCODE -ne 0) { throw "No se pudo crear $contenedor." }
    Esperar-Modulo $contenedor 'FO_READY'
    Write-Host "FO autorizado y preparado: http://localhost:$PuertoWeb"
}