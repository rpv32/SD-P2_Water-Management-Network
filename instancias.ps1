$ErrorActionPreference = "Stop"

$script:ComposeInstancias = Join-Path `
    $PSScriptRoot "kafka\compose.instancias.yml"

function Inicializar-WM {
    $imagen = docker inspect central --format '{{.Config.Image}}'

    if ($LASTEXITCODE -ne 0) {
        throw "No se encuentra el contenedor central."
    }

    $datos = docker inspect central | ConvertFrom-Json

    if (-not $datos[0].State.Running) {
        throw "Central debe estar arrancada antes de crear instancias."
    }


    $red = $datos[0].NetworkSettings.Networks.PSObject.Properties.Name |
        Select-Object -First 1

    if (-not $red) {
        throw "No se encuentra la red de Central."
    }

    $env:WM_IMAGE = $imagen.Trim()
    $env:WM_NETWORK = $red

    Write-Host "Imagen: $env:WM_IMAGE"
    Write-Host "Red: $env:WM_NETWORK"
}

function Nueva-WS {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Id,

        [Parameter(Mandatory = $true)]
        [string]$Ubicacion
    )

    if ($Id -cnotmatch '^[A-Za-z0-9_-]+$') {
        throw "ID no valido."
    }

    $monitor = "wm-monitor-$Id".ToLowerInvariant()
    $engine = "wm-engine-$Id".ToLowerInvariant()
    $volumen = "wm-engine-$Id-data".ToLowerInvariant()

    docker exec central java -cp app.jar WM_Alta `
        /data/wm.db station $Id $Ubicacion

    if ($LASTEXITCODE -ne 0) {
        throw "No se pudo dar de alta $Id."
    }

    docker compose -f $script:ComposeInstancias run `
        -d `
        --no-deps `
        --name $monitor `
        monitor `
        -cp app.jar WM_WS_M 6000 central 5000 $Id

    if ($LASTEXITCODE -ne 0) {
        throw "No se pudo crear Monitor para $Id."
    }

    # Esperar a que el servidor del Monitor acepte conexiones.
    $preparado = $false

    for ($intento = 0; $intento -lt 15; $intento++) {
        $logs = docker logs $monitor 2>&1

        if (($logs | Out-String) -match "Puerto reservado para el Engine") {
            $preparado = $true
            break
        }

        Start-Sleep -Seconds 1
    }

    if (-not $preparado) {
        throw "Monitor no parece preparado. Revisa: docker logs $monitor"
    }

    docker compose -f $script:ComposeInstancias run `
        -d `
        --no-deps `
        --name $engine `
        -v "${volumen}:/data" `
        engine `
        "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn" `
        -cp app.jar Engine $monitor 6000 $Id broker:29092

    if ($LASTEXITCODE -ne 0) {
        throw "No se pudo crear Engine para $Id."
    }

    Write-Host "WS creada: $Id"
    Write-Host "Monitor: $monitor"
    Write-Host "Engine: $engine"
}

function Nuevo-FO {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Id,

        [Parameter(Mandatory = $true)]
        [int]$PuertoWeb,

        [string]$Nombre = ""
    )

    if ($Id -cnotmatch '^[A-Za-z0-9_-]+$') {
        throw "ID no valido."
    }

    if ($PuertoWeb -lt 1024 -or $PuertoWeb -gt 65535) {
        throw "Puerto web no valido."
    }

    if ([string]::IsNullOrWhiteSpace($Nombre)) {
        $Nombre = $Id
    }

    $contenedor = "wm-fo-$Id".ToLowerInvariant()
    $volumen = "wm-fo-$Id-data".ToLowerInvariant()

    docker exec central java -cp app.jar WM_Alta `
        /data/wm.db operator $Id $Nombre

    if ($LASTEXITCODE -ne 0) {
        throw "No se pudo dar de alta $Id."
    }

    docker compose -f $script:ComposeInstancias run `
        -d `
        --no-deps `
        --name $contenedor `
        -p "${PuertoWeb}:8081" `
        -v "${volumen}:/app/archivos_fo" `
        fo `
        "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn" `
        -cp app.jar WM_FO broker:29092 $Id

    if ($LASTEXITCODE -ne 0) {
        throw "No se pudo crear FO para $Id."
    }

    Write-Host "FO creado: $Id"
    Write-Host "Web: http://localhost:$PuertoWeb"
}