cd "[ruta]\SD-P2_Water-Management-Network"
.\mvnw.cmd clean package -DskipTests dependency:copy-dependencies "-DoutputDirectory=target/dependency"
docker compose -f .\kafka\compose.yml down
Remove-Item .\wm.db -ErrorAction SilentlyContinue
Remove-Item .\archivos_fo -Recurse -Force -ErrorAction SilentlyContinue
docker compose -f .\kafka\compose.yml up -d broker
Start-Sleep -Seconds 10
docker ps --filter "name=broker"
$topics = @(
  "wm.watering.requests",
  "wm.station.commands",
  "wm.station.telemetry",
  "wm.station.results",
  "wm.fo.OP-01.events",
  "wm.fo.OP-02.events"
)

foreach ($t in $topics) {
  docker exec broker /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server broker:29092 `
    --create --if-not-exists `
    --topic $t `
    --partitions 1 `
    --replication-factor 1
}

docker exec broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker:29092 --list

