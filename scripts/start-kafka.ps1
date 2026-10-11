# 브로커 3대를 docker run 으로 띄운다. Compose 는 쓰지 않는다.
# 복제 3, 최소 동기 복제 2. 브로커 1대가 죽어도 acks=all 전송은 남는다.

$network = 'kafka-net'
$image = 'apache/kafka:4.1.2'
$clusterId = '4L6g3nShT-eMCtK--X86sw'
$voters = '1@kafka1:29093,2@kafka2:29093,3@kafka3:29093'

docker network inspect $network 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) {
    docker network create $network | Out-Null
}

# 예전 1대 구성의 이름 kafka 와 포트 9092 가 겹치지 않게 지운다.
docker rm -f kafka kafka1 kafka2 kafka3 2>$null | Out-Null

$brokers = @(
    @{ Id = 1; Name = 'kafka1'; HostPort = 9092 },
    @{ Id = 2; Name = 'kafka2'; HostPort = 9094 },
    @{ Id = 3; Name = 'kafka3'; HostPort = 9096 }
)

foreach ($broker in $brokers) {
    docker run -d `
        --name $broker.Name `
        --hostname $broker.Name `
        --network $network `
        -p "$($broker.HostPort):9092" `
        -e "KAFKA_NODE_ID=$($broker.Id)" `
        -e KAFKA_PROCESS_ROLES=broker,controller `
        -e KAFKA_LISTENERS=CONTROLLER://:29093,INTERNAL://:19092,EXTERNAL://:9092 `
        -e "KAFKA_ADVERTISED_LISTENERS=INTERNAL://$($broker.Name):19092,EXTERNAL://localhost:$($broker.HostPort)" `
        -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT `
        -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL `
        -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER `
        -e "KAFKA_CONTROLLER_QUORUM_VOTERS=$voters" `
        -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3 `
        -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=3 `
        -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=2 `
        -e KAFKA_DEFAULT_REPLICATION_FACTOR=3 `
        -e KAFKA_MIN_INSYNC_REPLICAS=2 `
        -e KAFKA_NUM_PARTITIONS=1 `
        -e KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR=3 `
        -e KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR=2 `
        -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 `
        -e "CLUSTER_ID=$clusterId" `
        $image | Out-Null
    Write-Host "started $($broker.Name) on localhost:$($broker.HostPort)"
}
