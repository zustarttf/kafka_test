# 브로커 3대를 docker run 으로 띄운다. Compose 는 쓰지 않는다.
# 복제 3, 최소 동기 복제 2. 브로커 1대가 죽어도 acks=all 전송은 남는다.

$network = 'kafka-net' # 브로커끼리 이름으로 통신할 도커 네트워크
$image = 'apache/kafka:4.1.2' # KRaft 모드(주키퍼 없음) 이미지
$clusterId = '4L6g3nShT-eMCtK--X86sw' # 세 브로커가 같은 클러스터로 붙는 고정 ID
$voters = '1@kafka1:29093,2@kafka2:29093,3@kafka3:29093' # 컨트롤러 투표 멤버. 노드번호@호스트:컨트롤러포트

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
    $dockerArgs = @(
        'run', '-d' # 백그라운드로 실행
        '--name', $broker.Name # 컨테이너 이름
        '--hostname', $broker.Name # 다른 브로커가 찾는 이름
        '--network', $network
        '-p', "$($broker.HostPort):9092" # PC의 9092/9094/9096 → 컨테이너 9092
        '-e', "KAFKA_NODE_ID=$($broker.Id)" # 클러스터 안에서의 이 브로커 번호
        '-e', 'KAFKA_PROCESS_ROLES=broker,controller' # 한 프로세스가 메시지 브로커이면서 KRaft 컨트롤러
        '-e', 'KAFKA_LISTENERS=CONTROLLER://:29093,INTERNAL://:19092,EXTERNAL://:9092' # 컨트롤러 29093, 브로커끼리 19092, 바깥 클라이언트 9092
        '-e', "KAFKA_ADVERTISED_LISTENERS=INTERNAL://$($broker.Name):19092,EXTERNAL://localhost:$($broker.HostPort)" # 다른 브로커는 컨테이너 이름, PC의 앱은 localhost 포트로 접속
        '-e', 'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT' # 세 리스너 모두 암호 없는 PLAINTEXT
        '-e', 'KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL' # 복제 트래픽은 INTERNAL 리스너
        '-e', 'KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER' # 리더 선출은 CONTROLLER 리스너
        '-e', "KAFKA_CONTROLLER_QUORUM_VOTERS=$voters" # 과반(2대)이 살아 있어야 클러스터가 유지된다
        '-e', 'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3' # 컨슈머 오프셋 토픽을 3대에 복제
        '-e', 'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=3' # 트랜잭션 상태 로그도 3대에 복제
        '-e', 'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=2' # 트랜잭션 기록은 동기 복제 2대 이상이 있어야 성공
        '-e', 'KAFKA_DEFAULT_REPLICATION_FACTOR=3' # 새로 만드는 토픽의 기본 복제 수
        '-e', 'KAFKA_MIN_INSYNC_REPLICAS=2' # acks=all 일 때 최소 2대가 저장을 확인해야 성공
        '-e', 'KAFKA_NUM_PARTITIONS=1' # 토픽을 자동 생성할 때의 파티션 수
        '-e', 'KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR=3' # Kafka 4 공유 컨슈머 그룹용 내부 토픽 복제 수
        '-e', 'KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR=2'
        '-e', 'KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0' # 컨슈머가 붙으면 리밸런스를 기다리지 않고 바로 시작
        '-e', "CLUSTER_ID=$clusterId" # 세 컨테이너가 같은 KRaft 클러스터라는 표시
        $image
    )
    docker @dockerArgs | Out-Null
    Write-Host "started $($broker.Name) on localhost:$($broker.HostPort)"
}
