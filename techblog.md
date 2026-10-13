# 레거시 고객 테이블을 Kafka로 표준 DB에 옮긴 기록

원천 DB의 컬럼 이름은 `cust_no`, `brdt`처럼 예전 시스템 그대로다. 받는 쪽은 `customerId`, `birthDate` 같은 표준 이름을 쓴다. 두 DB를 직접 연결하지 않고, 가운데에 Kafka 토픽 하나를 두고 발행 서버와 소비 서버를 나눴다.

이 글은 그 실습 프로젝트를 메시지 한 건이 지나가는 순서로 정리한 것이다.

## 무엇을 만들었나

프로세스는 세 덩어리다.

| 구성 | 하는 일 |
| --- | --- |
| `publish_server` | `practice.src_cust`를 읽어 토픽 `std.customer.snapshot.v1`로 보낸다. HTTP 포트는 8080 |
| Kafka 브로커 3대 | 메시지를 저장한다. 내용은 해석하지 않는다 |
| `consumer_server` | 같은 토픽을 읽어 `standard.std_customer`에 넣는다. HTTP는 없다 |

발행은 두 입구가 있다. 앱이 뜨면 `CustomerExtractStartupRunner`가 한 번 보내고, 다시 보내고 싶을 때는 `POST /api/extract/customers`를 호출한다. 둘 다 `CustomerPublishService.publishAll()`만 탄다.

## 왜 가운데에 Kafka를 두었나

원천 서버가 표준 DB에 INSERT를 직접 치면, 표준 DB가 죽거나 스키마가 바뀌는 순간 원천 쪽 발행이 같이 멈춘다. Kafka에 먼저 넣어 두면 발행은 “토픽에 기록했다”에서 끝나고, 적재는 소비 서버가 자기 속도로 한다.

브로커는 메시지를 JSON으로 고쳐 주지 않는다. 받은 바이트를 저장했다가 같은 바이트를 컨슈머에게 넘긴다. 형식은 보내는 쪽과 받는 쪽이 미리 약속한다.

## 발행 서버를 읽는 순서

웹 MVC에 대응하면 컨트롤러, 서비스, 매퍼다.

1. `CustomerExtractStartupRunner` 또는 `CustomerExtractController`  
   입구. 기동 시 자동 발행이거나 HTTP 수동 발행이다.
2. `CustomerPublishService`  
   서비스. 전 건을 조회한 뒤 한 건씩 보낸다.
3. `LegacyCustomerMapper`와 `LegacyCustomerMapper.xml`  
   매퍼. `src_cust`를 읽으면서 컬럼 이름과 값을 표준 형태로 바꾼다.
4. `StandardCustomerMessage`  
   그 조회 결과가 담기는 객체다.

SQL에서 바꾸는 대표적인 값은 이렇다.

- `cust_no` → `customerId`
- `brdt`의 `19900315` → `1990-03-15`. 빈 값이나 없는 날짜는 null
- `sex_cd`의 `1` → `MALE`
- `use_yn`이 `Y`일 때만 `active = true`

자바 서비스는 이 변환을 하지 않는다. 조회가 끝난 시점의 객체는 이미 표준 필드다.

## 객체를 보냈는데 JSON이 되는 이유

`publishAll()`은 `SnapshotSender`가 `kafkaTemplate.send(토픽, 파티션, "src_cust", message)`에 객체를 그대로 넘긴다. 이 메서드 안에는 JSON을 만드는 코드가 없다. 파티션 수는 보내기 전에 브로커에서 조회하고, 행마다 0, 1, 2번으로 돌아간다.

`application.yml`의 프로듀서 설정이 변환기를 지정한다.

```yaml
key-serializer: org.apache.kafka.common.serialization.StringSerializer
value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
```

호출 흐름은 이렇다.

1. `send`는 토픽, 파티션 번호, 키, 객체를 레코드로 포장한다.
2. Kafka 클라이언트 `KafkaProducer.doSend`가 전송 직전에 `valueSerializer.serialize(...)`를 호출한다.
3. `JsonSerializer`가 필드 이름을 JSON 키로 적고, 그 글자를 UTF-8 바이트로 만든다.

이 바이트로 만드는 단계를 직렬화라고 부른다. 이 프로젝트에서의 직렬화는 “객체를 JSON으로 바꾼 뒤, 그 JSON을 전송용 바이트로 쓰는 일”이다. 레코드 키는 JSON이 아니고 테이블명 `src_cust` 문자열이다. 고객번호는 값 안의 `customerId` 필드로만 들어간다.

로그에 찍는 `ObjectMapper.writeValueAsString`은 사람이 보려고 만든 복사본이다. 브로커로 나가는 변환은 `JsonSerializer`가 따로 한다.

토픽에 남는 한 건은 대략 다음 모양이다.

```text
토픽  std.customer.snapshot.v1
파티션  0
키    src_cust
값    {"customerId":"C20240001","customerName":"김민수","birthDate":"1990-03-15","gender":"MALE","active":true, ...}
```

`spring.json.add.type.headers`는 `false`다. 자바 클래스 이름은 헤더에 붙지 않는다. 받는 쪽은 클래스 이름이 아니라 JSON 키 이름으로 필드를 채운다.

## 한 번 호출의 성공 건수

`published`는 서버에 저장되는 누적 카운터가 아니다. `publishAll()`이 도는 동안, 브로커가 저장했다고 응답한 행만 세는 지역 변수다. 메서드가 끝나면 그 변수는 사라지고, 숫자는 `PublishResult`에 복사되어 HTTP 응답이나 기동 로그로 한 번 나간다.

조회한 행 수를 그대로 성공으로 치면, 10초 안에 응답이 없어 실패한 행까지 성공처럼 보인다. 그래서 `send(...).get(10초)`가 예외 없이 끝난 뒤에만 `published++`를 한다. 한 건이 실패해도 다음 고객은 계속 보낸다. 실패한 고객번호는 `failedKeys`에 담아 이번 응답으로 돌려준다. 카프카에 실리는 키는 테이블명이라, 실패 목록의 고객번호와는 다르다.

## 소비 서버를 읽는 순서

역시 컨트롤러, 서비스, 매퍼 순서로 보면 된다. JSON을 객체로 되돌리는 일은 리스너 코드에 없다. 리스너가 호출되기 전에 끝난다.

1. `application.yml`  
   토픽, 컨슈머 그룹 `standard-snapshot-consumer`, 리스너 스레드 3개, `JsonDeserializer`가 있다.
2. `CustomerSnapshotListener`  
   입구. `onCustomer`가 메시지 한 건을 받는다. `record.value()`는 이미 객체다. 파티션 번호는 이 메서드의 인자가 아니다. `concurrency: 3`으로 생긴 스레드 3개 중, 그 파티션을 맡은 스레드가 같은 메서드를 호출한다.
3. `message/StandardCustomerMessage`  
   발행 서버의 클래스와 패키지가 달라도, JSON 키 이름이 같아서 같은 필드에 채워진다.
4. `CustomerSinkService`  
   문자열 날짜를 `LocalDate`, `OffsetDateTime`으로 바꿔 행 객체를 만든다.
5. `StdCustomerRow`  
   `std_customer`에 넣을 한 행이다.
6. `StdCustomerMapper`와 `StdCustomerMapper.xml`  
   `INSERT ... ON CONFLICT (customer_id) DO UPDATE`로 넣거나 갱신한다.

역직렬화는 이 반대다. 컨슈머가 토픽에서 꺼낸 바이트를 JSON 글자로 읽고, 키와 값을 `StandardCustomerMessage` 필드에 넣는다. 설정은 다음이다.

```yaml
value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
spring.json.use.type.headers: false
spring.json.value.default.type: com.practice.consumer.message.StandardCustomerMessage
```

타입 헤더가 없으므로, “이 JSON은 저 클래스로 읽어라”를 yml에 적어 둔다.

## 저장이 끝난 뒤에만 오프셋을 커밋한다

`enable-auto-commit`은 꺼 두었다. `CustomerSinkService.upsert`가 트랜잭션을 커밋한 뒤에만 리스너가 `acknowledgment.acknowledge()`를 호출한다.

저장 전에 프로세스가 죽으면 그 메시지는 커밋되지 않아서 다시 들어온다. 같은 고객이 두 번 들어와도 `customer_id`가 PK이고 upsert라서 행은 한 줄로 유지된다. 이 조합이 이 프로젝트의 at-least-once다. 메시지는 한 번 이상 도착할 수 있고, DB 반영은 같은 키를 덮어써서 결과를 한 행으로 맞춘다.

그룹 이름이 고정되어 있어서, 컨슈머를 재시작하면 마지막으로 커밋한 오프셋 다음부터 읽는다. 그룹에 저장된 위치가 없을 때만 `auto-offset-reset: earliest`가 적용되어 토픽에 남아 있는 첫 메시지부터 읽는다.

## 브로커는 세 대다

`scripts/start-kafka.ps1`은 ZooKeeper 없이 KRaft로 브로커 3대를 띄운다. 한 프로세스가 브로커이면서 컨트롤러다.

새로 만드는 토픽의 복제 수는 3, `min.insync.replicas`는 2다. 발행 서버의 `acks=all`은 최소 2대가 저장을 확인해야 성공이라는 뜻이다. 브로커 한 대가 죽어도 전송은 남는다. 컨트롤러는 세 대 중 과반이 살아 있어야 클러스터가 유지된다.

PC의 앱은 `localhost:9092`, `9094`, `9096`으로 접속한다. 브로커끼리의 복제는 도커 네트워크 안의 `kafka1`, `kafka2`, `kafka3` 이름을 쓴다.

## 이 구성에서 일부러 하지 않은 것

호출할 때마다 `src_cust` 전체를 다시 보낸다. 마지막 전송 시각을 기억해서 변경분만 보내지 않는다. 그래서 같은 고객이 토픽에 여러 번 쌓인다. 중복을 걸러 최종 상태로 맞추는 일은 소비 서버의 upsert가 한다.

Kafka 한 클러스터가 JSON과 ASN.1을 동시에 받을 수는 있다. 다만 브로커가 둘을 하나의 형식으로 통일해 전달하지는 않는다. 형식마다 토픽을 나누고, 컨슈머의 역직렬화 설정도 그 형식에 맞추는 쪽이 이 구조와 같다.

## 한 줄로 다시 보면

```text
src_cust
  → SQL이 표준 필드로 채운 자바 객체
  → JsonSerializer가 만든 JSON 바이트
  → 토픽 std.customer.snapshot.v1
  → JsonDeserializer가 다시 만든 자바 객체
  → std_customer upsert
  → 그 다음에 오프셋 커밋
```

발행 서버의 `send`와 소비 서버의 `onCustomer`만 보면 변환 코드가 없어 보인다. 변환은 그 메서드 안이 아니라, yml에 등록된 `JsonSerializer`와 `JsonDeserializer`가 라이브러리 안에서 한다.

====

## 기본 흐름 다음에 어디를 깊게 쓸까

기능 나열을 더 붙이면 글이 길어질 뿐 깊어지지 않는다. 이 저장소에 이미 있는 설정 하나를 고르고, 그 값이 깨지는 경우를 적어 보면 된다.

순서는 이렇다.

1. 코드나 yml에서 숫자 하나를 집는다. 예를 들면 `acks=all`, `min.insync.replicas=2`, `acknowledge()`의 위치.
2. 브로커가 한 대 죽었을 때, 컨슈머가 저장 직전에 죽었을 때처럼 실패를 하나 가정한다.
3. 그때 메시지가 토픽에 남는지, DB 행이 두 줄이 되는지, 순서가 깨지는지를 결과로 적는다.
4. 그 결과를 만드는 Kafka 장치 이름(ISR, 오프셋 커밋, 파티션)을 그 다음에 붙인다.

아래 세 가지는 그 방식으로, 이 프로젝트를 다시 읽은 것이다.

## acks=all은 “브로커 3대가 모두 응답”이 아니다

발행 서버는 `acks=all`이다. 기동 스크립트는 토픽 기본 복제 수를 3, `min.insync.replicas`를 2로 둔다. 이 세 값을 같이 읽어야 한다.

복제본 3개 가운데 리더와 따라잡은 팔로워를 ISR(in-sync replicas)이라고 한다. `acks=all`은 클러스터 전부가 아니라, 지금 ISR에 들어 있는 복제본이 저장을 확인해야 프로듀서에게 성공을 돌려준다는 설정이다. `min.insync.replicas=2`는 그 ISR이 최소 2대는 되어야 `acks=all` 전송을 받는다는 제한이다.

그래서 경우의 수는 이렇게 갈린다.

| 살아 있는 브로커 | ISR | `acks=all` 전송 |
| --- | --- | --- |
| 3대 | 3 | 성공 |
| 2대 | 2 | 성공. 죽은 1대의 복제본은 빠진다 |
| 1대 | 1 | 실패. 최소 2대를 못 채운다 |

한 대가 죽어도 발행이 되는 이유는 복제가 3이라서가 아니라, 최소 동기 복제가 2라서다. 복제 3은 “사본을 세 장 둔다”이고, 최소 2는 “사본이 두 장 미만이면 성공으로 치지 않는다”이다. 두 대가 죽으면 데이터는 남은 한 대에 있을 수 있지만, 이 설정에서는 새 전송을 거절한다. 성공으로 기록된 메시지가 디스크 한 장에만 있는 상태를 막기 위해서다.

컨트롤러 과반과는 별개다. 컨트롤러 3대 중 2대가 죽으면 리더를 뽑지 못해 클러스터 자체가 멈춘다. 복제 수와 최소 ISR은 토픽에 쓰는 조건이고, 과반은 KRaft가 리더를 유지하는 조건이다.

## 프로듀서 재시도의 중복과, 컨슈머가 다시 읽는 중복은 다르다

Kafka 클라이언트 3.0부터 프로듀서의 `enable.idempotence` 기본값은 true다. 이 프로젝트 yml에는 그 키를 적지 않았으므로 기본값이 켜진다. 조건은 `acks=all`이라 이미 맞다.

이게 막는 중복은 좁다. 프로듀서가 같은 레코드를 보냈는데 응답이 타임아웃나서 다시 보낸 경우, 브로커가 프로듀서 ID와 시퀀스로 “이미 받은 재시도”를 알아채고 토픽에 두 번 붙이지 않는다. `send().get(10초)`가 던진 예외 뒤의 클라이언트 재시도가 이 범위다.

이게 막지 못하는 중복이 이 프로젝트에는 두 개 있다.

첫째, `publishAll()`은 호출마다 `src_cust` 전 건을 새 레코드로 보낸다. 재시도가 아니라 새 메시지다. 멱등 프로듀서는 이 둘을 같은 전송으로 보지 않으므로 토픽에는 같은 고객 JSON이 호출 횟수만큼 쌓인다.

둘째, 컨슈머가 `upsert`까지 하고 `acknowledge()` 전에 죽으면, 커밋되지 않은 오프셋부터 다시 읽는다. 브로커 안의 레코드는 한 개인데 처리가 두 번이다. 프로듀서 멱등과는 층이 다르다.

그래서 DB 쪽 멱등은 SQL에 따로 있다. `std_customer.customer_id`가 PK이고 `ON CONFLICT DO UPDATE`다. 같은 고객 JSON이 두 번 들어와도 행은 한 줄이고, `consumed_at`만 `now()`로 갱신된다. 행 수가 늘지 않는 것으로 “처리가 한 번이었다”를 증명할 수는 없다. 최종 컬럼 값이 마지막 스냅샷과 같다는 것만 보장한다.

이 구성을 exactly-once라고 부르면 안 된다. 토픽 기록은 한 번 이상 남을 수 있고, 컨슈머 처리도 한 번 이상일 수 있다. 결과가 한 행으로 모이는 이유는 Kafka 트랜잭션이 아니라 upsert다.

`ack-mode: manual_immediate`는 `acknowledge()`를 호출한 그 시점에 오프셋 커밋을 보낸다는 뜻이다. 리스너 메서드가 끝난 뒤 모아서 커밋하지 않는다. 커밋 요청 자체는 `__consumer_offsets` 토픽에 기록되고, 기동 스크립트는 그 내부 토픽 복제 수도 3으로 맞춰 두었다. 컨슈머가 재시작할 때 찾는 “어디까지 읽었는가”도 일반 토픽과 같이 복제된 로그다.

## 파티션 3개와 컨슈머 스레드 3개

기동 스크립트의 `KAFKA_NUM_PARTITIONS`는 3이다. 토픽이 이 기본값으로 새로 만들어지면 칸이 3개다. 이미 파티션 1개로 만들어진 토픽은 브로커를 스크립트로 다시 띄우기 전까지 칸이 늘지 않는다.

고객 토픽의 레코드 키는 `src_cust`로 고정한다. 계좌는 `src_acct`, 상품은 `src_prod`다. 키만 고정해서 보내면 해시가 항상 같아서 한 칸으로만 간다. `SnapshotSender`는 보내기 전에 파티션 수를 조회하고, `send(토픽, 파티션, 키, 값)`으로 0, 1, 2번을 행마다 지정한다. 그래서 같은 고객의 다음 스냅샷이 다른 칸에 들어갈 수 있다. Kafka의 순서는 그 칸에 쌓인 줄 안에서만 유지되고, 고객 한 명의 이전·이후는 보장되지 않는다.

컨슈머 그룹은 `standard-snapshot-consumer` 하나다. 그룹 수는 브로커 3대나 토픽 수를 따라가지 않는다. `consumer_server`의 `concurrency: 3`이 리스너마다 스레드 3개를 만든다. 리스너 메서드는 파티션을 고르는 코드가 없다. 스레드가 그룹에 들어가면 브로커가 파티션을 하나씩 배정하고, 각 스레드는 맡은 칸의 리더에게 다음 오프셋을 요청한다. 저장이 끝난 뒤 `acknowledge`는 그 칸의 오프셋만 남긴다.

스레드 3개가 동시에 `upsert`하므로, 서로 다른 칸의 행은 함께 저장된다. 같은 고객이 두 칸으로 나뉘면 나중 스냅샷보다 예전 스냅샷의 저장이 늦게 끝날 수 있다. 컨슈머를 4개로 늘려도 파티션이 3개면 한 스레드는 칸이 없어 쉬고, 처리 속도는 늘지 않는다.
