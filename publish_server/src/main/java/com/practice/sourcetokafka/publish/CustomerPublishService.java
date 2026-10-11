package com.practice.sourcetokafka.publish;

import com.practice.sourcetokafka.persistence.LegacyCustomerMapper;
import com.practice.sourcetokafka.standard.StandardCustomerMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

/**
 * 표준 메시지로 조회한 행을 Kafka에 넣는 중심 흐름.
 *
 * <p>기동 러너와 HTTP 트리거가 모두 이 클래스의 {@link #publishAll()}만 호출한다.
 * 컬럼 변환은 {@code LegacyCustomerMapper.xml}의 SELECT가 한다.
 * 증분 조회는 하지 않고, 호출할 때마다 {@code src_cust} 전체를 다시 보낸다.
 */
@Slf4j
@Service
public class CustomerPublishService {

    /** 브로커 ack를 기다리는 시간. 이 시간을 넘기면 그 행은 실패로 센다. */
    private static final long SEND_TIMEOUT_SECONDS = 10;

    private final LegacyCustomerMapper legacyCustomerMapper;
    private final KafkaTemplate<String, StandardCustomerMessage> kafkaTemplate;

    /**
     * 조회와 전송에 필요한 협력 객체를 받는다.
     *
     * <p>{@code KafkaTemplate}의 기본 토픽은 {@code spring.kafka.template.default-topic}이다.
     * 토픽 이름을 이 생성자에서 따로 받지 않는 이유는 {@code sendDefault}가 그 설정을 사용하기 때문이다.
     */
    public CustomerPublishService(
            LegacyCustomerMapper legacyCustomerMapper,
            KafkaTemplate<String, StandardCustomerMessage> kafkaTemplate) {
        // 조회는 매퍼, 전송은 템플릿으로 고정한다. 토픽 이름은 템플릿 설정에 들어 있어 여기서 보관하지 않는다.
        this.legacyCustomerMapper = legacyCustomerMapper;
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * 테이블 전 건을 표준 메시지로 조회해 기본 토픽으로 발행한다.
     *
     * <p>흐름은 다음과 같다.
     * <ol>
     *   <li>기본 토픽 이름을 읽는다. yml의 {@code default-topic}이다.</li>
     *   <li>{@code findAll}로 표준 컬럼이 채워진 {@code src_cust} 전 건을 가져온다. 여기서 실패하면 예외가 호출자에게 올라간다.</li>
     *   <li>{@code sendDefault(customerId, message)}로 보낸다.
     *       키가 {@code customerId}라서 같은 고객은 같은 파티션에 쌓인다.
     *       이 호출이 브로커 메타데이터 조회와 저장 ack를 겸한다. 토픽이 없으면 브로커가 이때 만든다.</li>
     *   <li>{@code get(10초)}로 ack를 확인한 뒤에만 성공 건수를 올린다.</li>
     * </ol>
     * 한 행의 전송 실패는 나머지 행을 막지 않는다.
     * 스레드가 인터럽트된 경우에만 남은 행을 중단한다.
     */
    public PublishResult publishAll() {
        // yml의 spring.kafka.template.default-topic을 읽는다.
        // sendDefault가 이 이름을 토픽으로 쓰므로, 로그와 응답에 같은 이름을 남기려고 전송 전에 한 번 꺼낸다.
        String topic = kafkaTemplate.getDefaultTopic();
        // LegacyCustomerMapper.xml의 SELECT가 레거시 컬럼을 표준 필드로 바꿔 전 건을 가져온다.
        // 여기서 실패하면 한 건도 보내지 않고 기동 러너나 HTTP 호출자까지 예외가 올라간다.
        List<StandardCustomerMessage> messages = legacyCustomerMapper.findAll();
        // 브로커 ack가 확인된 행만 센다. 조회 건수와 달라질 수 있다.
        int published = 0;
        // 전송에 실패한 cust_no를 모은다. 응답에서 어떤 고객이 빠졌는지 구분하려고 둔다.
        List<String> failedKeys = new ArrayList<>();
        log.info("[publisher] practice.src_cust 조회 완료. topic={} count={}", topic, messages.size());

        for (StandardCustomerMessage message : messages) {
            try {
                log.info(
                        "[publisher] 카프카로 전송. topic={} key={} data={}",
                        topic,
                        message.getCustomerId(),
                        describe(message));
                // 키는 customerId다. 같은 고객은 같은 파티션에 순서대로 쌓이게 하려고 값의 고객번호를 키로 쓴다.
                // get은 ack가 올 때까지 이 스레드를 최대 10초 기다린다. 그 전에 published를 올리지 않는다.
                // 토픽이 없으면 브로커가 이 첫 전송 때 기본 설정으로 만든다.
                SendResult<String, StandardCustomerMessage> sent = kafkaTemplate
                        .sendDefault(message.getCustomerId(), message)
                        .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                published++;
                // 단위 테스트의 mock은 결과가 null일 수 있다. 실제 전송은 파티션과 오프셋이 들어 있다.
                RecordMetadata metadata = sent == null ? null : sent.getRecordMetadata();
                if (metadata == null) {
                    log.info("[publisher] 카프카 저장 응답. topic={} key={}", topic, message.getCustomerId());
                } else {
                    // 브로커가 배정한 파티션과 오프셋이다. 이 값이 그 레코드의 저장 위치다.
                    log.info(
                            "[publisher] 카프카 저장 응답. topic={} partition={} offset={} key={}",
                            metadata.topic(),
                            metadata.partition(),
                            metadata.offset(),
                            message.getCustomerId());
                }
            } catch (InterruptedException ex) {
                // 중단 요청 플래그를 다시 세운다. 지우면 이 스레드를 멈추려던 신호가 사라진다.
                Thread.currentThread().interrupt();
                failedKeys.add(message.getCustomerId());
                log.error("Interrupted while publishing customerId={}", message.getCustomerId(), ex);
                // ack를 못 받은 상태에서 프로세스가 멈추는 중이므로 남은 고객은 보내지 않는다.
                break;
            } catch (Exception ex) {
                // 타임아웃이나 브로커 오류는 이 행만 실패로 남긴다. 다음 고객 전송은 계속한다.
                failedKeys.add(message.getCustomerId());
                log.error("Failed to publish customerId={}", message.getCustomerId(), ex);
            }
        }

        // 조회 수, ack 수, 실패 키를 한 객체로 묶는다. 기동 로그와 HTTP 응답이 같은 값을 쓴다.
        // copyOf로 목록을 고정해 호출자가 실패 키를 나중에 바꾸지 못하게 한다.
        PublishResult result = PublishResult.builder()
                .topic(topic)
                .selectedCount(messages.size())
                .publishedCount(published)
                .failedCount(failedKeys.size())
                .failedKeys(List.copyOf(failedKeys))
                .build();
        log.info(
                "Extract finished. topic={} selected={} published={} failed={}",
                result.getTopic(),
                result.getSelectedCount(),
                result.getPublishedCount(),
                result.getFailedCount());
        return result;
    }

    /** 로그에 실릴 표준 필드 한 줄. JSON 원문이 아니라 사람이 읽는 형태다. */
    private String describe(StandardCustomerMessage message) {
        return "customerId=" + message.getCustomerId()
                + ", customerName=" + message.getCustomerName()
                + ", mobilePhoneNumber=" + message.getMobilePhoneNumber()
                + ", email=" + message.getEmail()
                + ", birthDate=" + message.getBirthDate()
                + ", gender=" + message.getGender()
                + ", roadAddress=" + message.getRoadAddress()
                + ", registeredAt=" + message.getRegisteredAt()
                + ", modifiedAt=" + message.getModifiedAt()
                + ", active=" + message.isActive()
                + ", sourceSystem=" + message.getSourceSystem()
                + ", eventType=" + message.getEventType()
                + ", extractedAt=" + message.getExtractedAt();
    }
}
