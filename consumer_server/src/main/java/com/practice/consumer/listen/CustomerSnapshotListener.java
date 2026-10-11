package com.practice.consumer.listen;

import com.practice.consumer.message.StandardCustomerMessage;
import com.practice.consumer.sink.CustomerSinkService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * 표준 고객 토픽을 폴링해서 DB에 넣은 뒤, 그 오프셋을 커밋한다.
 *
 * <p>자동 커밋은 꺼져 있다. DB 반영이 끝난 뒤에만 {@code acknowledge} 하므로,
 * 저장 전에 프로세스가 죽으면 같은 레코드를 다시 받는다.
 * 그룹 이름은 {@code customer-snapshot-consumer}로 고정되어 있어 재기동 시 이어서 읽는다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class CustomerSnapshotListener {

    private final CustomerSinkService customerSinkService;

    // app.kafka.topic 한 개만 구독한다. 그룹 customer-snapshot-consumer의 커밋된 다음 오프셋부터 폴링한다.
    @KafkaListener(topics = "${app.kafka.topic}")
    public void onCustomer(ConsumerRecord<String, StandardCustomerMessage> record, Acknowledgment acknowledgment) {
        // 레코드의 값이 퍼블리셔 JSON이다. 키는 고객번호이고, 여기 저장에는 값 안의 customerId를 쓴다.
        StandardCustomerMessage message = record.value();
        log.info(
                "[consumer] 카프카에서 수신. topic={} partition={} offset={} key={} data={}",
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                describe(message));
        // 트랜잭션이 커밋된 뒤에만 이 호출이 반환된다. 저장 예외가 나면 아래 acknowledge까지 가지 않는다.
        customerSinkService.upsert(message);
        log.info(
                "[consumer] standard.std_customer 저장. customerId={} customerName={}",
                message.getCustomerId(),
                message.getCustomerName());
        // 자동 커밋이 꺼져 있어서, 이 호출이 있어야 이 오프셋을 처리 완료로 브로커에 남긴다.
        // 그 전에 프로세스가 죽으면 같은 레코드를 다시 받는다.
        acknowledgment.acknowledge();
        // 다음에 읽을 위치는 방금 처리한 오프셋의 다음 번호다. 커밋 자체가 그 위치를 저장한다.
        log.info(
                "[consumer] 오프셋 커밋. topic={} partition={} nextOffset={}",
                record.topic(),
                record.partition(),
                record.offset() + 1);
    }

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
