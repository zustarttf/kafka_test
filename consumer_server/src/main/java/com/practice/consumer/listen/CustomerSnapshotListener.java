package com.practice.consumer.listen;

import com.practice.consumer.message.StandardCustomerMessage;
import com.practice.consumer.sink.CustomerSinkService;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/** 고객 토픽을 DB에 넣은 뒤 그 오프셋을 커밋한다. */
@Component
@RequiredArgsConstructor
public class CustomerSnapshotListener {

    private final CustomerSinkService customerSinkService;

    /** 저장이 끝난 뒤에만 acknowledge 한다. 이 토픽의 JSON은 고객 메시지로만 읽는다. */
    @KafkaListener(
            topics = "${app.kafka.topics.customer}",
            properties = "spring.json.value.default.type=com.practice.consumer.message.StandardCustomerMessage")
    public void onCustomer(ConsumerRecord<String, StandardCustomerMessage> record, Acknowledgment acknowledgment) {
        customerSinkService.upsert(record.value());
        acknowledgment.acknowledge();
    }
}
