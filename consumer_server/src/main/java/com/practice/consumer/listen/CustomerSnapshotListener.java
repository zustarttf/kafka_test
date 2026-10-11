package com.practice.consumer.listen;

import com.practice.consumer.message.StandardCustomerMessage;
import com.practice.consumer.sink.CustomerSinkService;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/** 표준 고객 토픽을 DB에 넣은 뒤 그 오프셋을 커밋한다. */
@Component
@RequiredArgsConstructor
public class CustomerSnapshotListener {

    private final CustomerSinkService customerSinkService;

    /** 저장이 끝난 뒤에만 acknowledge 한다. 저장 전에 죽으면 같은 레코드를 다시 받는다. */
    @KafkaListener(topics = "${app.kafka.topic}")
    public void onCustomer(ConsumerRecord<String, StandardCustomerMessage> record, Acknowledgment acknowledgment) {
        customerSinkService.upsert(record.value());
        acknowledgment.acknowledge();
    }
}
