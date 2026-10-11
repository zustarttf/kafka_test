package com.practice.consumer.listen;

import com.practice.consumer.message.StandardProductMessage;
import com.practice.consumer.sink.ProductSinkService;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/** 상품 토픽을 DB에 넣은 뒤 그 오프셋을 커밋한다. */
@Component
@RequiredArgsConstructor
public class ProductSnapshotListener {

    private final ProductSinkService productSinkService;

    /** 저장이 끝난 뒤에만 acknowledge 한다. 이 토픽의 JSON은 상품 메시지로만 읽는다. */
    @KafkaListener(
            topics = "${app.kafka.topics.product}",
            properties = "spring.json.value.default.type=com.practice.consumer.message.StandardProductMessage")
    public void onProduct(ConsumerRecord<String, StandardProductMessage> record, Acknowledgment acknowledgment) {
        productSinkService.upsert(record.value());
        acknowledgment.acknowledge();
    }
}
