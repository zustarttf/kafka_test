package com.practice.consumer.listen;

import com.practice.consumer.message.StandardAccountMessage;
import com.practice.consumer.sink.AccountSinkService;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/** 계좌 토픽을 DB에 넣은 뒤 그 오프셋을 커밋한다. */
@Component
@RequiredArgsConstructor
public class AccountSnapshotListener {

    private final AccountSinkService accountSinkService;

    /** 저장이 끝난 뒤에만 acknowledge 한다. 이 토픽의 JSON은 계좌 메시지로만 읽는다. */
    @KafkaListener(
            topics = "${app.kafka.topics.account}",
            properties = "spring.json.value.default.type=com.practice.consumer.message.StandardAccountMessage")
    public void onAccount(ConsumerRecord<String, StandardAccountMessage> record, Acknowledgment acknowledgment) {
        accountSinkService.upsert(record.value());
        acknowledgment.acknowledge();
    }
}
