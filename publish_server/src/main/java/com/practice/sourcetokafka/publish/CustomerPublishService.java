package com.practice.sourcetokafka.publish;

import com.practice.sourcetokafka.persistence.LegacyCustomerMapper;
import com.practice.sourcetokafka.standard.StandardCustomerMessage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/** {@code src_cust} 전 건을 고객 토픽으로 보낸다. */
@Service
public class CustomerPublishService {

    private final LegacyCustomerMapper legacyCustomerMapper;
    private final SnapshotSender snapshotSender;
    private final String topic;

    public CustomerPublishService(
            LegacyCustomerMapper legacyCustomerMapper,
            SnapshotSender snapshotSender,
            @Value("${app.kafka.topics.customer}") String topic) {
        this.legacyCustomerMapper = legacyCustomerMapper;
        this.snapshotSender = snapshotSender;
        this.topic = topic;
    }

    /** 조회한 전 건을 {@code std.customer.snapshot.v1}로 보낸다. 키는 customerId다. */
    public PublishResult publishAll() {
        return snapshotSender.publish(topic, legacyCustomerMapper.findAll(), StandardCustomerMessage::getCustomerId);
    }
}
