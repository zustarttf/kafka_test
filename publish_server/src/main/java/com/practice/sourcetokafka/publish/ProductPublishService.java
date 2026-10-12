package com.practice.sourcetokafka.publish;

import com.practice.sourcetokafka.persistence.LegacyProductMapper;
import com.practice.sourcetokafka.standard.StandardProductMessage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/** {@code src_prod} 전 건을 상품 토픽으로 보낸다. */
@Service
public class ProductPublishService {

    private final LegacyProductMapper legacyProductMapper;
    private final SnapshotSender snapshotSender;
    private final String topic;

    public ProductPublishService(
            LegacyProductMapper legacyProductMapper,
            SnapshotSender snapshotSender,
            @Value("${app.kafka.topics.product}") String topic) {
        this.legacyProductMapper = legacyProductMapper;
        this.snapshotSender = snapshotSender;
        this.topic = topic;
    }

    /** 조회한 전 건을 {@code std.product.snapshot.v1}로 보낸다. 키는 {@code src_prod}로 고정한다. */
    public PublishResult publishAll() {
        return snapshotSender.publish(topic, legacyProductMapper.findAll(), "src_prod", StandardProductMessage::getProductCode);
    }
}
