package com.practice.sourcetokafka.publish;

import com.practice.sourcetokafka.persistence.LegacyAccountMapper;
import com.practice.sourcetokafka.standard.StandardAccountMessage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/** {@code src_acct} 전 건을 계좌 토픽으로 보낸다. */
@Service
public class AccountPublishService {

    private final LegacyAccountMapper legacyAccountMapper;
    private final SnapshotSender snapshotSender;
    private final String topic;

    public AccountPublishService(
            LegacyAccountMapper legacyAccountMapper,
            SnapshotSender snapshotSender,
            @Value("${app.kafka.topics.account}") String topic) {
        this.legacyAccountMapper = legacyAccountMapper;
        this.snapshotSender = snapshotSender;
        this.topic = topic;
    }

    /** 조회한 전 건을 {@code std.account.snapshot.v1}로 보낸다. 키는 {@code src_acct}로 고정한다. */
    public PublishResult publishAll() {
        return snapshotSender.publish(topic, legacyAccountMapper.findAll(), "src_acct", StandardAccountMessage::getAccountId);
    }
}
