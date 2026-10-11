package com.practice.sourcetokafka.standard;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** Kafka로 나가는 표준 계좌 메시지. 컬럼 변환은 {@code LegacyAccountMapper.xml}이 한다. */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class StandardAccountMessage {

    private String accountId;
    private String customerId;
    private String accountType;
    private String balance;
    private String openedAt;
    private boolean active;
    private String sourceSystem;
    private String eventType;
    private String extractedAt;
}
