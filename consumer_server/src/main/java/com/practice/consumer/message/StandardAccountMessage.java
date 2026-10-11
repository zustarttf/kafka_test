package com.practice.consumer.message;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** 퍼블리셔가 보낸 표준 계좌 JSON. */
@Getter
@Setter
@NoArgsConstructor
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
