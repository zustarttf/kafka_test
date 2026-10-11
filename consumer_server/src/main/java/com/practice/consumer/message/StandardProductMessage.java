package com.practice.consumer.message;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** 퍼블리셔가 보낸 표준 상품 JSON. */
@Getter
@Setter
@NoArgsConstructor
public class StandardProductMessage {

    private String productCode;
    private String productName;
    private String productType;
    private boolean onSale;
    private String registeredAt;
    private String sourceSystem;
    private String eventType;
    private String extractedAt;
}
