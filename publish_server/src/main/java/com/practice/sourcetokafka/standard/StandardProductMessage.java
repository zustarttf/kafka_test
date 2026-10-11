package com.practice.sourcetokafka.standard;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** Kafka로 나가는 표준 상품 메시지. 컬럼 변환은 {@code LegacyProductMapper.xml}이 한다. */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
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
