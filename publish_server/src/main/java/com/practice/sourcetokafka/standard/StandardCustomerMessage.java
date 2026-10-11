package com.practice.sourcetokafka.standard;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** Kafka로 나가는 표준 고객 메시지. 컬럼 변환은 {@code LegacyCustomerMapper.xml}이 한다. */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class StandardCustomerMessage {

    private String customerId;
    private String customerName;
    private String mobilePhoneNumber;
    private String email;
    private String birthDate;
    private String gender;
    private String roadAddress;
    private String registeredAt;
    private String modifiedAt;
    private boolean active;
    private String sourceSystem;
    private String eventType;
    private String extractedAt;
}
