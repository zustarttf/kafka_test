package com.practice.consumer.message;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** 퍼블리셔가 보낸 표준 고객 JSON. */
@Getter
@Setter
@NoArgsConstructor
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
