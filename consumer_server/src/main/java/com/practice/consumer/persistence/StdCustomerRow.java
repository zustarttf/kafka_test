package com.practice.consumer.persistence;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** {@code std_customer}에 넣는 한 행. */
@Getter
@Setter
@NoArgsConstructor
public class StdCustomerRow {

    private String customerId;
    private String customerName;
    private String mobilePhoneNumber;
    private String email;
    private LocalDate birthDate;
    private String gender;
    private String roadAddress;
    private OffsetDateTime registeredAt;
    private OffsetDateTime modifiedAt;
    private boolean active;
    private String sourceSystem;
    private String eventType;
    private OffsetDateTime extractedAt;
}
