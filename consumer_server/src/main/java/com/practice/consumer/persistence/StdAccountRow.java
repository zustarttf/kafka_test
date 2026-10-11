package com.practice.consumer.persistence;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** {@code std_account}에 넣는 한 행. */
@Getter
@Setter
@NoArgsConstructor
public class StdAccountRow {

    private String accountId;
    private String customerId;
    private String accountType;
    private BigDecimal balance;
    private OffsetDateTime openedAt;
    private boolean active;
    private String sourceSystem;
    private String eventType;
    private OffsetDateTime extractedAt;
}
