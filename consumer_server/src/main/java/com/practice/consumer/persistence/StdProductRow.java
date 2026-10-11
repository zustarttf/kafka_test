package com.practice.consumer.persistence;

import java.time.OffsetDateTime;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** {@code std_product}에 넣는 한 행. */
@Getter
@Setter
@NoArgsConstructor
public class StdProductRow {

    private String productCode;
    private String productName;
    private String productType;
    private boolean onSale;
    private OffsetDateTime registeredAt;
    private String sourceSystem;
    private String eventType;
    private OffsetDateTime extractedAt;
}
