package com.practice.consumer.sink;

import com.practice.consumer.message.StandardProductMessage;
import com.practice.consumer.persistence.StdProductMapper;
import com.practice.consumer.persistence.StdProductRow;
import java.time.OffsetDateTime;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** 상품 메시지 한 건을 표준 테이블에 반영한다. 정상 반환이면 DB 트랜잭션이 커밋된 상태다. */
@Service
@RequiredArgsConstructor
public class ProductSinkService {

    private final StdProductMapper stdProductMapper;

    /** 같은 productCode는 새 행을 만들지 않고 갱신한다. */
    @Transactional
    public void upsert(StandardProductMessage message) {
        StdProductRow row = new StdProductRow();
        row.setProductCode(message.getProductCode());
        row.setProductName(message.getProductName());
        row.setProductType(message.getProductType());
        row.setOnSale(message.isOnSale());
        row.setRegisteredAt(parseDateTime(message.getRegisteredAt()));
        row.setSourceSystem(message.getSourceSystem());
        row.setEventType(message.getEventType());
        row.setExtractedAt(parseDateTime(message.getExtractedAt()));
        stdProductMapper.upsert(row);
    }

    /** 비어 있으면 null. 파싱 실패는 저장을 롤백하게 예외로 둔다. */
    private OffsetDateTime parseDateTime(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return OffsetDateTime.parse(value);
    }
}
