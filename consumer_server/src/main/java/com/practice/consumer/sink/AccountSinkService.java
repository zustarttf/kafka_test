package com.practice.consumer.sink;

import com.practice.consumer.message.StandardAccountMessage;
import com.practice.consumer.persistence.StdAccountMapper;
import com.practice.consumer.persistence.StdAccountRow;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** 계좌 메시지 한 건을 표준 테이블에 반영한다. 정상 반환이면 DB 트랜잭션이 커밋된 상태다. */
@Service
@RequiredArgsConstructor
public class AccountSinkService {

    private final StdAccountMapper stdAccountMapper;

    /** 같은 accountId는 새 행을 만들지 않고 갱신한다. */
    @Transactional
    public void upsert(StandardAccountMessage message) {
        StdAccountRow row = new StdAccountRow();
        row.setAccountId(message.getAccountId());
        row.setCustomerId(message.getCustomerId());
        row.setAccountType(message.getAccountType());
        row.setBalance(new BigDecimal(message.getBalance()));
        row.setOpenedAt(parseDateTime(message.getOpenedAt()));
        row.setActive(message.isActive());
        row.setSourceSystem(message.getSourceSystem());
        row.setEventType(message.getEventType());
        row.setExtractedAt(parseDateTime(message.getExtractedAt()));
        stdAccountMapper.upsert(row);
    }

    /** 비어 있으면 null. 파싱 실패는 저장을 롤백하게 예외로 둔다. */
    private OffsetDateTime parseDateTime(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return OffsetDateTime.parse(value);
    }
}
