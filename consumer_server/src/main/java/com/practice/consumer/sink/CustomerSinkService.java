package com.practice.consumer.sink;

import com.practice.consumer.message.StandardCustomerMessage;
import com.practice.consumer.persistence.StdCustomerMapper;
import com.practice.consumer.persistence.StdCustomerRow;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** 메시지 한 건을 표준 테이블에 반영한다. 정상 반환이면 DB 트랜잭션이 커밋된 상태다. */
@Service
@RequiredArgsConstructor
public class CustomerSinkService {

    private final StdCustomerMapper stdCustomerMapper;

    /** 같은 customerId는 새 행을 만들지 않고 갱신한다. */
    @Transactional
    public void upsert(StandardCustomerMessage message) {
        StdCustomerRow row = new StdCustomerRow();
        row.setCustomerId(message.getCustomerId());
        row.setCustomerName(message.getCustomerName());
        row.setMobilePhoneNumber(message.getMobilePhoneNumber());
        row.setEmail(message.getEmail());
        row.setBirthDate(parseDate(message.getBirthDate()));
        row.setGender(message.getGender());
        row.setRoadAddress(message.getRoadAddress());
        row.setRegisteredAt(parseDateTime(message.getRegisteredAt()));
        row.setModifiedAt(parseDateTime(message.getModifiedAt()));
        row.setActive(message.isActive());
        row.setSourceSystem(message.getSourceSystem());
        row.setEventType(message.getEventType());
        row.setExtractedAt(parseDateTime(message.getExtractedAt()));
        stdCustomerMapper.upsert(row);
    }

    /** 비어 있으면 null. 형식이 아니면 예외를 내 이 건의 커밋을 막는다. */
    private LocalDate parseDate(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return LocalDate.parse(value);
    }

    /** 비어 있으면 null. 파싱 실패는 저장을 롤백하게 예외로 둔다. */
    private OffsetDateTime parseDateTime(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return OffsetDateTime.parse(value);
    }
}
