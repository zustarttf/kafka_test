package com.practice.consumer.sink;

import com.practice.consumer.message.StandardCustomerMessage;
import com.practice.consumer.persistence.StdCustomerMapper;
import com.practice.consumer.persistence.StdCustomerRow;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 메시지 한 건을 표준 테이블에 반영한다.
 * 이 메서드가 정상 반환되면 DB 트랜잭션이 커밋된 상태다.
 */
@Service
@RequiredArgsConstructor
public class CustomerSinkService {

    private final StdCustomerMapper stdCustomerMapper;

    // 이 메서드가 정상 반환되어야 트랜잭션이 커밋된다. 리스너는 그 다음에만 오프셋을 커밋한다.
    @Transactional
    public void upsert(StandardCustomerMessage message) {
        // MyBatis XML이 이 객체의 필드를 std_customer 컬럼에 넣는다. JSON 문자열 날짜는 아래에서 타입을 맞춘다.
        StdCustomerRow row = new StdCustomerRow();
        row.setCustomerId(message.getCustomerId());
        row.setCustomerName(message.getCustomerName());
        row.setMobilePhoneNumber(message.getMobilePhoneNumber());
        row.setEmail(message.getEmail());
        // 퍼블리셔 SQL이 yyyy-MM-dd 문자열로 보냈고, 컬럼은 DATE라서 LocalDate로 바꾼다. 잘못된 생일은 null이다.
        row.setBirthDate(parseDate(message.getBirthDate()));
        row.setGender(message.getGender());
        row.setRoadAddress(message.getRoadAddress());
        // +09:00이 붙은 문자열이다. timestamptz 컬럼에 넣으려고 OffsetDateTime으로 해석한다.
        row.setRegisteredAt(parseDateTime(message.getRegisteredAt()));
        row.setModifiedAt(parseDateTime(message.getModifiedAt()));
        row.setActive(message.isActive());
        row.setSourceSystem(message.getSourceSystem());
        row.setEventType(message.getEventType());
        row.setExtractedAt(parseDateTime(message.getExtractedAt()));
        // customer_id가 있으면 UPDATE, 없으면 INSERT다. 스냅샷을 다시 받아도 행이 늘지 않게 하려는 쿼리다.
        stdCustomerMapper.upsert(row);
    }

    /** 생일이 비어 있으면 null. 형식 오류는 예외로 남겨 이 건의 오프셋 커밋을 막는다. */
    private LocalDate parseDate(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return LocalDate.parse(value);
    }

    /** 시각 문자열이 비어 있으면 null. 파싱 실패는 저장을 롤백하게 예외로 둔다. */
    private OffsetDateTime parseDateTime(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return OffsetDateTime.parse(value);
    }
}
