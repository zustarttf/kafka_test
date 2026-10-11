package com.practice.sourcetokafka.publish;

import java.util.List;
import lombok.Builder;
import lombok.Value;

/**
 * {@link CustomerPublishService#publishAll()} 한 번의 결과.
 *
 * <p>기동 로그와 {@code POST /api/extract/customers} 응답이 같은 값을 쓴다.
 * 일부 행만 실패해도 이 객체가 정상 반환되며, 실패는 {@code failedCount}와 {@code failedKeys}에 담긴다.
 */
@Value
@Builder
public class PublishResult {

    /** 실제로 사용한 토픽. {@code spring.kafka.template.default-topic}. */
    String topic;

    /** {@code src_cust}에서 읽은 행 수. */
    int selectedCount;

    /** 브로커 ack까지 확인된 행 수. */
    int publishedCount;

    /** 변환 또는 전송에 실패한 행 수. */
    int failedCount;

    /** 실패한 행의 원천 키 {@code cust_no}. 성공한 행은 들어가지 않는다. */
    List<String> failedKeys;
}
