package com.practice.sourcetokafka.publish;

import java.util.List;
import lombok.Builder;
import lombok.Value;

/** {@link CustomerPublishService#publishAll()} 한 번의 결과. */
@Value
@Builder
public class PublishResult {

    /** 사용한 토픽. */
    String topic;

    /** {@code src_cust}에서 읽은 행 수. */
    int selectedCount;

    /** 브로커 ack까지 확인된 행 수. */
    int publishedCount;

    /** 전송에 실패한 행 수. */
    int failedCount;

    /** 실패한 행의 customerId. */
    List<String> failedKeys;
}
