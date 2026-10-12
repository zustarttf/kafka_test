package com.practice.sourcetokafka.publish;

import java.util.List;
import lombok.Builder;
import lombok.Value;

/** 스냅샷 발행 한 번의 결과. */
@Value
@Builder
public class PublishResult {

    /** 사용한 토픽. */
    String topic;

    /** 조회한 행 수. */
    int selectedCount;

    /** 브로커 ack까지 확인된 행 수. */
    int publishedCount;

    /** 전송에 실패한 행 수. */
    int failedCount;

    /** 실패한 행의 아이디. 카프카 키는 테이블명으로 고정되어 있다. */
    List<String> failedKeys;
}
