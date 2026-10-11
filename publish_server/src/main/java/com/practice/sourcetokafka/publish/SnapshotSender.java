package com.practice.sourcetokafka.publish;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** 토픽 하나에 스냅샷 목록을 보낸다. 테이블마다 이 클래스를 다시 쓰지 않는다. */
@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotSender {

    /** 이 시간 안에 브로커 ack가 없으면 그 행만 실패로 센다. */
    private static final long SEND_TIMEOUT_SECONDS = 10;

    /** JsonSerializer와 같이 필드 이름을 그대로 JSON으로 만든다. */
    private static final ObjectMapper JSON = new ObjectMapper();

    private final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * ack를 받은 행만 성공으로 세고, 한 건이 실패해도 다음 행은 계속 보낸다.
     * 키는 그 테이블의 PK다. 같은 키는 같은 파티션에 쌓인다.
     */
    public <T> PublishResult publish(String topic, List<T> messages, Function<T, String> keyOf) {
        int published = 0;
        List<String> failedKeys = new ArrayList<>();

        for (T message : messages) {
            String key = keyOf.apply(message);
            try {
                log.info("전송. 토픽={} 키={} 값={}", topic, key, JSON.writeValueAsString(message));
                kafkaTemplate.send(topic, key, message).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                published++;
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                failedKeys.add(key);
                log.error("발행이 중단됨. topic={} key={}", topic, key, ex);
                break;
            } catch (Exception ex) {
                failedKeys.add(key);
                log.error("발행 실패. topic={} key={}", topic, key, ex);
            }
        }

        return PublishResult.builder()
                .topic(topic)
                .selectedCount(messages.size())
                .publishedCount(published)
                .failedCount(failedKeys.size())
                .failedKeys(List.copyOf(failedKeys))
                .build();
    }
}
