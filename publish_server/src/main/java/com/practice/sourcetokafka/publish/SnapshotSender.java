package com.practice.sourcetokafka.publish;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.PartitionInfo;
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

    /** 토픽마다 다음에 넣을 파티션. 키와 무관하게 돌아가며 나눈다. */
    private final ConcurrentMap<String, AtomicInteger> nextPartition = new ConcurrentHashMap<>();

    /**
     * ack를 받은 행만 성공으로 세고, 한 건이 실패해도 다음 행은 계속 보낸다.
     * 키는 테이블명으로 고정하고, 파티션은 조회한 개수로 돌아가며 넣는다.
     */
    public <T> PublishResult publish(String topic, List<T> messages, String tableKey, Function<T, String> rowIdOf) {
        int published = 0;
        List<String> failedKeys = new ArrayList<>();
        int partitionCount = partitionCount(topic, messages, rowIdOf, failedKeys);
        if (partitionCount == 0) {
            return result(topic, messages.size(), published, failedKeys);
        }

        for (T message : messages) {
            String rowId = rowIdOf.apply(message);
            int partition = Math.floorMod(nextPartition.computeIfAbsent(topic, ignored -> new AtomicInteger()).getAndIncrement(), partitionCount);
            try {
                log.info("전송. 토픽={} 파티션={} 키={} 값={}", topic, partition, tableKey, JSON.writeValueAsString(message));
                kafkaTemplate.send(topic, partition, tableKey, message).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                published++;
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                failedKeys.add(rowId);
                log.error("발행이 중단됨. topic={} partition={} key={} rowId={}", topic, partition, tableKey, rowId, ex);
                break;
            } catch (Exception ex) {
                failedKeys.add(rowId);
                log.error("발행 실패. topic={} partition={} key={} rowId={}", topic, partition, tableKey, rowId, ex);
            }
        }

        return result(topic, messages.size(), published, failedKeys);
    }

    /** 파티션 수를 못 얻으면 남은 행을 실패로 남기고 0을 반환한다. */
    private <T> int partitionCount(String topic, List<T> messages, Function<T, String> rowIdOf, List<String> failedKeys) {
        if (messages.isEmpty()) {
            return 0;
        }
        try {
            List<PartitionInfo> partitions = kafkaTemplate.partitionsFor(topic);
            if (partitions == null || partitions.isEmpty()) {
                messages.forEach(message -> failedKeys.add(rowIdOf.apply(message)));
                log.error("파티션이 없음. topic={}", topic);
                return 0;
            }
            return partitions.size();
        } catch (Exception ex) {
            messages.forEach(message -> failedKeys.add(rowIdOf.apply(message)));
            log.error("파티션 조회 실패. topic={}", topic, ex);
            return 0;
        }
    }

    private PublishResult result(String topic, int selectedCount, int published, List<String> failedKeys) {
        return PublishResult.builder()
                .topic(topic)
                .selectedCount(selectedCount)
                .publishedCount(published)
                .failedCount(failedKeys.size())
                .failedKeys(List.copyOf(failedKeys))
                .build();
    }
}
