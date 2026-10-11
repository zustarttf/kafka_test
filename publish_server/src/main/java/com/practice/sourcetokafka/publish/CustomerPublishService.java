package com.practice.sourcetokafka.publish;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.practice.sourcetokafka.persistence.LegacyCustomerMapper;
import com.practice.sourcetokafka.standard.StandardCustomerMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/** {@code src_cust} 전 건을 Kafka로 보내고, 이번 호출의 성공·실패 건수를 반환한다. */
@Slf4j
@Service
public class CustomerPublishService {

    /** 이 시간 안에 브로커 ack가 없으면 그 행만 실패로 센다. */
    private static final long SEND_TIMEOUT_SECONDS = 10;

    /** JsonSerializer와 같이 필드 이름을 그대로 JSON으로 만든다. */
    private static final ObjectMapper JSON = new ObjectMapper();

    private final LegacyCustomerMapper legacyCustomerMapper;
    private final KafkaTemplate<String, StandardCustomerMessage> kafkaTemplate;

    public CustomerPublishService(
            LegacyCustomerMapper legacyCustomerMapper,
            KafkaTemplate<String, StandardCustomerMessage> kafkaTemplate) {
        this.legacyCustomerMapper = legacyCustomerMapper;
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * 조회한 전 건을 기본 토픽으로 보낸다.
     * ack를 받은 행만 성공으로 세고, 한 건이 실패해도 다음 행은 계속 보낸다.
     */
    public PublishResult publishAll() {
        String topic = kafkaTemplate.getDefaultTopic();
        List<StandardCustomerMessage> messages = legacyCustomerMapper.findAll();
        int published = 0;
        List<String> failedKeys = new ArrayList<>();

        for (StandardCustomerMessage message : messages) {
            try {
                log.info(
                        "전송. 토픽={} 키={} 값={}",
                        topic,
                        message.getCustomerId(),
                        JSON.writeValueAsString(message));
                kafkaTemplate
                        .sendDefault(message.getCustomerId(), message)
                        .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                published++;
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                failedKeys.add(message.getCustomerId());
                log.error("발행이 중단됨. customerId={}", message.getCustomerId(), ex);
                break;
            } catch (Exception ex) {
                failedKeys.add(message.getCustomerId());
                log.error("발행 실패. customerId={}", message.getCustomerId(), ex);
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
