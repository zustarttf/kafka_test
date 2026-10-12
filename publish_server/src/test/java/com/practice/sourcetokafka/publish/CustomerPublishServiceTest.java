package com.practice.sourcetokafka.publish;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.practice.sourcetokafka.persistence.LegacyCustomerMapper;
import com.practice.sourcetokafka.standard.StandardCustomerMessage;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

@ExtendWith(MockitoExtension.class)
class CustomerPublishServiceTest {

    private static final String TOPIC = "std.customer.snapshot.v1";

    @Mock
    private LegacyCustomerMapper legacyCustomerMapper;

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    private CustomerPublishService customerPublishService;

    @BeforeEach
    void setUp() {
        customerPublishService = new CustomerPublishService(legacyCustomerMapper, new SnapshotSender(kafkaTemplate), TOPIC);
    }

    @Test
    void publishesEachRowRoundRobinWithTableKey() {
        StandardCustomerMessage first = message("C001");
        when(legacyCustomerMapper.findAll()).thenReturn(List.of(first, message("C002")));
        when(kafkaTemplate.partitionsFor(TOPIC)).thenReturn(threePartitions());
        when(kafkaTemplate.send(eq(TOPIC), anyInt(), eq("src_cust"), any())).thenReturn(CompletableFuture.completedFuture(null));

        PublishResult result = customerPublishService.publishAll();

        assertThat(result.getTopic()).isEqualTo(TOPIC);
        assertThat(result.getSelectedCount()).isEqualTo(2);
        assertThat(result.getPublishedCount()).isEqualTo(2);
        assertThat(result.getFailedCount()).isZero();
        assertThat(result.getFailedKeys()).isEmpty();
        verify(kafkaTemplate).send(TOPIC, 0, "src_cust", first);
        verify(kafkaTemplate).send(eq(TOPIC), eq(1), eq("src_cust"), any(StandardCustomerMessage.class));
    }

    @Test
    void continuesWhenOneSendFails() {
        when(legacyCustomerMapper.findAll()).thenReturn(List.of(message("C001"), message("C002"), message("C003")));
        when(kafkaTemplate.partitionsFor(TOPIC)).thenReturn(threePartitions());
        when(kafkaTemplate.send(eq(TOPIC), eq(0), eq("src_cust"), any())).thenReturn(failedSend());
        when(kafkaTemplate.send(eq(TOPIC), eq(1), eq("src_cust"), any())).thenReturn(CompletableFuture.completedFuture(null));
        when(kafkaTemplate.send(eq(TOPIC), eq(2), eq("src_cust"), any())).thenReturn(failedSend());

        PublishResult result = customerPublishService.publishAll();

        assertThat(result.getSelectedCount()).isEqualTo(3);
        assertThat(result.getPublishedCount()).isEqualTo(1);
        assertThat(result.getFailedCount()).isEqualTo(2);
        assertThat(result.getFailedKeys()).containsExactly("C001", "C003");
        verify(kafkaTemplate).send(eq(TOPIC), eq(1), eq("src_cust"), any(StandardCustomerMessage.class));
    }

    private List<PartitionInfo> threePartitions() {
        return List.of(
                new PartitionInfo(TOPIC, 0, null, new Node[0], new Node[0]),
                new PartitionInfo(TOPIC, 1, null, new Node[0], new Node[0]),
                new PartitionInfo(TOPIC, 2, null, new Node[0], new Node[0]));
    }

    private CompletableFuture<SendResult<String, Object>> failedSend() {
        CompletableFuture<SendResult<String, Object>> future = new CompletableFuture<>();
        future.completeExceptionally(new RuntimeException("broker down"));
        return future;
    }

    private StandardCustomerMessage message(String customerId) {
        return StandardCustomerMessage.builder()
                .customerId(customerId)
                .customerName("고객")
                .gender("MALE")
                .birthDate("1990-03-15")
                .active(true)
                .build();
    }
}
