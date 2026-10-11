package com.practice.sourcetokafka.publish;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.practice.sourcetokafka.persistence.LegacyCustomerMapper;
import com.practice.sourcetokafka.standard.StandardCustomerMessage;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
    private KafkaTemplate<String, StandardCustomerMessage> kafkaTemplate;

    private CustomerPublishService customerPublishService;

    @BeforeEach
    void setUp() {
        customerPublishService = new CustomerPublishService(legacyCustomerMapper, kafkaTemplate);
        when(kafkaTemplate.getDefaultTopic()).thenReturn(TOPIC);
    }

    @Test
    void publishesEachRowWithCustomerIdAsKey() {
        StandardCustomerMessage first = message("C001");
        when(legacyCustomerMapper.findAll()).thenReturn(List.of(first, message("C002")));
        when(kafkaTemplate.sendDefault(any(), any(StandardCustomerMessage.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        PublishResult result = customerPublishService.publishAll();

        assertThat(result.getTopic()).isEqualTo(TOPIC);
        assertThat(result.getSelectedCount()).isEqualTo(2);
        assertThat(result.getPublishedCount()).isEqualTo(2);
        assertThat(result.getFailedCount()).isZero();
        assertThat(result.getFailedKeys()).isEmpty();
        verify(kafkaTemplate).sendDefault("C001", first);
        verify(kafkaTemplate).sendDefault(eq("C002"), any(StandardCustomerMessage.class));
    }

    @Test
    void continuesWhenOneSendFails() {
        when(legacyCustomerMapper.findAll()).thenReturn(List.of(message("C001"), message("C002"), message("C003")));
        when(kafkaTemplate.sendDefault(eq("C001"), any(StandardCustomerMessage.class))).thenReturn(failedSend());
        when(kafkaTemplate.sendDefault(eq("C002"), any(StandardCustomerMessage.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(kafkaTemplate.sendDefault(eq("C003"), any(StandardCustomerMessage.class))).thenReturn(failedSend());

        PublishResult result = customerPublishService.publishAll();

        assertThat(result.getSelectedCount()).isEqualTo(3);
        assertThat(result.getPublishedCount()).isEqualTo(1);
        assertThat(result.getFailedCount()).isEqualTo(2);
        assertThat(result.getFailedKeys()).containsExactly("C001", "C003");
        verify(kafkaTemplate).sendDefault(eq("C002"), any(StandardCustomerMessage.class));
    }

    private CompletableFuture<SendResult<String, StandardCustomerMessage>> failedSend() {
        CompletableFuture<SendResult<String, StandardCustomerMessage>> future = new CompletableFuture<>();
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
