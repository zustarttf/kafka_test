package com.practice.consumer.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

import com.practice.consumer.message.StandardCustomerMessage;
import com.practice.consumer.persistence.StdCustomerMapper;
import com.practice.consumer.persistence.StdCustomerRow;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CustomerSinkServiceTest {

    @Mock
    private StdCustomerMapper stdCustomerMapper;

    @Test
    void upsertsParsedStandardFields() {
        CustomerSinkService service = new CustomerSinkService(stdCustomerMapper);
        StandardCustomerMessage message = new StandardCustomerMessage();
        message.setCustomerId("C20240001");
        message.setCustomerName("김민수");
        message.setMobilePhoneNumber("01012345678");
        message.setEmail("minsu.kim@example.com");
        message.setBirthDate("1990-03-15");
        message.setGender("MALE");
        message.setRoadAddress("서울특별시 중구 세종대로 110");
        message.setRegisteredAt("2024-01-15T09:30:00+09:00");
        message.setModifiedAt("2024-11-02T14:10:00+09:00");
        message.setActive(true);
        message.setSourceSystem("LEGACY_CUST");
        message.setEventType("CUSTOMER_SNAPSHOT");
        message.setExtractedAt("2026-10-10T10:00:00+09:00");

        service.upsert(message);

        ArgumentCaptor<StdCustomerRow> rowCaptor = ArgumentCaptor.forClass(StdCustomerRow.class);
        verify(stdCustomerMapper).upsert(rowCaptor.capture());
        StdCustomerRow row = rowCaptor.getValue();
        assertThat(row.getCustomerId()).isEqualTo("C20240001");
        assertThat(row.getBirthDate()).isEqualTo(LocalDate.of(1990, 3, 15));
        assertThat(row.getRegisteredAt()).isEqualTo(OffsetDateTime.parse("2024-01-15T09:30:00+09:00"));
        assertThat(row.isActive()).isTrue();
        assertThat(row.getGender()).isEqualTo("MALE");
    }
}
