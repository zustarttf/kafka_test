package com.practice.sourcetokafka.standard;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Kafka로 나가는 표준 고객 메시지.
 *
 * <p>{@code LegacyCustomerMapper.xml}의 SELECT가 컬럼을 이 필드에 맞춰 담는다.
 * MyBatis가 값을 넣으려면 기본 생성자와 setter가 필요하다.
 * {@code KafkaTemplate}이 JSON으로 직렬화한다. 날짜는 이미 문자열이므로 타임스탬프 숫자로 나가지 않는다.
 * 자바 클래스 이름은 헤더에 붙이지 않는다({@code spring.json.add.type.headers=false}).
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class StandardCustomerMessage {

    /** 원천 {@code cust_no}. Kafka 메시지 키로도 사용한다. */
    private String customerId;

    /** 원천 {@code cust_nm}. */
    private String customerName;

    /** 원천 {@code mbl_telno}. */
    private String mobilePhoneNumber;

    /** 원천 {@code email_addr}. */
    private String email;

    /** 원천 {@code brdt}를 {@code yyyy-MM-dd}로 바꾼 값. 없거나 형식이 아니면 null. */
    private String birthDate;

    /** 원천 {@code sex_cd}를 바꾼 값. {@code MALE}, {@code FEMALE}, {@code UNKNOWN}. */
    private String gender;

    /** 원천 {@code rdnm_addr}. */
    private String roadAddress;

    /** 원천 {@code reg_dt}. 예: {@code 2024-01-15T09:30:00+09:00}. */
    private String registeredAt;

    /** 원천 {@code mdfcn_dt}. {@code registeredAt}과 같은 형식. */
    private String modifiedAt;

    /** 원천 {@code use_yn}이 {@code Y}이면 true. */
    private boolean active;

    /** 어느 원천에서 왔는지. 이 조회에서는 {@code LEGACY_CUST}로 고정한다. */
    private String sourceSystem;

    /** 메시지 종류. 이 조회에서는 전량 스냅샷이라 {@code CUSTOMER_SNAPSHOT}으로 고정한다. */
    private String eventType;

    /** 이 행을 조회해 표준 메시지로 만든 시각. */
    private String extractedAt;
}
