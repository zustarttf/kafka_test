package com.practice.sourcetokafka.persistence;

import com.practice.sourcetokafka.standard.StandardCustomerMessage;
import java.util.List;
import org.apache.ibatis.annotations.Mapper;

/**
 * 원천 테이블을 표준 메시지로 조회한다.
 *
 * <p>SQL은 {@code classpath:mapper/LegacyCustomerMapper.xml}에 있다.
 * 컬럼 이름과 날짜, 성별, 사용 여부 변환은 그 SELECT에서 한다.
 * 조건이나 마지막 처리 시각으로 자르지 않는다.
 */
@Mapper
public interface LegacyCustomerMapper {

    /**
     * {@code src_cust} 전 건을 표준 필드로 바꿔 {@code cust_no} 오름차순으로 반환한다.
     * 테이블이 비어 있으면 빈 리스트이며, 이 경우 발행 건수는 0이다.
     */
    List<StandardCustomerMessage> findAll();
}
