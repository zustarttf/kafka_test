package com.practice.sourcetokafka.persistence;

import com.practice.sourcetokafka.standard.StandardCustomerMessage;
import java.util.List;
import org.apache.ibatis.annotations.Mapper;

/** {@code src_cust}를 MyBatis로 조회한다. 컬럼 변환 SQL은 {@code LegacyCustomerMapper.xml}에 있다. */
@Mapper
public interface LegacyCustomerMapper {

    /** {@code src_cust} 전 건을 {@code StandardCustomerMessage}로 반환한다. */
    List<StandardCustomerMessage> findAll();
}
