package com.practice.sourcetokafka.persistence;

import com.practice.sourcetokafka.standard.StandardProductMessage;
import java.util.List;
import org.apache.ibatis.annotations.Mapper;

/** {@code src_prod}를 MyBatis로 조회한다. 컬럼 변환 SQL은 {@code LegacyProductMapper.xml}에 있다. */
@Mapper
public interface LegacyProductMapper {

    /** {@code src_prod} 전 건을 {@code StandardProductMessage}로 반환한다. */
    List<StandardProductMessage> findAll();
}
