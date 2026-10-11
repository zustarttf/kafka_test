package com.practice.sourcetokafka.persistence;

import com.practice.sourcetokafka.standard.StandardAccountMessage;
import java.util.List;
import org.apache.ibatis.annotations.Mapper;

/** {@code src_acct}를 MyBatis로 조회한다. 컬럼 변환 SQL은 {@code LegacyAccountMapper.xml}에 있다. */
@Mapper
public interface LegacyAccountMapper {

    /** {@code src_acct} 전 건을 {@code StandardAccountMessage}로 반환한다. */
    List<StandardAccountMessage> findAll();
}
