package com.practice.consumer.persistence;

import org.apache.ibatis.annotations.Mapper;

/**
 * 표준 고객 적재.
 * 같은 {@code customer_id}는 새 행을 만들지 않고 최신 스냅샷으로 갱신한다.
 */
@Mapper
public interface StdCustomerMapper {

    /**
     * {@code StdCustomerMapper.xml}의 INSERT ... ON CONFLICT로 한 행을 넣거나 갱신한다.
     * 같은 customer_id의 이전 스냅샷은 이 행 내용으로 덮인다.
     */
    void upsert(StdCustomerRow row);
}
