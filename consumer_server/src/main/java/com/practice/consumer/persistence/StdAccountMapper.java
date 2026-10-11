package com.practice.consumer.persistence;

import org.apache.ibatis.annotations.Mapper;

/** 같은 accountId는 새 행을 만들지 않고 최신 스냅샷으로 갱신한다. */
@Mapper
public interface StdAccountMapper {

    /** INSERT ... ON CONFLICT로 한 행을 넣거나 갱신한다. */
    void upsert(StdAccountRow row);
}
