-- 원천 고객 테이블. 컬럼 이름은 레거시 그대로 둔다.
-- 표준 이름으로 바꾸는 SELECT는 mapper/LegacyCustomerMapper.xml 에 있다.
CREATE TABLE IF NOT EXISTS src_cust (
    cust_no     VARCHAR(20)  PRIMARY KEY,
    cust_nm     VARCHAR(100) NOT NULL,
    mbl_telno   VARCHAR(20),
    email_addr  VARCHAR(200),
    brdt        VARCHAR(8),
    sex_cd      VARCHAR(1),
    rdnm_addr   VARCHAR(500),
    reg_dt      TIMESTAMP,
    mdfcn_dt    TIMESTAMP,
    use_yn      VARCHAR(1)   NOT NULL
);
