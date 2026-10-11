INSERT INTO src_cust (
    cust_no, cust_nm, mbl_telno, email_addr, brdt, sex_cd, rdnm_addr, reg_dt, mdfcn_dt, use_yn
) VALUES
(
    'C20240001',
    '김민수',
    '01012345678',
    'minsu.kim@example.com',
    '19900315',
    '1',
    '서울특별시 중구 세종대로 110',
    TIMESTAMP '2024-01-15 09:30:00',
    TIMESTAMP '2024-11-02 14:10:00',
    'Y'
),
(
    'C20240002',
    '이서연',
    '01098765432',
    'seoyeon.lee@example.com',
    '19880721',
    '2',
    '부산광역시 해운대구 센텀중앙로 79',
    TIMESTAMP '2024-02-03 11:00:00',
    TIMESTAMP '2025-01-20 08:45:00',
    'Y'
),
(
    'C20240003',
    '박지훈',
    '01055551212',
    'jihun.park@example.com',
    '20011201',
    '9',
    '대구광역시 중구 동성로 1',
    TIMESTAMP '2023-06-18 16:20:00',
    TIMESTAMP '2023-06-18 16:20:00',
    'N'
)
ON CONFLICT (cust_no) DO UPDATE SET
    cust_nm = EXCLUDED.cust_nm,
    mbl_telno = EXCLUDED.mbl_telno,
    email_addr = EXCLUDED.email_addr,
    brdt = EXCLUDED.brdt,
    sex_cd = EXCLUDED.sex_cd,
    rdnm_addr = EXCLUDED.rdnm_addr,
    reg_dt = EXCLUDED.reg_dt,
    mdfcn_dt = EXCLUDED.mdfcn_dt,
    use_yn = EXCLUDED.use_yn;

INSERT INTO src_acct (
    acct_no, cust_no, acct_dv_cd, bal_amt, open_dt, use_yn
) VALUES
    ('A20240001', 'C20240001', '1', 1500000.00, TIMESTAMP '2024-01-15 09:40:00', 'Y'),
    ('A20240002', 'C20240001', '2', 5000000.00, TIMESTAMP '2024-03-01 10:00:00', 'Y'),
    ('A20240003', 'C20240002', '1', 32000.50, TIMESTAMP '2024-02-03 11:10:00', 'N')
ON CONFLICT (acct_no) DO UPDATE SET
    cust_no = EXCLUDED.cust_no,
    acct_dv_cd = EXCLUDED.acct_dv_cd,
    bal_amt = EXCLUDED.bal_amt,
    open_dt = EXCLUDED.open_dt,
    use_yn = EXCLUDED.use_yn;

INSERT INTO src_prod (
    prod_cd, prod_nm, prod_dv_cd, sale_yn, reg_dt
) VALUES
    ('P001', '보통예금', '1', 'Y', TIMESTAMP '2020-01-01 00:00:00'),
    ('P002', '신용대출', '2', 'Y', TIMESTAMP '2021-06-01 00:00:00'),
    ('P003', '단종적금', '1', 'N', TIMESTAMP '2018-04-01 00:00:00')
ON CONFLICT (prod_cd) DO UPDATE SET
    prod_nm = EXCLUDED.prod_nm,
    prod_dv_cd = EXCLUDED.prod_dv_cd,
    sale_yn = EXCLUDED.sale_yn,
    reg_dt = EXCLUDED.reg_dt;
