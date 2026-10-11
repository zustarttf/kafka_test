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
