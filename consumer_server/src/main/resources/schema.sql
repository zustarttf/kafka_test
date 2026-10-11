-- 표준 고객 적재 테이블. Kafka 메시지 한 건이 이 테이블의 한 행이 된다.
-- customer_id 가 이미 있으면 같은 행을 최신 스냅샷으로 덮어쓴다.
CREATE TABLE IF NOT EXISTS std_customer (
    customer_id          VARCHAR(20)  PRIMARY KEY,
    customer_name        VARCHAR(100) NOT NULL,
    mobile_phone_number  VARCHAR(20),
    email                VARCHAR(200),
    birth_date           DATE,
    gender               VARCHAR(10)  NOT NULL,
    road_address         VARCHAR(500),
    registered_at        TIMESTAMPTZ,
    modified_at          TIMESTAMPTZ,
    active               BOOLEAN      NOT NULL,
    source_system        VARCHAR(50)  NOT NULL,
    event_type           VARCHAR(50)  NOT NULL,
    extracted_at         TIMESTAMPTZ  NOT NULL,
    consumed_at          TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS std_account (
    account_id    VARCHAR(20)    PRIMARY KEY,
    customer_id   VARCHAR(20)    NOT NULL,
    account_type  VARCHAR(10)    NOT NULL,
    balance       NUMERIC(18, 2) NOT NULL,
    opened_at     TIMESTAMPTZ,
    active        BOOLEAN        NOT NULL,
    source_system VARCHAR(50)    NOT NULL,
    event_type    VARCHAR(50)    NOT NULL,
    extracted_at  TIMESTAMPTZ    NOT NULL,
    consumed_at   TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS std_product (
    product_code  VARCHAR(20)  PRIMARY KEY,
    product_name  VARCHAR(100) NOT NULL,
    product_type  VARCHAR(10)  NOT NULL,
    on_sale       BOOLEAN      NOT NULL,
    registered_at TIMESTAMPTZ,
    source_system VARCHAR(50)  NOT NULL,
    event_type    VARCHAR(50)  NOT NULL,
    extracted_at  TIMESTAMPTZ  NOT NULL,
    consumed_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);
