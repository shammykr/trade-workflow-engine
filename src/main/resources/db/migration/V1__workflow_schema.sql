-- RFQ workflow state. `version` columns back JPA optimistic locking (@Version).
create table rfq (
    id                 uuid primary key,
    client_id          varchar(64)    not null,
    underlier          varchar(32)    not null,
    product_type       varchar(40)    not null,
    notional           numeric(19, 2) not null check (notional > 0),
    currency           char(3)        not null,
    maturity_months    integer        not null check (maturity_months > 0),
    status             varchar(16)    not null,
    rejection_reason   varchar(32),
    rejection_detail   varchar(512),
    pricing_request_id varchar(64),
    trade_id           varchar(64),
    created_at         timestamptz    not null,
    updated_at         timestamptz    not null,
    version            bigint         not null default 0
);
create index rfq_status_idx on rfq (status);
create index rfq_client_idx on rfq (client_id);

create table quote (
    id                 uuid primary key,
    rfq_id             uuid          not null references rfq (id) on delete cascade,
    price_pct          numeric(9, 4) not null,
    pricing_request_id varchar(64)   not null,
    status             varchar(16)   not null,
    valid_until        timestamptz   not null,
    created_at         timestamptz   not null,
    version            bigint        not null default 0
);
create index quote_rfq_idx on quote (rfq_id);
create index quote_active_expiry_idx on quote (valid_until) where status = 'ACTIVE';

create table rfq_transition (
    id          bigserial primary key,
    rfq_id      uuid         not null references rfq (id) on delete cascade,
    from_status varchar(16),
    to_status   varchar(16)  not null,
    detail      varchar(512),
    actor       varchar(64)  not null,
    occurred_at timestamptz  not null
);
create index rfq_transition_rfq_idx on rfq_transition (rfq_id);
