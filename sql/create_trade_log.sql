CREATE TABLE `slice_balance_example` (
    `id`            INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `user_id`       INT UNSIGNED NOT NULL,
    `account`       INT UNSIGNED NOT NULL,
    `asset`         VARCHAR(30) NOT NULL,
    `t`             TINYINT UNSIGNED NOT NULL,
    `balance`       DECIMAL(40,24) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `slice_update_example` (
    `id`            INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `create_time`   DOUBLE NOT NULL,
    `user_id`       INT UNSIGNED NOT NULL,
    `account`       INT UNSIGNED NOT NULL,
    `asset`         VARCHAR(30) NOT NULL,
    `business`      VARCHAR(30) NOT NULL,
    `business_id`   BIGINT UNSIGNED NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `slice_order_example` (
    `id`            BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    `t`             TINYINT UNSIGNED NOT NULL,
    `side`          TINYINT UNSIGNED NOT NULL,
    `create_time`   DOUBLE NOT NULL,
    `update_time`   DOUBLE NOT NULL,
    `user_id`       INT UNSIGNED NOT NULL,
    `account`       INT UNSIGNED NOT NULL,
    `option`        INT UNSIGNED NOT NULL,
    `market`        VARCHAR(30) NOT NULL,
    `source`        VARCHAR(30) NOT NULL,
    `fee_asset`     VARCHAR(30) NOT NULL,
    `client_id`     VARCHAR(32) NOT NULL default "",
    `fee_discount`  DECIMAL(40,4) NOT NULL,
    `price`         DECIMAL(40,12) NOT NULL,
    `amount`        DECIMAL(40,8) NOT NULL,
    `taker_fee`     DECIMAL(40,4) NOT NULL,
    `maker_fee`     DECIMAL(40,4) NOT NULL,
    `left`          DECIMAL(40,8) NOT NULL,
    `frozen`        DECIMAL(40,24) NOT NULL,
    `deal_stock`    DECIMAL(40,8) NOT NULL,
    `deal_money`    DECIMAL(40,20) NOT NULL,
    `money_fee`     DECIMAL(40,24) NOT NULL,
    `stock_fee`     DECIMAL(40,24) NOT NULL,
    `asset_fee`     DECIMAL(40,24) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `slice_stop_example` (
    `id`            BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    `t`             TINYINT UNSIGNED NOT NULL,
    `side`          TINYINT UNSIGNED NOT NULL,
    `create_time`   DOUBLE NOT NULL,
    `update_time`   DOUBLE NOT NULL,
    `user_id`       INT UNSIGNED NOT NULL,
    `account`       INT UNSIGNED NOT NULL,
    `option`        INT UNSIGNED NOT NULL,
    `market`        VARCHAR(30) NOT NULL,
    `source`        VARCHAR(30) NOT NULL,
    `fee_asset`     VARCHAR(30) NOT NULL,
    `client_id`     VARCHAR(32) NOT NULL default "",
    `fee_discount`  DECIMAL(40,4) NOT NULL,
    `stop_price`    DECIMAL(40,12) NOT NULL,
    `price`         DECIMAL(40,12) NOT NULL,
    `amount`        DECIMAL(40,8) NOT NULL,
    `taker_fee`     DECIMAL(40,4) NOT NULL,
    `maker_fee`     DECIMAL(40,4) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `slice_history` (
    `id`            INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `time`          BIGINT NOT NULL,
    `end_oper_id`   BIGINT UNSIGNED NOT NULL,
    `end_order_id`  BIGINT UNSIGNED NOT NULL,
    `end_deals_id`  BIGINT UNSIGNED NOT NULL,
    `market_price`  TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `operlog_example` (
    `id`            BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    `time`          DOUBLE NOT NULL,
    `detail`        TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `indexlog_example` (
    `id`            BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `time`          DOUBLE NOT NULL,
    `market`        VARCHAR(30) NOT NULL,
    `price`         DECIMAL(40,12),
    `detail`        TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `kline_history_example` (
    `id`            BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `market`        VARCHAR(64) NOT NULL,
    `t`             TINYINT UNSIGNED NOT NULL,
    `timestamp`     BIGINT UNSIGNED NOT NULL,
    `open`          DECIMAL(40,20) NOT NULL,
    `close`         DECIMAL(40,20) NOT NULL,
    `high`          DECIMAL(40,20) NOT NULL,
    `low`           DECIMAL(40,20) NOT NULL,
    `volume`        DECIMAL(40,20) NOT NULL,
    `deal`          DECIMAL(40,20) NOT NULL,
    INDEX `idx_market_type` (`market`, `t`),
    INDEX `idx_market_type_timestamp` (`market`, `t`, `timestamp`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;