-- 用户每日交易量统计历史表，按月分表
CREATE TABLE `user_trade_summary_example` (
    `id`                    INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `trade_date`            DATE NOT NULL,
    `user_id`               INT UNSIGNED NOT NULL,
    `market`                VARCHAR(30) NOT NULL,
    `stock_asset`           VARCHAR(30) NOT NULL,
    `money_asset`           VARCHAR(30) NOT NULL,
    `deal_amount`           DECIMAL(40,20) NOT NULL,
    `deal_volume`           DECIMAL(40,20) NOT NULL,
    `buy_amount`            DECIMAL(40,20) NOT NULL,
    `buy_volume`            DECIMAL(40,20) NOT NULL,
    `sell_amount`           DECIMAL(40,20) NOT NULL,
    `sell_volume`           DECIMAL(40,20) NOT NULL,
    `deal_count`            INT UNSIGNED NOT NULL,
    `deal_buy_count`        INT UNSIGNED NOT NULL,
    `deal_sell_count`       INT UNSIGNED NOT NULL,
    `limit_buy_order`       INT UNSIGNED NOT NULL,
    `limit_sell_order`      INT UNSIGNED NOT NULL,
    `market_buy_order`      INT UNSIGNED NOT NULL,
    `market_sell_order`     INT UNSIGNED NOT NULL,
    `maker_volume`          DECIMAL(40,20) NOT NULL default 0,
    `maker_amount`          DECIMAL(40,20) NOT NULL default 0,
    `taker_volume`          DECIMAL(40,20) NOT NULL default 0,
    `taker_amount`          DECIMAL(40,20) NOT NULL default 0,
    INDEX `idx_date` (`trade_date`),
    INDEX `idx_user_date` (`user_id`, `trade_date`),
    INDEX `idx_stock_date` (`stock_asset`, `trade_date`),
    UNIQUE KEY `idx_user_market_date` (`user_id`, `market`, `trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 用户每日交易费统计历史表，按月分表
CREATE TABLE `user_fee_summary_example` (
    `id`                    INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `trade_date`            DATE NOT NULL,
    `user_id`               INT UNSIGNED NOT NULL,
    `market`                VARCHAR(30) NOT NULL,
    `asset`                 VARCHAR(30) NOT NULL,
    `fee`                   DECIMAL(40,20) NOT NULL,
    `taker_fee`             DECIMAL(40,20) NOT NULL,
    `maker_fee`             DECIMAL(40,20) NOT NULL,
    INDEX `idx_date` (`trade_date`),
    INDEX `idx_asset_date` (`asset`, `trade_date`),
    INDEX `idx_user_date` (`user_id`, `trade_date`),
    UNIQUE KEY `idx_user_market_asset_date` (`user_id`, `market`, `asset`, `trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- client_id每日交易量统计历史表，按月分表
CREATE TABLE `client_trade_summary_example` (
    `id`                    INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `trade_date`            DATE NOT NULL,
    `client_id`             VARCHAR(32) NOT NULL default "",
    `user_id`               INT UNSIGNED NOT NULL,
    `market`                VARCHAR(30) NOT NULL,
    `stock_asset`           VARCHAR(30) NOT NULL,
    `money_asset`           VARCHAR(30) NOT NULL,
    `deal_amount`           DECIMAL(40,20) NOT NULL,
    `deal_volume`           DECIMAL(40,20) NOT NULL,
    `buy_amount`            DECIMAL(40,20) NOT NULL,
    `buy_volume`            DECIMAL(40,20) NOT NULL,
    `sell_amount`           DECIMAL(40,20) NOT NULL,
    `sell_volume`           DECIMAL(40,20) NOT NULL,
    `deal_count`            INT UNSIGNED NOT NULL,
    `deal_buy_count`        INT UNSIGNED NOT NULL,
    `deal_sell_count`       INT UNSIGNED NOT NULL,
    `limit_buy_order`       INT UNSIGNED NOT NULL,
    `limit_sell_order`      INT UNSIGNED NOT NULL,
    `market_buy_order`      INT UNSIGNED NOT NULL,
    `market_sell_order`     INT UNSIGNED NOT NULL,
    `maker_volume`          DECIMAL(40,20) NOT NULL default 0,
    `maker_amount`          DECIMAL(40,20) NOT NULL default 0,
    `taker_volume`          DECIMAL(40,20) NOT NULL default 0,
    `taker_amount`          DECIMAL(40,20) NOT NULL default 0,
    INDEX `idx_date` (`trade_date`),
    INDEX `idx_client_market_date` (`client_id`, `market`, `trade_date`),
    UNIQUE KEY `idx_client_market_user_date` (`client_id`, `market`, `user_id`, `trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- client_id每日交易费统计历史表，按月分表
CREATE TABLE `client_fee_summary_example` (
    `id`                    INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `trade_date`            DATE NOT NULL,
    `client_id`             VARCHAR(32) NOT NULL default "",
    `user_id`               INT UNSIGNED NOT NULL,
    `market`                VARCHAR(30) NOT NULL,
    `asset`                 VARCHAR(30) NOT NULL,
    `fee`                   DECIMAL(40,20) NOT NULL,
    INDEX `idx_date` (`trade_date`),
    INDEX `idx_client_asset_date` (`client_id`, `asset`, `trade_date`),
    INDEX `idx_client_asset_user_date` (`client_id`, `asset`, `user_id`, `trade_date`),
    UNIQUE KEY `idx_user_client_market_asset_date` (`user_id`, `client_id`, `market`, `asset`, `trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 市场每日交易统计历史表，不分表
CREATE TABLE `coin_trade_summary` (
    `id`                    INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `trade_date`            DATE NOT NULL,
    `market`                VARCHAR(30) NOT NULL,
    `stock_asset`           VARCHAR(30) NOT NULL,
    `money_asset`           VARCHAR(30) NOT NULL,
    `deal_amount`           DECIMAL(40,20) NOT NULL,
    `deal_volume`           DECIMAL(40,20) NOT NULL,
    `deal_count`            INT UNSIGNED NOT NULL,
    `deal_user_count`       INT UNSIGNED NOT NULL,
    `deal_user_list`        TEXT NOT NULL,
    `taker_buy_amount`      DECIMAL(40,20) NOT NULL,
    `taker_sell_amount`     DECIMAL(40,20) NOT NULL,
    `taker_buy_count`       INT UNSIGNED NOT NULL,
    `taker_sell_count`      INT UNSIGNED NOT NULL,
    `limit_buy_order`       INT UNSIGNED NOT NULL,
    `limit_sell_order`      INT UNSIGNED NOT NULL,
    `market_buy_order`      INT UNSIGNED NOT NULL,
    `market_sell_order`     INT UNSIGNED NOT NULL,
    INDEX `idx_date` (`trade_date`),
    INDEX `idx_stock_date` (`stock_asset`, `trade_date`),
    UNIQUE KEY `idx_market_date` (`market`, `trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `user_detail_example` (
    `id`              INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `user_id`         INT UNSIGNED NOT NULL,
    `market`          VARCHAR(30) NOT NULL,
    `time`            BIGINT NOT NULL,
    `buy_amount`      DECIMAL(40,20) NOT NULL,
    `sell_amount`     DECIMAL(40,20) NOT NULL,
    `buy_volume`      DECIMAL(40,20) NOT NULL,
    `sell_volume`     DECIMAL(40,20) NOT NULL,
    INDEX `idx_time` (`time`),
    UNIQUE KEY `idx_user_market_time` (`user_id`, `market`, `time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `dump_history` (
    `id`                    INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `time`                  BIGINT NOT NULL,
    `trade_date`            DATE NOT NULL,
    `deals_offset`          BIGINT NOT NULL,
    `orders_offset`         BIGINT NOT NULL
    INDEX `idx_date` (`trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

