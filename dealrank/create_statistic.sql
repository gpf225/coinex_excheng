-- 交易统计历史表，以年月日分表：例如：statistic_deal_history_20190314
CREATE TABLE `statistic_deal_history_example` (
    `id`                    BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `market`                VARCHAR(30) NOT NULL,
    `stock`                 VARCHAR(16) NOT NULL,
    `user_id`               INT UNSIGNED NOT NULL,
    `volume_bid`            DECIMAL(40,8) NOT NULL,
    `volume_ask`            DECIMAL(40,8) NOT NULL,
    `deal_bid`              DECIMAL(40,8) NOT NULL,
    `deal_ask`              DECIMAL(40,8) NOT NULL,
    `volume_taker_bid`      DECIMAL(40,8) NOT NULL,
    `volume_taker_ask`      DECIMAL(40,8) NOT NULL,
    `num_taker_bid`         INT UNSIGNED NOT NULL,   
    `num_taker_ask`         INT UNSIGNED NOT NULL,  
    `num_total`             INT UNSIGNED NOT NULL,
    INDEX `idx_stock_volume_bid` (`stock`, `volume_bid`),
    INDEX `idx_stock_volume_ask` (`stock`, `volume_ask`),
    INDEX `idx_stock_deal_bid` (`stock`, `deal_bid`),
    INDEX `idx_stock_volume_taker_bid` (`stock`, `volume_taker_bid`),
    INDEX `idx_stock_volume_taker_ask` (`stock`, `volume_taker_ask`),
    INDEX `idx_stock_num_taker_bid` (`stock`, `num_taker_bid`),
    INDEX `idx_stock_num_taker_ask` (`stock`, `num_taker_ask`),
    INDEX `idx_market_volume_bid` (`market`, `volume_bid`),
    INDEX `idx_market_volume_ask` (`market`, `volume_ask`),
    INDEX `idx_market_deal_bid` (`market`, `deal_bid`),
    INDEX `idx_market_volume_taker_bid` (`market`, `volume_taker_bid`),
    INDEX `idx_market_volume_taker_ask` (`market`, `volume_taker_ask`),
    INDEX `idx_market_num_taker_bid` (`market`, `num_taker_bid`),
    INDEX `idx_market_num_taker_ask` (`market`, `num_taker_ask`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 费用统计历史表，以年月日分表：例如：statistic_fee_history_20190314
CREATE TABLE `statistic_fee_history_example` (
    `id`                    BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `market`                VARCHAR(30) NOT NULL,
    `stock`                 VARCHAR(16) NOT NULL,
    `user_id`               INT UNSIGNED NOT NULL,
    `fee_asset`             VARCHAR(30) NOT NULL,
    `fee`                   DECIMAL(40,8) NOT NULL,
    INDEX `idx_market_asset_fee` (`market`, `fee_asset`, `fee`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

ECTBCH
-- 费率统计历史表，以年月日分表：例如：statistic_fee_rate_history_20190314
CREATE TABLE `statistic_fee_rate_history_example` (
    `id`                    BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `market`                VARCHAR(30) NOT NULL,
    `stock`                 VARCHAR(16) NOT NULL,
    `gear1`                 DECIMAL(40,8) NOT NULL,
    `gear2`                 DECIMAL(40,8) NOT NULL,
    `gear3`                 DECIMAL(40,8) NOT NULL,
    `gear4`                 DECIMAL(40,8) NOT NULL,
    `gear5`                 DECIMAL(40,8) NOT NULL,
    `gear6`                 DECIMAL(40,8) NOT NULL,
    `gear7`                 DECIMAL(40,8) NOT NULL,
    `gear8`                 DECIMAL(40,8) NOT NULL,
    `gear9`                 DECIMAL(40,8) NOT NULL,
    `gear10`                DECIMAL(40,8) NOT NULL,
    `gear11`                DECIMAL(40,8) NOT NULL,
    `gear12`                DECIMAL(40,8) NOT NULL,
    `gear13`                DECIMAL(40,8) NOT NULL,
    `gear14`                DECIMAL(40,8) NOT NULL,
    `gear15`                DECIMAL(40,8) NOT NULL,
    `gear16`                DECIMAL(40,8) NOT NULL,
    `gear17`                DECIMAL(40,8) NOT NULL,
    `gear18`                DECIMAL(40,8) NOT NULL,
    `gear19`                DECIMAL(40,8) NOT NULL,
    `gear20`                DECIMAL(40,8) NOT NULL,
    INDEX `idx_market` (`market`),
    INDEX `idx_stock` (`stock`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 最近一天的数据，用于程序重启加载最近一天的数据
CREATE TABLE `statistic_operlog` (
    `id`                    BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `market`                VARCHAR(30) NOT NULL,
    `stock`                 VARCHAR(16) NOT NULL,
    `ask_user_id`           INT UNSIGNED NOT NULL,
    `bid_user_id`           INT UNSIGNED NOT NULL,
    `taker_user_id`         INT UNSIGNED NOT NULL,
    `timestamp`             DOUBLE NOT NULL,
    `amount`                DECIMAL(40,4) NOT NULL,
    `price`                 DECIMAL(40,4) NOT NULL,
    `ask_fee_asset`         VARCHAR(30) NOT NULL,
    `bid_fee_asset`         VARCHAR(30) NOT NULL,
    `ask_fee`               DECIMAL(40,4) NOT NULL,
    `bid_fee`               DECIMAL(40,4) NOT NULL,
    `ask_fee_rate`          DECIMAL(40,4) NOT NULL,
    `bid_fee_rate`          DECIMAL(40,4) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



