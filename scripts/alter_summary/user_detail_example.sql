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
