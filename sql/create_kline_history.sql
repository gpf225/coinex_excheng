CREATE TABLE `kline_history_example` (
    `id`            BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `market`        VARCHAR(64) NOT NULL,
    `class`         TINYINT UNSIGNED NOT NULL,
    `timestamp`     BIGINT UNSIGNED NOT NULL,
    `open`          DECIMAL(40,20) NOT NULL,
    `close`         DECIMAL(40,20) NOT NULL,
    `high`          DECIMAL(40,20) NOT NULL,
    `low`           DECIMAL(40,20) NOT NULL,
    `volume`        DECIMAL(40,20) NOT NULL,
    `deal`          DECIMAL(40,20) NOT NULL,
    INDEX `idx_market_class` (`market`, `class`),
    INDEX `idx_market_class_timestamp` (`market`, `class`, `timestamp`),
    UNIQUE INDEX `uidx_market_class_timestamp` (`market`, `class`, `timestamp`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;