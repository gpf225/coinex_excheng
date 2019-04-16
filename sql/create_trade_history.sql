-- split by user_id
CREATE TABLE `balance_history_example` (
    `id`            BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `time`          DOUBLE NOT NULL COMMENT "交易时间",
    `user_id`       INT UNSIGNED NOT NULL COMMENT "用户ID",
    `asset`         VARCHAR(30) NOT NULL COMMENT "资产名称",
    `business`      VARCHAR(30) NOT NULL COMMENT "业务简介",
    `change`        DECIMAL(40,20) NOT NULL COMMENT "资产变更",  # 由原来8位变成20位
    `balance`       DECIMAL(40,20) NOT NULL COMMENT "变更后余额",
    `detail`        TEXT NOT NULL COMMENT "明细信息",
    INDEX `idx_time` (`time`),
    INDEX `idx_user_time` (`user_id`, `time`),
    INDEX `idx_user_business_time` (`user_id`, `business`, `time`),
    INDEX `idx_user_asset_business_time` (`user_id`, `asset`, `business`, `time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- split by user_id
CREATE TABLE `order_history_example` (
    `id`            BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `create_time`   DOUBLE NOT NULL COMMENT "委托单创建时间",
    `finish_time`   DOUBLE NOT NULL COMMENT "委托单结束时间",
    `user_id`       INT UNSIGNED NOT NULL COMMENT "用户ID",
    `order_id`      BIGINT UNSIGNED NOT NULL COMMENT "订单ID",
    `market`        VARCHAR(30) NOT NULL COMMENT "市场名称",
    `source`        VARCHAR(30) NOT NULL COMMENT "订单来源",
    `fee_asset`     VARCHAR(30) NOT NULL COMMENT "手续费货币类型",
    `fee_discount`  DECIMAL(40,4) NOT NULL COMMENT "手续费折扣",
    `t`             TINYINT UNSIGNED NOT NULL COMMENT "订单类型，市价单或者限价单",
    `side`          TINYINT UNSIGNED NOT NULL COMMENT "订单方向，买入或者卖出",
    `price`         DECIMAL(40,20) NOT NULL COMMENT "委托价格", # 由原来8位变成20位
    `amount`        DECIMAL(40,8) NOT NULL COMMENT "委托数量",
    `taker_fee`     DECIMAL(40,4) NOT NULL COMMENT "taker费率",
    `maker_fee`     DECIMAL(40,4) NOT NULL COMMENT "maker费率",
    `deal_stock`    DECIMAL(40,8) NOT NULL COMMENT "交易币种已交易数量",
    `deal_money`    DECIMAL(40,20) NOT NULL COMMENT "定价币种已交易数量", # 由原来16位变成20位，8+12=20，如果价格扩展到20位，那么我们就做四舍五入.
    `deal_fee`      DECIMAL(40,20) NOT NULL COMMENT "定价货币已产生的手续费",
    `asset_fee`     DECIMAL(40,20) NOT NULL COMMENT "手续费币种已产生的手续费",
    INDEX `idx_time` (`create_time`),
    INDEX `idx_user_time` (`user_id`, `create_time`),
    INDEX `idx_user_side_time` (`user_id`, `side`, `create_time`),
    INDEX `idx_user_market_time` (`user_id`, `market`, `create_time`),
    INDEX `idx_user_market_side_time` (`user_id`, `market`, `side`, `create_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- split by user_id
CREATE TABLE `stop_history_example` (
    `id`            BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `create_time`   DOUBLE NOT NULL COMMENT "计划委托单创建时间",
    `finish_time`   DOUBLE NOT NULL COMMENT "计划委托单结束时间",
    `user_id`       INT UNSIGNED NOT NULL COMMENT "用户ID",
    `order_id`      BIGINT UNSIGNED NOT NULL COMMENT "订单ID",
    `market`        VARCHAR(30) NOT NULL COMMENT "市场名称",
    `source`        VARCHAR(30) NOT NULL COMMENT "订单来源",
    `fee_asset`     VARCHAR(30) NOT NULL COMMENT "手续费货币类型",
    `fee_discount`  DECIMAL(40,4) NOT NULL COMMENT "手续费折扣",
    `t`             TINYINT UNSIGNED NOT NULL COMMENT "订单类型，市价单或者限价单",
    `side`          TINYINT UNSIGNED NOT NULL COMMENT "订单方向，买入或者卖出",
    `stop_price`    DECIMAL(40,20) NOT NULL COMMENT "计划委托单触发价格",  # 由原来8位变成20位
    `price`         DECIMAL(40,20) NOT NULL COMMENT "计划委托单挂单价格",  # 由原来8位变成20位
    `amount`        DECIMAL(40,8) NOT NULL COMMENT "委托数量",
    `taker_fee`     DECIMAL(40,4) NOT NULL COMMENT "taker费率",
    `maker_fee`     DECIMAL(40,4) NOT NULL COMMENT "maker费率",
    `status`        TINYINT UNSIGNED NOT NULL COMMENT "计划委托单状态，成功，失败",
    INDEX `idx_time` (`create_time`),
    INDEX `idx_user_time` (`user_id`, `create_time`),
    INDEX `idx_user_side_time` (`user_id`, `side`, `create_time`),
    INDEX `idx_user_market_time` (`user_id`, `market`, `create_time`),
    INDEX `idx_user_market_side_time` (`user_id`, `market`, `side`, `create_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- split by user_id
CREATE TABLE `user_deal_history_example` (
    `id`            BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `time`          DOUBLE NOT NULL COMMENT "交易时间",
    `user_id`       INT UNSIGNED NOT NULL COMMENT "用户id",
    `deal_user_id`  INT UNSIGNED NOT NULL COMMENT "对手用户id",
    `market`        VARCHAR(30) NOT NULL COMMENT "市场名称",
    `deal_id`       BIGINT UNSIGNED NOT NULL COMMENT "交易id",
    `order_id`      BIGINT UNSIGNED NOT NULL COMMENT "订单ID",
    `deal_order_id` BIGINT UNSIGNED NOT NULL COMMENT "对手单订单ID",
    `side`          TINYINT UNSIGNED NOT NULL COMMENT "订单方向，买入或者卖出",
    `role`          TINYINT UNSIGNED NOT NULL COMMENT "maker或者taker",
    `price`         DECIMAL(40,20) NOT NULL COMMENT "交易价格",  # 由原来8位变成20位
    `amount`        DECIMAL(40,8) NOT NULL COMMENT "交易数量",
    `deal`          DECIMAL(40,20) NOT NULL COMMENT "交易额",    # 由原来16位变成20位
    `fee`           DECIMAL(40,20) NOT NULL COMMENT "本交易手续费",
    `deal_fee`      DECIMAL(40,20) NOT NULL COMMENT "对手单交易手续费",
    `fee_asset`      VARCHAR(30) NOT NULL COMMENT "本交易手续费货币类型",
    `deal_fee_asset` VARCHAR(30) NOT NULL COMMENT "对手交易手续费货币类型",
    INDEX `idx_time` (`time`),
    INDEX `idx_user_time` (`user_id`, `time`),
    INDEX `idx_user_side_time` (`user_id`, `side`, `time`),
    INDEX `idx_user_market_time` (`user_id`, `market`, `time`),
    INDEX `idx_user_market_side_time` (`user_id`, `market`, `side`, `time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
