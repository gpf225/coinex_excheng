CREATE TABLE `slice_update_example` (
    `id`            INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `create_time`   DOUBLE NOT NULL,
    `user_id`       INT UNSIGNED NOT NULL,
    `asset`         VARCHAR(30) NOT NULL,
    `business`      VARCHAR(30) NOT NULL,
    `business_id`   BIGINT UNSIGNED NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
