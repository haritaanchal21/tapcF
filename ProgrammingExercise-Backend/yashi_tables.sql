
/*Table structure for table `zz__yashi_cgn` */

CREATE TABLE `zz__yashi_cgn` (
  `campaign_id` INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,
  `yashi_campaign_id` INT(11) UNSIGNED DEFAULT NULL,
  `name` VARCHAR(255) DEFAULT NULL,
  `yashi_advertiser_id` INT(11) UNSIGNED DEFAULT NULL,
  `advertiser_name` VARCHAR(100) DEFAULT NULL,
  PRIMARY KEY (`campaign_id`)
) ENGINE=INNODB DEFAULT CHARSET=utf8;

/*Table structure for table `zz__yashi_cgn_data` */

CREATE TABLE `zz__yashi_cgn_data` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `campaign_id` INT(11) UNSIGNED DEFAULT NULL,
  `log_date` INT(11) DEFAULT NULL,
  `impression_count` INT(11) DEFAULT NULL,
  `click_count` INT(11) DEFAULT NULL,
  `25viewed_count` INT(11) DEFAULT NULL,
  `50viewed_count` INT(11) DEFAULT NULL,
  `75viewed_count` INT(11) DEFAULT NULL,
  `100viewed_count` INT(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `campaign_id_UNIQUE` (`campaign_id`,`log_date`),
  KEY `fk_zz__yashi_cgn_data_campaign_id_idx` (`campaign_id`),
  CONSTRAINT `fk_zz__yashi_cgn_campaign_id` FOREIGN KEY (`campaign_id`) REFERENCES `zz__yashi_cgn` (`campaign_id`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE=INNODB DEFAULT CHARSET=utf8;


/*Table structure for table `zz__yashi_order` */

CREATE TABLE `zz__yashi_order` (
  `order_id` INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,
  `campaign_id` INT(11) UNSIGNED DEFAULT NULL,
  `yashi_order_id` INT(20) DEFAULT NULL,
  `name` VARCHAR(200) DEFAULT NULL,
  PRIMARY KEY (`order_id`),
  KEY `fk_zz__yashi_order_campaign_id_idx` (`campaign_id`),
  CONSTRAINT `fk_zz__yashi_order_campaign_id` FOREIGN KEY (`campaign_id`) REFERENCES `zz__yashi_cgn` (`campaign_id`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE=INNODB DEFAULT CHARSET=utf8;

/*Table structure for table `zz__yashi_order_data` */

CREATE TABLE `zz__yashi_order_data` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `order_id` INT(11) UNSIGNED DEFAULT NULL,
  `log_date` INT(11) DEFAULT NULL,
  `impression_count` INT(11) DEFAULT NULL,
  `click_count` INT(11) DEFAULT NULL,
  `25viewed_count` INT(11) DEFAULT NULL,
  `50viewed_count` INT(11) DEFAULT NULL,
  `75viewed_count` INT(11) DEFAULT NULL,
  `100viewed_count` INT(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `order_id` (`order_id`,`log_date`),
  KEY `fk_zz__yashi_order_data_order_id_idx` (`order_id`),
  CONSTRAINT `fk_zz__yashi_order_data_order_id` FOREIGN KEY (`order_id`) REFERENCES `zz__yashi_order` (`order_id`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE=INNODB DEFAULT CHARSET=utf8;


/*Table structure for table `zz__yashi_creative` */

CREATE TABLE `zz__yashi_creative` (
  `creative_id` INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,
  `order_id` INT(11) UNSIGNED DEFAULT NULL,
  `yashi_creative_id` INT(11) DEFAULT NULL,
  `name` VARCHAR(255) DEFAULT NULL,
  `preview_url` VARCHAR(255) DEFAULT NULL,
  PRIMARY KEY (`creative_id`),
  KEY `fk_zz__yashi_creative_order_id_idx` (`order_id`),
  CONSTRAINT `fk_zz__yashi_creative_order_id` FOREIGN KEY (`order_id`) REFERENCES `zz__yashi_order` (`order_id`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE=INNODB DEFAULT CHARSET=utf8;

/*Table structure for table `zz__yashi_creative_data` */

CREATE TABLE `zz__yashi_creative_data` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `creative_id` INT(11) UNSIGNED DEFAULT NULL,
  `log_date` INT(11) DEFAULT NULL,
  `impression_count` INT(11) DEFAULT NULL,
  `click_count` INT(11) DEFAULT NULL,
  `25viewed_count` INT(11) DEFAULT NULL,
  `50viewed_count` INT(11) DEFAULT NULL,
  `75viewed_count` INT(11) DEFAULT NULL,
  `100viewed_count` INT(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `creative_id_UNIQUE` (`creative_id`,`log_date`),
  KEY `fk_zz__yashi_creative_data_creative_id_idx` (`creative_id`),
  CONSTRAINT `fk_zz__yashi_creative_data_creative_id` FOREIGN KEY (`creative_id`) REFERENCES `zz__yashi_creative` (`creative_id`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE=INNODB DEFAULT CHARSET=utf8;

/* Truncate Queries */

set foreign_key_checks = 0;
TRUNCATE table `zz__yashi_cgn`;
TRUNCATE table `zz__yashi_cgn_data`;
TRUNCATE table `zz__yashi_creative`;
TRUNCATE table `zz__yashi_creative_data`;
TRUNCATE table `zz__yashi_order`;
TRUNCATE table `zz__yashi_order_data`;
set foreign_key_checks = 1;

/* Test query */

SELECT
  cgn.campaign_id,
  SUM(cgn_data.impression_count) AS cgn_impressions,
  SUM(cgn_data.click_count) AS cgn_clicks,
  SUM(cgn_data.25viewed_count) AS cgn_25views,
  SUM(cgn_data.50viewed_count) AS cgn_50views,
  SUM(cgn_data.75viewed_count) AS cgn_75views,
  SUM(cgn_data.100viewed_count) AS cgn_100views,
  SUM(creative_data.impression_count) AS creative_impressions,
  SUM(creative_data.click_count) AS creative_clicks,
  SUM(creative_data.25viewed_count) AS creative_25views,
  SUM(creative_data.50viewed_count) AS creative_50views,
  SUM(creative_data.75viewed_count) AS creative_75views,
  SUM(creative_data.100viewed_count) AS creative_100views
FROM
  zz__yashi_cgn AS cgn
  JOIN zz__yashi_cgn_data AS cgn_data ON cgn.campaign_id = cgn_data.campaign_id
  JOIN zz__yashi_order AS ord ON cgn.campaign_id = ord.campaign_id
  JOIN zz__yashi_creative AS creative ON ord.order_id = creative.order_id
  JOIN zz__yashi_creative_data AS creative_data ON creative.creative_id = creative_data.creative_id
GROUP BY
  cgn.campaign_id
HAVING
  cgn_impressions = creative_impressions
  AND cgn_clicks = creative_clicks
  AND cgn_25views = creative_25views
  AND cgn_50views = creative_50views
  AND cgn_75views = creative_75views
  AND cgn_100views = creative_100views;


