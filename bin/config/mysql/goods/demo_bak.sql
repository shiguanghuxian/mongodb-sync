CREATE TABLE `demo_bak` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) COLLATE utf8mb4_bin DEFAULT '',
  `document_key` varchar(60) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '标识一条唯一数据，保留字段',
  PRIMARY KEY (`id`),
  KEY `idx_ document_key` (`document_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;