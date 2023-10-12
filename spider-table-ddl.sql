CREATE TABLE `bilibili_rank_items` (
  `id` bigint(10) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `crawl_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '爬取时间',
  `channel_name` varchar(10) NOT NULL COMMENT '频道名称',
  `bv` varchar(50) NOT NULL COMMENT 'BV号',
  `block_name` varchar(50) NOT NULL COMMENT '小类区名',
  `block_hot_time_range` varchar(5) DEFAULT NULL COMMENT '排行时间范围',
  `order_num` int(10) NOT NULL COMMENT '排行序号',
  `href` varchar(255) NOT NULL COMMENT '链接',
  `title` varchar(255) NOT NULL COMMENT '标题',
  `point` varchar(255) DEFAULT NULL COMMENT '综合评分',
  `pubman_name` varchar(255) NOT NULL COMMENT '发布人名称',
  `desc` text COMMENT '视频简介',
  `time` varchar(100) NOT NULL COMMENT '视频发布时间',
  `play` varchar(50) NOT NULL COMMENT '视频播放量',
  `danmu` varchar(50) NOT NULL COMMENT '视频弹幕量',
  `star` varchar(50) NOT NULL COMMENT '视频收藏量',
  `coin` varchar(50) NOT NULL COMMENT '视频投币量',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=54 DEFAULT CHARSET=utf8mb4;

CREATE TABLE `bilibili_ups_info` (
  `id` bigint(10) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `crawl_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '爬取时间',
  `bv` varchar(50) NOT NULL COMMENT 'BV号',
  `up_uid` varchar(50) NOT NULL COMMENT 'up uid',
  `up_name` varchar(255) NOT NULL COMMENT 'up名称',
  `up_desc` text COMMENT 'up简介',
  `up_gz` varchar(10) NOT NULL COMMENT 'up关注量',
  `up_fs` varchar(10) NOT NULL COMMENT 'up粉丝量',
  `up_tg` varchar(10) NOT NULL COMMENT 'up投稿量',
  `up_hj` varchar(10) NOT NULL COMMENT 'up合集量',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=47 DEFAULT CHARSET=utf8mb4;

CREATE TABLE `bilibili_videos_danmus` (
  `id` bigint(10) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `crawl_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '爬取时间',
  `bv` varchar(255) NOT NULL COMMENT 'BV号',
  `pubtime_in_video` varchar(10) NOT NULL COMMENT '弹幕发送的视频时间节点',
  `context` varchar(255) NOT NULL COMMENT '弹幕内容',
  `pubtime` varchar(50) NOT NULL COMMENT '弹幕发送时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1355 DEFAULT CHARSET=utf8mb4;

CREATE TABLE `bilibili_videos_detail` (
  `id` bigint(10) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `crawl_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '爬取时间',
  `bv` varchar(255) NOT NULL COMMENT 'BV号',
  `title` varchar(255) NOT NULL COMMENT '视频标题',
  `play` varchar(50) NOT NULL COMMENT '视频播放量',
  `danmu` varchar(50) NOT NULL COMMENT '视频弹幕量',
  `pubtime` varchar(50) NOT NULL COMMENT '视频发布具体时间',
  `like` varchar(50) NOT NULL COMMENT '视频点赞量',
  `coin` varchar(50) NOT NULL COMMENT '视频投币量',
  `star` varchar(50) NOT NULL COMMENT '视频收藏量',
  `share` varchar(50) NOT NULL COMMENT '视频转发量',
  `desc` text COMMENT '视频简介',
  `reply` varchar(50) NOT NULL COMMENT '视频总评论量',
  `up_link` varchar(255) NOT NULL COMMENT '视频发布up个人空间链接',
  `up_name` varchar(255) NOT NULL COMMENT 'up名称',
  `up_desc` text COMMENT 'up简介',
  `up_gz` varchar(255) DEFAULT NULL COMMENT 'up被关注量',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=47 DEFAULT CHARSET=utf8mb4;

CREATE TABLE `bilibili_videos_replys` (
  `id` bigint(10) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `crawl_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '爬取时间',
  `bv` varchar(50) NOT NULL COMMENT 'BV号',
  `context` text COMMENT '评论内容',
  `time` varchar(50) NOT NULL COMMENT '评论时间',
  `like` varchar(10) DEFAULT NULL COMMENT '评论点赞数',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=85 DEFAULT CHARSET=utf8mb4;

CREATE TABLE `bilibili_videos_tags` (
  `id` bigint(10) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `crawl_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '爬取时间',
  `bv` varchar(50) NOT NULL COMMENT 'BV号',
  `tag` varchar(50) NOT NULL COMMENT '标签',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
