/*
 Navicat Premium Data Transfer

 Source Server         : first
 Source Server Type    : MySQL
 Source Server Version : 50718
 Source Host           : localhost
 Source Database       : test

 Target Server Type    : MySQL
 Target Server Version : 50718
 File Encoding         : utf-8

 Date: 11/30/2018 14:20:34 PM
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

drop database test;
create database test;
use test;
-- ----------------------------
--  Table structure for `shopUser`
-- ----------------------------
DROP TABLE IF EXISTS `shopUser`;
CREATE TABLE `shopUser` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `loginName` varchar(255) NOT NULL,
  `password` varchar(255) NOT NULL,
  `realName` varchar(255) NOT NULL,
  `demo` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=12 DEFAULT CHARSET=utf8;

SET FOREIGN_KEY_CHECKS = 1;
