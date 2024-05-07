-- SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
-- SPDX-License-Identifier: MIT
--
-- Test case for multiple INSERT statements in the same SQL dump
-- https://github.com/brawer/wikidata-qrank/issues/30

--
-- Table structure for table `page_props`
--

DROP TABLE IF EXISTS `page_props`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `page_props` (
  `pp_page` int(10) unsigned NOT NULL,
  `pp_propname` varbinary(60) NOT NULL DEFAULT '',
  `pp_value` blob NOT NULL,
  `pp_sortkey` float DEFAULT NULL,
  PRIMARY KEY (`pp_page`,`pp_propname`),
  UNIQUE KEY `pp_propname_page` (`pp_propname`,`pp_page`),
  UNIQUE KEY `pp_propname_sortkey_page` (`pp_propname`,`pp_sortkey`,`pp_page`)
) ENGINE=InnoDB DEFAULT CHARSET=binary;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `page_props`
--

/*!40000 ALTER TABLE `page_props` DISABLE KEYS */;
INSERT INTO `page_props` VALUES (1,'wikibase_item','Q1',NULL),(2,'wikibase_item','Q2',NULL);
INSERT INTO `page_props` VALUES (3,'wikibase_item','Q3',NULL);

INSERT INTO `page_props` VALUES (4,'wikibase_item','Q4',NULL);
