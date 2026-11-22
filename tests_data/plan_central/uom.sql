CREATE TABLE `uom` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `organization_code` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL,
  `code` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL,
  `name` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `base_unit` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL,
  `conversion_factor` decimal(24,15) NOT NULL,
  `whole_number` bit(1) NOT NULL,
  `active` bit(1) NOT NULL,
  `created_date` datetime(6) NOT NULL,
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `last_modified_date` datetime(6) NOT NULL,
  `last_modified_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_uom_organization_code` (`organization_code`,`code`),
  CONSTRAINT `fk_uom_organization` FOREIGN KEY (`organization_code`) REFERENCES `organization` (`code`),
  CONSTRAINT `ck_uom_base_unit` CHECK ((`base_unit` in (_utf8mb4'KILOGRAM',_utf8mb4'LITER',_utf8mb4'COUNT',_utf8mb4'METER'))),
  CONSTRAINT `ck_uom_conversion_factor` CHECK ((`conversion_factor` > 0))
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
