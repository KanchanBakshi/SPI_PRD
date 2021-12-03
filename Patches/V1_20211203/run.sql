SET DEFINE OFF
PROMPT migration starts
 
/*
SPI - 100 : Fix duplicate values in Spi_acb_tab
*/
@<BASE_PATH>\SPI_APEX_PRD\Patches\V1_20211203\packages\spec\SPI_CLEANUP_PKG.sql
PROMPT SPI_CLEANUP_PKG.sql is migrated
@<BASE_PATH>\SPI_APEX_PRD\Patches\V1_20211203\packages\body\SPI_CLEANUP_PKG.sql
PROMPT SPI_CLEANUP_PKG.sql is migrated
@<BASE_PATH>\SPI_APEX_PRD\Patches\V1_20211203\packages\body\SPI_WPL_CREATION.sql
PROMPT SPI_WPL_CREATION.sql is migrated

