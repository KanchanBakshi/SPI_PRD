create or replace PACKAGE       SPI_CLEANUP_PKG
IS
/*
*****************************************************************************
||
||  Purpose              : To handle various SPI cleanup activities
||
||  REVISIONS            :
||  Ver       Date               Author                      Description
||  -----     -----------       ---------------------------  ---------------------------------------------------------------------------------------
||  1.0       05 Jun2020        Saurav                       Built the package
||  1.2       05 Aug2021        Saurav                       SPI-4301 Added Procedure Identify_fix_aff_hier_roof

|| *****************************************************************************************
*/
PROCEDURE SPI_PERF_WRITERS_TO_MECH;                                                                                             
PROCEDURE SPI_ADMIN_SHARE_FIX;                                                           
PROCEDURE SPI_FIX_DUP_HIER_SAME_ONR_ADM;                             
PROCEDURE spi_rollup_owners;  --  check with sachin before adding
PROCEDURE spi_admin_rollup; -- check with sachin before adding
PROCEDURE SPI_Process_OWR_Trx_To_70; 
PROCEDURE FIX_ISWC; 
PROCEDURE CLEANUP_CONTACTS;
Procedure Identify_fix_aff_hier_roof;
END SPI_CLEANUP_PKG;