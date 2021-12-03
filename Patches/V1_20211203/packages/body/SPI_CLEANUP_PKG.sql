create or replace PACKAGE BODY       SPI_CLEANUP_PKG
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
||  1.1       20 Jan2021        Rishi Rawat                  Changed the procedure SPI_FIX_DUP_HIER_SAME_ONR_ADM as part of SPI-3723.
||  1.2       05 Aug2021        Saurav                       SPI-4301 Added Procedure Identify_fix_aff_hier_roof
||  1.3       18 Oct2021        Saurav                       BTLAFF-41 Modified CLEANUP_CONTACTS, retain old permissions and do not overwrite them

|| *****************************************************************************************
*/

PROCEDURE CLEANUP_CONTACTS AS
BEGIN

    begin
    FOR REC in (
    Select Air.Relationship_Id, Air.Cont_Type_Id, Air.Status 
     From Aff_Ip_Contact_Relationship Air 
    Where   
    (
       Air.CAN_VIEW_STATEMENTS       Is Null
    or Air.CAN_VIEW_CATALOG          Is Null
    or Air.CAN_ADD_WORKS             Is Null
    or Air.CAN_ADD_RECORDING         Is Null
    or Air.CAN_ADD_COMMERCIAL        Is Null
    or Air.CAN_ADD_DIRECT_DEPOSIT    Is Null
    or Air.CAN_VIEW_ADD_LIVE_PERF    Is Null
    or Air.CAN_UPDATE_ACCT_INFO      Is Null
    or Air.CAN_OPT_OUT_OF_PAPERLESS  Is Null
    or Air.CAN_CHANGE_PERMISSIONS    Is Null
    or Air.CAN_TERMINATE             Is Null
    or Air.CAN_OPT_OUT_MONTHLY_DIST  Is Null
    or Air.CAN_LOGIN                 Is Null
    )
    ) LOOP

        FOR r1 in (
            SELECT * FROM aff_ip_cont_type_permissions 
            WHERE contact_type_id = REC.CONT_TYPE_ID) LOOP
            UPDATE aff_ip_contact_relationship Air SET
            LAST_UPDATED_BY           = 'SPI-3254',
            LAST_UPDATE_DATE          = SYSDATE,
            CAN_VIEW_STATEMENTS       = Nvl(Air.CAN_VIEW_STATEMENTS, r1.CAN_VIEW_STATEMENTS),
            CAN_VIEW_CATALOG          = Nvl(Air.CAN_VIEW_CATALOG, r1.CAN_VIEW_CATALOG),
            CAN_ADD_WORKS             = Nvl(Air.CAN_ADD_WORKS, r1.CAN_ADD_WORKS),
            CAN_ADD_RECORDING         = Nvl(Air.CAN_ADD_RECORDING, r1.CAN_ADD_RECORDING),
            CAN_ADD_COMMERCIAL        = Nvl(Air.CAN_ADD_COMMERCIAL, r1.CAN_ADD_COMMERCIAL),
            CAN_ADD_DIRECT_DEPOSIT    = Nvl(Air.CAN_ADD_DIRECT_DEPOSIT, r1.CAN_ADD_DIRECT_DEPOSIT),
            CAN_VIEW_ADD_LIVE_PERF    = Nvl(Air.CAN_VIEW_ADD_LIVE_PERF, r1.CAN_VIEW_ADD_LIVE_PERF),
            CAN_UPDATE_ACCT_INFO      = Nvl(Air.CAN_UPDATE_ACCT_INFO, r1.CAN_UPDATE_ACCT_INFO),
            CAN_OPT_OUT_OF_PAPERLESS  = Nvl(Air.CAN_OPT_OUT_OF_PAPERLESS, r1.CAN_OPT_OUT_OF_PAPERLESS),
            CAN_CHANGE_PERMISSIONS    = Nvl(Air.CAN_CHANGE_PERMISSIONS, r1.CAN_CHANGE_PERMISSIONS),
            CAN_TERMINATE             = Nvl(Air.CAN_TERMINATE, r1.CAN_TERMINATE),
            CAN_OPT_OUT_MONTHLY_DIST  = Nvl(Air.CAN_OPT_OUT_MONTHLY_DIST, r1.CAN_OPT_OUT_MONTHLY_DIST),
            CAN_LOGIN                 = Nvl(Air.CAN_LOGIN, r1.CAN_LOGIN)
            WHERE Relationship_Id = REC.Relationship_Id;

            sf_outbound_api.send_acc_cont_details(REC.relationship_id, REC.status);
        END LOOP;
    END LOOP;
        commit;
    END;

    DELETE aff_ip_contact_relationship  aicr
    WHERE  ip_contact_id IN (select ip_contact_id from aff_ip_contacts aic where status = 'I');

    DELETE aff_ip_contact_points aicp
    WHERE ip_contact_id IN (select ip_contact_id from aff_ip_contacts aic where status = 'I');

    DELETE aff_ip_contacts aic
    WHERE ip_contact_id IN (select ip_contact_id from aff_ip_contacts aic where status = 'I');

    COMMIT;    


END CLEANUP_CONTACTS;


PROCEDURE spi_perf_writers_to_mech
AS
	tmp_share_coll_id       NUMBER;
    m_count                 NUMBER := 0;
BEGIN
	FOR cur_psc IN(SELECT psc.share_collector_id,psc.collector_id,psc.property_id,psc.role_id,psc.affiliated_society_id,psc.hold_type_id,psc.hold_reason, psc.SUBMITTER_INTERNAL_NUMBER
                              FROM prop_share_collectors psc, prop_properties pp
                              WHERE psc.role_id IN(SELECT role_id FROM aff_roles WHERE summary_role_code = 'W' AND status = 'A')
                              AND psc.right_type_id = 1
                              AND psc.collector_id != 602200
                        --    AND psc.property_id = 852105511
                              --AND psc.last_updated_by NOT LIKE'%PERF_SHARE_OWNER_LOAD%'
                              --AND psc.last_updated_by NOT LIKE'APCTS%'
                              AND psc.end_date_active IS NULL
                              AND NOT EXISTS(SELECT 1 FROM prop_share_collectors psc2 WHERE role_id IN(SELECT role_id FROM aff_roles WHERE summary_role_code = 'W' AND status = 'A') 
                                                      AND psc2.right_type_id = 2 
                                                      AND psc2.end_date_active IS NULL
                                                      AND psc2.collector_id = psc.collector_id
                                                      AND psc2.property_id = psc.property_id
                                                      AND psc2.collector_id != 602200)
                    AND psc.property_id = pp.property_id  
                    AND pp.property_type_id = 1
                    AND pp.source_id = 7)
      LOOP
            m_count := m_count + 1;
            tmp_share_coll_id := prop_share_collector_s.NEXTVAL;
            INSERT INTO prop_share_collectors
                        VALUES
                        (
                            tmp_share_coll_id,
                            cur_psc.collector_id,
                            0,
                            'INS_WRI_TO_MECH',
                            SYSDATE,
                            'INS_WRI_TO_MECH',
                            SYSDATE,
                            2,
                            cur_psc.property_id,
                            cur_psc.role_id,
                            cur_psc.affiliated_society_id,
                            SYSDATE,
                            NULL,
                            cur_psc.hold_type_id,
                            cur_psc.hold_reason,
                                          cur_psc.SUBMITTER_INTERNAL_NUMBER,
                            'N'
                        );

                                    -- Insert territory
                                    FOR rec IN
                                          (
                                                SELECT
                                                      territory_id,
                                                      include_exclude_code,
                                                      sequence_number
                                                FROM prop_share_coll_territories
                                                WHERE share_collector_id = cur_psc.share_collector_id
                                          )
                                    LOOP

                                                INSERT INTO prop_share_coll_territories
                                                VALUES
                                                (
                                                      prop_share_coll_territories_s.NEXTVAL,
                                                      rec.include_exclude_code,
                                                      tmp_share_coll_id,
                                                      rec.territory_id,
                                                      'INS_WRI_TO_MECH',
                                                      SYSDATE,
                                                      'INS_WRI_TO_MECH',
                                                      SYSDATE,
                                                      rec.sequence_number
                                                );

                                    END loop;

            IF(m_count >= 100) THEN
                  COMMIT;
                  m_count := 0;
            END IF;
      END LOOP;
	  COMMIT;
END spi_perf_writers_to_mech;


PROCEDURE SPI_ADMIN_SHARE_FIX
AS
--Fix Admin shares, make them equal to Owners share

--Remove duplicate ADmins	

BEGIN
	UPDATE prop_share_admins 
	SET 
	end_date_active = SYSDATE-1,
	last_updated_by = 'SPI_DUP_ADMIN_FIX',
	last_update_date = SYSDATE
	WHERE share_admin_id in 
	(
		select tbd from 
		(
			select psc.property_id, psc.share_collector_id, admin_id, admin_share_percent, min(share_admin_id) tbd 
			from prop_share_admins psa, prop_share_collectors psc, prop_properties pp
			where psc.share_collector_id = psa.share_collector_id
			and psc.property_id = pp.property_id
            and pp.source_id = 7
			and psc.end_date_active is null
			and psa.end_date_active is null
			group by psc.property_id, psc.share_collector_id, admin_id, admin_share_percent
			having count(1) > 1
		)
	);  

    EXECUTE IMMEDIATE 'DROP TABLE spi_own_adm_share_diff' ; 

    EXECUTE IMMEDIATE '
        CREATE TABLE spi_own_adm_share_diff TABLESPACE TS_BULK_DATA AS
        SELECT 
            psc.property_id,  psc.share_collector_id, psc.collector_id,
            spi_screen_utility.get_party_name(psc.collector_id) owner,
            psc.share_percentage, 
            sum(psa.admin_share_percent) as Admin_share, count(psa.share_admin_id) as admin_count
        FROM prop_share_collectors psc, prop_share_admins psa, aff_ip_names ain, prop_properties pp 
        WHERE psc.share_collector_id = psa.share_collector_id
            and psc.collector_id = ain.ip_id
            and ain.name_type_id = 6
            and psc.property_id = pp.property_id
            and pp.source_id = 7
            and pp.status = ''A''
            and psc.end_date_active is null
            and psa.end_date_active is null
        GROUP BY psc.property_id, psc.share_collector_id, psc.collector_id,  
            nvl(ain.full_name, ain.first_name||'' ''||ain.name),
            psc.share_percentage
        HAVING psc.share_percentage <> sum(psa.admin_share_percent)
        ';

        EXECUTE IMMEDIATE 'ALTER TABLE spi_own_adm_share_diff ADD (hier_exists VARCHAR(20))';
		EXECUTE IMMEDIATE 'CREATE INDEX spi_own_adm_share_diff_idx ON spi_own_adm_share_diff(share_collector_id) COMPUTE STATISTICS'; -- SPI-4309

--------------------------------------------------------------------------------------------------

--SELECT admin_count, 
--count(*) 
--from spi_own_adm_share_diff
--group by  admin_count;

--------------------------------------------------------------------------------------------------

    DECLARE
        l_hier_Count    NUMBER;
        l_hier_exists	spi_own_adm_share_diff.hier_exists%TYPE;
    BEGIN
        FOR r1 IN 
        (
            SELECT psc.share_collector_id, collector_id, admin_id
            FROM prop_share_collectors psc, prop_share_admins psa
            WHERE psc.share_collector_id = psa.share_collector_id
            AND psc.end_date_active IS NULL
            AND psa.end_date_active IS NULL
            AND psc.share_collector_id IN (SELECT share_collector_id FROM spi_own_adm_share_diff )
        )      
        LOOP
            SELECT COUNT(1)
            INTO l_hier_count
            FROM aff_hierarchy
            WHERE child_party_id = r1.collector_id
            AND parent_party_id = r1.admin_id
            AND end_date IS NULL
            AND hier_level = 0
            AND nowed_aff_hier_id IS NULL;

            IF l_hier_count > 0 THEN 
                l_hier_exists := 'Y';
            ELSE
                l_hier_exists := ' ['||r1.admin_id || '-N]';
            END IF;

        UPDATE spi_own_adm_share_diff SET
        hier_exists = NVL2(hier_exists, l_hier_exists, ' '||l_hier_exists)
        WHERE share_collector_id = r1.share_collector_id;

        END LOOP;
        COMMIT;
    END;



--SELECT admin_count, count(*) 
--from spi_own_adm_share_diff
--where hier_exists like '%Y%'
--group by  admin_count
--order by 1, 2 desc;
--
--SELECT admin_count, count(*) 
--from spi_own_adm_share_diff
--where hier_exists like '%N%'
--group by  admin_count
--order by 1, 2 desc;


--Fix where admin count = 1 and hierarchy is valid/exists
-- Step 1 - Equate where hierarchy exists

    UPDATE prop_share_admins psa set
    admin_share_percent = (SELECT share_percentage from spi_own_adm_share_diff WHERE share_collector_id = psa.share_collector_id),
    last_updated_by = 'SPI_ADMIN_SHARE_FIX',
    last_update_date = SYSDATE
    WHERE share_collector_id in 
    (select share_collector_id FROM spi_own_adm_share_diff WHERE admin_count =1 AND hier_exists LIKE '%Y%'  )
    ;

    COMMIT;

END SPI_ADMIN_SHARE_FIX;



PROCEDURE SPI_FIX_DUP_HIER_SAME_ONR_ADM AS

	l_update_count NUMBER ;

BEGIN

	FOR cur_h IN 
	(	
		Select a.child_party_id, b.parent_party_id,
			   b.relate_account_number, b.creation_date, b.created_by, b.last_updated_by,
			   b.AFF_HIER_ID,
			   MIN(b.AFF_HIER_ID) OVER (PARTITION BY b.child_party_id, b.parent_party_id ORDER BY b.CREATION_DATE DESC) to_keep,
			   MAX(b.AFF_HIER_ID) OVER (PARTITION BY b.child_party_id, b.parent_party_id ORDER BY b.CREATION_DATE DESC) to_remove
	  from (
	  select  child_party_id, parent_party_id, count(*)
	  from apcts.aff_hierarchy
		where hier_level = 0
		  and end_date is null
		 and NOWED_AFF_HIER_ID is null   
		  and child_party_id is not null
		  and parent_party_id is not null
	   group by child_party_id, parent_party_id
	   having count(*) > 1)  a
	   join apcts.aff_hierarchy b
		 on b.child_party_id = a.child_party_id
		and b.parent_party_id = a.parent_party_id
		and b.hier_level = 0      and b.end_date is null
		  and b.NOWED_AFF_HIER_ID is null
	      --and (b.child_party_id in (7290074, 7294126) )
	--      and b.parent_party_id not in (7322661)
		  Order by  b.parent_party_id   ,a.child_party_id, b.creation_date
	  )

    LOOP
		l_update_count := 0;
		UPDATE aff_hierarchy SET
		end_date = SYSDATE,
		last_updated_by = 'DUP_HIERARCHY',
		last_update_date = SYSDATE
		WHERE aff_hier_id = cur_h.to_remove
		AND end_date is null;

		l_update_count := SQL%ROWCOUNT;
--		DBMS_OUTPUT.PUT_LINE(cur_h.to_remove);
--		DBMS_OUTPUT.PUT_LINE(l_update_count);

        --SPI-3723: To end date remaining active levels of above end dated hierarchy.
        UPDATE aff_hierarchy
           SET end_date = SYSDATE,
               last_updated_by = 'DUP_HIERARCHY',
               last_update_date = SYSDATE
        WHERE group_number = (
                                SELECT group_number 
                                  FROM aff_hierarchy 
                                 WHERE aff_hier_id = cur_h.to_remove 
                             ) 
          AND hier_level > 0
          AND end_date IS NULL;                         

		COMMIT;

		IF l_update_count > 0 THEN
			FOR r1 in    
			(
				select property_id, psc.share_collector_id sc_id
				from prop_share_collectors psc, prop_share_admins psa
				where psc.share_collector_id = psa.share_collector_id
				and psc.collector_id = cur_h.child_party_id
				and psa.admin_id = cur_h.parent_party_id    
				and psc.right_type_id = 2
				and psc.role_id <= 6
				and psc.end_date_active IS NULL
				and psa.end_date_active IS NULL
				and psc.property_id in (select property_id from prop_properties where property_type_id = 1 and status = 'A' and source_id = 7)
			)   

			LOOP
				UPDATE prop_share_collectors SET 
					last_updated_by = 'DUP_HIERARCHY',
					last_update_date = SYSDATE
				WHERE 
					share_collector_id = r1.sc_id;

				/*
				UPDATE prop_share_admins SET 
					last_updated_by = 'DUP_HIERARCHY',
					last_update_date = SYSDATE
				WHERE 
					share_collector_id = r1.sc_id;

				spi_collection_agreements.new_owner_added_to_song
				(  
					P_collector_id => r1.collector_id,
					p_property_id   => r1.property_id,
					p_source        => 'PROPERTY SHARES',
					p_right_type_id => r1.right_type_id
				);

				prop_share_owner.add_remove_song_mem_agg( r1.property_id, r1.right_type_id);
				*/

            END LOOP;

            COMMIT;
		END IF;

    END LOOP;
END SPI_FIX_DUP_HIER_SAME_ONR_ADM;

PROCEDURE spi_rollup_owners
AS
	m_total_share_per_owner			NUMBER;
	m_total_share_per_admin			NUMBER;
	m_share_collector_id_smallest	NUMBER;
	m_share_collector_id_new		NUMBER;
	m_role_id_count					NUMBER;
	m_authoritative_flag_count		NUMBER;
	m_aff_society_id				NUMBER;
	m_authoritative_flag_cnt		NUMBER;
	m_existing_share_admin_id		NUMBER;
	m_new_admins_total_per			NUMBER;
	m_authoritative_flag			VARCHAR2(2);
	m_admin_exists					NUMBER := 0;
	m_count							NUMBER := 0;
	m_self_hier_count				NUMBER;
	m_same_owner_admin_cnt			NUMBER;
	m_diff_owner_admin_cnt			NUMBER;
	m_submitter_internal_number		VARCHAR2(100);
	m_role_id_to_use				NUMBER;
	m_rollup						NUMBER;
	m_proceed						NUMBER;
	m_share_on_role_4				NUMBER;
	m_share_on_non_role_4			NUMBER;
	m_rollup_1						NUMBER;
	m_rollup_2                      NUMBER;
    m_rollup_3                      NUMBER;
	m_rollup_4						NUMBER;
	m_rollup_5						NUMBER;
	m_rollup_6						NUMBER;
	m_rollup_7						NUMBER;
	m_rollup_8						NUMBER;
	m_rollup_9						NUMBER;
	m_rollup_10						NUMBER;
	m_rollup_11						NUMBER;
	m_rollup_12						NUMBER;
	m_rollup_13						NUMBER;
	m_rollup_14						NUMBER;
	m_rollup_15						NUMBER;
	m_rollup_16						NUMBER;

	m_rollup_17						NUMBER;
	m_rollup_18						NUMBER;
	m_rollup_19						NUMBER;
	m_rollup_20						NUMBER;
	m_rollup_21						NUMBER;
	m_rollup_22						NUMBER;
	m_rollup_23						NUMBER;
	m_rollup_24						NUMBER;

BEGIN
	FOR cur_psc IN(SELECT psc.collector_id, psc.property_id, psc.right_type_id, MIN(start_date_active) min_start_date
					FROM prop_share_collectors psc, prop_properties pp
					WHERE psc.end_date_active IS NULL
					AND psc.property_id = pp.property_id   
					AND pp.property_type_id = 1
					AND pp.source_id = 7
					AND pp.status = 'A' 
					AND psc.collector_id != 602200
					AND psc.right_type_id IN(1,2)
					AND NOT EXISTS(SELECT 1 FROM prop_share_admins psa WHERE psa.share_collector_id = psc.share_collector_id AND admin_id = 602200 
					AND psa.end_date_active IS NULL)
				--	AND (SELECT COUNT(DISTINCT role_id) FROM prop_share_admins psa WHERE psa.share_collector_id = psc.share_collector_id AND TRUNC(NVL(psa.end_date_active,SYSDATE+1)) > TRUNC(SYSDATE)) <= 1
					AND (SELECT SUM(share_percentage) FROM prop_share_collectors psc1 WHERE psc.collector_id = psc1.collector_id AND psc.property_id = psc1.property_id AND psc.right_type_id = psc1.right_type_id AND TRUNC(NVL(psc1.end_date_active,SYSDATE+1)) > TRUNC(SYSDATE)) <= 100
					GROUP BY psc.collector_id, psc.property_id, psc.right_type_id
					HAVING COUNT(*) > 1)
	LOOP
		SELECT COUNT(DISTINCT role_id) INTO m_role_id_count
		FROM prop_share_collectors 
		WHERE collector_id = cur_psc.collector_id
		AND property_id = cur_psc.property_id
		AND right_type_id = cur_psc.right_type_id
		AND end_date_active IS NULL;

		SELECT COUNT(1) INTO m_rollup -- role_id 2 to 4
		FROM prop_share_collectors
		WHERE collector_id = cur_psc.collector_id
		AND property_id = cur_psc.property_id
		AND right_type_id = cur_psc.right_type_id
		AND role_id NOT IN(2,4)
		AND end_date_active IS NULL;

		SELECT COUNT(1) INTO m_rollup_10 -- role_id 6 to 4  -- SPI-3091
		FROM prop_share_collectors
		WHERE collector_id = cur_psc.collector_id
		AND property_id = cur_psc.property_id
		AND right_type_id = cur_psc.right_type_id
		AND role_id NOT IN(6,4)
		AND end_date_active IS NULL;

		SELECT COUNT(1) INTO m_rollup_17 -- role_id 5 to 4  -- SPI-3091
		FROM prop_share_collectors
		WHERE collector_id = cur_psc.collector_id
		AND property_id = cur_psc.property_id
		AND right_type_id = cur_psc.right_type_id
		AND role_id NOT IN(5,4)
		AND end_date_active IS NULL;



		SELECT COUNT(1) INTO m_rollup_1 -- role_id 10,11 to 11
		FROM prop_share_collectors
		WHERE collector_id = cur_psc.collector_id
		AND property_id = cur_psc.property_id
		AND right_type_id = cur_psc.right_type_id
		AND role_id NOT IN(10,11)
		AND end_date_active IS NULL;

		-- SPI-3015
		SELECT COUNT(1) INTO m_rollup_2 -- role_id 9,10 to 11
		FROM prop_share_collectors
		WHERE collector_id = cur_psc.collector_id
		AND property_id = cur_psc.property_id
		AND right_type_id = cur_psc.right_type_id
		AND role_id NOT IN(10,9)
		AND end_date_active IS NULL;

		SELECT COUNT(1) INTO m_rollup_3 -- role_id 9,10,11 to 11
		FROM prop_share_collectors
		WHERE collector_id = cur_psc.collector_id
		AND property_id = cur_psc.property_id
		AND right_type_id = cur_psc.right_type_id
		AND role_id NOT IN(9,10,11)
		AND end_date_active IS NULL;

		SELECT COUNT(1) INTO m_rollup_14 -- role_id 10,7 to 7 -- SPI-3091
		FROM prop_share_collectors
		WHERE collector_id = cur_psc.collector_id
		AND property_id = cur_psc.property_id
		AND right_type_id = cur_psc.right_type_id
		AND role_id NOT IN(10,7)
		AND end_date_active IS NULL;

		SELECT COUNT(1) INTO m_rollup_15 -- role_id 11,7 to 7 -- SPI-3091
		FROM prop_share_collectors
		WHERE collector_id = cur_psc.collector_id
		AND property_id = cur_psc.property_id
		AND right_type_id = cur_psc.right_type_id
		AND role_id NOT IN(11,7)
		AND end_date_active IS NULL;

		SELECT COUNT(1) INTO m_rollup_23 -- role_id 13,11 to 11 -- SPI-3091
		FROM prop_share_collectors
		WHERE collector_id = cur_psc.collector_id
		AND property_id = cur_psc.property_id
		AND right_type_id = cur_psc.right_type_id
		AND role_id NOT IN(13,11)
		AND end_date_active IS NULL;

		SELECT COUNT(1) INTO m_rollup_24 -- role_id 15,11 to 11 -- SPI-3091
		FROM prop_share_collectors
		WHERE collector_id = cur_psc.collector_id
		AND property_id = cur_psc.property_id
		AND right_type_id = cur_psc.right_type_id
		AND role_id NOT IN(15,11)
		AND end_date_active IS NULL;

		SELECT COUNT(1) INTO m_rollup_16 -- role_id 10,11,7 to 7 -- SPI-3091
		FROM prop_share_collectors
		WHERE collector_id = cur_psc.collector_id
		AND property_id = cur_psc.property_id
		AND right_type_id = cur_psc.right_type_id
		AND role_id NOT IN(10,11,7)
		AND end_date_active IS NULL;

		SELECT COUNT(1) INTO m_rollup_18 -- role_id 7,9,10 to 7 -- SPI-3091
		FROM prop_share_collectors
		WHERE collector_id = cur_psc.collector_id
		AND property_id = cur_psc.property_id
		AND right_type_id = cur_psc.right_type_id
		AND role_id NOT IN(7,9,10)
		AND end_date_active IS NULL;

		SELECT COUNT(1) INTO m_rollup_19 -- role_id 7,9 to 7 -- SPI-3091
		FROM prop_share_collectors
		WHERE collector_id = cur_psc.collector_id
		AND property_id = cur_psc.property_id
		AND right_type_id = cur_psc.right_type_id
		AND role_id NOT IN(7,9)
		AND end_date_active IS NULL;

		SELECT COUNT(1) INTO m_rollup_20 -- role_id 5,6 to 6 -- SPI-3091
		FROM prop_share_collectors
		WHERE collector_id = cur_psc.collector_id
		AND property_id = cur_psc.property_id
		AND right_type_id = cur_psc.right_type_id
		AND role_id NOT IN(5,6)
		AND end_date_active IS NULL;

		SELECT COUNT(1) INTO m_rollup_21 -- role_id 10,14 to 14 -- SPI-3091
		FROM prop_share_collectors
		WHERE collector_id = cur_psc.collector_id
		AND property_id = cur_psc.property_id
		AND right_type_id = cur_psc.right_type_id
		AND role_id NOT IN(10,14)
		AND end_date_active IS NULL;

		SELECT COUNT(1) INTO m_rollup_22 -- role_id 8,9,13 to 8 -- SPI-3091
		FROM prop_share_collectors
		WHERE collector_id = cur_psc.collector_id
		AND property_id = cur_psc.property_id
		AND right_type_id = cur_psc.right_type_id
		AND role_id NOT IN(8,9,13)
		AND end_date_active IS NULL;

		SELECT COUNT(1) INTO m_rollup_5 -- role_id 10,8 to 8 -- SPI-3091
		FROM prop_share_collectors
		WHERE collector_id = cur_psc.collector_id
		AND property_id = cur_psc.property_id
		AND right_type_id = cur_psc.right_type_id
		AND role_id NOT IN(10,8)
		AND end_date_active IS NULL;

		SELECT COUNT(1) INTO m_rollup_6 -- role_id 9,11 to 11 -- SPI-3091
		FROM prop_share_collectors
		WHERE collector_id = cur_psc.collector_id
		AND property_id = cur_psc.property_id
		AND right_type_id = cur_psc.right_type_id
		AND role_id NOT IN(9,11)
		AND end_date_active IS NULL;

		SELECT COUNT(1) INTO m_rollup_7 -- role_id 11,8 to 8 -- SPI-3091
		FROM prop_share_collectors
		WHERE collector_id = cur_psc.collector_id
		AND property_id = cur_psc.property_id
		AND right_type_id = cur_psc.right_type_id
		AND role_id NOT IN(11,8)
		AND end_date_active IS NULL;

		SELECT COUNT(1) INTO m_rollup_4 -- role_id 2,4,6 to 4 -- SPI-3091
		FROM prop_share_collectors
		WHERE collector_id = cur_psc.collector_id
		AND property_id = cur_psc.property_id
		AND right_type_id = cur_psc.right_type_id
		AND role_id NOT IN(2,4,6)
		AND end_date_active IS NULL;

		SELECT COUNT(1) INTO m_rollup_8 -- role_id 6,2 to 2 -- SPI-3091
		FROM prop_share_collectors
		WHERE collector_id = cur_psc.collector_id
		AND property_id = cur_psc.property_id
		AND right_type_id = cur_psc.right_type_id
		AND role_id NOT IN(6,2)
		AND end_date_active IS NULL;

		SELECT COUNT(1) INTO m_rollup_9 -- role_id 9,8 to 8 -- SPI-3091
		FROM prop_share_collectors
		WHERE collector_id = cur_psc.collector_id
		AND property_id = cur_psc.property_id
		AND right_type_id = cur_psc.right_type_id
		AND role_id NOT IN(9,8)
		AND end_date_active IS NULL;

		SELECT COUNT(1) INTO m_rollup_11 -- role_id 8,9,10 to 8 -- SPI-3091
		FROM prop_share_collectors
		WHERE collector_id = cur_psc.collector_id
		AND property_id = cur_psc.property_id
		AND right_type_id = cur_psc.right_type_id
		AND role_id NOT IN(8,9,10)
		AND end_date_active IS NULL;

		SELECT COUNT(1) INTO m_rollup_12 -- role_id 8,9,10,11 to 8 -- SPI-3091
		FROM prop_share_collectors
		WHERE collector_id = cur_psc.collector_id
		AND property_id = cur_psc.property_id
		AND right_type_id = cur_psc.right_type_id
		AND role_id NOT IN(8,9,10,11)
		AND end_date_active IS NULL;

		SELECT COUNT(1) INTO m_rollup_13 -- role_id 8,9,11 to 8 -- SPI-3091
		FROM prop_share_collectors
		WHERE collector_id = cur_psc.collector_id
		AND property_id = cur_psc.property_id
		AND right_type_id = cur_psc.right_type_id
		AND role_id NOT IN(8,9,11)
		AND end_date_active IS NULL;

		IF(m_role_id_count = 2 AND m_rollup = 0) THEN -- role_id 2 to 4
			m_role_id_to_use := 4;
			m_proceed := 1;
		ELSIF(m_role_id_count = 3 AND m_rollup_10 = 0) THEN	-- role_id 6,4 to 4 if share is on 4	
			SELECT NVL(SUM(share_percentage), 0) INTO m_share_on_role_4
			FROM prop_share_collectors psc
			WHERE collector_id = cur_psc.collector_id
			AND property_id = cur_psc.property_id
			AND right_type_id = cur_psc.right_type_id
			AND role_id = 4
			AND end_date_active IS NULL;

			SELECT NVL(SUM(share_percentage), 0) INTO m_share_on_non_role_4
			FROM prop_share_collectors psc
			WHERE collector_id = cur_psc.collector_id
			AND property_id = cur_psc.property_id
			AND right_type_id = cur_psc.right_type_id
			AND role_id != 4
			AND end_date_active IS NULL;

			IF(m_share_on_role_4 > 0 AND m_share_on_non_role_4 = 0) THEN
				m_role_id_to_use := 4;
				m_proceed := 1;	
			END IF;	
		ELSIF(m_role_id_count = 2 AND m_rollup_17 = 0) THEN	-- role_id 5,4 to 4	
			m_role_id_to_use := 4;
			m_proceed := 1;	
		ELSIF(m_role_id_count = 2 AND m_rollup_8 = 0) THEN	-- role_id 6,2 to 2	
			m_role_id_to_use := 2;
			m_proceed := 1;		
		ELSIF(m_role_id_count = 3 AND m_rollup_4 = 0) THEN	-- role_id 2,4,6 to 4 if share is on 4	
			SELECT NVL(SUM(share_percentage), 0) INTO m_share_on_role_4
			FROM prop_share_collectors psc
			WHERE collector_id = cur_psc.collector_id
			AND property_id = cur_psc.property_id
			AND right_type_id = cur_psc.right_type_id
			AND role_id = 4
			AND end_date_active IS NULL;

			SELECT NVL(SUM(share_percentage), 0) INTO m_share_on_non_role_4
			FROM prop_share_collectors psc
			WHERE collector_id = cur_psc.collector_id
			AND property_id = cur_psc.property_id
			AND right_type_id = cur_psc.right_type_id
			AND role_id != 4
			AND end_date_active IS NULL;

			IF(m_share_on_role_4 > 0 AND m_share_on_non_role_4 = 0) THEN
				m_role_id_to_use := 4;
				m_proceed := 1;	
			END IF;	
		ELSIF(m_role_id_count = 2 AND m_rollup_1 = 0) THEN	-- role_id 10 to 11
			m_role_id_to_use := 11;
			m_proceed := 1;	
		ELSIF(m_role_id_count = 2 AND m_rollup_2 = 0) THEN	-- role_id 9,10 to 11	
			m_role_id_to_use := 11;
			m_proceed := 1;			
		ELSIF(m_role_id_count = 2 AND m_rollup_5 = 0) THEN	-- role_id 10,8 to 8	
			m_role_id_to_use := 8;
			m_proceed := 1;	
		ELSIF(m_role_id_count = 2 AND m_rollup_6 = 0) THEN	-- role_id 9,11 to 11	
			m_role_id_to_use := 11;
			m_proceed := 1;		
		ELSIF(m_role_id_count = 2 AND m_rollup_7 = 0) THEN	-- role_id 11,8 to 8	
			m_role_id_to_use := 8;
			m_proceed := 1;	
		ELSIF(m_role_id_count = 2 AND m_rollup_20 = 0) THEN	-- role_id 5,6 to 6
			m_role_id_to_use := 6;
			m_proceed := 1;		
		ELSIF(m_role_id_count = 2 AND m_rollup_9 = 0) THEN	-- role_id 9,8 to 8	
			m_role_id_to_use := 8;
			m_proceed := 1;	
		ELSIF(m_role_id_count = 2 AND m_rollup_19 = 0) THEN	-- role_id 9,7 to 7
			m_role_id_to_use := 7;
			m_proceed := 1;	
		ELSIF(m_role_id_count = 3 AND m_rollup_22 = 0) THEN	-- role_id 8,9,13 to 8	
			m_role_id_to_use := 8;
			m_proceed := 1;		
		ELSIF(m_role_id_count = 2 AND m_rollup_14 = 0) THEN	-- role_id 10,7 to 7
			m_role_id_to_use := 7;
			m_proceed := 1;	
		ELSIF(m_role_id_count = 2 AND m_rollup_21 = 0) THEN	-- role_id 10,14 to 14
			m_role_id_to_use := 14;
			m_proceed := 1;	
		ELSIF(m_role_id_count = 2 AND m_rollup_15 = 0) THEN	-- role_id 11,7 to 7
			m_role_id_to_use := 7;
			m_proceed := 1;
		ELSIF(m_role_id_count = 2 AND m_rollup_23 = 0) THEN	-- role_id 13,11 to 11
			m_role_id_to_use := 11;
			m_proceed := 1;
		ELSIF(m_role_id_count = 2 AND m_rollup_24 = 0) THEN	-- role_id 15,11 to 11
			m_role_id_to_use := 11;
			m_proceed := 1;
		ELSIF(m_role_id_count = 3 AND m_rollup_18 = 0) THEN	-- role_id 9,10,7 to 7
			m_role_id_to_use := 7;
			m_proceed := 1;	
		ELSIF(m_role_id_count = 3 AND m_rollup_16 = 0) THEN	-- role_id 10,11,7 to 7
			m_role_id_to_use := 7;
			m_proceed := 1;	
		ELSIF(m_role_id_count = 3 AND m_rollup_3 = 0) THEN	-- role_id 9,10,11 to 11	
			m_role_id_to_use := 11;
			m_proceed := 1;		
		ELSIF(m_role_id_count = 3 AND m_rollup_11 = 0) THEN	-- role_id 8,9,10 to 8	
			m_role_id_to_use := 8;
			m_proceed := 1;	
		ELSIF(m_role_id_count = 4 AND m_rollup_12 = 0) THEN	-- role_id 8,9,10,11 to 8	
			m_role_id_to_use := 8;
			m_proceed := 1;
		ELSIF(m_role_id_count = 3 AND m_rollup_13 = 0) THEN	-- role_id 8,9,11 to 8	
			m_role_id_to_use := 8;
			m_proceed := 1;		
		ELSIF(m_role_id_count = 1) THEN
        BEGIN
			SELECT DISTINCT role_id INTO m_role_id_to_use
			FROM prop_share_collectors 
			WHERE collector_id = cur_psc.collector_id
			AND property_id = cur_psc.property_id
			AND right_type_id = cur_psc.right_type_id
			AND end_date_active IS NULL;
    --    EXCEPTION WHEN NO_DATA_FOUND THEN
    --        m_role_id_to_use := 4;
        END;    
			m_proceed := 1;
		ELSE
			m_proceed := 0;
		END IF;

		IF(m_proceed = 1) THEN
			m_count := m_count + 1;
			SELECT SUM(share_percentage),MAX(affiliated_society_id) INTO m_total_share_per_owner,m_aff_society_id
			FROM prop_share_collectors
			WHERE  end_date_active IS NULL
			AND collector_id = cur_psc.collector_id
			AND property_id = cur_psc.property_id
			AND right_type_id = cur_psc.right_type_id;

			SELECT COUNT(1) INTO m_authoritative_flag_cnt
			FROM prop_share_collectors
			WHERE end_date_active IS NULL
			AND collector_id = cur_psc.collector_id
			AND property_id = cur_psc.property_id
			AND right_type_id = cur_psc.right_type_id
			AND authoritative_flag = 'Y';

			BEGIN
				SELECT submitter_internal_number INTO m_submitter_internal_number
				FROM prop_share_collectors 
				WHERE share_collector_id IN(
											SELECT MAX(share_collector_id)
											FROM prop_share_collectors
											WHERE end_date_active IS NULL
											AND collector_id = cur_psc.collector_id
											AND property_id = cur_psc.property_id
											AND right_type_id = cur_psc.right_type_id
											AND submitter_internal_number IS NOT NULL);
			EXCEPTION WHEN others THEN
				m_submitter_internal_number := NULL;
			END;

			IF(m_authoritative_flag_cnt > 0) THEN
				m_authoritative_flag := 'Y';
			ELSE
				m_authoritative_flag := 'N';
			END IF;

			INSERT INTO prop_share_collectors(share_collector_id,collector_id,share_percentage,created_by,creation_date,last_updated_by,last_update_date,right_type_id,property_id,role_id,affiliated_society_id,start_date_active,end_date_active,hold_type_id,hold_reason,submitter_internal_number,authoritative_flag)
			VALUES(prop_share_collector_s.NEXTVAL,cur_psc.collector_id,m_total_share_per_owner,'HFALIC-619-ROLLUP',SYSDATE,'HFALIC-619-ROLLUP',SYSDATE,cur_psc.right_type_id,cur_psc.property_id,m_role_id_to_use,m_aff_society_id,NVL(cur_psc.min_start_date, SYSDATE),NULL,NULL,NULL,m_submitter_internal_number,m_authoritative_flag)
			RETURNING share_collector_id INTO m_share_collector_id_new;

			INSERT INTO prop_share_coll_territories
							(
								collector_territory_id, 
								include_exclude_code, 
								sequence_number, 
								share_collector_id, 
								territory_id,
								created_by,
								creation_date,
								last_updated_by,
								last_update_date
							)
							SELECT 
								prop_share_coll_territories_s.NEXTVAL, 
								include_exclude_code, 
								sequence_number, 
								m_share_collector_id_new, 
								territory_id,
								'HFALIC-619-ROLLUP',
								SYSDATE,
								'HFALIC-619-ROLLUP',
								SYSDATE								
							FROM prop_share_coll_territories psct
							WHERE share_collector_Id IN(SELECT share_collector_id
														FROM prop_share_collectors
														WHERE end_date_active IS NULL
														AND collector_id = cur_psc.collector_id
														AND property_id = cur_psc.property_id
														AND right_type_id = cur_psc.right_type_id)
							AND NOT EXISTS(SELECT 1 -- SPI-4036
											FROM prop_share_coll_territories psct1
											WHERE psct1.include_exclude_code = psct.include_exclude_code
											AND psct1.territory_id = psct.territory_id
											AND psct1.share_collector_id = m_share_collector_id_new
											AND NVL(psct1.sequence_number, '-99') = NVL(psct.sequence_number, '-99'));

			FOR cur_psa IN(SELECT * FROM prop_share_admins WHERE share_collector_id IN(SELECT share_collector_id
																						FROM prop_share_collectors
																						WHERE end_date_active IS NULL
																						AND collector_id = cur_psc.collector_id
																						AND property_id = cur_psc.property_id
																						AND right_type_id = cur_psc.right_type_id)
																						AND share_collector_id != m_share_collector_id_new
																						AND end_date_active IS NULL)
			LOOP
				m_admin_exists := 1;
				IF(cur_psa.admin_id = 602200) THEN
					INSERT INTO prop_share_admins
					SELECT prop_share_admins_s.NEXTVAL,m_share_collector_id_new,cur_psa.admin_id,cur_psa.admin_share_percent,SYSDATE,NULL,'HFALIC-619-ROLLUP',SYSDATE,'HFALIC-619-ROLLUP',SYSDATE,NVL(role_id, m_role_id_to_use),submitter_internal_number
					FROM prop_share_admins WHERE share_admin_id = cur_psa.share_admin_id;

					INSERT INTO prop_share_admin_territories
							(
								admin_territory_id, 
								include_exclude_code, 
								sequence_number, 
								share_admin_id, 
								territory_id
							)
							SELECT 
								prop_share_admin_territories_s.NEXTVAL,
								include_exclude_code,
								sequence_number,
								prop_share_admins_s.CURRVAL,
								territory_id
							FROM prop_share_admin_territories
							WHERE share_admin_id = cur_psa.share_admin_id;

						UPDATE prop_share_admins
						SET end_date_active = SYSDATE - 1
						,last_updated_by = 'HFALIC-619-ROLLUP'
						,last_update_date = SYSDATE
						WHERE share_admin_id = cur_psa.share_admin_id;				
				ELSE
					m_existing_share_admin_id := NULL;
					BEGIN
						SELECT share_admin_id INTO m_existing_share_admin_id
						FROM prop_share_admins
						WHERE share_collector_id = m_share_collector_id_new
						AND admin_id = cur_psa.admin_id
						AND end_date_active IS NULL;
					EXCEPTION WHEN no_data_found THEN
						m_existing_share_admin_id := NULL;
					END;

					IF(m_existing_share_admin_id IS NOT NULL) THEN
						UPDATE prop_share_admins
						SET admin_share_percent = admin_share_percent + cur_psa.admin_share_percent
						,last_updated_by = 'HFALIC-619-ROLLUP'
						,last_update_date = SYSDATE
						WHERE share_admin_id = m_existing_share_admin_id;

						INSERT INTO prop_share_admin_territories
								(
									admin_territory_id, 
									include_exclude_code, 
									sequence_number, 
									share_admin_id, 
									territory_id
								)
								SELECT 
									prop_share_admin_territories_s.NEXTVAL,
									t1.include_exclude_code,
									t1.sequence_number,
									m_existing_share_admin_id,
									t1.territory_id
								FROM prop_share_admin_territories t1
								WHERE t1.share_admin_id = cur_psa.share_admin_id
								AND NOT EXISTS(SELECT 1 FROM prop_share_admin_territories t2 WHERE t2.share_admin_id = m_existing_share_admin_id AND t2.include_exclude_code = t1.include_exclude_code AND t2.territory_id = t1.territory_id) ;
					ELSE
						IF(cur_psa.admin_share_percent != 0) THEN
							INSERT INTO prop_share_admins
							SELECT prop_share_admins_s.NEXTVAL,m_share_collector_id_new,cur_psa.admin_id,cur_psa.admin_share_percent,SYSDATE,NULL,'HFALIC-619-ROLLUP',SYSDATE,'HFALIC-619-ROLLUP',SYSDATE,role_id,NULL
							FROM prop_share_admins WHERE share_admin_id = cur_psa.share_admin_id;

							INSERT INTO prop_share_admin_territories
								(
									admin_territory_id, 
									include_exclude_code, 
									sequence_number, 
									share_admin_id, 
									territory_id
								)
								SELECT 
									prop_share_admin_territories_s.NEXTVAL,
									include_exclude_code,
									sequence_number,
									prop_share_admins_s.CURRVAL,
									territory_id
								FROM prop_share_admin_territories
								WHERE share_admin_id = cur_psa.share_admin_id;
						END IF;	
					END IF;

					UPDATE prop_share_admins
						SET end_date_active = SYSDATE - 1
						,last_updated_by = 'HFALIC-619-ROLLUP'
						,last_update_date = SYSDATE
						WHERE share_admin_id = cur_psa.share_admin_id;	

				END IF;

			END LOOP;

			-- Check for admin total share < owner total share


			IF(m_admin_exists = 1) THEN
				SELECT SUM(admin_share_percent) INTO m_new_admins_total_per
				FROM prop_share_admins
				WHERE share_collector_id = m_share_collector_id_new
				AND end_date_active IS NULL;

				IF(m_new_admins_total_per < m_total_share_per_owner) THEN
					SELECT COUNT(aff_hier_id) INTO m_self_hier_count
					FROM aff_hierarchy 
					WHERE child_party_id = parent_party_id
					AND child_party_id = cur_psc.collector_id
					AND hier_level = 0
					AND TRUNC(NVL(end_date,SYSDATE+1)) > TRUNC(SYSDATE)
					AND nowed_aff_hier_id IS NULL;

					IF(m_self_hier_count = 0) THEN
						spi_create_self_aff_hierarchy(cur_psc.collector_id);
						DEBUG_WRITE('m_self_hier_count = '||m_self_hier_count||' cur_psc.collector_id = '||cur_psc.collector_id, 'HFALIC-619-ROLLUP-O');
					END IF;

					m_existing_share_admin_id := NULL;
					BEGIN	
						SELECT share_admin_id INTO m_existing_share_admin_id
						FROM prop_share_admins
						WHERE share_collector_id = m_share_collector_id_new
						AND admin_id = cur_psc.collector_id
						AND end_date_active IS NULL;
					EXCEPTION WHEN no_data_found THEN
						m_existing_share_admin_id := NULL;
					END;

					IF(m_existing_share_admin_id IS NULL) THEN
						INSERT INTO prop_share_admins(share_admin_id,share_collector_id,admin_id,admin_share_percent,start_date_active,end_date_active,created_by,creation_date,last_updated_by,last_update_date,role_id,submitter_internal_number)
						VALUES(prop_share_admins_s.NEXTVAL,m_share_collector_id_new,cur_psc.collector_id,(m_total_share_per_owner - m_new_admins_total_per),SYSDATE,NULL,'HFALIC-619-ROLLUP',SYSDATE,'HFALIC-619-ROLLUP',SYSDATE,m_role_id_to_use,NULL);

						INSERT INTO prop_share_admin_territories
								(
									admin_territory_id, 
									include_exclude_code, 
									sequence_number, 
									share_admin_id, 
									territory_id
								)
								SELECT 
									prop_share_admin_territories_s.NEXTVAL,
									include_exclude_code,
									sequence_number,
									prop_share_admins_s.CURRVAL,
									territory_id
								FROM prop_share_coll_territories
								WHERE share_collector_id = m_share_collector_id_new;
					ELSE
						UPDATE prop_share_admins
						SET admin_share_percent = admin_share_percent + (m_total_share_per_owner - m_new_admins_total_per)
						,last_updated_by = 'HFALIC-619-ROLLUP'
						,last_update_date = SYSDATE
						WHERE share_admin_id = m_existing_share_admin_id;
					END IF;					
				END IF;
			END IF;

			m_admin_exists := 0;

			UPDATE prop_share_collectors 
			SET end_date_active = SYSDATE - 1
				,last_updated_by = 'HFALIC-619-ROLLUP'
				,last_update_date = SYSDATE
			WHERE end_date_active IS NULL
			AND collector_id = cur_psc.collector_id
			AND property_id = cur_psc.property_id
			AND right_type_id = cur_psc.right_type_id
			AND share_collector_id != m_share_collector_id_new; 

			DEBUG_WRITE('Inserted: '||m_share_collector_id_new, 'HFALIC-619-ROLLUP-O');

			-- Delete admins id if owner = admin(and no other admins exist) 

			SELECT COUNT(1) INTO m_same_owner_admin_cnt
			FROM prop_share_admins
			WHERE admin_id IN(SELECT collector_id FROM prop_share_collectors WHERE share_collector_Id = m_share_collector_id_new
							  UNION ALL
							  SELECT parent_party_id FROM aff_hierarchy WHERE child_party_id = cur_psc.collector_id AND relate_account_number IS NULL AND hier_level = 0 AND end_date IS NULL AND nowed_aff_hier_id IS NULL)
			AND share_collector_id = m_share_collector_id_new
			AND end_date_active IS NULL;

			SELECT COUNT(1) INTO m_diff_owner_admin_cnt
			FROM prop_share_admins
			WHERE admin_id NOT IN(SELECT collector_id FROM prop_share_collectors WHERE share_collector_Id = m_share_collector_id_new
								  UNION ALL
							      SELECT parent_party_id FROM aff_hierarchy WHERE child_party_id = cur_psc.collector_id AND relate_account_number IS NULL AND hier_level = 0 AND end_date IS NULL AND nowed_aff_hier_id IS NULL)
			AND share_collector_id = m_share_collector_id_new
			AND end_date_active IS NULL;

			IF(m_same_owner_admin_cnt > 0 AND m_diff_owner_admin_cnt = 0) THEN
				UPDATE prop_share_admins
				SET end_date_active = SYSDATE - 1
				,last_updated_by = 'HFALIC-619-ROLLUP'
				,last_update_date = SYSDATE
				WHERE share_collector_id = m_share_collector_id_new;
			END IF;

			IF(m_count >= 100) THEN
				  COMMIT;
				  m_count := 0;
			END IF;	
		END IF;	
	END LOOP;

	-- For cases where role ids like 2,2,11
	m_count := 0;
	FOR cur_psc IN(SELECT psc.collector_id, psc.property_id, psc.right_type_id, psc.role_id, MIN(start_date_active) min_start_date
					FROM prop_share_collectors psc, prop_properties pp
					WHERE psc.end_date_active IS NULL
					AND psc.property_id = pp.property_id   
					AND pp.property_type_id = 1
					AND pp.source_id = 7
					AND pp.status = 'A' 
					AND psc.collector_id != 602200
					AND psc.right_type_id IN(1,2)
					AND NOT EXISTS(SELECT 1 FROM prop_share_admins psa WHERE psa.share_collector_id = psc.share_collector_id AND admin_id = 602200 
					AND psa.end_date_active IS NULL)
				--	AND (SELECT COUNT(DISTINCT role_id) FROM prop_share_admins psa WHERE psa.share_collector_id = psc.share_collector_id AND TRUNC(NVL(psa.end_date_active,SYSDATE+1)) > TRUNC(SYSDATE)) <= 1
					AND (SELECT SUM(share_percentage) FROM prop_share_collectors psc1 WHERE psc.collector_id = psc1.collector_id AND psc.property_id = psc1.property_id AND psc.right_type_id = psc1.right_type_id AND TRUNC(NVL(psc1.end_date_active,SYSDATE+1)) > TRUNC(SYSDATE)) <= 100
					GROUP BY psc.collector_id, psc.property_id, psc.right_type_id, psc.role_id
					HAVING COUNT(*) > 1)
	LOOP
		SELECT COUNT(DISTINCT role_id) INTO m_role_id_count
		FROM prop_share_collectors 
		WHERE collector_id = cur_psc.collector_id
		AND property_id = cur_psc.property_id
		AND right_type_id = cur_psc.right_type_id
		AND role_id = cur_psc.role_id
		AND end_date_active IS NULL;

		IF(m_role_id_count = 1) THEN
			m_role_id_to_use := cur_psc.role_id;
			m_proceed := 1;
		END IF;

		IF(m_proceed = 1) THEN
			m_count := m_count + 1;
			SELECT SUM(share_percentage),MAX(affiliated_society_id) INTO m_total_share_per_owner,m_aff_society_id
			FROM prop_share_collectors
			WHERE  end_date_active IS NULL
			AND collector_id = cur_psc.collector_id
			AND property_id = cur_psc.property_id
			AND right_type_id = cur_psc.right_type_id
			AND role_id = cur_psc.role_id;

			SELECT COUNT(1) INTO m_authoritative_flag_cnt
			FROM prop_share_collectors
			WHERE end_date_active IS NULL
			AND collector_id = cur_psc.collector_id
			AND property_id = cur_psc.property_id
			AND right_type_id = cur_psc.right_type_id
			AND role_id = cur_psc.role_id
			AND authoritative_flag = 'Y';

			BEGIN
				SELECT submitter_internal_number INTO m_submitter_internal_number
				FROM prop_share_collectors 
				WHERE share_collector_id IN(
											SELECT MAX(share_collector_id)
											FROM prop_share_collectors
											WHERE end_date_active IS NULL
											AND collector_id = cur_psc.collector_id
											AND property_id = cur_psc.property_id
											AND right_type_id = cur_psc.right_type_id
											AND role_id = cur_psc.role_id
											AND submitter_internal_number IS NOT NULL);
			EXCEPTION WHEN others THEN
				m_submitter_internal_number := NULL;
			END;

			IF(m_authoritative_flag_cnt > 0) THEN
				m_authoritative_flag := 'Y';
			ELSE
				m_authoritative_flag := 'N';
			END IF;

			INSERT INTO prop_share_collectors(share_collector_id,collector_id,share_percentage,created_by,creation_date,last_updated_by,last_update_date,right_type_id,property_id,role_id,affiliated_society_id,start_date_active,end_date_active,hold_type_id,hold_reason,submitter_internal_number,authoritative_flag)
			VALUES(prop_share_collector_s.NEXTVAL,cur_psc.collector_id,m_total_share_per_owner,'HFALIC-619-ROLLUP',SYSDATE,'HFALIC-619-ROLLUP',SYSDATE,cur_psc.right_type_id,cur_psc.property_id,m_role_id_to_use,m_aff_society_id,NVL(cur_psc.min_start_date, SYSDATE),NULL,NULL,NULL,m_submitter_internal_number,m_authoritative_flag)
			RETURNING share_collector_id INTO m_share_collector_id_new;

			INSERT INTO prop_share_coll_territories
							(
								collector_territory_id, 
								include_exclude_code, 
								sequence_number, 
								share_collector_id, 
								territory_id,
								created_by,
								creation_date,
								last_updated_by,
								last_update_date
							)
							SELECT 
								prop_share_coll_territories_s.NEXTVAL, 
								include_exclude_code, 
								sequence_number, 
								m_share_collector_id_new, 
								territory_id,
								'HFALIC-619-ROLLUP',
								SYSDATE,
								'HFALIC-619-ROLLUP',
								SYSDATE
							FROM prop_share_coll_territories psct
							WHERE share_collector_Id IN(SELECT share_collector_id
														FROM prop_share_collectors
														WHERE end_date_active IS NULL
														AND collector_id = cur_psc.collector_id
														AND property_id = cur_psc.property_id
														AND right_type_id = cur_psc.right_type_id
														AND role_id = cur_psc.role_id)
							AND NOT EXISTS(SELECT 1 -- SPI-4036
											FROM prop_share_coll_territories psct1
											WHERE psct1.include_exclude_code = psct.include_exclude_code
											AND psct1.territory_id = psct.territory_id
											AND psct1.share_collector_id = m_share_collector_id_new
											AND NVL(psct1.sequence_number, '-99') = NVL(psct.sequence_number, '-99'));							

			FOR cur_psa IN(SELECT * FROM prop_share_admins WHERE share_collector_id IN(SELECT share_collector_id
																						FROM prop_share_collectors
																						WHERE end_date_active IS NULL
																						AND collector_id = cur_psc.collector_id
																						AND property_id = cur_psc.property_id
																						AND right_type_id = cur_psc.right_type_id
																						AND role_id = cur_psc.role_id)
							AND share_collector_id != m_share_collector_id_new
							AND end_date_active IS NULL)
			LOOP
				m_admin_exists := 1;
				IF(cur_psa.admin_id = 602200) THEN
					INSERT INTO prop_share_admins
					SELECT prop_share_admins_s.NEXTVAL,m_share_collector_id_new,cur_psa.admin_id,cur_psa.admin_share_percent,SYSDATE,NULL,'HFALIC-619-ROLLUP',SYSDATE,'HFALIC-619-ROLLUP',SYSDATE,NVL(role_id, m_role_id_to_use),submitter_internal_number
					FROM prop_share_admins WHERE share_admin_id = cur_psa.share_admin_id;

					INSERT INTO prop_share_admin_territories
							(
								admin_territory_id, 
								include_exclude_code, 
								sequence_number, 
								share_admin_id, 
								territory_id
							)
							SELECT 
								prop_share_admin_territories_s.NEXTVAL,
								include_exclude_code,
								sequence_number,
								prop_share_admins_s.CURRVAL,
								territory_id
							FROM prop_share_admin_territories
							WHERE share_admin_id = cur_psa.share_admin_id;

						UPDATE prop_share_admins
						SET end_date_active = SYSDATE - 1
						,last_updated_by = 'HFALIC-619-ROLLUP'
						,last_update_date = SYSDATE
						WHERE share_admin_id = cur_psa.share_admin_id;				
				ELSE
					m_existing_share_admin_id := NULL;
					BEGIN
						SELECT share_admin_id INTO m_existing_share_admin_id
						FROM prop_share_admins
						WHERE share_collector_id = m_share_collector_id_new
						AND admin_id = cur_psa.admin_id
						AND end_date_active IS NULL;
					EXCEPTION WHEN no_data_found THEN
						m_existing_share_admin_id := NULL;
					END;

					IF(m_existing_share_admin_id IS NOT NULL) THEN
						UPDATE prop_share_admins
						SET admin_share_percent = admin_share_percent + cur_psa.admin_share_percent
						,last_updated_by = 'HFALIC-619-ROLLUP'
						,last_update_date = SYSDATE
						WHERE share_admin_id = m_existing_share_admin_id;

						INSERT INTO prop_share_admin_territories
								(
									admin_territory_id, 
									include_exclude_code, 
									sequence_number, 
									share_admin_id, 
									territory_id
								)
								SELECT 
									prop_share_admin_territories_s.NEXTVAL,
									t1.include_exclude_code,
									t1.sequence_number,
									m_existing_share_admin_id,
									t1.territory_id
								FROM prop_share_admin_territories t1
								WHERE t1.share_admin_id = cur_psa.share_admin_id
								AND NOT EXISTS(SELECT 1 FROM prop_share_admin_territories t2 WHERE t2.share_admin_id = m_existing_share_admin_id AND t2.include_exclude_code = t1.include_exclude_code AND t2.territory_id = t1.territory_id) ;
					ELSE
						IF(cur_psa.admin_share_percent != 0) THEN
							INSERT INTO prop_share_admins
							SELECT prop_share_admins_s.NEXTVAL,m_share_collector_id_new,cur_psa.admin_id,cur_psa.admin_share_percent,SYSDATE,NULL,'HFALIC-619-ROLLUP',SYSDATE,'HFALIC-619-ROLLUP',SYSDATE,role_id,NULL
							FROM prop_share_admins WHERE share_admin_id = cur_psa.share_admin_id;

							INSERT INTO prop_share_admin_territories
								(
									admin_territory_id, 
									include_exclude_code, 
									sequence_number, 
									share_admin_id, 
									territory_id
								)
								SELECT 
									prop_share_admin_territories_s.NEXTVAL,
									include_exclude_code,
									sequence_number,
									prop_share_admins_s.CURRVAL,
									territory_id
								FROM prop_share_admin_territories
								WHERE share_admin_id = cur_psa.share_admin_id;
						END IF;	
					END IF;

					UPDATE prop_share_admins
						SET end_date_active = SYSDATE - 1
						,last_updated_by = 'HFALIC-619-ROLLUP'
						,last_update_date = SYSDATE
						WHERE share_admin_id = cur_psa.share_admin_id;	

				END IF;

			END LOOP;

			-- Check for admin total share < owner total share


			IF(m_admin_exists = 1) THEN
				SELECT SUM(admin_share_percent) INTO m_new_admins_total_per
				FROM prop_share_admins
				WHERE share_collector_id = m_share_collector_id_new
				AND end_date_active IS NULL;

				IF(m_new_admins_total_per < m_total_share_per_owner) THEN
					SELECT COUNT(aff_hier_id) INTO m_self_hier_count
					FROM aff_hierarchy 
					WHERE child_party_id = parent_party_id
					AND child_party_id = cur_psc.collector_id
					AND hier_level = 0
					AND TRUNC(NVL(end_date,SYSDATE+1)) > TRUNC(SYSDATE)
					AND nowed_aff_hier_id IS NULL;

					IF(m_self_hier_count = 0) THEN
						spi_create_self_aff_hierarchy(cur_psc.collector_id);
						DEBUG_WRITE('m_self_hier_count = '||m_self_hier_count||' cur_psc.collector_id = '||cur_psc.collector_id, 'HFALIC-619-ROLLUP-O');
					END IF;

					m_existing_share_admin_id := NULL;
					BEGIN	
						SELECT share_admin_id INTO m_existing_share_admin_id
						FROM prop_share_admins
						WHERE share_collector_id = m_share_collector_id_new
						AND admin_id = cur_psc.collector_id
						AND end_date_active IS NULL;
					EXCEPTION WHEN no_data_found THEN
						m_existing_share_admin_id := NULL;
					END;

					IF(m_existing_share_admin_id IS NULL) THEN
						INSERT INTO prop_share_admins(share_admin_id,share_collector_id,admin_id,admin_share_percent,start_date_active,end_date_active,created_by,creation_date,last_updated_by,last_update_date,role_id,submitter_internal_number)
						VALUES(prop_share_admins_s.NEXTVAL,m_share_collector_id_new,cur_psc.collector_id,(m_total_share_per_owner - m_new_admins_total_per),SYSDATE,NULL,'HFALIC-619-ROLLUP',SYSDATE,'HFALIC-619-ROLLUP',SYSDATE,m_role_id_to_use,NULL);

						INSERT INTO prop_share_admin_territories
								(
									admin_territory_id, 
									include_exclude_code, 
									sequence_number, 
									share_admin_id, 
									territory_id
								)
								SELECT 
									prop_share_admin_territories_s.NEXTVAL,
									include_exclude_code,
									sequence_number,
									prop_share_admins_s.CURRVAL,
									territory_id
								FROM prop_share_coll_territories
								WHERE share_collector_id = m_share_collector_id_new;
					ELSE
						UPDATE prop_share_admins
						SET admin_share_percent = admin_share_percent + (m_total_share_per_owner - m_new_admins_total_per)
						,last_updated_by = 'HFALIC-619-ROLLUP'
						,last_update_date = SYSDATE
						WHERE share_admin_id = m_existing_share_admin_id;
					END IF;					
				END IF;
			END IF;

			m_admin_exists := 0;

			UPDATE prop_share_collectors 
			SET end_date_active = SYSDATE - 1
				,last_updated_by = 'HFALIC-619-ROLLUP'
				,last_update_date = SYSDATE
			WHERE end_date_active IS NULL
			AND collector_id = cur_psc.collector_id
			AND property_id = cur_psc.property_id
			AND right_type_id = cur_psc.right_type_id
			AND role_id = cur_psc.role_id
			AND share_collector_id != m_share_collector_id_new; 

			DEBUG_WRITE('Inserted: '||m_share_collector_id_new, 'HFALIC-619-ROLLUP-O');

			-- Delete admins id if owner = admin(and no other admins exist)

			SELECT COUNT(1) INTO m_same_owner_admin_cnt
			FROM prop_share_admins
			WHERE admin_id IN(SELECT collector_id FROM prop_share_collectors WHERE share_collector_Id = m_share_collector_id_new
							  UNION ALL
							  SELECT parent_party_id FROM aff_hierarchy WHERE child_party_id = cur_psc.collector_id AND relate_account_number IS NULL AND hier_level = 0 AND end_date IS NULL AND nowed_aff_hier_id IS NULL)
			AND share_collector_id = m_share_collector_id_new
			AND end_date_active IS NULL;

			SELECT COUNT(1) INTO m_diff_owner_admin_cnt
			FROM prop_share_admins
			WHERE admin_id NOT IN(SELECT collector_id FROM prop_share_collectors WHERE share_collector_Id = m_share_collector_id_new
								  UNION ALL
								  SELECT parent_party_id FROM aff_hierarchy WHERE child_party_id = cur_psc.collector_id AND relate_account_number IS NULL AND hier_level = 0 AND end_date IS NULL AND nowed_aff_hier_id IS NULL)
			AND share_collector_id = m_share_collector_id_new
			AND end_date_active IS NULL;

			IF(m_same_owner_admin_cnt > 0 AND m_diff_owner_admin_cnt = 0) THEN
				UPDATE prop_share_admins
				SET end_date_active = SYSDATE - 1
				,last_updated_by = 'HFALIC-619-ROLLUP'
				,last_update_date = SYSDATE
				WHERE share_collector_id = m_share_collector_id_new
				AND end_date_active IS NULL;
			END IF;

			IF(m_count >= 100) THEN
				  COMMIT;
				  m_count := 0;
			END IF;	
		END IF;	
	END LOOP;
	COMMIT;
END spi_rollup_owners;


PROCEDURE spi_admin_rollup 
AS
	m_admin_per_total	NUMBER;
    m_count             NUMBER := 0;
	m_share_admin_id	NUMBER;
BEGIN
	FOR cur_red IN(SELECT psc.*
					FROM prop_share_collectors psc, prop_properties pp
					WHERE psc.share_collector_id IN(            
													SELECT share_collector_id FROM prop_share_admins psa 
													WHERE psa.end_date_active IS NULL
													AND psa.admin_id != 602200
													GROUP BY share_collector_id, admin_id
													HAVING COUNT(*) > 1)  
					AND (SELECT SUM(admin_share_percent) FROM prop_share_admins psa WHERE psa.share_collector_id = psc.share_collector_id AND TRUNC(NVL(psa.end_date_active,SYSDATE+1)) > TRUNC(SYSDATE)) <= 100								
					AND psc.end_date_active IS NULL
					AND psc.property_id = pp.property_id  
					AND psc.collector_id != 602200
					AND psc.right_type_id IN(1,2)
					AND pp.property_type_id = 1
					AND pp.source_id = 7
					AND pp.status = 'A')
	LOOP
        m_count := m_count + 1;
		FOR cur_psa IN(SELECT psa.admin_id, SUM(admin_share_percent) total_share, MIN(share_admin_id) min_id, MIN(start_date_active) min_start_date
						FROM prop_share_admins psa 
						WHERE psa.end_date_active IS NULL
						AND psa.share_collector_id = cur_red.share_collector_id
						AND psa.admin_id != 602200
						GROUP BY psa.admin_id
						HAVING COUNT(*) > 1)
		LOOP
			UPDATE prop_share_admins
			SET end_date_active = SYSDATE - 1
			,last_updated_by = 'HFALIC-619-ROLLUP'
			,last_update_date = SYSDATE
			WHERE admin_id = cur_psa.admin_id
			AND share_collector_id = cur_red.share_collector_id
			AND end_date_active IS NULL;

			m_share_admin_id := prop_share_admins_s.NEXTVAL;

			INSERT INTO prop_share_admins
			SELECT m_share_admin_id,cur_red.share_collector_id,cur_psa.admin_id,cur_psa.total_share,cur_psa.min_start_date,NULL,'HFALIC-619-ROLLUP',SYSDATE,'HFALIC-619-ROLLUP',SYSDATE,role_id,NULL
			FROM prop_share_admins WHERE share_admin_id = cur_psa.min_id;

			FOR cur_admin_terr IN(SELECT DISTINCT territory_id, sequence_number, include_exclude_code
									FROM prop_share_admin_territories
									WHERE share_admin_id IN(SELECT share_admin_id FROM prop_share_admins WHERE admin_id = cur_psa.admin_id
																										AND share_collector_id = cur_red.share_collector_id
																										AND end_date_active IS NOT NULL))
			LOOP
				INSERT INTO prop_share_admin_territories
								(
									admin_territory_id, 
									include_exclude_code, 
									sequence_number, 
									share_admin_id, 
									territory_id
								)
				VALUES
								(
									prop_share_admin_territories_s.NEXTVAL,
									cur_admin_terr.include_exclude_code,
									cur_admin_terr.sequence_number,
									m_share_admin_id,
									cur_admin_terr.territory_id
								);
			END LOOP;

		END LOOP;

        IF(m_count >= 1000) THEN
			COMMIT;
			m_count := 0;
		END IF;
    END LOOP;
COMMIT;
END;


Procedure SPI_Process_OWR_Trx_To_70 
As
  Wrk_Property_Id  Prop_Properties.Property_Id%Type;

Begin
  For Arec In (Select *
                 From Spi_Transaction_Status
                Where Source_Id = 403
                      And Transaction_Status = '62'
                      And Selected_Sesac_Property_Id Is Null)
  Loop
    Begin
      Select Prp2.Property_Id
        Into Wrk_Property_Id
        From Prop_Properties Prp1
            ,Prop_Properties Prp2
       Where Prp1.Property_Id = Arec.Property_Id
             And Prp1.Property_Number = Prp2.Property_Number
             And Prp2.Source_Id = 7
             And Prp2.Property_Type_Id = 1
             And Rownum = 1;


	--Create self hierarchy, collection agreements, membership agreements
	---------------------------------------------------------------------------
	For Rec_Psc In (Select Psc.Share_Collector_Id
						  ,Psc.Property_Id
						  ,Psc.Collector_Id
						  ,Psc.Right_Type_Id
					  From Prop_Share_Collectors Psc
					 Where Psc.Property_Id = Wrk_Property_Id
						   And Psc.Right_Type_Id In (1, 2)
						   And Psc.Collector_Id Not In (602200, 7089410)
						   And Psc.End_Date_Active Is Null) 
	Loop

	  --Creating self hierarchies for IPs 
	  SPI_CREATE_SELF_AFF_HIERARCHY(Rec_Psc.collector_id);

	  --Creating Collection Agreements
	  Spi_Collection_Agreements.New_Owner_Added_To_Song(P_Collector_Id => Rec_Psc.Collector_Id
													   ,P_Property_Id => Rec_Psc.Property_Id
													   ,P_Hier_Id   => Null
												   ,P_Source    => 'SPI2389');
    End Loop;

	--Creating Membership Agreements
    For R1 In 1 .. 2 Loop
      Prop_Share_Owner.Add_Remove_Song_Mem_Agg(Wrk_Property_Id, R1);
    End Loop;
	---------------------------------------------------------------------------


      Update Spi_Transaction_Status
         Set Transaction_Status = '70'
            ,Last_Updated_By = 'SPI2389'
       Where Transaction_Status_Id = Arec.Transaction_Status_Id;   

      /*BTLT-567 Added by SC on 14-OCT-2020 */
      Update Owr_Song_Registration
           Set Error_Status   = 'ACCEPTED'
         Where Submission_Id = (Select Submission_Id
                                  From Spi_Transaction_Status
                                 Where Transaction_Status_Id = Arec.Transaction_Status_Id
                                 And Nvl(Transaction_Status, 10) = 70); 

       Update Prop_Properties
         Set Status = 'A'
       Where Property_Id In (Arec.Property_Id,Wrk_Property_Id)
       And status <> 'A';

    Exception
      When No_Data_Found Then
        Null;
    End;

	Commit;
    Dbms_Output.Put_Line(Arec.Transaction_Status_Id||'   '||Arec.Property_Id||'   '||Wrk_Property_Id);
  End Loop;
End;

PROCEDURE FIX_ISWC is

   --
   -- ******************************************************************
   -- Fix invalid ISWC if adding "T" in the very begging makes it Valid  
   -- ****************************************************************** 
   --

   -- ========================================
   -- Declare collection for holding data  
   -- ======================================== 

   type ISWC_Rec is record(
       R_PROPERTY_ID                NUMBER
      ,R_FIXED_ISWC                 VARCHAR2(11)
      ) ;

   type T_ISWC is table of ISWC_Rec
    INDEX BY BINARY_INTEGER;

   ISWC_Data   T_ISWC;

  -- ==============================
  -- Decalre ISWC  Cursor
  -- ==============================

    Cursor ISWC_cur is

  SELECT  pp.property_id
          ,'T' || ps.ISWC        Fixed_ISWC 
    FROM   PROP_PROPERTIES  pp
          ,PROP_SONGS       ps
   WHERE  pp.property_type_id = 1
     AND  pp.status = 'A'
     AND  pp.source_id = 7  
     AND  ps.property_id= pp.property_id
     AND  ps.ISWC IS NOT NULL
     AND  PROP_API.MLC_ISWC_FORMAT_VALIDATE(ps.iswc) = 'INVALID'
     AND  regexp_like(ISWC, '^[^a-zA-Z]*$')                          -- digits only
     AND  PROP_API.MLC_ISWC_FORMAT_VALIDATE('T' || ps.ISWC) = 'VALID'
    ; 

Begin 

-- ======================================================
-- Lopp via ISWC_cur   
-- ======================================================


    OPEN ISWC_cur;

        LOOP

            Fetch ISWC_cur BULK COLLECT INTO ISWC_Data LIMIT 5000;
            EXIT WHEN ISWC_data.count=0;

            -- ============================================== 
            -- Insert Pubs
            -- ============================================== 

            FORALL i IN ISWC_Data.FIRST .. ISWC_Data.LAST
              Update PROP_SONGS
                Set ISWC = ISWC_Data(i).R_FIXED_ISWC,
                    LAST_UPDATED_BY = 'FIX_ISWC', 
                    LAST_UPDATE_DATE = sysdate
               Where property_id = ISWC_Data(i).R_PROPERTY_ID ;

            COMMIT;	

        END LOOP;

    CLOSE ISWC_cur;

End FIX_ISWC;

Procedure Identify_fix_aff_hier_roof
Is
    M_Body                  Clob;
	l_Count                 Number;
Begin
	Execute Immediate 'Truncate Table Spi_Bad_Hier_Roofs';
	
	--Non-Relate Accounts Only 
	Insert Into Spi_Bad_Hier_Roofs
	Select * From 
	(
		Select Ah.Group_Number
		,Ah.Child_Party_Id
		,Ah.Parent_Party_Id
		,Ah.Relate_Account_Number
		,Apcts.Spi_Screen_Utility.Get_Top_Publisher_Ip(Group_Number) Hier_Top_Pub_Ip_Id,
		(
			Select Apcts.Spi_Screen_Utility.Get_Top_Publisher_Ip(Ah2.Group_Number) 
			From Apcts.Aff_Hierarchy Ah2 
			Where Ah2.Relate_Account_Number Is Null
			And Ah2.End_Date Is Null
			And Ah2.Nowed_Aff_Hier_Id Is Null
			And Ah2.Hier_Level = 0 
			And Ah2.Child_Party_Id = Ah.Parent_Party_Id
			And Rownum = 1
		) Parent_Top_Ip_Id
		--,ah.*
		From Apcts.Aff_Hierarchy Ah
		Where Relate_Account_Number Is Null
		And Child_Party_Id != Parent_Party_Id
		And Child_Party_Id In (Select Ip_Id From Aff_Interested_Parties Where Status_Code_Id = 120)
		And End_Date Is Null
		And Nowed_Aff_Hier_Id Is Null
		And Hier_Level = 0 
	)
	Where nvl(Hier_Top_Pub_Ip_Id, -1) != nvl(Parent_Top_Ip_Id, -1);
	Commit;

	--Relate Accounts Only 
	Insert Into Spi_Bad_Hier_Roofs
	Select * From 
	(
		Select Ah.Group_Number
		,Ah.Child_Party_Id
		,Ah.Parent_Party_Id
		,Ah.Relate_Account_Number
		,Apcts.Spi_Screen_Utility.Get_Top_Publisher_Ip(Group_Number) Hier_Top_Pub_Ip_Id,
		(
			Select Apcts.Spi_Screen_Utility.Get_Top_Publisher_Ip(Ah2.Group_Number) 
			From Apcts.Aff_Hierarchy Ah2 
			Where Ah2.Relate_Account_Number Is Null
			And Ah2.End_Date Is Null
			And Ah2.Nowed_Aff_Hier_Id Is Null
			And Ah2.Hier_Level = 0 
			And Ah2.Child_Party_Id = Ah.Parent_Party_Id
			And Rownum = 1
		) Parent_Top_Ip_Id
		--,ah.*
		From Apcts.Aff_Hierarchy Ah
		Where Relate_Account_Number Is Not Null
		And Child_Party_Id != Parent_Party_Id
		And Child_Party_Id In (Select Ip_Id From Aff_Interested_Parties Where Status_Code_Id = 120)
		And End_Date Is Null
		And Nowed_Aff_Hier_Id Is Null
		And Hier_Level = 0 
	)
	Where nvl(Hier_Top_Pub_Ip_Id, -1) != nvl(Parent_Top_Ip_Id, -1);
	Commit;
    
	Select Count(*)
    Into l_Count
    From Spi_Bad_Hier_Roofs Ah
	Where Nvl(Hier_Top_Pub_Ip_Id, -1) != Nvl(Parent_Top_Ip_Id, -1)
	And Parent_Top_Ip_Id Is Not Null;
	
	If l_Count > 0 Then
	
        --rebuild roof where parent is correct and has a non-relate/self hierarchy	
        Declare
            L_Hier_Level		Number;
            L_Child_Party_Id	Number;
        Begin
                
            For R1 In
            (
                Select * 
                From Spi_Bad_Hier_Roofs
                Where Nvl(Hier_Top_Pub_Ip_Id, -1) != Nvl(Parent_Top_Ip_Id, -1)
                And Parent_Top_Ip_Id Is Not Null
            ) Loop
                Dbms_Output.Put_Line('Group_Number - '||R1.Group_Number);
                Delete From Apcts.Aff_Hierarchy Where Hier_Level > 0 And Group_Number = R1.Group_Number;
                L_Hier_Level := 0;
                
                For R3 In (
                    Select 
                        Child_Party_Id, Parent_Party_Id, 
                        Hier_Level, Start_Date
                    From Apcts.Aff_Hierarchy
                    Where Group_Number In
                    (
                        Select 
                            Group_Number
                        From Apcts.Aff_Hierarchy Ahx
                        Where Ahx.Child_Party_Id = R1.Parent_Party_Id
                        And Ahx.Hier_Level = 0
                        And Ahx.Relate_Account_Number Is Null
                        And Ahx.End_Date Is Null
                        And Ahx.Nowed_Aff_Hier_Id Is Null
                    )
                    Order By Hier_Level
                ) Loop
                    L_Hier_Level := L_Hier_Level + 1;
                    
                    Insert Into Apcts.Aff_Hierarchy (
                       Child_Party_Id, Parent_Party_Id, 
                       Hier_Level, Start_Date, 
                       End_Date, Created_By, Creation_Date, 
                       Group_Number) 
                    Values
                    ( 
                        R3.Child_Party_Id, R3.Parent_Party_Id, 
                       L_Hier_Level, R3.Start_Date, 
                       Null, 'Spi-4301_Fix_Roof', Sysdate, 
                       R1.Group_Number
                    );
                End Loop;
                Commit;
            End Loop;

        End;
    End If;
    
    Select Count(*)
    Into l_Count
    From Spi_Bad_Hier_Roofs Ah
	Where Nvl(Hier_Top_Pub_Ip_Id, -1) != Nvl(Parent_Top_Ip_Id, -1)
	And Relate_Account_Number Is Null
	And Parent_Top_Ip_Id Is Null;
	
	If l_Count > 0 Then
	
        M_Body := '
    <style type="text/css" scoped>
    table.unstyledTable {
    }
    table.unstyledTable td, table.unstyledTable th {
      border: 1px solid #AAAAAA;
    }
    table.unstyledTable tbody td {
      font-size: 13px;
    }
    table.unstyledTable thead {
      background: #DDDDDD;
    }
    table.unstyledTable thead th {
      font-weight: bold;
    }
    </style>
    <table class="unstyledTable">
    <thead>
    <tr>
    <th>Group Number.</th>
    <th>Relate_Account_Number</th>
    <th>Hier Level</th>
    <th>Child HFA#</th>
    <th>Parent HFA#</th>
    <th>Child Name</th>
    <th>Parent Name</th>
    <th>Hier Top HFA#</th>
    <th>Hier Top Name</th>
    <th>Parent IP Status</th>
    </tr>
    </thead>
    <tbody>
    ';
        For A In (
        Select 1, Group_Number, Relate_Account_Number, 0 As Hier_Level
        ,Apcts.Spi_Screen_Utility.Get_Hfa_Number(Ah.Child_Party_Id) Child_Hfa
        ,Apcts.Spi_Screen_Utility.Get_Hfa_Number(Ah.Parent_Party_Id) Parent_Hfa
        ,Apcts.Spi_Screen_Utility.Get_Party_Name(Ah.Child_Party_Id) Child_Name
        ,Apcts.Spi_Screen_Utility.Get_Party_Name(Ah.Parent_Party_Id) Parent_Name
        ,Apcts.Spi_Screen_Utility.Get_Hfa_Number(Ah.Hier_Top_Pub_Ip_Id) Hier_Top_Hfa
        ,Apcts.Spi_Screen_Utility.Get_Party_Name(Ah.Hier_Top_Pub_Ip_Id) Hier_Top_Name
        ,(Select Case When Status_Code_Id = 120 Then 'A' Else 'I' End From Apcts.Aff_Interested_Parties Where Ip_Id = Ah.Parent_Party_Id) Parent_Ip_Status
        From Spi_Bad_Hier_Roofs Ah
        Where Nvl(Hier_Top_Pub_Ip_Id, -1) != Nvl(Parent_Top_Ip_Id, -1)
        And Relate_Account_Number Is Null
        And Parent_Top_Ip_Id Is Null
        Union All
        Select 2, Group_Number, Relate_Account_Number, 0 As Hier_Level
        ,Apcts.Spi_Screen_Utility.Get_Hfa_Number(Ah.Child_Party_Id) Child_Hfa
        ,Apcts.Spi_Screen_Utility.Get_Hfa_Number(Ah.Parent_Party_Id) Parent_Hfa
        ,Apcts.Spi_Screen_Utility.Get_Party_Name(Ah.Child_Party_Id) Child_Name
        ,Apcts.Spi_Screen_Utility.Get_Party_Name(Ah.Parent_Party_Id) Parent_Name
        ,Apcts.Spi_Screen_Utility.Get_Hfa_Number(Ah.Hier_Top_Pub_Ip_Id) Hier_Top_Hfa
        ,Apcts.Spi_Screen_Utility.Get_Party_Name(Ah.Hier_Top_Pub_Ip_Id) Hier_Top_Name
        ,(Select Case When Status_Code_Id = 120 Then 'A' Else 'I' End From Apcts.Aff_Interested_Parties Where Ip_Id = Ah.Parent_Party_Id) Parent_Ip_Status
        From Spi_Bad_Hier_Roofs Ah
        Where Nvl(Hier_Top_Pub_Ip_Id, -1) != Nvl(Parent_Top_Ip_Id, -1)
        And Relate_Account_Number Is Not Null
        And Parent_Top_Ip_Id Is Null
        Order By 1, 9
        ) Loop
            M_Body := M_Body || '<tr>';
            M_Body := M_Body || '<td align="right">'||A.Group_Number||'</td>';
            M_Body := M_Body || '<td>'||A.Relate_Account_Number||'</td>';
            M_Body := M_Body || '<td>'||A.Hier_Level||'</td>';
            M_Body := M_Body || '<td>'||A.Child_Hfa||'</td>';
            M_Body := M_Body || '<td>'||A.Parent_Hfa||'</td>';
            M_Body := M_Body || '<td>'||A.Child_Name||'</td>';
            M_Body := M_Body || '<td>'||A.Parent_Name||'</td>';
            M_Body := M_Body || '<td>'||A.Hier_Top_Hfa||'</td>';
            M_Body := M_Body || '<td>'||A.Hier_Top_Name||'</td>';
            M_Body := M_Body || '<td>'||A.Parent_Ip_Status||'</td>';
            M_Body := M_Body || '</tr>';
        End Loop;
        M_Body := M_Body || '</tbody></table>';
        M_Body := M_Body || '</hr>';
        
        Apcts.Spi_Create_Match_Writers_Api.Notify_Email('Aff Hierarchies with Inccorect Top: '||Trunc(Sysdate), M_Body);
    End If;

End Identify_fix_aff_hier_roof;


END SPI_CLEANUP_PKG;