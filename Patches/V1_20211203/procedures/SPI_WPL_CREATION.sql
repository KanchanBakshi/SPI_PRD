create or replace PROCEDURE SPI_WPL_CREATION
AS
 
	-- Cursor Modified by Saurav on 08-DEC-2018
	-- THis return property_ids having only 1 Publisher and 1 Writer
	CURSOR c_pir(p_right_type_id IN NUMBER, p_last_run_date IN TIMESTAMP)  IS
	SELECT property_id
	FROM
	(
		SELECT pp.property_id,
		SUM(CASE WHEN psc.role_id IN (
									SELECT role_id 
									  FROM  aff_roles 
									 WHERE summary_role_code = 'P' --Only for Publisher role
								  ) THEN 1 ELSE 0 END) p_count,
		SUM(CASE WHEN psc.role_id IN (
									SELECT role_id 
									  FROM  aff_roles 
									 WHERE summary_role_code = 'W' --Only for Writer role
								  ) THEN 1 ELSE 0 END) w_count
			FROM prop_share_collectors psc,
				prop_properties pp
			WHERE  psc.property_id     = pp.property_id
			--AND pp.property_id = 805894613
			AND pp.source_id        = 7
			AND pp.property_type_id = 1
			AND pp.status           = 'A'
			AND SYSDATE BETWEEN NVL(psc.START_DATE_ACTIVE, SYSDATE) AND NVL(psc.END_DATE_ACTIVE, SYSDATE)
			AND psc.right_type_id    = p_right_type_id
			AND PSC.LAST_UPDATE_DATE > p_last_run_date  /* procedure will run only for the properties which were updated after last successful run of the job--Richa */
			AND NOT EXISTS 
			(
				SELECT NULL
				FROM aff_ip_names AIN --@apcts_prd.sesac.com
				WHERE name_type_id=6
				AND 
				(   full_name = 'COPYRIGHT CONTROL'
					OR FULL_NAME LIKE '%INCONNU EDITEUR%'
					OR FULL_NAME LIKE '%UNKNOWN%'
				)
				AND AIN.IP_ID = PSC.COLLECTOR_ID
			) 
		GROUP BY pp.property_id
	) x
	WHERE p_count = w_count 
	AND p_count = 1;
										


									
	--v_rec             c_pir%ROWTYPE;
	v_ip_id_pub       NUMBER;
	v_ip_id_wrt       NUMBER;
	v_rec_exist       NUMBER;
	v_right_type_id   NUMBER;
	v_counter         NUMBER  DEFAULT 0;
	V_LAST_RUN_DATE			TIMESTAMP;
	TYPE myarray IS TABLE OF c_pir%ROWTYPE;
	v_rec myarray;

BEGIN
	SELECT max(LOG_DATE) INTO V_LAST_RUN_DATE  
			FROM ALL_SCHEDULER_JOB_RUN_DETAILS
			WHERE JOB_NAME = 'SPI_WPL_CREATION_JOB'
			and status='SUCCEEDED' ; 
	v_right_type_id  :=  1;
	OPEN c_pir(v_right_type_id,V_LAST_RUN_DATE);    --Execute for Performing right type
	LOOP
	  FETCH c_pir BULK COLLECT INTO v_rec LIMIT 2500;
		for i in 1..v_rec.count
	   LOOP
	  
	  BEGIN
		--Retrieving publisher and expecting only one publisher to appear if we want to continue.
		 --INSERT INTO WPL_LOG (PROPERTY_ID) VALUES (v_rec(i).property_id) ;
		 --COMMIT;
		SELECT psc.collector_id
		  INTO v_ip_id_pub
		  FROM prop_share_collectors psc, 
			   aff_interested_parties aip
		 WHERE property_id      = v_rec(i).property_id
		   AND SYSDATE BETWEEN NVL(psc.START_DATE_ACTIVE, SYSDATE) AND NVL(psc.END_DATE_ACTIVE, SYSDATE)
		   AND psc.collector_id = aip.ip_id
		   AND psc.right_type_id = v_right_type_id
		   AND psc.role_id IN (
								SELECT role_id 
								  FROM  aff_roles 
								 WHERE summary_role_code = 'P' --Only for publsher role
							  )
			AND NOT EXISTS 
							(SELECT NULL
								FROM aff_ip_names AIN --@apcts_prd.sesac.com
									WHERE name_type_id=6
								   AND (full_name ='COPYRIGHT CONTROL'
									OR FULL_NAME LIKE '%INCONNU EDITEUR%'
									OR FULL_NAME LIKE '%UNKNOWN%')
								   AND AIN.IP_ID = PSC.COLLECTOR_ID
							)  ;
	  
		   
	  EXCEPTION
		WHEN OTHERS THEN
		CONTINUE; --In case of any error i.e., NO_DATA_FOUND or multiple publishser then ignore the processing.
	  END;
		 
	  BEGIN
		--Retrieving writer and expecting only one writer to appear if we want to continue.
		 
		SELECT psc.collector_id
		  INTO v_ip_id_wrt
		  FROM prop_share_collectors psc, 
			   aff_interested_parties aip
		 WHERE property_id      = v_rec(i).property_id
		   AND SYSDATE BETWEEN NVL(psc.START_DATE_ACTIVE, SYSDATE) AND NVL(psc.END_DATE_ACTIVE, SYSDATE)
		   AND psc.collector_id = aip.ip_id
		   AND psc.right_type_id = v_right_type_id
		   AND psc.role_id IN (
								SELECT role_id 
								  FROM  aff_roles 
								 WHERE summary_role_code = 'W' --Only for writer role
							  );
		   
	  EXCEPTION
		WHEN OTHERS THEN
		CONTINUE; --In case of any error i.e., NO_DATA_FOUND or multiple writer then ignore the processing.
	  END;
	  
	  
	  IF v_ip_id_pub IS NOT NULL AND v_ip_id_wrt IS NOT NULL
	  THEN
	  
		  --Check whether writer and publisher already exist
		  SELECT COUNT(*) INTO v_rec_exist
			FROM prop_ip_relations pir
		   WHERE pir.property_id     = v_rec(i).property_id
			 and pir.publisher_ip_id = v_ip_id_pub
			 AND pir.writer_ip_id    = v_ip_id_wrt;
		   
		   IF v_rec_exist = 0
		   THEN
			  
			  INSERT INTO prop_ip_relations
			  (
					relation_id,
					property_id,
					writer_ip_id,
					publisher_ip_id,
					creation_date,
					created_by,
				   last_update_date,
					last_updated_by
			  )
			  VALUES
			  (
				  prop_ip_relations_s.NEXTVAL,
				  v_rec(i).property_id,
				  v_ip_id_wrt,
				  v_ip_id_pub,
				  SYSDATE,
				  'RGARG',
				  SYSDATE,
				  'RGARG'
			  );
			  
		   END IF;
	  END IF;
	  
	  v_counter := v_counter + 1;
	  
	  IF v_counter >=500
	  then
	  commit;
	  v_counter :=0;
	  
	  END IF;
	  
	--Resetting variables before next iteration
	v_ip_id_pub      := NULL;
	v_ip_id_wrt      := NULL;
	v_rec_exist      := NULL;
	 END LOOP;
	  EXIT WHEN c_pir%NOTFOUND;
	END LOOP;
	CLOSE c_pir;
	 
	 
	v_right_type_id := 2;
	OPEN c_pir(v_right_type_id,V_LAST_RUN_DATE);    --Execute for Mechnical right type
	LOOP
	  FETCH c_pir BULK COLLECT INTO v_rec LIMIT 2500;
	  
	  for i in 1..v_rec.count
	  LOOP
	  BEGIN
	   --INSERT INTO WPL_LOG (PROPERTY_ID) VALUES (v_rec(i).property_id) ;
		-- COMMIT;
		--Retrieving publisher and expecting only one publisher to appear if we want to continue.
		SELECT psc.collector_id
		  INTO v_ip_id_pub
		  FROM prop_share_collectors psc, 
			   aff_interested_parties aip
		 WHERE property_id      = v_rec(i).property_id
		   AND SYSDATE BETWEEN NVL(psc.START_DATE_ACTIVE, SYSDATE) AND NVL(psc.END_DATE_ACTIVE, SYSDATE)
		   AND psc.collector_id = aip.ip_id
		   AND psc.right_type_id = v_right_type_id
		   AND psc.role_id IN (
								SELECT role_id 
								  FROM  aff_roles 
								 WHERE summary_role_code = 'P' --Only for publsher role
							  );
		   
	  EXCEPTION
		WHEN OTHERS THEN
		CONTINUE; --In case of any error i.e., NO_DATA_FOUND or multiple publishser then ignore the processing.
	  END;
		 
	  BEGIN
		--Retrieving writer and expecting only one writer to appear if we want to continue.
		SELECT psc.collector_id
		  INTO v_ip_id_wrt
		  FROM prop_share_collectors psc, 
			   aff_interested_parties aip
		 WHERE property_id      = v_rec(i).property_id
		   AND SYSDATE BETWEEN NVL(psc.START_DATE_ACTIVE, SYSDATE) AND NVL(psc.END_DATE_ACTIVE, SYSDATE)
		   AND psc.collector_id = aip.ip_id
		   AND psc.right_type_id = v_right_type_id
		   AND psc.role_id IN (
								SELECT role_id 
								  FROM  aff_roles 
								 WHERE summary_role_code = 'W' --Only for writer role
							  );
		   
	  EXCEPTION
		WHEN OTHERS THEN
		CONTINUE; --In case of any error i.e., NO_DATA_FOUND or multiple writer then ignore the processing.
	  END;
	  
	  
	  IF v_ip_id_pub IS NOT NULL AND v_ip_id_wrt IS NOT NULL
	  THEN
	  
		  --Check whether writer and publisher already exist
		  SELECT COUNT(*) INTO v_rec_exist
			FROM prop_ip_relations pir
		   WHERE pir.property_id     = v_rec(i).property_id
			 and pir.publisher_ip_id = v_ip_id_pub
			 AND pir.writer_ip_id    = v_ip_id_wrt;
		   
		   IF v_rec_exist = 0
		   THEN
			  
			  INSERT INTO prop_ip_relations
			  (
					relation_id,
					property_id,
					writer_ip_id,
					publisher_ip_id,
					creation_date,
					created_by,
					last_update_date,
					last_updated_by
			  )
			  VALUES
			  (
				  prop_ip_relations_s.NEXTVAL,
				  v_rec(i).property_id,
				  v_ip_id_wrt,
				  v_ip_id_pub,
				  SYSDATE,
				  'RGARG',
				  SYSDATE,
				  'RGARG'
			  );
			  
		   END IF;
	  END IF;
	 v_counter := v_counter + 1;
	  
	  IF v_counter >=500
	  then
	  commit;
	  v_counter :=0;
	  
	  END IF;
	--Resetting variables before next iteration
	v_ip_id_pub      := NULL;
	v_ip_id_wrt      := NULL;
	v_rec_exist      := NULL;
	 
	END LOOP;
	EXIT WHEN c_pir%NOTFOUND;
	END LOOP;
	CLOSE c_pir;
	 
	 
	 
	COMMIT; 
 
EXCEPTION
WHEN OTHERS THEN
     RAISE;
END SPI_WPL_CREATION;