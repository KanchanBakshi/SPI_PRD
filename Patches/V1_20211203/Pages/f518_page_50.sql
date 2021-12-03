prompt --application/set_environment
set define off verify off feedback off
whenever sqlerror exit sql.sqlcode rollback
--------------------------------------------------------------------------------
--
-- ORACLE Application Express (APEX) export file
--
-- You should run the script connected to SQL*Plus as the Oracle user
-- APEX_180200 or as the owner (parsing schema) of the application.
--
-- NOTE: Calls to apex_application_install override the defaults below.
--
--------------------------------------------------------------------------------
begin
wwv_flow_api.import_begin (
 p_version_yyyy_mm_dd=>'2018.05.24'
,p_release=>'18.2.0.00.12'
,p_default_workspace_id=>1307600888716932
,p_default_application_id=>518
,p_default_owner=>'APCTS'
);
end;
/
 
prompt APPLICATION 518 - Single Point Ingestion (SPI)
--
-- Application Export:
--   Application:     518
--   Name:            Single Point Ingestion (SPI)
--   Date and Time:   08:17 Wednesday November 17, 2021
--   Exported By:     SCHOPRA
--   Flashback:       0
--   Export Type:     Page Export
--   Version:         18.2.0.00.12
--   Instance ID:     69431613733717
--

prompt --application/pages/delete_00050
begin
wwv_flow_api.remove_page (p_flow_id=>wwv_flow.g_flow_id, p_page_id=>50);
end;
/
prompt --application/pages/page_00050
begin
wwv_flow_api.create_page(
 p_id=>50
,p_user_interface_id=>wwv_flow_api.id(51875501499240850)
,p_name=>'Property Ingestion Search Status'
,p_step_title=>'Property Ingestion Search Status'
,p_reload_on_submit=>'A'
,p_warn_on_unsaved_changes=>'N'
,p_step_sub_title=>'Search Requests'
,p_step_sub_title_type=>'TEXT_WITH_SUBSTITUTIONS'
,p_autocomplete_on_off=>'ON'
,p_group_id=>wwv_flow_api.id(68043639130234200)
,p_javascript_code=>wwv_flow_string.join(wwv_flow_t_varchar2(
'var waitPopup;',
'var waitPopup1;'))
,p_page_template_options=>'#DEFAULT#'
,p_required_role=>wwv_flow_api.id(69303254223531622)
,p_help_text=>'No help is available for this page.'
,p_last_updated_by=>'SCHOPRA'
,p_last_upd_yyyymmddhh24miss=>'20210928115723'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(4377461342140824)
,p_plug_name=>'RDS'
,p_region_template_options=>'#DEFAULT#:t-TabsRegion-mod--simple'
,p_plug_template=>wwv_flow_api.id(51851408341240826)
,p_plug_display_sequence=>50
,p_include_in_reg_disp_sel_yn=>'Y'
,p_plug_display_point=>'BODY'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_attribute_01=>'N'
,p_attribute_02=>'HTML'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(4377566689140825)
,p_plug_name=>'Conflicts'
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>40
,p_plug_display_point=>'BODY'
,p_query_type=>'SQL'
,p_plug_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select * from ',
'(   --to get only Rejected works',
'    SELECT DISTINCT sts.transaction_status_id ',
'		  ,sts.creation_date AS submission_date',
'		  ,(select ps.source_code from prop_sources ps where ps.source_id=sts.source_id) source_id',
'		  ,sts.file_name',
'		  ,sts.submission_id',
'		  ,sts.submitter_work_id',
'          ,asu.submitting_publisher_number',
'		  ,sts.title',
'		  ,sts.transaction_status',
'		  ,pl.description "STATUS"',
'		  ,(SELECT property_number from prop_properties where property_id = sts.property_id) property_number',
'		  ,sts.property_id',
'		  ,spx.sesac_property_id',
'		  ,sts.selected_sesac_property_id',
'		  ,(SELECT property_number from prop_properties where property_id = NVL(sts.selected_sesac_property_id, spx.sesac_property_id))  SESAC_PROPERTY',
'          ,(',
'                SELECT REPLACE(REPLACE(error_message,''#''),'''''''') error_message',
'                FROM cwr_messages ',
'                WHERE message_type = ''REJ''',
'                AND sts.rejection_code = cwr_message_id',
'           ) Remarks',
'           ,''N'' jingle_flag,',
'    (SELECT MAX(DECODE(aka_type_id,51,aka_name)) FROM prop_akas aka WHERE aka.property_id =  NVL(sts.selected_sesac_property_id, spx.sesac_property_id) and source_id = 7) HFA_Song_Code,',
'    spi_screen_utility.get_writers(NVL(sts.selected_sesac_property_id, spx.sesac_property_id)) SESAC_Song_writers,',
'    xxsec_util.csv_string(',
'    ''SELECT ',
'        RPAD(spi_screen_utility.get_hfa_number(psc.collector_id, psa.admin_id), 10, '''' '''')||'''' ''''||    ',
'        RPAD(TO_CHAR(NVL(admin_share_percent, share_percentage), ''''999D999''''), 10, '''' '''')||'''' ''''|| ',
'        CASE ',
'            WHEN psa.admin_id IS NOT NULL AND psc.collector_id != psa.admin_id  THEN SUBSTRB(',
'                            spi_screen_utility.get_party_name(psa.admin_id) || '''' OBO '''' || spi_screen_utility.get_party_name(psc.collector_id),',
'                            1, 300',
'                        ) ',
'                ELSE spi_screen_utility.get_party_name(collector_id) ',
'                END nm',
'        from prop_share_collectors psc, prop_share_admins psa ',
'        where property_id = ''||NVL(sts.selected_sesac_property_id, spx.sesac_property_id)||'' ',
'        and right_type_id = 2',
'        and psc.role_id IN (SELECT role_id FROM aff_roles WHERE summary_role_code = ''''P'''')',
'        and psc.share_collector_id = psa.share_collector_id(+)',
'        /*and share_percentage > 0*/',
'        and psc.end_date_active is null',
'        and psa.end_date_active is null'',',
'    chr(10)',
'    ) sesac_song_publishers_mech',
'	FROM   spi_transaction_status sts',
'		  ,spi_property_xref spx',
'		  ,prop_lookups pl',
'          ,(',
'            SELECT TO_CHAR(id) AS submission_id, submitting_publisher_number ',
'            FROM ape_submission ',
'            UNION ALL',
'            SELECT submission_id, administrator_num AS submitting_publisher_number ',
'            FROM owr_song_registration',
'           )  asu',
'	WHERE  	1=1',
'    AND     spx.submitter_work_id = sts.submitter_work_id',
'	AND    	spx.submitter_source_id = sts.source_id',
'	AND    	pl.lookup_code = sts.transaction_status',
'    AND     TO_CHAR(asu.submission_id(+)) = TO_CHAR(sts.submission_id)',
'	AND    	pl.lookup_type = ''SPI_STATUS'' ',
'	AND		sts.transaction_status = 90',
'	AND     sts.transaction_status = NVL(:P50_TRANSACTION_STATUS, sts.transaction_status)     ',
'	AND		sts.transaction_status_id = NVL(:P50_SPI_TRANSACTION_STATUS_ID, sts.transaction_status_id)',
'	AND     sts.source_id = NVL(:P50_PUB_SRC,sts.source_id)',
'    AND     NVL(asu.SUBMITTING_PUBLISHER_NUMBER, ''99.99'') = NVL(:P50_SUB_PUBLISHER_NUMBER, NVL(asu.SUBMITTING_PUBLISHER_NUMBER, ''99.99''))',
'    AND     NVL(sts.file_name, ''X'')  = NVL(:P50_FILE_NAME,NVL(sts.file_name, ''X''))',
'	AND     sts.SUBMISSION_ID = NVL(TRIM(:P50_SUBMISSION_ID), sts.SUBMISSION_ID)',
'	AND    	NVL(sts.creation_date, SYSDATE) BETWEEN NVL(TO_DATE(:P50_SUBMISSION_DATE_FROM,''YYYY-MM-DD''),NVL(sts.creation_date, SYSDATE))',
'							   AND     NVL(TO_DATE(:P50_SUBMISSION_DATE_TO,''YYYY-MM-DD''),NVL(sts.creation_date, SYSDATE))',
'    AND    	sts.title LIKE NVL((''%''||UPPER(:P50_SONG_TITLE)||''%''),UPPER(sts.title))',
'    AND (',
'            :P50_SUBMITTER_WORK_NO IS NULL OR EXISTS (                  ',
'                                                        select 1 from (  ',
'                                                                        select qry.val from  ',
'                                                                               (',
'                                                                                     select regexp_substr(:P50_SUBMITTER_WORK_NO,''[^,]+'', 1, level) val',
'                                                                                     from dual ',
'                                                                                     connect BY regexp_substr(:P50_SUBMITTER_WORK_NO, ''[^,]+'', 1, level) ',
'                                                                                     is not null',
'                                                                               ) qry where qry.val=sts.submitter_work_id',
'                                                                       )',
'                                                    )',
'        )',
'    AND     ',
'    (       :P50_PROPERTY_NUMBER IS NULL',
'            OR',
'            (',
'                :P50_PROPERTY_NUMBER IS NOT NULL AND',
'                EXISTS',
'                (',
'                    SELECT 1 FROM prop_properties where property_id = sts.property_id and property_number =  :P50_PROPERTY_NUMBER',
'                )',
'            )       ',
'    )',
'    --AND (:P50_HFA_SONG_CODE is null or exists (select 1 from prop_akas aka where aka.property_id = sts.property_id and aka.AKA_TYPE_ID=51 and aka.aka_name=:P50_HFA_SONG_CODE ))',
'    AND',
'    (',
'        (:P50_PARTY_NAME IS NULL AND :P50_PARTY_IPI_NUMBER IS NULL AND :P50_PARTY_AFFILIATION IS NULL AND :P50_WRI_PUB_INDICATOR IS NULL)',
'        OR',
'        (:P50_TRANSACTION_STATUS = 90 AND (:P50_PARTY_NAME IS NOT NULL OR :P50_PARTY_IPI_NUMBER IS NOT NULL OR :P50_PARTY_AFFILIATION IS NOT NULL OR :P50_WRI_PUB_INDICATOR IS NOT NULL ))',
'        AND EXISTS',
'        ( ',
'            SELECT NULL',
'             FROM   prop_share_collectors pso, aff_roles ar',
'             WHERE pso.property_id = sts.property_id',
'             AND pso.end_date_active IS NULL',
'             AND (:P50_PARTY_AFFILIATION is null or NVL(pso.affiliated_society_id, -1) = NVL(:P50_PARTY_AFFILIATION, NVL(pso.affiliated_society_id, -1)))',
'             AND ar.role_id = pso.role_id',
'             AND ',
'                ( ',
'                    ( :P50_WRI_PUB_INDICATOR = ''Y'' AND ar.summary_role_code = ''W'')',
'                OR',
'                    NVL(:P50_WRI_PUB_INDICATOR,''N'') = ''N''',
'                )',
'             AND ',
'             (  :P50_PARTY_NAME IS NULL',
'                OR',
'                (',
'                    :P50_PARTY_NAME IS NOT NULL  ',
'                    AND EXISTS',
'                    (',
'                        SELECT 1 FROM spi_share_owner_xref sx',
'                        WHERE sx.submitter_party_id = pso.submitter_internal_number',
'                        AND sx.submitter_source_id = sts.source_id',
'                        AND INSTR((TRIM(Upper(sx.first_name||'' ''||sx.name))),UPPER(TRIM(:P50_PARTY_NAME)))>0',
'                        AND (:P50_PARTY_IPI_NUMBER is null or sx.IPI_NAME_NUMBER = NVL(:P50_PARTY_IPI_NUMBER,sx.IPI_NAME_NUMBER)) ',
'                        AND NVL(sx.sesac_ip_id, 602200) = pso.collector_id',
'                        UNION ALL',
'                        SELECT 1 FROM aff_ip_names ain',
'                        WHERE ain.ip_id = pso.collector_id',
'                        AND ain.name_type_id = 6',
'                        AND INSTR((Upper(ain.full_name)),UPPER(:P50_PARTY_NAME))>0',
'                        AND (:P50_PARTY_IPI_NUMBER is null or ain.name_number = NVL(:P50_PARTY_IPI_NUMBER,ain.name_number))',
'                    ) ',
'                )',
'            )',
'        )',
'    )',
'   AND	( :P50_TRANS_TYPE=''A'' OR  EXISTS ( SELECT 1',
'                                     FROM   cwr_ack_history cah',
'                                     WHERE  cah.property_id=sts.property_id',
'                                     and cah.transaction_type=:P50_TRANS_TYPE',
'                                    ) )',
'    AND     (',
'                :P50_SPI_TRANSACTION_STATUS_ID IS NOT NULL OR :P50_PUB_SRC IS NOT NULL OR :P50_FILE_NAME IS NOT NULL OR :P50_SUBMISSION_DATE_FROM IS NOT NULL OR ',
'                :P50_SUBMISSION_DATE_TO IS NOT NULL OR :P50_SONG_TITLE IS NOT NULL OR :P50_PROPERTY_NUMBER IS NOT NULL OR --:P50_HFA_SONG_CODE IS NOT NULL OR',
'                :P50_TRANSACTION_STATUS IS NOT NULL OR :P50_SUBMITTER_WORK_NO IS NOT NULL OR',
'                :P50_PARTY_NAME IS NOT NULL OR :P50_PARTY_IPI_NUMBER IS NOT NULL OR :P50_PARTY_AFFILIATION IS NOT NULL OR ',
'                :P50_WRI_PUB_INDICATOR IS NOT NULL OR :P50_TRANS_TYPE IS NOT NULL',
'            )',
')',
'where :P50_QUERY = ''Y''',
'and (jingle_flag = :P50_JINGLE_INDICATOR OR :P50_JINGLE_INDICATOR IS NULL) ',
''))
,p_plug_source_type=>'NATIVE_IR'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_plug_display_condition_type=>'PLSQL_EXPRESSION'
,p_plug_display_when_condition=>wwv_flow_string.join(wwv_flow_t_varchar2(
':P50_INQ_SELECTION = ''S'' AND :P50_CONFLICT_FLAG = ''Y''',
'and v(''APP_USER'') in (''SCHOPRA'', ''NMARKMAN'' ,''KBAKSHI'')'))
,p_prn_content_disposition=>'ATTACHMENT'
,p_prn_document_header=>'APEX'
,p_prn_units=>'INCHES'
,p_prn_paper_size=>'LETTER'
,p_prn_width=>8.5
,p_prn_height=>11
,p_prn_orientation=>'HORIZONTAL'
,p_prn_page_header_font_color=>'#000000'
,p_prn_page_header_font_family=>'Helvetica'
,p_prn_page_header_font_weight=>'normal'
,p_prn_page_header_font_size=>'12'
,p_prn_page_footer_font_color=>'#000000'
,p_prn_page_footer_font_family=>'Helvetica'
,p_prn_page_footer_font_weight=>'normal'
,p_prn_page_footer_font_size=>'12'
,p_prn_header_bg_color=>'#9bafde'
,p_prn_header_font_color=>'#000000'
,p_prn_header_font_family=>'Helvetica'
,p_prn_header_font_weight=>'normal'
,p_prn_header_font_size=>'10'
,p_prn_body_bg_color=>'#efefef'
,p_prn_body_font_color=>'#000000'
,p_prn_body_font_family=>'Helvetica'
,p_prn_body_font_weight=>'normal'
,p_prn_body_font_size=>'10'
,p_prn_border_width=>.5
,p_prn_page_header_alignment=>'CENTER'
,p_prn_page_footer_alignment=>'CENTER'
);
wwv_flow_api.create_worksheet(
 p_id=>wwv_flow_api.id(4377636504140826)
,p_max_row_count=>'1000000'
,p_no_data_found_message=>'no data found!'
,p_show_nulls_as=>'-'
,p_pagination_type=>'ROWS_X_TO_Y'
,p_pagination_display_pos=>'BOTTOM_RIGHT'
,p_report_list_mode=>'TABS'
,p_show_detail_link=>'N'
,p_show_notify=>'Y'
,p_download_formats=>'CSV:HTML:EMAIL:XLS'
,p_owner=>'KBAKSHI'
,p_internal_uid=>4377636504140826
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(4377793530140827)
,p_db_column_name=>'TRANSACTION_STATUS_ID'
,p_display_order=>10
,p_column_identifier=>'A'
,p_column_label=>'SPI Reference#'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(4377836980140828)
,p_db_column_name=>'SUBMISSION_DATE'
,p_display_order=>20
,p_column_identifier=>'B'
,p_column_label=>'Submission Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(4377979136140829)
,p_db_column_name=>'SOURCE_ID'
,p_display_order=>30
,p_column_identifier=>'C'
,p_column_label=>'Source'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(4378076845140830)
,p_db_column_name=>'FILE_NAME'
,p_display_order=>40
,p_column_identifier=>'D'
,p_column_label=>'File Name'
,p_column_link=>'f?p=&APP_ID.:19:&SESSION.::&DEBUG.:RP,19:P19_FILE_NAME:#FILE_NAME#'
,p_column_linktext=>'#FILE_NAME#'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(4378181885140831)
,p_db_column_name=>'SUBMISSION_ID'
,p_display_order=>50
,p_column_identifier=>'E'
,p_column_label=>'Submission Id'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(4378284710140832)
,p_db_column_name=>'SUBMITTER_WORK_ID'
,p_display_order=>60
,p_column_identifier=>'F'
,p_column_label=>'Submitter Work Id'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(4378380644140833)
,p_db_column_name=>'SUBMITTING_PUBLISHER_NUMBER'
,p_display_order=>70
,p_column_identifier=>'G'
,p_column_label=>'Submitting Publisher Number'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(4378494460140834)
,p_db_column_name=>'TITLE'
,p_display_order=>80
,p_column_identifier=>'H'
,p_column_label=>'Title'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(4378501392140835)
,p_db_column_name=>'TRANSACTION_STATUS'
,p_display_order=>90
,p_column_identifier=>'I'
,p_column_label=>'Transaction Status'
,p_column_type=>'STRING'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(4378669334140836)
,p_db_column_name=>'STATUS'
,p_display_order=>100
,p_column_identifier=>'J'
,p_column_label=>'Status'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(4378765350140837)
,p_db_column_name=>'PROPERTY_NUMBER'
,p_display_order=>110
,p_column_identifier=>'K'
,p_column_label=>'Property Number'
,p_column_link=>'f?p=&APP_ID.:310:&SESSION.::&DEBUG.:RP,310:P310_PROP_ID,P310_CALLING_PAGE_ID:#PROPERTY_ID#,50'
,p_column_linktext=>'#PROPERTY_NUMBER#'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(4378814056140838)
,p_db_column_name=>'PROPERTY_ID'
,p_display_order=>120
,p_column_identifier=>'L'
,p_column_label=>'Property Id'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(4378929149140839)
,p_db_column_name=>'SESAC_PROPERTY_ID'
,p_display_order=>130
,p_column_identifier=>'M'
,p_column_label=>'SESAC Property'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(4379057005140840)
,p_db_column_name=>'SESAC_PROPERTY'
,p_display_order=>140
,p_column_identifier=>'N'
,p_column_label=>'SESAC Property'
,p_column_link=>'f?p=&APP_ID.:310:&SESSION.::&DEBUG.:RP:P310_PROP_ID,P310_CALLING_PAGE_ID:#SESAC_PROPERTY_ID#,50'
,p_column_linktext=>'#SESAC_PROPERTY#'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(4379184154140841)
,p_db_column_name=>'REMARKS'
,p_display_order=>150
,p_column_identifier=>'O'
,p_column_label=>'Remarks'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(4379290352140842)
,p_db_column_name=>'JINGLE_FLAG'
,p_display_order=>160
,p_column_identifier=>'P'
,p_column_label=>'Jingle Flag'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(4379334356140843)
,p_db_column_name=>'HFA_SONG_CODE'
,p_display_order=>170
,p_column_identifier=>'Q'
,p_column_label=>'HFA Song Code'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(4379466736140844)
,p_db_column_name=>'SESAC_SONG_WRITERS'
,p_display_order=>180
,p_column_identifier=>'R'
,p_column_label=>'SESAC Song Writers'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(4379591090140845)
,p_db_column_name=>'SESAC_SONG_PUBLISHERS_MECH'
,p_display_order=>190
,p_column_identifier=>'S'
,p_column_label=>'SESAC Song Publishers Mech'
,p_column_type=>'STRING'
,p_display_text_as=>'WITHOUT_MODIFICATION'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(5271085385813507)
,p_db_column_name=>'SELECTED_SESAC_PROPERTY_ID'
,p_display_order=>200
,p_column_identifier=>'T'
,p_column_label=>'Selected Sesac Property Id'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(5405632015239301)
,p_application_user=>'APXWS_DEFAULT'
,p_report_seq=>10
,p_report_alias=>'54057'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'TRANSACTION_STATUS_ID:SOURCE_ID:SUBMISSION_DATE:FILE_NAME:TITLE:SUBMISSION_ID:SUBMITTER_WORK_ID:PROPERTY_NUMBER:SESAC_PROPERTY:HFA_SONG_CODE:SUBMITTING_PUBLISHER_NUMBER:STATUS:REMARKS:SESAC_SONG_WRITERS:SESAC_SONG_PUBLISHERS_MECH:JINGLE_FLAG:'
,p_sort_column_1=>'TRANSACTION_STATUS_ID'
,p_sort_direction_1=>'ASC'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(19714217653749026)
,p_plug_name=>'Summary by Source'
,p_region_template_options=>'#DEFAULT#:t-Form--slimPadding'
,p_plug_template=>wwv_flow_api.id(51848808455240825)
,p_plug_display_sequence=>60
,p_plug_display_point=>'BODY'
,p_query_type=>'SQL'
,p_plug_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select ',
'    (SELECT ps.source_code||'' - ''||ps.description "Description"',
'FROM   prop_sources ps',
'WHERE  ps.source_id = spi.source_id) AS source_id, ',
'    file_name, ',
'    CASE WHEN  to_number(transaction_status) IN (70,80,100,110) THEN 110 ELSE to_number(transaction_status) end AS transaction_status, ',
'    count(1) AS count,',
'    source_id as pub_source_id,',
'    CASE WHEN  to_number(transaction_status) IN (70,80,100,110) THEN 110 ELSE  to_number(transaction_status) end AS transaction_status_val, ',
'    batch_id',
'from spi_transaction_status spi',
'group by source_id, file_name, CASE WHEN  to_number(transaction_status) IN (70,80,100,110) THEN 110 ELSE  to_number(transaction_status) end, batch_id'))
,p_plug_source_type=>'NATIVE_IR'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_plug_display_condition_type=>'NEVER'
,p_prn_content_disposition=>'ATTACHMENT'
,p_prn_document_header=>'APEX'
,p_prn_units=>'INCHES'
,p_prn_paper_size=>'LETTER'
,p_prn_width=>8.5
,p_prn_height=>11
,p_prn_orientation=>'HORIZONTAL'
,p_prn_page_header_font_color=>'#000000'
,p_prn_page_header_font_family=>'Helvetica'
,p_prn_page_header_font_weight=>'normal'
,p_prn_page_header_font_size=>'12'
,p_prn_page_footer_font_color=>'#000000'
,p_prn_page_footer_font_family=>'Helvetica'
,p_prn_page_footer_font_weight=>'normal'
,p_prn_page_footer_font_size=>'12'
,p_prn_header_bg_color=>'#9bafde'
,p_prn_header_font_color=>'#000000'
,p_prn_header_font_family=>'Helvetica'
,p_prn_header_font_weight=>'normal'
,p_prn_header_font_size=>'10'
,p_prn_body_bg_color=>'#efefef'
,p_prn_body_font_color=>'#000000'
,p_prn_body_font_family=>'Helvetica'
,p_prn_body_font_weight=>'normal'
,p_prn_body_font_size=>'10'
,p_prn_border_width=>.5
,p_prn_page_header_alignment=>'CENTER'
,p_prn_page_footer_alignment=>'CENTER'
);
wwv_flow_api.create_worksheet(
 p_id=>wwv_flow_api.id(38400600604535529)
,p_max_row_count=>'1000000'
,p_show_nulls_as=>'-'
,p_pagination_type=>'ROWS_X_TO_Y'
,p_pagination_display_pos=>'BOTTOM_RIGHT'
,p_report_list_mode=>'TABS'
,p_show_detail_link=>'N'
,p_show_notify=>'Y'
,p_download_formats=>'CSV:HTML:EMAIL:XLS:PDF:RTF'
,p_owner=>'SCHOPRA'
,p_internal_uid=>38400600604535529
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38401204289535535)
,p_db_column_name=>'SOURCE_ID'
,p_display_order=>10
,p_column_identifier=>'F'
,p_column_label=>'Source'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38400897287535531)
,p_db_column_name=>'FILE_NAME'
,p_display_order=>20
,p_column_identifier=>'B'
,p_column_label=>'File name'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38401854130535541)
,p_db_column_name=>'TRANSACTION_STATUS'
,p_display_order=>30
,p_column_identifier=>'K'
,p_column_label=>'Transaction Status'
,p_column_type=>'NUMBER'
,p_display_text_as=>'LOV_ESCAPE_SC'
,p_rpt_named_lov=>wwv_flow_api.id(10837137064964149)
,p_rpt_show_filter_lov=>'1'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38401156646535534)
,p_db_column_name=>'COUNT'
,p_display_order=>40
,p_column_identifier=>'E'
,p_column_label=>'Count'
,p_column_link=>'f?p=&APP_ID.:50:&SESSION.::&DEBUG.:RP,50:P50_INQ_SELECTION,P50_PUB_SRC,P50_FILE_NAME,P50_TRANSACTION_STATUS,P50_QUERY:S,#PUB_SOURCE_ID#,#BATCH_ID#,#TRANSACTION_STATUS_VAL#,Y'
,p_column_linktext=>'<span title="View results">#COUNT#</span>'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38401690362535539)
,p_db_column_name=>'BATCH_ID'
,p_display_order=>50
,p_column_identifier=>'I'
,p_column_label=>'Batch id'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38401745588535540)
,p_db_column_name=>'PUB_SOURCE_ID'
,p_display_order=>60
,p_column_identifier=>'J'
,p_column_label=>'Pub source id'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38401930808535542)
,p_db_column_name=>'TRANSACTION_STATUS_VAL'
,p_display_order=>70
,p_column_identifier=>'L'
,p_column_label=>'Transaction status val'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(38582408204950408)
,p_application_user=>'APXWS_DEFAULT'
,p_report_seq=>10
,p_report_alias=>'385825'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_display_rows=>100
,p_report_columns=>'SOURCE_ID:FILE_NAME:COUNT:BATCH_ID:PUB_SOURCE_ID'
,p_sort_column_1=>'SOURCE_ID'
,p_sort_direction_1=>'ASC'
,p_sort_column_2=>'FILE_NAME'
,p_sort_direction_2=>'ASC'
,p_sort_column_3=>'TRANSACTION_STATUS_VAL'
,p_sort_direction_3=>'ASC'
,p_sort_column_4=>'0'
,p_sort_direction_4=>'ASC'
,p_sort_column_5=>'0'
,p_sort_direction_5=>'ASC'
,p_sort_column_6=>'0'
,p_sort_direction_6=>'ASC'
,p_break_on=>'0:0:0:0:0'
,p_break_enabled_on=>'0:0:0:0:0'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(38583468526004647)
,p_application_user=>'APXWS_ALTERNATIVE'
,p_name=>'Source '
,p_report_seq=>10
,p_report_alias=>'385835'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_display_rows=>100
,p_report_columns=>'SOURCE_ID:FILE_NAME:COUNT:BATCH_ID:PUB_SOURCE_ID'
,p_sort_column_1=>'SOURCE_ID'
,p_sort_direction_1=>'ASC'
,p_sort_column_2=>'FILE_NAME'
,p_sort_direction_2=>'ASC'
,p_sort_column_3=>'TRANSACTION_STATUS_VAL'
,p_sort_direction_3=>'ASC'
,p_sort_column_4=>'0'
,p_sort_direction_4=>'ASC'
,p_sort_column_5=>'0'
,p_sort_direction_5=>'ASC'
,p_sort_column_6=>'0'
,p_sort_direction_6=>'ASC'
,p_break_on=>'SOURCE_ID:0:0:0:0:0'
,p_break_enabled_on=>'SOURCE_ID:0:0:0:0:0'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(34938650060306535)
,p_plug_name=>'Summary by Source'
,p_region_name=>'391713'
,p_region_template_options=>'#DEFAULT#:t-Form--slimPadding'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51848808455240825)
,p_plug_display_sequence=>70
,p_plug_display_point=>'BODY'
,p_query_type=>'SQL'
,p_plug_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select * from ',
'(   --to get only Rejected works',
'    SELECT DISTINCT sts.transaction_status_id',
'		  ,sts.creation_date AS submission_date',
'		  ,sts.source_id',
'		  ,sts.file_name',
'		  ,sts.submitter_work_id',
'		  ,sts.title',
'		  ,sts.transaction_status',
'		  ,pl.description "STATUS"',
'		  ,NULL AS property_number',
'		  ,sts.property_id',
'		  ,spx.sesac_property_id',
'		  ,'' '' SESAC_PROPERTY',
'          ,(',
'                SELECT REPLACE(REPLACE(error_message,''#''),'''''''') error_message',
'                FROM cwr_messages ',
'                WHERE message_type = ''REJ''',
'                AND sts.rejection_code = cwr_message_id',
'           ) Remarks',
'           ,NULL jingle_flag',
'	FROM   spi_transaction_status sts',
'		  ,spi_property_xref spx',
'		  ,prop_lookups pl',
'	WHERE  	sts.property_id IS NULL ',
'    AND     spx.submitter_work_id = sts.submitter_work_id',
'	AND    	spx.submitter_source_id = sts.source_id',
'	AND    	pl.lookup_code = sts.transaction_status',
'	AND    	pl.lookup_type = ''SPI_STATUS'' ',
'	AND		sts.transaction_status = 90',
'	AND     sts.source_id = NVL(:P50_PUB_SRC,sts.source_id)',
'	AND    	sts.batch_id  = NVL(:P50_FILE_NAME,sts.batch_id)',
'	AND    	NVL(sts.creation_date, SYSDATE) BETWEEN NVL(TO_DATE(:P50_SUBMISSION_DATE_FROM,''YYYY-MM-DD''),NVL(sts.creation_date, SYSDATE))',
'							   AND     NVL(TO_DATE(:P50_SUBMISSION_DATE_TO,''YYYY-MM-DD''),NVL(sts.creation_date, SYSDATE))',
'    AND    	sts.title LIKE NVL((''%''||UPPER(:P50_SONG_TITLE)||''%''),UPPER(sts.title))',
'    AND     (:P50_PUB_SRC IS NOT NULL OR :P50_FILE_NAME IS NOT NULL OR :P50_SUBMISSION_DATE_FROM IS NOT NULL OR :P50_SUBMISSION_DATE_TO IS NOT NULL OR :P50_SONG_TITLE IS NOT NULL)',
'    UNION ALL',
'    -- TO get Non-Rejected works',
'	SELECT DISTINCT sts.transaction_status_id',
'		  ,sts.creation_date AS submission_date',
'		  ,sts.source_id',
'		  ,sts.file_name',
'		  ,sts.submitter_work_id',
'		  ,sts.title',
'		  ,sts.transaction_status',
'		  ,pl.description "STATUS"',
'		  ,pp.property_number',
'          --,(select property_number from prop_properties where property_id = spx.sesac_property_id)||'' (''||sts.transaction_status||'')'' prop_number_sesac_tst',
'		  ,sts.property_id',
'		  ,spx.sesac_property_id',
'		  ,CASE WHEN sts.transaction_status IN (70,80,100,110) THEN pp.title ELSE '' '' END SESAC_PROPERTY',
'          ,NULL Remarks -- previously used for displaying Reject Reason',
'           , DECODE(pcl.song_property_id, null, ''N'', ''Y'') jingle_flag',
'	FROM   spi_transaction_status sts',
'		  ,spi_property_xref spx',
'		  ,prop_lookups pl',
'		  ,prop_properties pp',
'          ,prop_commercial_lines pcl',
'	WHERE  	sts.property_id IS NOT NULL',
'	AND		sts.property_id = pp.property_id',
'	AND		spx.submitter_work_id = sts.submitter_work_id',
'	AND    	spx.submitter_source_id = sts.source_id',
'	AND    	pl.lookup_code = sts.transaction_status',
'	AND    	pl.lookup_type = ''SPI_STATUS'' ',
'	AND		sts.transaction_status_id = NVL(:P50_SPI_TRANSACTION_STATUS_ID, sts.transaction_status_id)',
'	AND     sts.source_id = NVL(:P50_PUB_SRC,sts.source_id)',
'	AND     pp.property_number = NVL(:P50_PROPERTY_NUMBER, pp.property_number)',
'	AND    	sts.batch_id  = NVL(:P50_FILE_NAME,sts.batch_id)',
'	--AND    NVL(sts.submission_date, SYSDATE) BETWEEN NVL(TO_DATE(:P50_SUBMISSION_DATE_FROM,''YYYY-MM-DD''),NVL(sts.submission_date, SYSDATE))',
'	--                           AND     NVL(TO_DATE(:P50_SUBMISSION_DATE_TO,''YYYY-MM-DD''),NVL(sts.submission_date, SYSDATE))',
'	AND    	NVL(sts.creation_date, SYSDATE) BETWEEN NVL(TO_DATE(:P50_SUBMISSION_DATE_FROM,''YYYY-MM-DD''),NVL(sts.creation_date, SYSDATE))',
'							   AND     NVL(TO_DATE(:P50_SUBMISSION_DATE_TO,''YYYY-MM-DD''),NVL(sts.creation_date, SYSDATE))',
'	AND    	sts.title LIKE NVL((''%''||UPPER(:P50_SONG_TITLE)||''%''),UPPER(sts.title))',
'	AND		( ',
'				:P50_PROP_ID_HFA_CODE IS NULL',
'				OR',
'				(',
'                    :P50_PROP_ID_HFA_CODE IS NOT NULL',
'                    AND EXISTS',
'                    ( ',
'                        SELECT aka_name',
'                        ,property_id',
'                        FROM prop_akas pa',
'                        WHERE aka_type_id = 51',
'                        AND pa.aka_name = UPPER(TRIM(:P50_PROP_ID_HFA_CODE))',
'                        AND pa.property_id = spx.sesac_property_id',
'                    )',
'                )',
'			) ',
'	AND    	sts.submitter_work_id LIKE NVL(''%''||TRIM(:P50_SUBMITTER_WORK_NO)||''%'',sts.submitter_work_id)',
'	AND ',
'	(',
'		:P50_TRANSACTION_STATUS IS NULL',
'		OR',
'		(:P50_TRANSACTION_STATUS IS NOT NULL AND CASE WHEN to_number(sts.transaction_status) IN (70,80,100,110) THEN 110 ELSE to_number(sts.transaction_status) END = :P50_TRANSACTION_STATUS)',
'	)',
'	/*AND    sts.transaction_status IN (select regexp_substr(NVL(:P50_TRANSACTION_STATUS,sts.transaction_status),''[^,]+'', 1, level) status from dual',
'			  connect by regexp_substr(NVL(:P50_TRANSACTION_STATUS,sts.transaction_status), ''[^,]+'', 1, level) is not null)',
'	*/          ',
'	AND',
'    (',
'        (:P50_PARTY_NAME IS NULL AND :P50_PARTY_IPI_NUMBER IS NULL)',
'        OR',
'        (:P50_PARTY_NAME IS NOT NULL OR :P50_PARTY_IPI_NUMBER IS NOT NULL)',
'        AND EXISTS',
'        ( ',
'            SELECT NULL',
'             FROM   prop_share_collectors pso',
'             WHERE pso.property_id = pp.property_id',
'             AND pso.end_date_active IS NULL',
'             --AND pso.affiliated_society_id = NVL(:P50_PARTY_AFFILIATION,pso.affiliated_society_id)',
'             AND (:P50_PARTY_AFFILIATION is null or pso.affiliated_society_id = NVL(:P50_PARTY_AFFILIATION,pso.affiliated_society_id))',
'             AND ',
'             (  :P50_PARTY_NAME IS NULL',
'                OR',
'                (',
'                    :P50_PARTY_NAME IS NOT NULL  ',
'                    AND EXISTS',
'                    (',
'                        SELECT 1 FROM spi_share_owner_xref sx',
'                        WHERE sx.submitter_party_id = pso.submitter_internal_number',
'                        AND sx.submitter_source_id = pp.source_id',
'                        AND INSTR((TRIM(Upper(sx.first_name||'' ''||sx.name))),UPPER(TRIM(:P50_PARTY_NAME)))>0',
'                        AND (:P50_PARTY_IPI_NUMBER is null or sx.IPI_NAME_NUMBER = NVL(:P50_PARTY_IPI_NUMBER,sx.IPI_NAME_NUMBER)) ',
'                        AND NVL(sx.sesac_ip_id, 602200) = pso.collector_id',
'                        UNION ALL',
'                        SELECT 1 FROM aff_ip_names ain',
'                        WHERE ain.ip_id = pso.collector_id',
'                        AND ain.name_type_id = 6',
'                        AND INSTR((Upper(ain.full_name)),UPPER(:P50_PARTY_NAME))>0',
'                        AND (:P50_PARTY_IPI_NUMBER is null or ain.name_number = NVL(:P50_PARTY_IPI_NUMBER,ain.name_number))',
'                    ) ',
'                )',
'/*       AND   ar.role_id = pso.role_id',
'         AND  ( ( ar.summary_role_code = ''W''    -- Search by writers remove as part of ticket SPI-231',
'                  AND',
'                  :P50_WRI_PUB_INDICATOR = ''Y''',
'                )',
'                OR',
'                NVL(:P50_WRI_PUB_INDICATOR,''N'') = ''N''',
'              ) */',
'            )',
'        )',
'    )',
'---------',
'---jingle sort order removed and replaced with join below',
'AND spx.sesac_property_id = pcl.song_property_id (+)',
'--------',
'	AND    ( ( :P50_ISRC_REC_INFO = ''N''',
'			   AND',
'			   NOT EXISTS ( SELECT 1',
'							FROM   prop_recordings pr',
'							WHERE      /*--pr.property_id = pps.property_id*/',
'							pr.property_id IN (SELECT property_id FROM prop_properties where source_property_id =pp.property_id AND property_type_id=2) ',
'						  )',
'			 )',
'			 OR',
'			 ( :P50_ISRC_REC_INFO = ''Y''',
'			   AND',
'			   EXISTS ( SELECT 1',
'						FROM   prop_recordings pr',
'						WHERE      /*--pr.property_id = pps.property_id*/',
'						pr.property_id IN (SELECT property_id FROM prop_properties where source_property_id =pp.property_id AND property_type_id=2) ',
'					  )',
'			 )',
'			 OR',
'			 ( :P50_ISRC_REC_INFO IS NULL )',
'			   )		 ',
'	AND    ( ( :P50_QUEUE_NAME IS NOT NULL',
'				AND EXISTS ( SELECT 1',
'							 FROM   spi_work_queue_details swqd',
'							 WHERE  swqd.work_queue_header_id = :P50_QUEUE_NAME',
'							 AND    swqd.request_id IS NULL',
'							 AND    swqd.property_id = sts.property_id',
'							 UNION',
'							 SELECT 1',
'							 FROM   spi_work_queue_details swqd',
'								   ,spi_matching_requests mmr',
'							 WHERE  swqd.work_queue_header_id = :P50_QUEUE_NAME',
'							 AND    swqd.property_id IS NULL',
'							 AND    mmr.request_id = swqd.request_id',
'							 AND    mmr.product_number = sts.submitter_work_id',
'						   )',
'			  )',
'			  OR',
'			  :P50_QUEUE_NAME IS NULL',
'			 )',
'	--ORDER BY CASE WHEN :P50_DATE_SORT_ORDER = ''O'' THEN sts.creation_date ELSE NULL END ASC',
'		--	,CASE WHEN :P50_DATE_SORT_ORDER = ''N'' THEN sts.creation_date ELSE NULL END DESC',
')',
'where :P50_QUERY = ''Y''',
'and (jingle_flag = :P50_JINGLE_INDICATOR OR :P50_JINGLE_INDICATOR IS NULL)'))
,p_plug_source_type=>'NATIVE_IR'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_plug_display_condition_type=>'NEVER'
,p_prn_content_disposition=>'ATTACHMENT'
,p_prn_document_header=>'APEX'
,p_prn_units=>'INCHES'
,p_prn_paper_size=>'LETTER'
,p_prn_width=>8.5
,p_prn_height=>11
,p_prn_orientation=>'HORIZONTAL'
,p_prn_page_header_font_color=>'#000000'
,p_prn_page_header_font_family=>'Helvetica'
,p_prn_page_header_font_weight=>'normal'
,p_prn_page_header_font_size=>'12'
,p_prn_page_footer_font_color=>'#000000'
,p_prn_page_footer_font_family=>'Helvetica'
,p_prn_page_footer_font_weight=>'normal'
,p_prn_page_footer_font_size=>'12'
,p_prn_header_bg_color=>'#9bafde'
,p_prn_header_font_color=>'#000000'
,p_prn_header_font_family=>'Helvetica'
,p_prn_header_font_weight=>'normal'
,p_prn_header_font_size=>'10'
,p_prn_body_bg_color=>'#efefef'
,p_prn_body_font_color=>'#000000'
,p_prn_body_font_family=>'Helvetica'
,p_prn_body_font_weight=>'normal'
,p_prn_body_font_size=>'10'
,p_prn_border_width=>.5
,p_prn_page_header_alignment=>'CENTER'
,p_prn_page_footer_alignment=>'CENTER'
);
end;
/
begin
wwv_flow_api.create_worksheet(
 p_id=>wwv_flow_api.id(34938734233306536)
,p_max_row_count=>'1000000'
,p_show_nulls_as=>'-'
,p_pagination_type=>'ROWS_X_TO_Y_OF_Z'
,p_pagination_display_pos=>'BOTTOM_RIGHT'
,p_report_list_mode=>'TABS'
,p_show_detail_link=>'C'
,p_show_notify=>'Y'
,p_download_formats=>'CSV:HTML:EMAIL:XLS:PDF:RTF'
,p_detail_link=>'f?p=&APP_ID.:50:&SESSION.::&DEBUG.:RP:P50_QUERY,P50_INQ_SELECTION:Y,C'
,p_detail_link_text=>'<img src="#IMAGE_PREFIX#app_ui/img/icons/apex-edit-pencil.png" class="apex-edit-pencil" alt="">'
,p_owner=>'RRAWAT'
,p_internal_uid=>34938734233306536
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34938858991306537)
,p_db_column_name=>'FILE_NAME'
,p_display_order=>20
,p_column_identifier=>'A'
,p_column_label=>'File Name'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(28658078858759510)
,p_db_column_name=>'TRANSACTION_STATUS_ID'
,p_display_order=>30
,p_column_identifier=>'M'
,p_column_label=>'Transaction status id'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(28658188197759511)
,p_db_column_name=>'SUBMISSION_DATE'
,p_display_order=>40
,p_column_identifier=>'N'
,p_column_label=>'Submission date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(28658263610759512)
,p_db_column_name=>'SOURCE_ID'
,p_display_order=>50
,p_column_identifier=>'O'
,p_column_label=>'Source id'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(28658339268759513)
,p_db_column_name=>'SUBMITTER_WORK_ID'
,p_display_order=>60
,p_column_identifier=>'P'
,p_column_label=>'Submitter work id'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(28658431226759514)
,p_db_column_name=>'TITLE'
,p_display_order=>70
,p_column_identifier=>'Q'
,p_column_label=>'Title'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(28658500399759515)
,p_db_column_name=>'TRANSACTION_STATUS'
,p_display_order=>80
,p_column_identifier=>'R'
,p_column_label=>'Transaction status'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(28658674050759516)
,p_db_column_name=>'STATUS'
,p_display_order=>90
,p_column_identifier=>'S'
,p_column_label=>'Status'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(28658743418759517)
,p_db_column_name=>'PROPERTY_NUMBER'
,p_display_order=>100
,p_column_identifier=>'T'
,p_column_label=>'Property number'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(28658867500759518)
,p_db_column_name=>'PROPERTY_ID'
,p_display_order=>110
,p_column_identifier=>'U'
,p_column_label=>'Property id'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(28658980354759519)
,p_db_column_name=>'SESAC_PROPERTY_ID'
,p_display_order=>120
,p_column_identifier=>'V'
,p_column_label=>'Sesac property id'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(28659000361759520)
,p_db_column_name=>'SESAC_PROPERTY'
,p_display_order=>130
,p_column_identifier=>'W'
,p_column_label=>'Sesac property'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(28659114377759521)
,p_db_column_name=>'REMARKS'
,p_display_order=>140
,p_column_identifier=>'X'
,p_column_label=>'Remarks'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(28659237433759522)
,p_db_column_name=>'JINGLE_FLAG'
,p_display_order=>150
,p_column_identifier=>'Y'
,p_column_label=>'Jingle flag'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(39144843809095742)
,p_application_user=>'APXWS_DEFAULT'
,p_report_seq=>10
,p_report_type=>'GROUP_BY'
,p_report_alias=>'391449'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'FILE_NAME:LINK_TO_REPORT_ID:SUBMISSION_DATE:SUBMITTER_WORK_ID:TITLE:STATUS:PROPERTY_NUMBER:PROPERTY_ID_ID:SESAC_PROPERTY:REMARKS:JINGLE_FLAG'
);
wwv_flow_api.create_worksheet_group_by(
 p_id=>wwv_flow_api.id(39336144927983069)
,p_report_id=>wwv_flow_api.id(39144843809095742)
,p_group_by_columns=>'SOURCE_ID:FILE_NAME:CREATION_DATE:TRANSACTION_STATUS'
,p_function_01=>'COUNT'
,p_function_column_01=>'SOURCE_ID'
,p_function_db_column_name_01=>'APXWS_GBFC_01'
,p_function_label_01=>'Count'
,p_function_format_mask_01=>'999G999G999G999G990'
,p_function_sum_01=>'N'
,p_sort_column_01=>'SOURCE_ID'
,p_sort_direction_01=>'ASC'
,p_sort_column_02=>'FILE_NAME'
,p_sort_direction_02=>'ASC'
,p_sort_column_03=>'CREATION_DATE'
,p_sort_direction_03=>'DESC'
,p_sort_column_04=>'TRANSACTION_STATUS'
,p_sort_direction_04=>'ASC'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(39171209308319756)
,p_application_user=>'APXWS_ALTERNATIVE'
,p_name=>'Source Report'
,p_report_seq=>10
,p_report_type=>'GROUP_BY'
,p_report_alias=>'391713'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'FILE_NAME:LINK_TO_REPORT_ID:SUBMISSION_DATE:SUBMITTER_WORK_ID:TITLE:STATUS:PROPERTY_NUMBER:PROPERTY_ID_ID:SESAC_PROPERTY:REMARKS:JINGLE_FLAG'
);
wwv_flow_api.create_worksheet_group_by(
 p_id=>wwv_flow_api.id(39435710269118540)
,p_report_id=>wwv_flow_api.id(39171209308319756)
,p_group_by_columns=>'SOURCE_ID:LINK_TO_REPORT'
,p_function_01=>'COUNT'
,p_function_column_01=>'SOURCE_ID'
,p_function_db_column_name_01=>'APXWS_GBFC_01'
,p_function_label_01=>'Count'
,p_function_format_mask_01=>'999G999G999G999G990'
,p_function_sum_01=>'N'
,p_sort_column_01=>'SOURCE_ID'
,p_sort_direction_01=>'ASC'
,p_sort_column_02=>'FILE_NAME'
,p_sort_direction_02=>'ASC'
,p_sort_column_03=>'TRANSACTION_STATUS'
,p_sort_direction_03=>'ASC'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(39173281980343677)
,p_application_user=>'APXWS_ALTERNATIVE'
,p_name=>'File Name Report'
,p_report_seq=>10
,p_report_type=>'GROUP_BY'
,p_report_alias=>'391733'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'FILE_NAME:LINK_TO_REPORT_ID:SUBMISSION_DATE:SUBMITTER_WORK_ID:TITLE:STATUS:PROPERTY_NUMBER:PROPERTY_ID_ID:SESAC_PROPERTY:REMARKS:JINGLE_FLAG'
);
wwv_flow_api.create_worksheet_group_by(
 p_id=>wwv_flow_api.id(39338974908016753)
,p_report_id=>wwv_flow_api.id(39173281980343677)
,p_group_by_columns=>'SOURCE_ID:FILE_NAME'
,p_function_01=>'COUNT'
,p_function_column_01=>'SOURCE_ID'
,p_function_db_column_name_01=>'APXWS_GBFC_01'
,p_function_label_01=>'Count'
,p_function_format_mask_01=>'999G999G999G999G990'
,p_function_sum_01=>'N'
,p_sort_column_01=>'SOURCE_ID'
,p_sort_direction_01=>'ASC'
,p_sort_column_02=>'FILE_NAME'
,p_sort_direction_02=>'ASC'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(39174269404351589)
,p_application_user=>'APXWS_ALTERNATIVE'
,p_name=>'Transaction Status Report'
,p_report_seq=>10
,p_report_type=>'GROUP_BY'
,p_report_alias=>'391743'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'FILE_NAME:LINK_TO_REPORT_ID:SUBMISSION_DATE:SUBMITTER_WORK_ID:TITLE:STATUS:PROPERTY_NUMBER:PROPERTY_ID_ID:SESAC_PROPERTY:REMARKS:JINGLE_FLAG'
);
wwv_flow_api.create_worksheet_group_by(
 p_id=>wwv_flow_api.id(39339723533023959)
,p_report_id=>wwv_flow_api.id(39174269404351589)
,p_group_by_columns=>'SOURCE_ID:FILE_NAME:TRANSACTION_STATUS'
,p_function_01=>'COUNT'
,p_function_column_01=>'SOURCE_ID'
,p_function_db_column_name_01=>'APXWS_GBFC_01'
,p_function_label_01=>'Count'
,p_function_format_mask_01=>'999G999G999G999G990'
,p_function_sum_01=>'N'
,p_sort_column_01=>'SOURCE_ID'
,p_sort_direction_01=>'ASC'
,p_sort_column_02=>'FILE_NAME'
,p_sort_direction_02=>'ASC'
,p_sort_column_03=>'TRANSACTION_STATUS'
,p_sort_direction_03=>'ASC'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(39177244029478446)
,p_application_user=>'APXWS_ALTERNATIVE'
,p_name=>'Submission Date Report'
,p_report_seq=>10
,p_report_type=>'GROUP_BY'
,p_report_alias=>'391773'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'FILE_NAME:LINK_TO_REPORT_ID:SUBMISSION_DATE:SUBMITTER_WORK_ID:TITLE:STATUS:PROPERTY_NUMBER:PROPERTY_ID_ID:SESAC_PROPERTY:REMARKS:JINGLE_FLAG'
);
wwv_flow_api.create_worksheet_group_by(
 p_id=>wwv_flow_api.id(39340381265035071)
,p_report_id=>wwv_flow_api.id(39177244029478446)
,p_group_by_columns=>'SOURCE_ID:FILE_NAME:CREATION_DATE'
,p_function_01=>'COUNT'
,p_function_column_01=>'SOURCE_ID'
,p_function_db_column_name_01=>'APXWS_GBFC_01'
,p_function_label_01=>'Count'
,p_function_format_mask_01=>'999G999G999G999G990'
,p_function_sum_01=>'N'
,p_sort_column_01=>'SOURCE_ID'
,p_sort_direction_01=>'ASC'
,p_sort_column_02=>'FILE_NAME'
,p_sort_direction_02=>'ASC'
,p_sort_column_03=>'CREATION_DATE'
,p_sort_direction_03=>'DESC'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(35501260440546849)
,p_plug_name=>'&nbsp'
,p_region_template_options=>'#DEFAULT#:t-Region--noPadding:t-Region--removeHeader:t-Region--scrollBody'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>15
,p_include_in_reg_disp_sel_yn=>'Y'
,p_plug_display_point=>'BODY'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_attribute_01=>'N'
,p_attribute_02=>'HTML'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(40198117209804233)
,p_plug_name=>'TAB Container'
,p_region_template_options=>'#DEFAULT#:t-TabsRegion-mod--simple'
,p_plug_template=>wwv_flow_api.id(51851408341240826)
,p_plug_display_sequence=>90
,p_include_in_reg_disp_sel_yn=>'Y'
,p_plug_display_point=>'BODY'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_plug_display_condition_type=>'VAL_OF_ITEM_IN_COND_EQ_COND2'
,p_plug_display_when_condition=>'P50_INQ_SELECTION'
,p_plug_display_when_cond2=>'C'
,p_attribute_01=>'N'
,p_attribute_02=>'HTML'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(25106599930958203)
,p_plug_name=>'Summary'
,p_parent_plug_id=>wwv_flow_api.id(40198117209804233)
,p_region_template_options=>'#DEFAULT#:t-Form--slimPadding'
,p_plug_template=>wwv_flow_api.id(51848808455240825)
,p_plug_display_sequence=>70
,p_plug_display_point=>'BODY'
,p_query_type=>'SQL'
,p_plug_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT ',
'        (SELECT ps.source_code||'' - ''||ps.description "Description"',
'           FROM   prop_sources ps',
'          WHERE  ps.source_id = spi.source_id) AS source_id, ',
'          spi.source_id src_id,',
'          NVL(FILE_NAME, ''<File name unavailable>'') FILE_NAME,',
'          -- batch_id||''-''||NVL(FILE_NAME, ''<File name unavailable>'') FILE_NAME,',
'        --file_name, ',
'        TRUNC(creation_date) creation_date,',
'       -- CASE WHEN  to_number(transaction_status) IN (70,80,100,110) THEN 110 ELSE to_number(transaction_status) end AS transaction_status,',
'        batch_id,',
'        COUNT(1) Count',
' FROM spi_transaction_status spi',
' WHERE TRUNC(NVL(spi.creation_date, SYSDATE)) --BETWEEN to_date(:P50_SUBMISSION_DATE_FROM_1,''YYYY-MM-DD'') and to_date(:P50_SUBMISSION_DATE_TO_1,''YYYY-MM-DD'')',
' BETWEEN NVL(TO_DATE(:P50_SUBMISSION_DATE_FROM_1,''YYYY-MM-DD''),TRUNC(NVL(spi.creation_date, SYSDATE)))',
'							   AND     NVL(TO_DATE(:P50_SUBMISSION_DATE_TO_1,''YYYY-MM-DD''),TRUNC(NVL(spi.creation_date, SYSDATE)))',
' --AND SOURCE_ID = nvl(:P50_SOURCE,SOURCE_ID) AND SPI.FILE_NAME=  NVL(:P50_FILE,SPI.FILE_NAME)',
' --AND SPI.BATCH_ID  = nvl(:P50_FILE,batch_id)',
'GROUP BY source_id,',
'        spi.source_id,',
'         file_name,',
'         TRUNC(creation_date),',
'         transaction_status,',
'         batch_id'))
,p_plug_source_type=>'NATIVE_IR'
,p_ajax_items_to_submit=>'P50_SOURCE,P50_FILE,P50_SUBMISSION_DATE_FROM_1,P50_SUBMISSION_DATE_TO_1'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_plug_display_condition_type=>'VAL_OF_ITEM_IN_COND_EQ_COND2'
,p_plug_display_when_condition=>'P50_INQ_SELECTION'
,p_plug_display_when_cond2=>'C'
,p_prn_content_disposition=>'ATTACHMENT'
,p_prn_document_header=>'APEX'
,p_prn_units=>'INCHES'
,p_prn_paper_size=>'LETTER'
,p_prn_width=>8.5
,p_prn_height=>11
,p_prn_orientation=>'HORIZONTAL'
,p_prn_page_header_font_color=>'#000000'
,p_prn_page_header_font_family=>'Helvetica'
,p_prn_page_header_font_weight=>'normal'
,p_prn_page_header_font_size=>'12'
,p_prn_page_footer_font_color=>'#000000'
,p_prn_page_footer_font_family=>'Helvetica'
,p_prn_page_footer_font_weight=>'normal'
,p_prn_page_footer_font_size=>'12'
,p_prn_header_bg_color=>'#9bafde'
,p_prn_header_font_color=>'#000000'
,p_prn_header_font_family=>'Helvetica'
,p_prn_header_font_weight=>'normal'
,p_prn_header_font_size=>'10'
,p_prn_body_bg_color=>'#efefef'
,p_prn_body_font_color=>'#000000'
,p_prn_body_font_family=>'Helvetica'
,p_prn_body_font_weight=>'normal'
,p_prn_body_font_size=>'10'
,p_prn_border_width=>.5
,p_prn_page_header_alignment=>'CENTER'
,p_prn_page_footer_alignment=>'CENTER'
);
wwv_flow_api.create_worksheet(
 p_id=>wwv_flow_api.id(34232488785807936)
,p_max_row_count=>'1000000'
,p_show_nulls_as=>'-'
,p_pagination_type=>'ROWS_X_TO_Y_OF_Z'
,p_pagination_display_pos=>'BOTTOM_RIGHT'
,p_report_list_mode=>'TABS'
,p_show_detail_link=>'N'
,p_show_notify=>'Y'
,p_download_formats=>'CSV:HTML:EMAIL:XLS:PDF:RTF'
,p_owner=>'KBAKSHI'
,p_internal_uid=>34232488785807936
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34232522196807937)
,p_db_column_name=>'SOURCE_ID'
,p_display_order=>10
,p_column_identifier=>'A'
,p_column_label=>'Source'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34232656602807938)
,p_db_column_name=>'SRC_ID'
,p_display_order=>20
,p_column_identifier=>'B'
,p_column_label=>'Src id'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34232772483807939)
,p_db_column_name=>'FILE_NAME'
,p_display_order=>30
,p_column_identifier=>'C'
,p_column_label=>'File name'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34232883574807940)
,p_db_column_name=>'CREATION_DATE'
,p_display_order=>40
,p_column_identifier=>'D'
,p_column_label=>'Submission Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34232955264807941)
,p_db_column_name=>'BATCH_ID'
,p_display_order=>50
,p_column_identifier=>'E'
,p_column_label=>'Batch id'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34233014700807942)
,p_db_column_name=>'COUNT'
,p_display_order=>60
,p_column_identifier=>'F'
,p_column_label=>'Count'
,p_column_link=>'f?p=&APP_ID.:50:&SESSION.::&DEBUG.:RP,50:P50_INQ_SELECTION,P50_PUB_SRC,P50_FILE_NAME,P50_QUERY:S,#SRC_ID#,#FILE_NAME#,Y'
,p_column_linktext=>'#COUNT#'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(34411802833608045)
,p_application_user=>'APXWS_DEFAULT'
,p_report_seq=>10
,p_report_alias=>'344119'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_display_rows=>15
,p_report_columns=>'SOURCE_ID:FILE_NAME:CREATION_DATE:COUNT:'
,p_sort_column_1=>'SOURCE_ID'
,p_sort_direction_1=>'ASC'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(40195611484804208)
,p_name=>'By Source & File'
,p_parent_plug_id=>wwv_flow_api.id(40198117209804233)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>80
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#:t-Report--altRowsDefault:t-Report--rowHighlight'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT ',
'        (SELECT ps.source_code||'' - ''||ps.description "Description"',
'           FROM   prop_sources ps',
'          WHERE  ps.source_id = spi.source_id) AS source_id,',
'          spi.source_id src_id,',
'           NVL(FILE_NAME, ''<File name unavailable>'') FILE_NAME,',
'         --batch_id||''-''||NVL(FILE_NAME, ''<File name unavailable>'') FILE_NAME, ',
'        --file_name,',
'        TRUNC(creation_date) creation_date,',
'        batch_id,',
'        --CASE WHEN  to_number(transaction_status) IN (70,80,100,110) THEN 110 ELSE to_number(transaction_status) end AS transaction_status,',
'        COUNT(1) Count',
' FROM spi_transaction_status spi',
'WHERE SOURCE_ID = nvl(:P50_SOURCE,SOURCE_ID) ',
'AND NVL(spi.file_name, ''-X'') = NVL (:P50_FILE, NVL(spi.file_name, ''-X''))',
'--AND SPI.FILE_NAME=  NVL(:P50_FILE,SPI.FILE_NAME)',
'--AND SPI.BATCH_ID  = nvl(:P50_FILE,batch_id)',
'GROUP BY source_id,',
'         spi.source_id,',
'         file_name,',
'         TRUNC(creation_date),',
'         batch_id',
'        -- transaction_status',
'ORDER BY source_id,file_name;'))
,p_display_condition_type=>'NEVER'
,p_ajax_enabled=>'Y'
,p_ajax_items_to_submit=>'P50_FILE,P50_SOURCE'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>'-'
,p_query_num_rows_type=>'ROW_RANGES_IN_SELECT_LIST'
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_csv_output=>'N'
,p_prn_output=>'N'
,p_sort_null=>'L'
,p_plug_query_strip_html=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(40195782119804209)
,p_query_column_id=>1
,p_column_alias=>'SOURCE_ID'
,p_column_display_sequence=>1
,p_column_heading=>'Source id'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(23427823188815905)
,p_query_column_id=>2
,p_column_alias=>'SRC_ID'
,p_column_display_sequence=>6
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(40195814645804210)
,p_query_column_id=>3
,p_column_alias=>'FILE_NAME'
,p_column_display_sequence=>2
,p_column_heading=>'File name'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(40195964694804211)
,p_query_column_id=>4
,p_column_alias=>'CREATION_DATE'
,p_column_display_sequence=>3
,p_column_heading=>'Creation date'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(40198316038804235)
,p_query_column_id=>5
,p_column_alias=>'BATCH_ID'
,p_column_display_sequence=>5
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(40196176061804213)
,p_query_column_id=>6
,p_column_alias=>'COUNT'
,p_column_display_sequence=>4
,p_column_heading=>'Count'
,p_column_link=>'f?p=&APP_ID.:50:&SESSION.::&DEBUG.:RP,50:P50_PUB_SRC,P50_FILE_NAME,P50_QUERY:#SRC_ID#,#FILE_NAME#,Y'
,p_column_linktext=>'#COUNT#'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(40196288147804214)
,p_plug_name=>'By File & transaction'
,p_parent_plug_id=>wwv_flow_api.id(40198117209804233)
,p_region_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51848808455240825)
,p_plug_display_sequence=>90
,p_plug_display_point=>'BODY'
,p_query_type=>'SQL'
,p_plug_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT ',
'        (SELECT ps.source_code||'' - ''||ps.description "Description"',
'           FROM   prop_sources ps',
'          WHERE  ps.source_id = spi.source_id) AS source_id, ',
'          spi.source_id src_id,',
'           NVL(FILE_NAME, ''<File name unavailable>'') FILE_NAME,',
'         --batch_id||''-''||NVL(FILE_NAME, ''<File name unavailable>'') FILE_NAME,',
'        --file_name, ',
'        TRUNC(creation_date) creation_date,',
'        CASE WHEN  to_number(transaction_status) IN (70,80,100,110) THEN 110 ELSE to_number(transaction_status) end AS transaction_status,',
'        batch_id,',
'        COUNT(1) Count,',
'        CASE WHEN  to_number(transaction_status) IN (70,80,100,110) THEN 110 ELSE to_number(transaction_status) end AS transaction_status2',
' FROM spi_transaction_status spi',
'WHERE  	TRUNC(NVL(spi.creation_date, SYSDATE)) BETWEEN NVL(TO_DATE(:P50_SUBMISSION_DATE_FROM_1,''YYYY-MM-DD''),TRUNC(NVL(spi.creation_date, SYSDATE)))',
'							   AND     NVL(TO_DATE(:P50_SUBMISSION_DATE_TO_1,''YYYY-MM-DD''),TRUNC(NVL(spi.creation_date, SYSDATE)))',
'--AND SOURCE_ID = nvl(:P50_SOURCE,SOURCE_ID) AND SPI.FILE_NAME=  NVL(:P50_FILE,SPI.FILE_NAME)',
'--AND SPI.BATCH_ID  = nvl(:P50_FILE,batch_id)',
'GROUP BY source_id,',
'        spi.source_id,',
'         file_name,',
'         TRUNC(creation_date),',
'         transaction_status,',
'         batch_id'))
,p_plug_source_type=>'NATIVE_IR'
,p_ajax_items_to_submit=>'P50_SOURCE,P50_FILE,P50_SUBMISSION_DATE_FROM_1,P50_SUBMISSION_DATE_TO_1'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_prn_content_disposition=>'ATTACHMENT'
,p_prn_document_header=>'APEX'
,p_prn_units=>'INCHES'
,p_prn_paper_size=>'LETTER'
,p_prn_width=>8.5
,p_prn_height=>11
,p_prn_orientation=>'HORIZONTAL'
,p_prn_page_header_font_color=>'#000000'
,p_prn_page_header_font_family=>'Helvetica'
,p_prn_page_header_font_weight=>'normal'
,p_prn_page_header_font_size=>'12'
,p_prn_page_footer_font_color=>'#000000'
,p_prn_page_footer_font_family=>'Helvetica'
,p_prn_page_footer_font_weight=>'normal'
,p_prn_page_footer_font_size=>'12'
,p_prn_header_bg_color=>'#9bafde'
,p_prn_header_font_color=>'#000000'
,p_prn_header_font_family=>'Helvetica'
,p_prn_header_font_weight=>'normal'
,p_prn_header_font_size=>'10'
,p_prn_body_bg_color=>'#efefef'
,p_prn_body_font_color=>'#000000'
,p_prn_body_font_family=>'Helvetica'
,p_prn_body_font_weight=>'normal'
,p_prn_body_font_size=>'10'
,p_prn_border_width=>.5
,p_prn_page_header_alignment=>'CENTER'
,p_prn_page_footer_alignment=>'CENTER'
);
wwv_flow_api.create_worksheet(
 p_id=>wwv_flow_api.id(34231789514807929)
,p_max_row_count=>'1000000'
,p_show_nulls_as=>'-'
,p_pagination_type=>'ROWS_X_TO_Y_OF_Z'
,p_pagination_display_pos=>'BOTTOM_RIGHT'
,p_report_list_mode=>'TABS'
,p_show_detail_link=>'N'
,p_show_notify=>'Y'
,p_download_formats=>'CSV:HTML:EMAIL:XLS:PDF:RTF'
,p_owner=>'KBAKSHI'
,p_internal_uid=>34231789514807929
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34231862831807930)
,p_db_column_name=>'SOURCE_ID'
,p_display_order=>10
,p_column_identifier=>'A'
,p_column_label=>'Source'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34231963014807931)
,p_db_column_name=>'SRC_ID'
,p_display_order=>20
,p_column_identifier=>'B'
,p_column_label=>'Src id'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34232000526807932)
,p_db_column_name=>'FILE_NAME'
,p_display_order=>30
,p_column_identifier=>'C'
,p_column_label=>'File name'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34232197967807933)
,p_db_column_name=>'TRANSACTION_STATUS'
,p_display_order=>40
,p_column_identifier=>'D'
,p_column_label=>'Transaction status'
,p_column_type=>'NUMBER'
,p_display_text_as=>'LOV_ESCAPE_SC'
,p_column_alignment=>'RIGHT'
,p_rpt_named_lov=>wwv_flow_api.id(10837137064964149)
,p_rpt_show_filter_lov=>'1'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34232247860807934)
,p_db_column_name=>'BATCH_ID'
,p_display_order=>50
,p_column_identifier=>'E'
,p_column_label=>'Batch id'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34232307928807935)
,p_db_column_name=>'COUNT'
,p_display_order=>60
,p_column_identifier=>'F'
,p_column_label=>'Count'
,p_column_link=>'f?p=&APP_ID.:50:&SESSION.::&DEBUG.:RP,50:P50_PUB_SRC,P50_FILE_NAME,P50_TRANSACTION_STATUS,P50_QUERY:#SRC_ID#,#FILE_NAME#,#TRANSACTION_STATUS2#,Y'
,p_column_linktext=>'#COUNT#'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(35374022940620903)
,p_db_column_name=>'TRANSACTION_STATUS2'
,p_display_order=>70
,p_column_identifier=>'G'
,p_column_label=>'Transaction status2'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34975531534886017)
,p_db_column_name=>'CREATION_DATE'
,p_display_order=>80
,p_column_identifier=>'H'
,p_column_label=>'Submission date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(34412445676608157)
,p_application_user=>'APXWS_DEFAULT'
,p_report_seq=>10
,p_report_alias=>'344125'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_display_rows=>15
,p_report_columns=>'SOURCE_ID:FILE_NAME:TRANSACTION_STATUS:COUNT::TRANSACTION_STATUS2:CREATION_DATE'
,p_sort_column_1=>'SOURCE_ID'
,p_sort_direction_1=>'ASC'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(40196841269804220)
,p_name=>'Summary by Source4'
,p_parent_plug_id=>wwv_flow_api.id(40198117209804233)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>100
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#:t-Report--altRowsDefault:t-Report--rowHighlight'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT ',
'        (SELECT ps.source_code||'' - ''||ps.description "Description"',
'           FROM   prop_sources ps',
'          WHERE  ps.source_id = spi.source_id) AS source_id, ',
'           batch_id||''-''||NVL(FILE_NAME, ''<File name unavailable>'') FILE_NAME,',
'       -- file_name, ',
'       -- TRUNC(creation_date) creation_date,',
'       -- CASE WHEN  to_number(transaction_status) IN (70,80,100,110) THEN 110 ELSE to_number(transaction_status) end AS transaction_status,',
'        COUNT(1) Count',
' FROM spi_transaction_status spi',
'WHERE SOURCE_ID = nvl(:P50_SOURCE,SOURCE_ID) AND SPI.BATCH_ID  = nvl(:P50_FILE,batch_id)',
'GROUP BY source_id,',
'  batch_id||''-''||NVL(FILE_NAME, ''<File name unavailable>'')',
'         --file_name',
'        -- TRUNC(creation_date),',
'        -- transaction_status',
'ORDER BY source_id,file_name;'))
,p_display_condition_type=>'NEVER'
,p_ajax_enabled=>'Y'
,p_ajax_items_to_submit=>'P50_SOURCE,P50_FILE'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>'-'
,p_query_num_rows_type=>'ROW_RANGES_IN_SELECT_LIST'
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_csv_output=>'N'
,p_prn_output=>'N'
,p_sort_null=>'L'
,p_plug_query_strip_html=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(40196958002804221)
,p_query_column_id=>1
,p_column_alias=>'SOURCE_ID'
,p_column_display_sequence=>1
,p_column_heading=>'Source id'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(40197097187804222)
,p_query_column_id=>2
,p_column_alias=>'FILE_NAME'
,p_column_display_sequence=>2
,p_column_heading=>'File name'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(40197374319804225)
,p_query_column_id=>3
,p_column_alias=>'COUNT'
,p_column_display_sequence=>3
,p_column_heading=>'Count'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(40197401492804226)
,p_plug_name=>'By Source'
,p_parent_plug_id=>wwv_flow_api.id(40198117209804233)
,p_region_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51848808455240825)
,p_plug_display_sequence=>110
,p_plug_display_point=>'BODY'
,p_query_type=>'SQL'
,p_plug_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT ',
'        (SELECT ps.source_code||'' - ''||ps.description "Description"',
'           FROM   prop_sources ps',
'          WHERE  ps.source_id = spi.source_id) AS source, ',
'          source_id,',
'        --file_name, ',
'        --TRUNC(creation_date) creation_date,',
'        --CASE WHEN  to_number(transaction_status) IN (70,80,100,110) THEN 110 ELSE to_number(transaction_status) end AS transaction_status,',
'        COUNT(1) Count',
' FROM spi_transaction_status spi',
' WHERE TRUNC(NVL(spi.creation_date, SYSDATE)) BETWEEN NVL(TO_DATE(:P50_SUBMISSION_DATE_FROM_1,''YYYY-MM-DD''),TRUNC(NVL(spi.creation_date, SYSDATE)))',
'							   AND     NVL(TO_DATE(:P50_SUBMISSION_DATE_TO_1,''YYYY-MM-DD''),TRUNC(NVL(spi.creation_date, SYSDATE)))',
' --SOURCE_ID = nvl(:P50_SOURCE,SOURCE_ID) ',
' -- AND SPI.FILE_NAME=  NVL(:P50_FILE,SPI.FILE_NAME)',
' --AND SPI.BATCH_ID  = nvl(:P50_FILE,batch_id)',
'GROUP BY source_id',
'         --file_name,',
'         --TRUNC(creation_date),',
'         --transaction_status',
''))
,p_plug_source_type=>'NATIVE_IR'
,p_ajax_items_to_submit=>'P50_SOURCE,P50_FILE,P50_SUBMISSION_DATE_TO_1,P50_SUBMISSION_DATE_FROM_1'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_prn_content_disposition=>'ATTACHMENT'
,p_prn_document_header=>'APEX'
,p_prn_units=>'INCHES'
,p_prn_paper_size=>'LETTER'
,p_prn_width=>8.5
,p_prn_height=>11
,p_prn_orientation=>'HORIZONTAL'
,p_prn_page_header_font_color=>'#000000'
,p_prn_page_header_font_family=>'Helvetica'
,p_prn_page_header_font_weight=>'normal'
,p_prn_page_header_font_size=>'12'
,p_prn_page_footer_font_color=>'#000000'
,p_prn_page_footer_font_family=>'Helvetica'
,p_prn_page_footer_font_weight=>'normal'
,p_prn_page_footer_font_size=>'12'
,p_prn_header_bg_color=>'#9bafde'
,p_prn_header_font_color=>'#000000'
,p_prn_header_font_family=>'Helvetica'
,p_prn_header_font_weight=>'normal'
,p_prn_header_font_size=>'10'
,p_prn_body_bg_color=>'#efefef'
,p_prn_body_font_color=>'#000000'
,p_prn_body_font_family=>'Helvetica'
,p_prn_body_font_weight=>'normal'
,p_prn_body_font_size=>'10'
,p_prn_border_width=>.5
,p_prn_page_header_alignment=>'CENTER'
,p_prn_page_footer_alignment=>'CENTER'
);
wwv_flow_api.create_worksheet(
 p_id=>wwv_flow_api.id(34233164618807943)
,p_max_row_count=>'1000000'
,p_show_nulls_as=>'-'
,p_pagination_type=>'ROWS_X_TO_Y_OF_Z'
,p_pagination_display_pos=>'BOTTOM_RIGHT'
,p_report_list_mode=>'TABS'
,p_show_detail_link=>'N'
,p_show_notify=>'Y'
,p_download_formats=>'CSV:HTML:EMAIL:XLS:PDF:RTF'
,p_owner=>'KBAKSHI'
,p_internal_uid=>34233164618807943
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34233297161807944)
,p_db_column_name=>'SOURCE'
,p_display_order=>10
,p_column_identifier=>'A'
,p_column_label=>'Source'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34233397431807945)
,p_db_column_name=>'SOURCE_ID'
,p_display_order=>20
,p_column_identifier=>'B'
,p_column_label=>'Source id'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34233403035807946)
,p_db_column_name=>'COUNT'
,p_display_order=>30
,p_column_identifier=>'C'
,p_column_label=>'Count'
,p_column_link=>'f?p=&APP_ID.:50:&SESSION.::&DEBUG.:RP,50:P50_PUB_SRC,P50_QUERY:#SOURCE_ID#,Y'
,p_column_linktext=>'#COUNT#'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(34413076736608247)
,p_application_user=>'APXWS_DEFAULT'
,p_report_seq=>10
,p_report_alias=>'344131'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_display_rows=>15
,p_report_columns=>'SOURCE:COUNT:'
,p_sort_column_1=>'SOURCE'
,p_sort_direction_1=>'ASC'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(43292541965079910)
,p_plug_name=>'Filter'
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>80
,p_plug_display_point=>'BODY'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_plug_display_condition_type=>'VAL_OF_ITEM_IN_COND_EQ_COND2'
,p_plug_display_when_condition=>'P50_INQ_SELECTION'
,p_plug_display_when_cond2=>'C'
,p_attribute_01=>'N'
,p_attribute_02=>'HTML'
);
end;
/
begin
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(67142388554896140)
,p_plug_name=>'Song Request Search'
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--scrollBody:t-Form--slimPadding:t-Form--stretchInputs'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>20
,p_plug_display_point=>'BODY'
,p_plug_query_num_rows=>15
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_plug_display_condition_type=>'VAL_OF_ITEM_IN_COND_EQ_COND2'
,p_plug_display_when_condition=>'P50_INQ_SELECTION'
,p_plug_display_when_cond2=>'S'
,p_attribute_01=>'N'
,p_attribute_02=>'HTML'
,p_attribute_03=>'Y'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(67170313313898703)
,p_plug_name=>'Search Results'
,p_region_name=>'myreport'
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>30
,p_plug_display_point=>'BODY'
,p_query_type=>'SQL'
,p_plug_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select APEX_ITEM.CHECKBOX2',
'                    (',
'                        p_idx => 1, ',
'                        p_value => ac1.c001, ',
'                        p_attributes => ''class="cl-col1" title="Select to Revaluate"'', ',
'						p_item_id => ''id-select-mark-gen-'' || ac1.c001,',
'                        p_checked_values => ac2.c001',
'                    ) "Select",',
'ac1.c001,ac1.c002,ac1.c003,ac1.c004,ac1.c005,ac1.c006,ac1.c007,ac1.c008,ac1.c009,ac1.c010,ac1.c011,ac1.c012,ac1.c013,ac1.c014',
',ac1.c015,ac1.c016,ac1.c017,ac1.c018,ac1.c019,ac1.c020',
'from apex_collections ac1 ,apex_collections ac2 ',
'where ac1.collection_name=''REVALUATE_STATUS''',
'and ac2.collection_name(+)=''REVALUATE_STATUS_1''',
'and ac1.c001=ac2.c001(+)',
'order by ac2.c001 nulls first',
'',
''))
,p_plug_source_type=>'NATIVE_IR'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_plug_display_condition_type=>'PLSQL_EXPRESSION'
,p_plug_display_when_condition=>':P50_INQ_SELECTION = ''S'' AND :P50_CONFLICT_FLAG = ''N'''
,p_prn_content_disposition=>'ATTACHMENT'
,p_prn_document_header=>'APEX'
,p_prn_units=>'INCHES'
,p_prn_paper_size=>'LETTER'
,p_prn_width=>8.5
,p_prn_height=>11
,p_prn_orientation=>'HORIZONTAL'
,p_prn_page_header_font_color=>'#000000'
,p_prn_page_header_font_family=>'Helvetica'
,p_prn_page_header_font_weight=>'normal'
,p_prn_page_header_font_size=>'12'
,p_prn_page_footer_font_color=>'#000000'
,p_prn_page_footer_font_family=>'Helvetica'
,p_prn_page_footer_font_weight=>'normal'
,p_prn_page_footer_font_size=>'12'
,p_prn_header_bg_color=>'#9bafde'
,p_prn_header_font_color=>'#000000'
,p_prn_header_font_family=>'Helvetica'
,p_prn_header_font_weight=>'normal'
,p_prn_header_font_size=>'10'
,p_prn_body_bg_color=>'#efefef'
,p_prn_body_font_color=>'#000000'
,p_prn_body_font_family=>'Helvetica'
,p_prn_body_font_weight=>'normal'
,p_prn_body_font_size=>'10'
,p_prn_border_width=>.5
,p_prn_page_header_alignment=>'CENTER'
,p_prn_page_footer_alignment=>'CENTER'
,p_plug_comment=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select * from ',
'(   --to get only Rejected works',
'    SELECT DISTINCT sts.transaction_status_id',
'		  ,sts.creation_date AS submission_date',
'		  ,sts.source_id',
'		  ,sts.file_name',
'		  ,sts.submission_id',
'		  ,sts.submitter_work_id',
'		  ,sts.title',
'		  ,sts.transaction_status',
'		  ,pl.description "STATUS"',
'		  ,NULL AS property_number',
'		  ,sts.property_id',
'		  ,spx.sesac_property_id',
'		  ,'' '' SESAC_PROPERTY',
'          ,(',
'                SELECT REPLACE(REPLACE(error_message,''#''),'''''''') error_message',
'                FROM cwr_messages ',
'                WHERE message_type = ''REJ''',
'                AND sts.rejection_code = cwr_message_id',
'           ) Remarks',
'           ,NULL jingle_flag',
'	FROM   spi_transaction_status sts',
'		  ,spi_property_xref spx',
'		  ,prop_lookups pl',
'	WHERE  	sts.property_id IS NULL ',
'    AND     spx.submitter_work_id = sts.submitter_work_id',
'	AND    	spx.submitter_source_id = sts.source_id',
'	AND    	pl.lookup_code = sts.transaction_status',
'	AND    	pl.lookup_type = ''SPI_STATUS'' ',
'	AND		sts.transaction_status = 90',
'	AND     sts.source_id = NVL(:P50_PUB_SRC,sts.source_id)',
'	AND    	sts.batch_id  = NVL(:P50_FILE_NAME,sts.batch_id)',
'	AND     sts.SUBMISSION_ID = NVL(TRIM(:P50_SUBMISSION_ID), sts.SUBMISSION_ID)',
'	AND    	NVL(sts.creation_date, SYSDATE) BETWEEN NVL(TO_DATE(:P50_SUBMISSION_DATE_FROM,''YYYY-MM-DD''),NVL(sts.creation_date, SYSDATE))',
'							   AND     NVL(TO_DATE(:P50_SUBMISSION_DATE_TO,''YYYY-MM-DD''),NVL(sts.creation_date, SYSDATE))',
'    AND    	sts.title LIKE NVL((''%''||UPPER(:P50_SONG_TITLE)||''%''),UPPER(sts.title))',
'    AND     (:P50_PUB_SRC IS NOT NULL OR :P50_FILE_NAME IS NOT NULL OR :P50_SUBMISSION_DATE_FROM IS NOT NULL OR :P50_SUBMISSION_DATE_TO IS NOT NULL OR :P50_SONG_TITLE IS NOT NULL OR :P50_PROPERTY_NUMBER IS NULL)',
'    UNION ALL',
'    -- TO get Non-Rejected works',
'	SELECT DISTINCT sts.transaction_status_id',
'		  ,sts.creation_date AS submission_date',
'		  ,sts.source_id',
'		  ,sts.file_name',
'		  ,sts.submission_id',
'		  ,sts.submitter_work_id',
'		  ,sts.title',
'		  ,sts.transaction_status',
'		  ,pl.description "STATUS"',
'		  ,pp.property_number',
'          --,(select property_number from prop_properties where property_id = spx.sesac_property_id)||'' (''||sts.transaction_status||'')'' prop_number_sesac_tst',
'		  ,sts.property_id',
'		  ,spx.sesac_property_id',
'		  ,CASE WHEN sts.transaction_status IN (70,80,100,110) THEN (SELECT title from prop_properties where property_id = spx.sesac_property_id) ELSE '' '' END SESAC_PROPERTY',
'          ,NULL Remarks -- previously used for displaying Reject Reason',
'           , DECODE(pcl.song_property_id, null, ''N'', ''Y'') jingle_flag',
'	FROM   spi_transaction_status sts',
'		  ,spi_property_xref spx',
'		  ,prop_lookups pl',
'		  ,prop_properties pp',
'          ,prop_commercial_lines pcl',
'	WHERE  	sts.property_id IS NOT NULL',
'	AND		sts.property_id = pp.property_id',
'	AND		spx.submitter_work_id = sts.submitter_work_id',
'	AND    	spx.submitter_source_id = sts.source_id',
'	AND    	pl.lookup_code = sts.transaction_status',
'	AND    	pl.lookup_type = ''SPI_STATUS'' ',
'	AND		sts.transaction_status_id = NVL(:P50_SPI_TRANSACTION_STATUS_ID, sts.transaction_status_id)',
'	AND     sts.source_id = NVL(:P50_PUB_SRC,sts.source_id)',
'	AND     pp.property_number = NVL(:P50_PROPERTY_NUMBER, pp.property_number)',
'	AND    	sts.batch_id  = NVL(:P50_FILE_NAME,sts.batch_id)',
'	AND     sts.SUBMISSION_ID = NVL(TRIM(:P50_SUBMISSION_ID), sts.SUBMISSION_ID)',
'	--AND    NVL(sts.submission_date, SYSDATE) BETWEEN NVL(TO_DATE(:P50_SUBMISSION_DATE_FROM,''YYYY-MM-DD''),NVL(sts.submission_date, SYSDATE))',
'	--                           AND     NVL(TO_DATE(:P50_SUBMISSION_DATE_TO,''YYYY-MM-DD''),NVL(sts.submission_date, SYSDATE))',
'	AND    	NVL(sts.creation_date, SYSDATE) BETWEEN NVL(TO_DATE(:P50_SUBMISSION_DATE_FROM,''YYYY-MM-DD''),NVL(sts.creation_date, SYSDATE))',
'							   AND     NVL(TO_DATE(:P50_SUBMISSION_DATE_TO,''YYYY-MM-DD''),NVL(sts.creation_date, SYSDATE))',
'	AND    	sts.title LIKE NVL((''%''||UPPER(:P50_SONG_TITLE)||''%''),UPPER(sts.title))',
'	AND		( ',
'				:P50_PROP_ID_HFA_CODE IS NULL',
'				OR',
'				(',
'               '))
);
wwv_flow_api.create_worksheet(
 p_id=>wwv_flow_api.id(35374247930620905)
,p_max_row_count=>'1000000'
,p_allow_save_rpt_public=>'Y'
,p_save_rpt_public_auth_scheme=>wwv_flow_api.id(6185302949729668)
,p_show_nulls_as=>'-'
,p_pagination_type=>'ROWS_X_TO_Y_OF_Z'
,p_pagination_display_pos=>'BOTTOM_RIGHT'
,p_report_list_mode=>'TABS'
,p_show_detail_link=>'N'
,p_show_notify=>'Y'
,p_download_formats=>'CSV:HTML:EMAIL:XLS:PDF:RTF'
,p_owner=>'KBAKSHI'
,p_internal_uid=>35374247930620905
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34977199666886033)
,p_db_column_name=>'C001'
,p_display_order=>20
,p_column_identifier=>'R'
,p_column_label=>'SPI Reference#'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34977275851886034)
,p_db_column_name=>'C002'
,p_display_order=>30
,p_column_identifier=>'S'
,p_column_label=>'Submission Date'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34977307867886035)
,p_db_column_name=>'C003'
,p_display_order=>40
,p_column_identifier=>'T'
,p_column_label=>'Source'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34977433685886036)
,p_db_column_name=>'C004'
,p_display_order=>50
,p_column_identifier=>'U'
,p_column_label=>'File Name'
,p_column_link=>'f?p=&APP_ID.:19:&SESSION.::&DEBUG.:RP,19:P19_FILE_NAME:#C004#'
,p_column_linktext=>'#C004#'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34977593093886037)
,p_db_column_name=>'C005'
,p_display_order=>60
,p_column_identifier=>'V'
,p_column_label=>'Submission ID'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34977622962886038)
,p_db_column_name=>'C006'
,p_display_order=>70
,p_column_identifier=>'W'
,p_column_label=>'Submitter Work ID'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34977707269886039)
,p_db_column_name=>'C007'
,p_display_order=>80
,p_column_identifier=>'X'
,p_column_label=>'Submitting Publisher Number'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34977857574886040)
,p_db_column_name=>'C008'
,p_display_order=>90
,p_column_identifier=>'Y'
,p_column_label=>'Title'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34977967455886041)
,p_db_column_name=>'C009'
,p_display_order=>100
,p_column_identifier=>'Z'
,p_column_label=>'Transaction status'
,p_column_type=>'STRING'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34978063941886042)
,p_db_column_name=>'C010'
,p_display_order=>110
,p_column_identifier=>'AA'
,p_column_label=>'Status'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34978147446886043)
,p_db_column_name=>'C011'
,p_display_order=>120
,p_column_identifier=>'AB'
,p_column_label=>'Property Number'
,p_column_link=>'f?p=&APP_ID.:310:&SESSION.::&DEBUG.:RP,310:P310_PROP_ID,P310_CALLING_PAGE_ID:#C012#,50'
,p_column_linktext=>'#C011#'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34978273741886044)
,p_db_column_name=>'C012'
,p_display_order=>130
,p_column_identifier=>'AC'
,p_column_label=>'Property id'
,p_column_type=>'STRING'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34978317800886045)
,p_db_column_name=>'C013'
,p_display_order=>140
,p_column_identifier=>'AD'
,p_column_label=>'Sesac Property id'
,p_column_type=>'STRING'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34978476133886046)
,p_db_column_name=>'C014'
,p_display_order=>150
,p_column_identifier=>'AE'
,p_column_label=>'SESAC Property'
,p_column_link=>'f?p=&APP_ID.:310:&SESSION.::&DEBUG.:RP:P310_PROP_ID,P310_CALLING_PAGE_ID:#C013#,50'
,p_column_linktext=>'#C014#'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34978588975886047)
,p_db_column_name=>'C015'
,p_display_order=>160
,p_column_identifier=>'AF'
,p_column_label=>'Remarks'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34978651324886048)
,p_db_column_name=>'C016'
,p_display_order=>170
,p_column_identifier=>'AG'
,p_column_label=>'Jingle'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(51572288405265341)
,p_db_column_name=>'C017'
,p_display_order=>180
,p_column_identifier=>'AK'
,p_column_label=>'HFA Song Code'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(80692456107697315)
,p_db_column_name=>'Select'
,p_display_order=>190
,p_column_identifier=>'AJ'
,p_column_label=>'<input type="checkbox" id="selectunselectall">'
,p_allow_sorting=>'N'
,p_allow_filtering=>'N'
,p_allow_highlighting=>'N'
,p_allow_ctrl_breaks=>'N'
,p_allow_aggregations=>'N'
,p_allow_computations=>'N'
,p_allow_charting=>'N'
,p_allow_group_by=>'N'
,p_allow_pivot=>'N'
,p_allow_hide=>'N'
,p_column_type=>'STRING'
,p_display_text_as=>'WITHOUT_MODIFICATION'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(11648188074834321)
,p_db_column_name=>'C018'
,p_display_order=>200
,p_column_identifier=>'AL'
,p_column_label=>'Priority'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(1614757239026279607)
,p_db_column_name=>'C019'
,p_display_order=>210
,p_column_identifier=>'AM'
,p_column_label=>'Hold Reason'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(729895856570569101)
,p_db_column_name=>'C020'
,p_display_order=>220
,p_column_identifier=>'AN'
,p_column_label=>'Last Updated'
,p_column_type=>'STRING'
,p_column_alignment=>'CENTER'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(35986744151329747)
,p_application_user=>'APXWS_DEFAULT'
,p_report_seq=>10
,p_report_alias=>'359868'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'Select:C001:C003:C002:C004:C008:C005:C006:C011:C014:C017:C007:C010:C015:C016:C018:C019:C020'
,p_sort_column_1=>'C001'
,p_sort_direction_1=>'ASC'
,p_sort_column_2=>'0'
,p_sort_direction_2=>'ASC'
,p_sort_column_3=>'0'
,p_sort_direction_3=>'ASC'
,p_sort_column_4=>'0'
,p_sort_direction_4=>'ASC'
,p_sort_column_5=>'0'
,p_sort_direction_5=>'ASC'
,p_sort_column_6=>'0'
,p_sort_direction_6=>'ASC'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(71722661035444107)
,p_plug_name=>'Breadcrumb'
,p_region_template_options=>'#DEFAULT#:t-BreadcrumbRegion--useBreadcrumbTitle'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51852637782240827)
,p_plug_display_sequence=>40
,p_plug_display_point=>'REGION_POSITION_01'
,p_menu_id=>wwv_flow_api.id(51876846391240858)
,p_plug_source_type=>'NATIVE_BREADCRUMB'
,p_menu_template_id=>wwv_flow_api.id(51870806884240842)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(5048236328834026)
,p_button_sequence=>30
,p_button_plug_id=>wwv_flow_api.id(67142388554896140)
,p_button_name=>'CLEAR'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Clear'
,p_button_position=>'BELOW_BOX'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:50:&SESSION.::&DEBUG.:RP,50::'
,p_icon_css_classes=>'fa-eraser'
,p_grid_new_grid=>false
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(11649722601834337)
,p_button_sequence=>50
,p_button_plug_id=>wwv_flow_api.id(67142388554896140)
,p_button_name=>'SEARCH_CONFLICT'
,p_button_action=>'DEFINED_BY_DA'
,p_button_template_options=>'#DEFAULT#'
,p_button_template_id=>wwv_flow_api.id(51870385911240841)
,p_button_image_alt=>'Search Conflict'
,p_button_position=>'BELOW_BOX'
,p_button_alignment=>'LEFT'
,p_warn_on_unsaved_changes=>null
,p_grid_new_grid=>false
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(5048672198834027)
,p_button_sequence=>60
,p_button_plug_id=>wwv_flow_api.id(67142388554896140)
,p_button_name=>'SEARCH'
,p_button_static_id=>'SEARCH'
,p_button_action=>'DEFINED_BY_DA'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Search'
,p_button_position=>'BELOW_BOX'
,p_button_alignment=>'LEFT'
,p_warn_on_unsaved_changes=>null
,p_icon_css_classes=>'fa-search'
,p_grid_new_grid=>false
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(5047820669834026)
,p_button_sequence=>10
,p_button_plug_id=>wwv_flow_api.id(67142388554896140)
,p_button_name=>'CANCEL'
,p_button_action=>'SUBMIT'
,p_button_template_options=>'#DEFAULT#:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Cancel'
,p_button_position=>'BOTTOM'
,p_button_alignment=>'LEFT'
,p_button_condition_type=>'NEVER'
,p_icon_css_classes=>'fa-close'
,p_grid_new_grid=>false
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(43293675481079921)
,p_button_sequence=>30
,p_button_plug_id=>wwv_flow_api.id(43292541965079910)
,p_button_name=>'P50_RESET'
,p_button_action=>'DEFINED_BY_DA'
,p_button_template_options=>'#DEFAULT#:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_is_hot=>'Y'
,p_button_image_alt=>'Reset'
,p_button_position=>'BOTTOM'
,p_button_alignment=>'LEFT'
,p_warn_on_unsaved_changes=>null
,p_icon_css_classes=>'fa-refresh'
,p_grid_new_grid=>false
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(35376828965620931)
,p_button_sequence=>40
,p_button_plug_id=>wwv_flow_api.id(43292541965079910)
,p_button_name=>'SEARCH_1'
,p_button_action=>'SUBMIT'
,p_button_template_options=>'#DEFAULT#:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Search'
,p_button_position=>'BOTTOM'
,p_button_alignment=>'LEFT'
,p_icon_css_classes=>'fa-search'
,p_grid_new_grid=>false
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(34976895512886030)
,p_button_sequence=>10
,p_button_plug_id=>wwv_flow_api.id(67170313313898703)
,p_button_name=>'Revaluate'
,p_button_action=>'SUBMIT'
,p_button_template_options=>'#DEFAULT#:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Revaluate Status for Checked Records'
,p_button_position=>'RIGHT_OF_IR_SEARCH_BAR'
,p_icon_css_classes=>'fa-cog'
,p_grid_new_grid=>false
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(5049040294834027)
,p_name=>'P50_QUEUE_TYPE'
,p_item_sequence=>20
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_prompt=>'Queue Type'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_lov=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT ''Property Match'' d',
'      ,1 r',
'FROM   dual',
'UNION',
'SELECT ''Party Match'' d',
'      ,2 r',
'FROM   dual',
'UNION',
'SELECT ''Property Resolution'' d',
'      ,3 r',
'FROM   dual',
'ORDER BY 1'))
,p_lov_display_null=>'YES'
,p_lov_null_text=>'- Select Queue Type -'
,p_cHeight=>1
,p_colspan=>4
,p_grid_column=>1
,p_grid_label_column_span=>1
,p_display_when_type=>'NEVER'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(5049422538834029)
,p_name=>'P50_AVAILABLE_QUEUES'
,p_item_sequence=>30
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_prompt=>'Saved Queue Criteria'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_lov=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT spqh.queue_name',
'      ,spqh.work_queue_header_id',
'FROM   spi_work_queue_header spqh',
'WHERE  spqh.queue_type = :P50_QUEUE_TYPE',
'AND    spqh.username = :APP_USER',
'AND    spqh.queue_name IS NOT NULL'))
,p_lov_display_null=>'YES'
,p_lov_null_text=>'- Select Queue -'
,p_lov_cascade_parent_items=>'P50_QUEUE_TYPE'
,p_ajax_items_to_submit=>'P50_QUEUE_TYPE'
,p_ajax_optimize_refresh=>'Y'
,p_cHeight=>1
,p_begin_on_new_line=>'N'
,p_colspan=>4
,p_grid_label_column_span=>1
,p_display_when_type=>'NEVER'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(5049854422834030)
,p_name=>'P50_PUB_SRC'
,p_item_sequence=>40
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_prompt=>'Publisher / Source'
,p_display_as=>'NATIVE_POPUP_LOV'
,p_lov=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT ps.source_code||'' - ''||ps.description "Description"',
'      ,ps.source_id "Source ID"',
'FROM   prop_sources ps',
'WHERE ',
'UPPER(source_group) LIKE ''CWR%'' OR ',
'UPPER(source_group) LIKE ''OWR%'' OR ',
'UPPER(source_group) LIKE ''ESONG%''OR',
'UPPER(source_group) LIKE ''MLC%''',
'ORDER BY ps.source_code'))
,p_lov_display_null=>'YES'
,p_cSize=>30
,p_colspan=>4
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'NOT_ENTERABLE'
,p_attribute_02=>'FIRST_ROWSET'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(5050263880834030)
,p_name=>'P50_FILE_ID'
,p_item_sequence=>90
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_prompt=>'File ID'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_lov=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT DISTINCT cb.batch_id||'' - ''||cb.file_name d',
'      ,cb.batch_id r',
'FROM   cwr_batches cb',
'WHERE  cb.batch_id IN (SELECT DISTINCT batch_id',
'                       FROM   spi_transaction_status sts',
'                       WHERE  sts.transaction_status IN (20,30,40)',
'                       AND    :P50_QUEUE_TYPE = 2',
'                       UNION',
'                       SELECT DISTINCT batch_id',
'                       FROM   spi_transaction_status sts',
'                       WHERE  sts.transaction_status IN (40,50)',
'                       AND    :P50_QUEUE_TYPE = 2',
'                      )',
'ORDER BY cb.batch_id'))
,p_lov_display_null=>'YES'
,p_lov_null_text=>'- Select File -'
,p_lov_cascade_parent_items=>'P50_QUEUE_TYPE'
,p_ajax_items_to_submit=>'P50_QUEUE_TYPE'
,p_ajax_optimize_refresh=>'Y'
,p_cHeight=>1
,p_colspan=>4
,p_grid_column=>1
,p_grid_label_column_span=>1
,p_display_when_type=>'NEVER'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(5050628429834030)
,p_name=>'P50_FILE_SORT_ORDER'
,p_item_sequence=>100
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_prompt=>'Sort Order'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_lov=>'STATIC2:Ascending;A,Descending;D'
,p_lov_display_null=>'YES'
,p_lov_null_text=>'- Select Sort Order -'
,p_cHeight=>1
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_column=>5
,p_grid_label_column_span=>1
,p_display_when_type=>'NEVER'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(5051043151834030)
,p_name=>'P50_TO_DO_COUNT'
,p_item_sequence=>110
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_use_cache_before_default=>'NO'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT work_queue_header_id',
'FROM   spi_work_queue_header',
'WHERE  username = v(''APP_USER'')',
'AND    status <> 40',
'AND    nvl(active_flag,''Y'') = ''Y'''))
,p_source_type=>'QUERY'
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(5051446476834031)
,p_name=>'P50_FILE_NAME'
,p_item_sequence=>60
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_prompt=>'File Name'
,p_display_as=>'NATIVE_POPUP_LOV'
,p_lov=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT DISTINCT NVL(FILE_NAME, ''<File name unavailable>'') D, --DISTINCT batch_id||''-''||NVL(FILE_NAME, ''<File name unavailable>'') D,',
'       sts.file_name R',
'       --batch_id R',
'FROM   spi_transaction_status sts',
'where (source_id = :P50_PUB_SRC or  :P50_PUB_SRC is null)',
'',
''))
,p_lov_display_null=>'YES'
,p_lov_null_text=>'-select-'
,p_lov_cascade_parent_items=>'P50_PUB_SRC'
,p_ajax_items_to_submit=>'P50_PUB_SRC'
,p_ajax_optimize_refresh=>'Y'
,p_cSize=>30
,p_colspan=>4
,p_grid_column=>1
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'NOT_ENTERABLE'
,p_attribute_02=>'FIRST_ROWSET'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(5051833608834032)
,p_name=>'P50_SUBMISSION_DATE_FROM'
,p_item_sequence=>120
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_prompt=>'Submission Date From'
,p_display_as=>'NATIVE_DATE_PICKER'
,p_cSize=>10
,p_cMaxlength=>4000
,p_colspan=>4
,p_grid_column=>1
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_04=>'button'
,p_attribute_05=>'N'
,p_attribute_07=>'NONE'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(5052296636834032)
,p_name=>'P50_SUBMISSION_DATE_TO'
,p_item_sequence=>130
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_prompt=>'Submission Date To'
,p_display_as=>'NATIVE_DATE_PICKER'
,p_cSize=>30
,p_cMaxlength=>4000
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_column=>5
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_04=>'button'
,p_attribute_05=>'N'
,p_attribute_07=>'NONE'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(5053002745834033)
,p_name=>'P50_PARTY_NAME'
,p_item_sequence=>180
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_prompt=>'Party Name'
,p_display_as=>'NATIVE_TEXT_FIELD'
,p_cSize=>30
,p_cMaxlength=>4000
,p_colspan=>4
,p_grid_column=>1
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'N'
,p_attribute_04=>'TEXT'
,p_attribute_05=>'BOTH'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(5053494532834033)
,p_name=>'P50_PARTY_IPI_NUMBER'
,p_item_sequence=>200
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_prompt=>'Party IPI Number'
,p_display_as=>'NATIVE_TEXT_FIELD'
,p_cSize=>30
,p_cMaxlength=>4000
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_column=>5
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'N'
,p_attribute_04=>'TEXT'
,p_attribute_05=>'BOTH'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(5053865567834033)
,p_name=>'P50_PARTY_AFFILIATION'
,p_item_sequence=>190
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_prompt=>'Party Affiliation'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_named_lov=>'SPI_SOCIETY_AFFILIATION'
,p_lov=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT society_code||'' - ''||society_name||'' - ''||society_country d',
'      ,society_id r',
'      --,1',
'FROM   aff_societies',
'WHERE  society_code = ''071''',
'UNION',
'SELECT society_code||'' - ''||society_name||'' - ''||society_country',
'      ,society_id',
'      --,2',
'FROM   aff_societies',
'WHERE  society_code <> ''071''',
'--ORDER BY 3'))
,p_lov_display_null=>'YES'
,p_lov_null_text=>'- Select Society Affiliation -'
,p_cHeight=>1
,p_begin_on_new_line=>'N'
,p_begin_on_new_field=>'N'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(5054290865834034)
,p_name=>'P50_SORT_ORDER8'
,p_item_sequence=>230
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_prompt=>'Sort Order'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_lov=>'STATIC2:Ascending;A,Descending;D'
,p_lov_display_null=>'YES'
,p_lov_null_text=>'- Select Sort Order -'
,p_cHeight=>1
,p_colspan=>3
,p_grid_column=>5
,p_grid_label_column_span=>1
,p_display_when_type=>'NEVER'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(5054605787834034)
,p_name=>'P50_PROPERTY_PARTY'
,p_item_sequence=>240
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_prompt=>'Property Count'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_lov=>'STATIC2:Ascending;A,Descending;D'
,p_lov_display_null=>'YES'
,p_lov_null_text=>'- Select Sort Order -'
,p_cHeight=>1
,p_colspan=>4
,p_grid_column=>1
,p_grid_label_column_span=>1
,p_display_when_type=>'NEVER'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(5055090589834034)
,p_name=>'P50_SORT_ORDER2'
,p_item_sequence=>250
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_prompt=>'Sort Order'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_lov=>'STATIC2:Ascending;A,Descending;D'
,p_lov_display_null=>'YES'
,p_lov_null_text=>'- Select Sort Order -'
,p_cHeight=>1
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_column=>5
,p_grid_label_column_span=>1
,p_display_when_type=>'NEVER'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(5055478752834035)
,p_name=>'P50_WRI_PUB_INDICATOR'
,p_item_sequence=>210
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_prompt=>'Writers'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_named_lov=>'YES_NO'
,p_lov=>'.'||wwv_flow_api.id(62410853427715388)||'.'
,p_lov_display_null=>'YES'
,p_lov_null_text=>'- Select Writers -'
,p_cHeight=>1
,p_begin_on_new_line=>'N'
,p_begin_on_new_field=>'N'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'NO'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(5055880564834035)
,p_name=>'P50_SORT_ORDER3'
,p_item_sequence=>260
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_prompt=>'Sort Order'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_lov=>'STATIC2:Ascending;A,Descending;D'
,p_lov_display_null=>'YES'
,p_lov_null_text=>'- Select Sort Order -'
,p_cHeight=>1
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_column=>5
,p_grid_label_column_span=>1
,p_display_when_type=>'NEVER'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(5056229158834035)
,p_name=>'P50_SORT_ORDER4'
,p_item_sequence=>270
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_prompt=>'Sort Order'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_lov=>'STATIC2:Ascending;A,Descending;D'
,p_lov_display_null=>'YES'
,p_lov_null_text=>'- Select Sort Order -'
,p_cHeight=>1
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_column=>5
,p_grid_label_column_span=>1
,p_display_when_type=>'NEVER'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(5056618879834036)
,p_name=>'P50_SONG_REQUEST_ID'
,p_item_sequence=>280
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_prompt=>'Song Request ID'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_lov=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT DISTINCT batch_id||''-''||file_name d, batch_id r',
'FROM   cwr_batches',
'ORDEr BY batch_id'))
,p_lov_display_null=>'YES'
,p_lov_null_text=>'- Select File -'
,p_cHeight=>1
,p_colspan=>4
,p_grid_column=>1
,p_grid_label_column_span=>1
,p_display_when_type=>'NEVER'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(5057017156834036)
,p_name=>'P50_SORT_ORDER5'
,p_item_sequence=>290
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_prompt=>'Sort Order'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_lov=>'STATIC2:Ascending;A,Descending;D'
,p_lov_display_null=>'YES'
,p_lov_null_text=>'- Select Sort Order -'
,p_cHeight=>1
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_column=>5
,p_grid_label_column_span=>1
,p_display_when_type=>'NEVER'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
end;
/
begin
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(5057409815834036)
,p_name=>'P50_SUBMITTER_WORK_NO'
,p_item_sequence=>140
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_prompt=>'Submitter Work Number'
,p_display_as=>'NATIVE_TEXTAREA'
,p_cSize=>30
,p_cMaxlength=>4000
,p_cHeight=>5
,p_colspan=>4
,p_grid_column=>1
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_inline_help_text=>'User can enter up to 50 values separated by commas.'
,p_attribute_01=>'Y'
,p_attribute_02=>'N'
,p_attribute_03=>'N'
,p_attribute_04=>'BOTH'
,p_item_comment=>'<b>User can enter up to 50 values separated by commas.</b>'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(5057823108834036)
,p_name=>'P50_PROP_ID_HFA_CODE'
,p_item_sequence=>150
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_prompt=>'HFA Song Code'
,p_display_as=>'NATIVE_TEXT_FIELD'
,p_cSize=>30
,p_cMaxlength=>4000
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_column=>5
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'N'
,p_attribute_04=>'TEXT'
,p_attribute_05=>'BOTH'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(5058227457834037)
,p_name=>'P50_SONG_TITLE'
,p_item_sequence=>160
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_prompt=>'Song Title'
,p_display_as=>'NATIVE_TEXT_FIELD'
,p_cSize=>30
,p_cMaxlength=>4000
,p_colspan=>4
,p_grid_column=>1
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'N'
,p_attribute_04=>'TEXT'
,p_attribute_05=>'BOTH'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(5058693915834037)
,p_name=>'P50_ISRC_REC_INFO'
,p_item_sequence=>300
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_prompt=>'ISRC/Recording Info'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_lov=>'STATIC2:Yes;Y,No;N'
,p_lov_display_null=>'YES'
,p_lov_null_text=>'- Select ISRC/Recording Info -'
,p_cHeight=>1
,p_colspan=>4
,p_grid_column=>1
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(5059400268834037)
,p_name=>'P50_JINGLE_INDICATOR'
,p_item_sequence=>310
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_prompt=>'Jingle Indicator'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_lov=>'STATIC2:Yes;Y,No;N'
,p_lov_display_null=>'YES'
,p_lov_null_text=>'- Select Jingle Indicator -'
,p_cHeight=>1
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(5060281650834038)
,p_name=>'P50_PARTY_IP_NUMBER'
,p_item_sequence=>220
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_prompt=>'IP Number'
,p_display_as=>'NATIVE_TEXT_FIELD'
,p_cSize=>30
,p_colspan=>4
,p_grid_column=>1
,p_grid_label_column_span=>1
,p_display_when_type=>'NEVER'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'N'
,p_attribute_04=>'TEXT'
,p_attribute_05=>'BOTH'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(5060607656834039)
,p_name=>'P50_QUEUE_NAME'
,p_item_sequence=>320
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_prompt=>'Already In Queue - User Name'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_lov=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT NVL(swqh.queue_name,''Unnamed Queue'')||'' - ''||swqh.username d',
'      ,swqh.work_queue_header_id',
'FROM   spi_work_queue_header swqh',
'WHERE  swqh.active_flag = ''Y''',
'ORDER BY swqh.creation_date'))
,p_lov_display_null=>'YES'
,p_lov_null_text=>'- Select Queue -'
,p_cHeight=>1
,p_colspan=>4
,p_grid_column=>1
,p_grid_label_column_span=>1
,p_display_when_type=>'NEVER'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'NO'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(7038148005528707)
,p_name=>'P50_TRANSACTION_STATUS'
,p_item_sequence=>50
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_prompt=>'Status'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_named_lov=>'SUMMARY_TRANSACTION_STATUS'
,p_lov=>'.'||wwv_flow_api.id(10837137064964149)||'.'
,p_lov_display_null=>'YES'
,p_lov_null_text=>'- Select Status -'
,p_cHeight=>1
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'NO'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(10492980444837707)
,p_name=>'P50_SPI_TRANSACTION_STATUS_ID'
,p_item_sequence=>70
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_prompt=>'SPI Reference#'
,p_display_as=>'NATIVE_TEXT_FIELD'
,p_cSize=>30
,p_cMaxlength=>4000
,p_colspan=>4
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'N'
,p_attribute_04=>'TEXT'
,p_attribute_05=>'BOTH'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(10626984045070249)
,p_name=>'P50_PRIORITY_FLAG'
,p_item_sequence=>350
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_prompt=>'High Priority'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_lov=>'STATIC2:Yes;Y,No;N'
,p_lov_display_null=>'YES'
,p_lov_null_text=>'-Select Priority Flag-'
,p_cHeight=>1
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(11649612727834336)
,p_name=>'P50_CONFLICT_FLAG'
,p_item_sequence=>360
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_source=>'N'
,p_source_type=>'STATIC'
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(15173631187593215)
,p_name=>'P50_QUERY'
,p_item_sequence=>330
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(23519684486313131)
,p_name=>'P50_PROPERTY_NUMBER'
,p_item_sequence=>170
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_prompt=>'Property Number'
,p_display_as=>'NATIVE_TEXT_FIELD'
,p_cSize=>30
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_column=>5
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'N'
,p_attribute_04=>'TEXT'
,p_attribute_05=>'BOTH'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(28659895354759528)
,p_name=>'P50_SUBMISSION_ID'
,p_item_sequence=>80
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_prompt=>'Submission ID'
,p_display_as=>'NATIVE_TEXT_FIELD'
,p_cSize=>30
,p_cMaxlength=>4000
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'N'
,p_attribute_04=>'TEXT'
,p_attribute_05=>'BOTH'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(33593278977844726)
,p_name=>'P50_SUB_PUBLISHER_NUMBER'
,p_item_sequence=>62
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_prompt=>'Submitting Publisher#'
,p_display_as=>'NATIVE_TEXT_FIELD'
,p_cSize=>30
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'Y'
,p_attribute_02=>'N'
,p_attribute_04=>'TEXT'
,p_attribute_05=>'BOTH'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(35375948859620922)
,p_name=>'P50_SUBMISSION_DATE_FROM_1'
,p_item_sequence=>30
,p_item_plug_id=>wwv_flow_api.id(43292541965079910)
,p_prompt=>'Submission Date From'
,p_display_as=>'NATIVE_DATE_PICKER'
,p_cSize=>30
,p_colspan=>4
,p_grid_column=>1
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_04=>'button'
,p_attribute_05=>'N'
,p_attribute_07=>'NONE'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(35376083814620923)
,p_name=>'P50_SUBMISSION_DATE_TO_1'
,p_item_sequence=>40
,p_item_plug_id=>wwv_flow_api.id(43292541965079910)
,p_prompt=>'Submission Date To'
,p_display_as=>'NATIVE_DATE_PICKER'
,p_cSize=>30
,p_begin_on_new_line=>'N'
,p_colspan=>4
,p_grid_column=>5
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_04=>'button'
,p_attribute_05=>'N'
,p_attribute_07=>'NONE'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(35501346432546850)
,p_name=>'P50_INQ_SELECTION'
,p_item_sequence=>10
,p_item_plug_id=>wwv_flow_api.id(35501260440546849)
,p_item_default=>'S'
,p_display_as=>'NATIVE_RADIOGROUP'
,p_lov=>'STATIC2:Inquiry Song Request;S,Inquiry Source Counts;C'
,p_field_template=>wwv_flow_api.id(51869900248240840)
,p_item_template_options=>'#DEFAULT#:margin-top-sm:margin-left-md'
,p_lov_display_extra=>'NO'
,p_attribute_01=>'2'
,p_attribute_02=>'REDIRECT_SET_VALUE'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(43292643944079911)
,p_name=>'P50_SOURCE'
,p_item_sequence=>10
,p_item_plug_id=>wwv_flow_api.id(43292541965079910)
,p_prompt=>'Source'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_lov=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT ps.source_code||'' - ''||ps.description "Description"',
'      ,ps.source_id "Source ID"',
'FROM   prop_sources ps',
'WHERE  ps.source_id IN ( SELECT DISTINCT sts.source_id',
'                        FROM   spi_transaction_status sts',
'					   )',
'ORDER BY ps.source_code'))
,p_lov_display_null=>'YES'
,p_lov_null_text=>'-Select-'
,p_cHeight=>1
,p_colspan=>4
,p_grid_column=>1
,p_grid_label_column_span=>1
,p_display_when_type=>'NEVER'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(43292780532079912)
,p_name=>'P50_FILE'
,p_item_sequence=>20
,p_item_plug_id=>wwv_flow_api.id(43292541965079910)
,p_prompt=>'File'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_lov=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT DISTINCT NVL(FILE_NAME, ''<File name unavailable>'') D,',
'       file_name R',
'       --DISTINCT batch_id||''-''||NVL(FILE_NAME, ''<File name unavailable>'') D,',
'       --batch_id R',
'FROM   spi_transaction_status sts',
'where (source_id = :P50_SOURCE or  :P50_SOURCE is null)',
'and sts.file_name is not null',
'ORDER BY D'))
,p_lov_display_null=>'YES'
,p_lov_null_text=>'-Select-                                      '
,p_lov_cascade_parent_items=>'P50_SOURCE'
,p_ajax_items_to_submit=>'P50_SOURCE'
,p_ajax_optimize_refresh=>'Y'
,p_cHeight=>1
,p_begin_on_new_line=>'N'
,p_colspan=>4
,p_grid_label_column_span=>1
,p_display_when_type=>'NEVER'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#:t-Form-fieldContainer--stretchInputs'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(80691009788697301)
,p_name=>'P50_TRANSACTION_STATUS_ID'
,p_item_sequence=>10
,p_item_plug_id=>wwv_flow_api.id(67170313313898703)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(80691142096697302)
,p_name=>'P50_SEQ_ID'
,p_item_sequence=>20
,p_item_plug_id=>wwv_flow_api.id(67170313313898703)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(89242450592561273)
,p_name=>'P50_TRANS_TYPE'
,p_item_sequence=>340
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_prompt=>'Transaction  Type'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_lov=>'STATIC:All;A,NWR;NWR,REV;REV'
,p_cHeight=>1
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_column=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'NO'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(1614757153377279606)
,p_name=>'P50_HOLD_VALUE'
,p_item_sequence=>370
,p_item_plug_id=>wwv_flow_api.id(67142388554896140)
,p_prompt=>'Hold Value'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_lov=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT ''-Select Hold-'' d, NULL r FROM DUAL',
'UNION ALL',
'SELECT ''Any'' d, ''-1'' as r FROM DUAL',
'UNION ALL',
'SELECT ''None'' d, ''-2'' as r FROM DUAL',
'UNION ALL',
'SELECT description d, lookup_code r FROM prop_lookups WHERE lookup_type = ''SPI_TRANS_PRC_REASONS''',
'ORDER BY R NULLS FIRST',
''))
,p_lov_display_null=>'YES'
,p_cHeight=>1
,p_colspan=>4
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(21092709884189716)
,p_name=>'On Press Enter Trigger Click On Search Button'
,p_event_sequence=>10
,p_bind_type=>'bind'
,p_bind_event_type=>'ready'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(21092898658189717)
,p_event_id=>wwv_flow_api.id(21092709884189716)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
'$(''input'').keyup(function(e) ',
'		{',
'			if (e.keyCode == ''13'')',
'			$(''#SEARCH'').trigger(''click'');',
'		});'))
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(39471808280177601)
,p_name=>'New'
,p_event_sequence=>20
,p_bind_type=>'bind'
,p_bind_event_type=>'ready'
,p_display_when_type=>'NEVER'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(39471964512177602)
,p_event_id=>wwv_flow_api.id(39471808280177601)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
'var varSelected = $("#apexir_SAVED_REPORTS option:selected").text();',
'alert (''hi'');',
'alert (varSelected);',
'',
''))
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(43292825868079913)
,p_name=>'Refresh_BySource&Files'
,p_event_sequence=>30
,p_triggering_element_type=>'ITEM'
,p_triggering_element=>'P50_SOURCE,P50_FILE'
,p_bind_type=>'bind'
,p_bind_event_type=>'change'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(43292972290079914)
,p_event_id=>wwv_flow_api.id(43292825868079913)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(40195611484804208)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(43293364333079918)
,p_event_id=>wwv_flow_api.id(43292825868079913)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(40196288147804214)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(43293480254079919)
,p_event_id=>wwv_flow_api.id(43292825868079913)
,p_event_result=>'TRUE'
,p_action_sequence=>40
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(40197401492804226)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(43293570868079920)
,p_event_id=>wwv_flow_api.id(43292825868079913)
,p_event_result=>'TRUE'
,p_action_sequence=>50
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(25106599930958203)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(43293796004079922)
,p_name=>'Click_refresh'
,p_event_sequence=>40
,p_triggering_element_type=>'BUTTON'
,p_triggering_button_id=>wwv_flow_api.id(43293675481079921)
,p_bind_type=>'bind'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(43293850776079923)
,p_event_id=>wwv_flow_api.id(43293796004079922)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_CLEAR'
,p_affected_elements_type=>'ITEM'
,p_affected_elements=>'P50_SOURCE,P50_FILE,P50_SUBMISSION_DATE_FROM_1,P50_SUBMISSION_DATE_TO_1'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(33355166968509003)
,p_event_id=>wwv_flow_api.id(43293796004079922)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(25106599930958203)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(33355222155509004)
,p_event_id=>wwv_flow_api.id(43293796004079922)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(40196288147804214)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(33355359988509005)
,p_event_id=>wwv_flow_api.id(43293796004079922)
,p_event_result=>'TRUE'
,p_action_sequence=>40
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(40197401492804226)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(80691213501697303)
,p_name=>'Collection Add/Remove'
,p_event_sequence=>50
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'input.cl-col1'
,p_triggering_condition_type=>'JAVASCRIPT_EXPRESSION'
,p_triggering_expression=>'($(this.triggeringElement).is('':checked''))'
,p_bind_type=>'live'
,p_bind_delegate_to_selector=>'#myreport'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(80691812801697309)
,p_event_id=>wwv_flow_api.id(80691213501697303)
,p_event_result=>'FALSE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>'waitPopup = apex.widget.waitPopup();'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(80691376038697304)
,p_event_id=>wwv_flow_api.id(80691213501697303)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>'waitPopup = apex.widget.waitPopup();'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(80691921947697310)
,p_event_id=>wwv_flow_api.id(80691213501697303)
,p_event_result=>'FALSE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_SET_VALUE'
,p_affected_elements_type=>'ITEM'
,p_affected_elements=>'P50_TRANSACTION_STATUS_ID'
,p_attribute_01=>'JAVASCRIPT_EXPRESSION'
,p_attribute_05=>'$(this.triggeringElement).val();'
,p_attribute_09=>'N'
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(80691472004697305)
,p_event_id=>wwv_flow_api.id(80691213501697303)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_SET_VALUE'
,p_affected_elements_type=>'ITEM'
,p_affected_elements=>'P50_TRANSACTION_STATUS_ID'
,p_attribute_01=>'JAVASCRIPT_EXPRESSION'
,p_attribute_05=>'$(this.triggeringElement).val();'
,p_attribute_09=>'N'
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(80692086601697311)
,p_event_id=>wwv_flow_api.id(80691213501697303)
,p_event_result=>'FALSE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>'null;'
,p_attribute_02=>'P50_TRANSACTION_STATUS_ID'
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(80691594433697306)
,p_event_id=>wwv_flow_api.id(80691213501697303)
,p_event_result=>'TRUE'
,p_action_sequence=>40
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>'null;'
,p_attribute_02=>'P50_TRANSACTION_STATUS_ID'
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(80692185867697312)
,p_event_id=>wwv_flow_api.id(80691213501697303)
,p_event_result=>'FALSE'
,p_action_sequence=>40
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_SET_VALUE'
,p_affected_elements_type=>'ITEM'
,p_affected_elements=>'P50_SEQ_ID'
,p_attribute_01=>'SQL_STATEMENT'
,p_attribute_03=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT replace(seq_id,'','')',
'FROM APEX_COLLECTIONS',
'WHERE COLLECTION_NAME = ''REVALUATE_STATUS_1''',
'AND C001 = :P50_TRANSACTION_STATUS_ID;'))
,p_attribute_07=>'P50_SEQ_ID'
,p_attribute_08=>'Y'
,p_attribute_09=>'N'
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(80691670568697307)
,p_event_id=>wwv_flow_api.id(80691213501697303)
,p_event_result=>'TRUE'
,p_action_sequence=>50
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'	l_seq_id	NUMBER;',
'    V_C001  VARCHAR2(4000);',
'    V_C003  VARCHAR2(4000);',
'    V_C006  VARCHAR2(4000);',
'    V_C013  VARCHAR2(4000);',
'BEGIN',
'',
'select c003,c006,c013 INTO',
'V_C003,V_C006,V_C013 from apex_collections where collection_name=''REVALUATE_STATUS''',
'                                                  and c001 = :P50_TRANSACTION_STATUS_ID;',
'',
'	l_seq_id := APEX_COLLECTION.ADD_MEMBER',
'				(',
'					p_collection_name => ''REVALUATE_STATUS_1'',',
'					p_c001	=>	:P50_TRANSACTION_STATUS_ID,',
'                    P_C002 =>  V_C003,',
'		            P_C003 => V_C006,',
'		            P_C004 => V_C013',
'				);',
'                                                ',
':P50_TRANSACTION_STATUS_ID := '''';',
'',
'END;'))
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(80692207236697313)
,p_event_id=>wwv_flow_api.id(80691213501697303)
,p_event_result=>'FALSE'
,p_action_sequence=>50
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
'BEGIN',
'',
'	APEX_COLLECTION.DELETE_MEMBER(',
'			p_collection_name => ''REVALUATE_STATUS_1'',',
'			p_seq => replace(:P50_SEQ_ID,'','') );',
'			',
'	:P50_SEQ_ID := NULL;',
'',
'END;'))
,p_attribute_02=>'P50_SEQ_ID'
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(80691759265697308)
,p_event_id=>wwv_flow_api.id(80691213501697303)
,p_event_result=>'TRUE'
,p_action_sequence=>60
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>'waitPopup.remove();'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(80692347850697314)
,p_event_id=>wwv_flow_api.id(80691213501697303)
,p_event_result=>'FALSE'
,p_action_sequence=>60
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>'waitPopup.remove();'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(80692819030697319)
,p_name=>'SHOW_SPINNER'
,p_event_sequence=>60
,p_bind_type=>'bind'
,p_bind_event_type=>'apexbeforepagesubmit'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(80692969590697320)
,p_event_id=>wwv_flow_api.id(80692819030697319)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>'apex.widget.waitPopup();'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(80887651848218405)
,p_name=>'SelectAll'
,p_event_sequence=>70
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'#selectunselectall'
,p_bind_type=>'live'
,p_bind_delegate_to_selector=>'#myreport'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(80887804097218407)
,p_event_id=>wwv_flow_api.id(80887651848218405)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>'waitPopup1 = apex.widget.waitPopup();'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(80887787004218406)
,p_event_id=>wwv_flow_api.id(80887651848218405)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
'if ($( ''#myreport #selectunselectall'' ).is('':checked'')) {',
'      $(''#myreport input[type=checkbox][name=f01]'').prop(''checked'',true);',
'        var ajaxRequest = new htmldb_Get(null,&APP_ID.,''APPLICATION_PROCESS=UPDATECHECKBOXVALUE'',50);',
'        ajaxRequest.addParam(''x01'',''insert'');',
'        gReturn = ajaxRequest.get();',
'        ajaxRequest = null;',
'    } else {',
'      $(''#myreport input[type=checkbox][name=f01]'').prop(''checked'',false);',
'        var ajaxRequest = new htmldb_Get(null,&APP_ID.,''APPLICATION_PROCESS=UPDATECHECKBOXVALUE'',50);',
'        ajaxRequest.addParam(''x01'',''delete'');',
'        gReturn = ajaxRequest.get();',
'        ajaxRequest = null;',
'    }'))
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(80887950669218408)
,p_event_id=>wwv_flow_api.id(80887651848218405)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>'waitPopup1.remove();'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(5813946334315104)
,p_name=>'P50_CONFLICT_REFRESH'
,p_event_sequence=>80
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'[href="#CONFLIC"]'
,p_bind_type=>'bind'
,p_bind_event_type=>'click'
,p_display_when_type=>'NEVER'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(5814069322315105)
,p_event_id=>wwv_flow_api.id(5813946334315104)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_ALERT'
,p_attribute_01=>'hI'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(11649858381834338)
,p_name=>'ONCLICK SEARCH CONFLICT'
,p_event_sequence=>90
,p_triggering_element_type=>'BUTTON'
,p_triggering_button_id=>wwv_flow_api.id(11649722601834337)
,p_bind_type=>'bind'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(11649966029834339)
,p_event_id=>wwv_flow_api.id(11649858381834338)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>':P50_CONFLICT_FLAG := ''Y'';'
,p_attribute_02=>'P50_CONFLICT_FLAG'
,p_attribute_03=>'P50_CONFLICT_FLAG'
,p_attribute_04=>'N'
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(11650000567834340)
,p_event_id=>wwv_flow_api.id(11649858381834338)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_SUBMIT_PAGE'
,p_attribute_02=>'Y'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(11650192215834341)
,p_name=>'ONCLICK SEARCH'
,p_event_sequence=>100
,p_triggering_element_type=>'BUTTON'
,p_triggering_button_id=>wwv_flow_api.id(5048672198834027)
,p_bind_type=>'bind'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(11650276509834342)
,p_event_id=>wwv_flow_api.id(11650192215834341)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>':P50_CONFLICT_FLAG := ''N'';'
,p_attribute_02=>'P50_CONFLICT_FLAG'
,p_attribute_03=>'P50_CONFLICT_FLAG'
,p_attribute_04=>'N'
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(11650376331834343)
,p_event_id=>wwv_flow_api.id(11650192215834341)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_SUBMIT_PAGE'
,p_attribute_02=>'Y'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(15173896009593217)
,p_process_sequence=>10
,p_process_point=>'AFTER_SUBMIT'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'Set flag'
,p_process_sql_clob=>':P50_QUERY := ''Y'';'
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(5380822713599127)
,p_process_sequence=>20
,p_process_point=>'AFTER_SUBMIT'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'Create Collection'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'BEGIN',
'DEBUG_WRITE(''START -1'', ''inquiry_song_requests''); -- Sachin',
'--RAISE_APPLICATION_ERROR(-20010,''Please enter a search criteria.'');',
'IF (APEX_COLLECTION.COLLECTION_EXISTS (p_collection_name => ''REVALUATE_STATUS'') ) THEN',
'APEX_COLLECTION.DELETE_COLLECTION(',
'        p_collection_name => ''REVALUATE_STATUS'');',
'',
'END IF;',
'DEBUG_WRITE(''START -2'', ''inquiry_song_requests''); -- Sachin',
'IF (APEX_COLLECTION.COLLECTION_EXISTS (p_collection_name => ''REVALUATE_STATUS_1'') ) THEN',
'APEX_COLLECTION.DELETE_COLLECTION(',
'        p_collection_name => ''REVALUATE_STATUS_1'');',
'',
'END IF;',
'DEBUG_WRITE(''START -3'', ''inquiry_song_requests''); -- Sachin',
'DEBUG_WRITE(''START'', ''inquiry_song_requests''); -- Sachin',
'--IF(:P50_SPI_TRANSACTION_STATUS_ID	IS NULL AND :P50_PUB_SRC IS NULL AND :P50_SUB_PUBLISHER_NUMBER	IS NULL AND :P50_FILE_NAME	IS NULL AND :P50_SUBMISSION_ID IS NULL AND :P50_SUBMISSION_DATE_FROM IS NULL AND :P50_SUBMISSION_DATE_TO IS NULL AND :P50_SO'
||'NG_TITLE IS NULL AND :P50_SUBMITTER_WORK_NO IS NULL AND :P50_PROPERTY_NUMBER IS NULL AND :P50_PARTY_NAME IS NULL AND :P50_PARTY_IPI_NUMBER	IS NULL AND :P50_PARTY_AFFILIATION	IS NULL AND :P50_WRI_PUB_INDICATOR	IS NULL AND :P50_TRANS_TYPE IS NULL ',
'--AND :P50_PROP_ID_HFA_CODE IS NULL AND :P50_TRANSACTION_STATUS IS NULL AND :P50_ISRC_REC_INFO	IS NULL AND :P50_QUEUE_NAME IS NULL AND :P50_JINGLE_INDICATOR IS NULL) THEN',
'--    DEBUG_WRITE(''IF'', ''inquiry_song_requests''); -- Sachin',
'--    NULL;',
'--ELSE',
'    DEBUG_WRITE(''ELSE'', ''inquiry_song_requests''); -- Sachin',
'    inquiry_song_requests(p_spi_transaction_status_id => :P50_SPI_TRANSACTION_STATUS_ID,',
'                        p_pub_src => :P50_PUB_SRC,',
'                        p_submitting_publisher_number => :P50_SUB_PUBLISHER_NUMBER,',
'                        p_file_name => :P50_FILE_NAME,',
'                        p_submission_id => :P50_SUBMISSION_ID,',
'                        p_submission_date_from => :P50_SUBMISSION_DATE_FROM,',
'                        p_submission_date_to => :P50_SUBMISSION_DATE_TO,',
'                        p_song_title => :P50_SONG_TITLE,',
'                        p_submitter_work_no => :P50_SUBMITTER_WORK_NO,',
'                        p_property_number => :P50_PROPERTY_NUMBER,',
'                        p_party_name => :P50_PARTY_NAME,',
'                        p_party_ipi_number => :P50_PARTY_IPI_NUMBER,',
'                        p_party_affiliation => :P50_PARTY_AFFILIATION,',
'                        p_wri_pub_indicator => :P50_WRI_PUB_INDICATOR,',
'                        p_trans_type => :P50_TRANS_TYPE,',
'                        p_prop_id_hfa_code => :P50_PROP_ID_HFA_CODE,',
'                        p_transaction_status => :P50_TRANSACTION_STATUS,',
'                        p_isrc_rec_info => :P50_ISRC_REC_INFO,',
'                        p_queue_name => :P50_QUEUE_NAME,',
'                        p_jingle_indicator => :P50_JINGLE_INDICATOR,',
'                        p_query => :P50_QUERY);',
'    DEBUG_WRITE(''START 1'', ''inquiry_song_requests''); -- Sachin',
'    APEX_COLLECTION.CREATE_OR_TRUNCATE_COLLECTION (p_collection_name=>''REVALUATE_STATUS_1'');',
'--END IF;',
'',
'DEBUG_WRITE(''END'', ''inquiry_song_requests''); -- Sachin',
'END;'))
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
,p_process_when_type=>'NEVER'
);
end;
/
begin
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(15173758094593216)
,p_process_sequence=>10
,p_process_point=>'BEFORE_HEADER'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'Set :P50_QUERY := ''N'';'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'if :P50_QUERY is null then ',
'  :P50_QUERY := ''N'';',
'end if;'))
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(43293026396079915)
,p_process_sequence=>20
,p_process_point=>'BEFORE_HEADER'
,p_process_type=>'NATIVE_SESSION_STATE'
,p_process_name=>'CLEAR_FILTER_REGION'
,p_attribute_01=>'CLEAR_CACHE_FOR_ITEMS'
,p_attribute_03=>'P50_SOURCE,P50_FILE'
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(34976926355886031)
,p_process_sequence=>30
,p_process_point=>'BEFORE_HEADER'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'CREATECOLL'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'BEGIN',
'',
'IF (APEX_COLLECTION.COLLECTION_EXISTS (p_collection_name => ''REVALUATE_STATUS'') ) THEN',
'APEX_COLLECTION.DELETE_COLLECTION(',
'        p_collection_name => ''REVALUATE_STATUS'');',
'',
'END IF;',
'',
'IF (APEX_COLLECTION.COLLECTION_EXISTS (p_collection_name => ''REVALUATE_STATUS_1'') ) THEN',
'APEX_COLLECTION.DELETE_COLLECTION(',
'        p_collection_name => ''REVALUATE_STATUS_1'');',
'',
'END IF;',
'',
'APEX_COLLECTION.CREATE_COLLECTION (p_collection_name=>''REVALUATE_STATUS'');',
'',
'    FOR rec1 in  (select * from ',
'(   --to get only Rejected works',
'    SELECT DISTINCT sts.transaction_status_id ',
'		  ,sts.creation_date AS submission_date',
'		  ,(select ps.source_code from prop_sources ps where ps.source_id=sts.source_id) source_id',
'		  ,sts.file_name',
'		  ,sts.submission_id',
'		  ,sts.submitter_work_id',
'          ,asu.submitting_publisher_number',
'		  ,sts.title',
'		  ,sts.transaction_status',
'		  ,pl.description "STATUS"',
'		  ,(SELECT property_number from prop_properties where property_id = sts.property_id) property_number',
'		  ,sts.property_id',
'		  ,spx.sesac_property_id',
'		  ,'' '' SESAC_PROPERTY',
'          ,(',
'                SELECT REPLACE(REPLACE(error_message,''#''),'''''''') error_message',
'                FROM cwr_messages ',
'                WHERE message_type = ''REJ''',
'                AND sts.rejection_code = cwr_message_id',
'           ) Remarks',
'           ,''N'' jingle_flag,',
'    (SELECT MAX(DECODE(aka_type_id,51,aka_name)) FROM prop_akas aka WHERE aka.property_id =  sts.property_id  ) HFA_Song_Code',
'	FROM   spi_transaction_status sts',
'		  ,spi_property_xref spx',
'		  ,prop_lookups pl',
'          ,(',
'            SELECT TO_CHAR(id) AS submission_id, submitting_publisher_number ',
'            FROM ape_submission ',
'            UNION ALL',
'            SELECT submission_id, administrator_num AS submitting_publisher_number ',
'            FROM owr_song_registration',
'           )  asu',
'	WHERE  	1=1',
'    AND     spx.submitter_work_id = sts.submitter_work_id',
'	AND    	spx.submitter_source_id = sts.source_id',
'	AND    	pl.lookup_code = sts.transaction_status',
'    AND     TO_CHAR(asu.submission_id(+)) = TO_CHAR(sts.submission_id)',
'	AND    	pl.lookup_type = ''SPI_STATUS'' ',
'	AND		sts.transaction_status = 90',
'	AND		sts.transaction_status_id = NVL(:P50_SPI_TRANSACTION_STATUS_ID, sts.transaction_status_id)',
'	AND     sts.source_id = NVL(:P50_PUB_SRC,sts.source_id)',
'    AND     NVL(asu.SUBMITTING_PUBLISHER_NUMBER, ''99.99'') = NVL(:P50_SUB_PUBLISHER_NUMBER, NVL(asu.SUBMITTING_PUBLISHER_NUMBER, ''99.99''))',
'	--AND    	sts.batch_id  = NVL(:P50_FILE_NAME,sts.batch_id)',
'    AND     NVL(sts.file_name, ''X'')  = NVL(:P50_FILE_NAME,NVL(sts.file_name, ''X''))',
'	AND     sts.SUBMISSION_ID = NVL(TRIM(:P50_SUBMISSION_ID), sts.SUBMISSION_ID)',
'	AND    	NVL(sts.creation_date, SYSDATE) BETWEEN NVL(TO_DATE(:P50_SUBMISSION_DATE_FROM,''YYYY-MM-DD''),NVL(sts.creation_date, SYSDATE))',
'							   AND     NVL(TO_DATE(:P50_SUBMISSION_DATE_TO,''YYYY-MM-DD''),NVL(sts.creation_date, SYSDATE))',
'    AND    	sts.title LIKE NVL((''%''||UPPER(:P50_SONG_TITLE)||''%''),UPPER(sts.title))',
'    --AND    	sts.submitter_work_id LIKE NVL(''%''||TRIM(:P50_SUBMITTER_WORK_NO)||''%'',sts.submitter_work_id)',
'    AND (',
'            :P50_SUBMITTER_WORK_NO IS NULL OR EXISTS (                  ',
'                                                        select 1 from (  ',
'                                                                        select qry.val from  ',
'                                                                               (',
'                                                                                     select regexp_substr(:P50_SUBMITTER_WORK_NO,''[^,]+'', 1, level) val',
'                                                                                     from dual ',
'                                                                                     connect BY regexp_substr(:P50_SUBMITTER_WORK_NO, ''[^,]+'', 1, level) ',
'                                                                                     is not null',
'                                                                               ) qry where qry.val=sts.submitter_work_id',
'                                                                       )',
'                                                    )',
'        )',
'    AND     ',
'    (       :P50_PROPERTY_NUMBER IS NULL',
'            OR',
'            (',
'                :P50_PROPERTY_NUMBER IS NOT NULL AND',
'                EXISTS',
'                (',
'                    SELECT 1 FROM prop_properties where property_id = sts.property_id and property_number =  :P50_PROPERTY_NUMBER',
'                )',
'            )       ',
'    )',
'    --AND (:P50_HFA_SONG_CODE is null or exists (select 1 from prop_akas aka where aka.property_id = sts.property_id and aka.AKA_TYPE_ID=51 and aka.aka_name=:P50_HFA_SONG_CODE ))',
'    AND',
'    (',
'        (:P50_PARTY_NAME IS NULL AND :P50_PARTY_IPI_NUMBER IS NULL AND :P50_PARTY_AFFILIATION IS NULL AND :P50_WRI_PUB_INDICATOR IS NULL)',
'        OR',
'        (:P50_TRANSACTION_STATUS = 90 AND (:P50_PARTY_NAME IS NOT NULL OR :P50_PARTY_IPI_NUMBER IS NOT NULL OR :P50_PARTY_AFFILIATION IS NOT NULL OR :P50_WRI_PUB_INDICATOR IS NOT NULL ))',
'        AND EXISTS',
'        ( ',
'            SELECT NULL',
'             FROM   prop_share_collectors pso, aff_roles ar',
'             WHERE pso.property_id = sts.property_id',
'             AND pso.end_date_active IS NULL',
'             AND (:P50_PARTY_AFFILIATION is null or NVL(pso.affiliated_society_id, -1) = NVL(:P50_PARTY_AFFILIATION, NVL(pso.affiliated_society_id, -1)))',
'             AND ar.role_id = pso.role_id',
'             AND ',
'                ( ',
'                    ( :P50_WRI_PUB_INDICATOR = ''Y'' AND ar.summary_role_code = ''W'')',
'                OR',
'                    NVL(:P50_WRI_PUB_INDICATOR,''N'') = ''N''',
'                )',
'             AND ',
'             (  :P50_PARTY_NAME IS NULL',
'                OR',
'                (',
'                    :P50_PARTY_NAME IS NOT NULL  ',
'                    AND EXISTS',
'                    (',
'                        SELECT 1 FROM spi_share_owner_xref sx',
'                        WHERE sx.submitter_party_id = pso.submitter_internal_number',
'                        AND sx.submitter_source_id = sts.source_id',
'                        AND INSTR((TRIM(Upper(sx.first_name||'' ''||sx.name))),UPPER(TRIM(:P50_PARTY_NAME)))>0',
'                        AND (:P50_PARTY_IPI_NUMBER is null or sx.IPI_NAME_NUMBER = NVL(:P50_PARTY_IPI_NUMBER,sx.IPI_NAME_NUMBER)) ',
'                        AND NVL(sx.sesac_ip_id, 602200) = pso.collector_id',
'                        UNION ALL',
'                        SELECT 1 FROM aff_ip_names ain',
'                        WHERE ain.ip_id = pso.collector_id',
'                        AND ain.name_type_id = 6',
'                        AND INSTR((Upper(ain.full_name)),UPPER(:P50_PARTY_NAME))>0',
'                        AND (:P50_PARTY_IPI_NUMBER is null or ain.name_number = NVL(:P50_PARTY_IPI_NUMBER,ain.name_number))',
'                    ) ',
'                )',
'            )',
'        )',
'    )',
'   AND	( :P50_TRANS_TYPE=''A'' OR  EXISTS ( SELECT 1',
'                                     FROM   cwr_ack_history cah',
'                                     WHERE  cah.property_id=sts.property_id',
'                                     and cah.transaction_type=:P50_TRANS_TYPE',
'                                    ) )',
'    AND     (',
'                :P50_SPI_TRANSACTION_STATUS_ID IS NOT NULL OR :P50_PUB_SRC IS NOT NULL OR :P50_FILE_NAME IS NOT NULL OR :P50_SUBMISSION_DATE_FROM IS NOT NULL OR ',
'                :P50_SUBMISSION_DATE_TO IS NOT NULL OR :P50_SONG_TITLE IS NOT NULL OR :P50_PROPERTY_NUMBER IS NOT NULL OR --:P50_HFA_SONG_CODE IS NOT NULL OR',
'                :P50_SPI_TRANSACTION_STATUS_ID IS NOT NULL OR :P50_SUBMITTER_WORK_NO IS NOT NULL OR',
'                :P50_PARTY_NAME IS NOT NULL OR :P50_PARTY_IPI_NUMBER IS NOT NULL OR :P50_PARTY_AFFILIATION IS NOT NULL OR ',
'                :P50_WRI_PUB_INDICATOR IS NOT NULL',
'            )',
'    UNION ALL',
'    -- TO get Non-Rejected works',
'	SELECT DISTINCT sts.transaction_status_id',
'		  ,sts.creation_date AS submission_date',
'		  ,(select ps.source_code from prop_sources ps where ps.source_id=sts.source_id) source_id',
'		  ,sts.file_name',
'		  ,sts.submission_id',
'		  ,sts.submitter_work_id',
'          ,asu.submitting_publisher_number',
'		  ,sts.title',
'		  ,sts.transaction_status',
'		  ,pl.description "STATUS"',
'		  ,pp.property_number',
'          --,(select property_number from prop_properties where property_id = spx.sesac_property_id)||'' (''||sts.transaction_status||'')'' prop_number_sesac_tst',
'		  ,sts.property_id',
'		  ,spx.sesac_property_id',
'		  ,CASE WHEN sts.transaction_status IN (70,80,100,110) THEN (SELECT title from prop_properties where property_id = spx.sesac_property_id) ELSE '' '' END SESAC_PROPERTY',
'          ,NULL Remarks -- previously used for displaying Reject Reason',
'           , DECODE(pcl.song_property_id, null, ''N'', ''Y'') jingle_flag,',
'    (SELECT MAX(DECODE(aka_type_id,51,aka_name)) FROM prop_akas aka WHERE aka.property_id =  spx.sesac_property_id  ) HFA_Song_Code',
'	FROM   spi_transaction_status sts',
'		  ,spi_property_xref spx',
'		  ,prop_lookups pl',
'		  ,prop_properties pp',
'          ,prop_commercial_lines pcl',
'          ,(',
'            SELECT TO_CHAR(id) AS submission_id, submitting_publisher_number ',
'            FROM ape_submission ',
'            UNION ALL',
'            SELECT submission_id, administrator_num AS submitting_publisher_number ',
'            FROM owr_song_registration',
'           ) asu',
'	WHERE  	sts.property_id IS NOT NULL',
'	AND		sts.property_id = pp.property_id',
'	AND		spx.submitter_work_id = sts.submitter_work_id',
'	AND    	spx.submitter_source_id = sts.source_id',
'	AND    	pl.lookup_code = sts.transaction_status',
'    AND     TO_CHAR(asu.submission_id(+)) = TO_CHAR(sts.submission_id)',
'	AND    	pl.lookup_type = ''SPI_STATUS'' ',
'	AND		sts.transaction_status_id = NVL(:P50_SPI_TRANSACTION_STATUS_ID, sts.transaction_status_id)',
'	AND     sts.transaction_status <> 90',
'	AND     sts.source_id = NVL(:P50_PUB_SRC,sts.source_id) ',
'	AND     pp.property_number = NVL(:P50_PROPERTY_NUMBER, pp.property_number)',
'    --AND     (:P50_HFA_SONG_CODE is null or exists (select 1 from prop_akas aka where aka.property_id = pp.property_id and aka.AKA_TYPE_ID=51 and aka.aka_name=:P50_HFA_SONG_CODE ))',
'    AND     NVL(asu.submitting_publisher_number, ''99.99'') = NVL(:P50_SUB_PUBLISHER_NUMBER, NVL(asu.submitting_publisher_number, ''99.99''))',
'	--AND    	sts.batch_id  = NVL(:P50_FILE_NAME,sts.batch_id)',
'    AND     NVL(sts.file_name, ''X'')  = NVL(:P50_FILE_NAME,NVL(sts.file_name, ''X''))',
'	AND     sts.SUBMISSION_ID = NVL(TRIM(:P50_SUBMISSION_ID), sts.SUBMISSION_ID)',
'	--AND    NVL(sts.submission_date, SYSDATE) BETWEEN NVL(TO_DATE(:P50_SUBMISSION_DATE_FROM,''YYYY-MM-DD''),NVL(sts.submission_date, SYSDATE))',
'	--                           AND     NVL(TO_DATE(:P50_SUBMISSION_DATE_TO,''YYYY-MM-DD''),NVL(sts.submission_date, SYSDATE))',
'	AND    	NVL(sts.creation_date, SYSDATE) BETWEEN NVL(TO_DATE(:P50_SUBMISSION_DATE_FROM,''YYYY-MM-DD''),NVL(sts.creation_date, SYSDATE))',
'							   AND     NVL(TO_DATE(:P50_SUBMISSION_DATE_TO,''YYYY-MM-DD''),NVL(sts.creation_date, SYSDATE))',
'	AND    	sts.title LIKE NVL((''%''||UPPER(:P50_SONG_TITLE)||''%''),UPPER(sts.title))',
'	AND		( ',
'				:P50_PROP_ID_HFA_CODE IS NULL',
'				OR',
'				(',
'                    :P50_PROP_ID_HFA_CODE IS NOT NULL',
'                    AND EXISTS',
'                    ( ',
'                        SELECT aka_name',
'                        ,property_id',
'                        FROM prop_akas pa',
'                        WHERE aka_type_id = 51',
'                        AND pa.aka_name = UPPER(TRIM(:P50_PROP_ID_HFA_CODE))',
'                        AND pa.property_id = spx.sesac_property_id',
'                    )',
'                )',
'			) ',
'	--AND    	sts.submitter_work_id LIKE NVL(''%''||TRIM(:P50_SUBMITTER_WORK_NO)||''%'',sts.submitter_work_id)',
'    AND (',
'        :P50_SUBMITTER_WORK_NO IS NULL OR EXISTS (                  ',
'                                                    select 1 from (  ',
'                                                                    select qry.val from  ',
'                                                                           (',
'                                                                                 select regexp_substr(:P50_SUBMITTER_WORK_NO,''[^,]+'', 1, level) val',
'                                                                                 from dual ',
'                                                                                 connect BY regexp_substr(:P50_SUBMITTER_WORK_NO, ''[^,]+'', 1, level) ',
'                                                                                 is not null',
'                                                                           ) qry where qry.val=sts.submitter_work_id',
'                                                                   )',
'                                                )',
'    )',
'	AND ',
'	(',
'		:P50_TRANSACTION_STATUS IS NULL',
'		OR',
'		(:P50_TRANSACTION_STATUS IS NOT NULL AND CASE WHEN to_number(sts.transaction_status) IN (70,80,100,110) THEN 110 ELSE to_number(sts.transaction_status) END = :P50_TRANSACTION_STATUS)',
'	)',
'	/*AND    sts.transaction_status IN (select regexp_substr(NVL(:P50_TRANSACTION_STATUS,sts.transaction_status),''[^,]+'', 1, level) status from dual',
'			  connect by regexp_substr(NVL(:P50_TRANSACTION_STATUS,sts.transaction_status), ''[^,]+'', 1, level) is not null)',
'	*/          ',
'	AND',
'    (',
'        (:P50_PARTY_NAME IS NULL AND :P50_PARTY_IPI_NUMBER IS NULL AND :P50_PARTY_AFFILIATION IS NULL AND :P50_WRI_PUB_INDICATOR IS NULL)',
'        OR',
'        (:P50_PARTY_NAME IS NOT NULL OR :P50_PARTY_IPI_NUMBER IS NOT NULL OR :P50_PARTY_AFFILIATION IS NOT NULL OR :P50_WRI_PUB_INDICATOR IS NOT NULL)',
'        AND EXISTS',
'        ( ',
'            SELECT NULL',
'             FROM   prop_share_collectors pso, aff_roles ar',
'             WHERE pso.property_id = pp.property_id',
'             AND pso.end_date_active IS NULL',
'             AND (:P50_PARTY_AFFILIATION is null or NVL(pso.affiliated_society_id, -1) = NVL(:P50_PARTY_AFFILIATION, NVL(pso.affiliated_society_id, -1)))',
'             AND ar.role_id = pso.role_id',
'             AND ',
'                ( ',
'                    ( :P50_WRI_PUB_INDICATOR = ''Y'' AND ar.summary_role_code = ''W'')',
'                OR',
'                    NVL(:P50_WRI_PUB_INDICATOR,''N'') = ''N''',
'                )',
'             AND ',
'             (  :P50_PARTY_NAME IS NULL',
'                OR',
'                (',
'                    :P50_PARTY_NAME IS NOT NULL  ',
'                    AND EXISTS',
'                    (',
'                        SELECT 1 FROM spi_share_owner_xref sx',
'                        WHERE sx.submitter_party_id = pso.submitter_internal_number',
'                        AND sx.submitter_source_id = pp.source_id',
'                        AND INSTR((TRIM(Upper(sx.first_name||'' ''||sx.name))),UPPER(TRIM(:P50_PARTY_NAME)))>0',
'                        AND (:P50_PARTY_IPI_NUMBER is null or sx.IPI_NAME_NUMBER = NVL(:P50_PARTY_IPI_NUMBER,sx.IPI_NAME_NUMBER)) ',
'                        AND NVL(sx.sesac_ip_id, 602200) = pso.collector_id',
'                        UNION ALL',
'                        SELECT 1 FROM aff_ip_names ain',
'                        WHERE ain.ip_id = pso.collector_id',
'                        AND ain.name_type_id = 6',
'                        AND INSTR((Upper(ain.full_name)),UPPER(:P50_PARTY_NAME))>0',
'                        AND (:P50_PARTY_IPI_NUMBER is null or ain.name_number = NVL(:P50_PARTY_IPI_NUMBER,ain.name_number))',
'                    ) ',
'                )',
'            )',
'        )',
'    )',
'   AND	( :P50_TRANS_TYPE=''A'' OR  EXISTS ( SELECT 1',
'                                     FROM   cwr_ack_history cah',
'                                     WHERE  cah.property_id=sts.property_id',
'                                     and cah.transaction_type=:P50_TRANS_TYPE',
'                                    ) )',
'---------',
'---jingle sort order removed and replaced with join below',
'AND spx.sesac_property_id = pcl.song_property_id (+)',
'--------',
'	AND    ( ( :P50_ISRC_REC_INFO = ''N''',
'			   AND',
'			   NOT EXISTS ( SELECT 1',
'							FROM   prop_recordings pr',
'							WHERE      /*--pr.property_id = pps.property_id*/',
'							pr.property_id IN (SELECT property_id FROM prop_properties where source_property_id =pp.property_id AND property_type_id=2) ',
'						  )',
'			 )',
'			 OR',
'			 ( :P50_ISRC_REC_INFO = ''Y''',
'			   AND',
'			   EXISTS ( SELECT 1',
'						FROM   prop_recordings pr',
'						WHERE      /*--pr.property_id = pps.property_id*/',
'						pr.property_id IN (SELECT property_id FROM prop_properties where source_property_id =pp.property_id AND property_type_id=2) ',
'					  )',
'			 )',
'			 OR',
'			 ( :P50_ISRC_REC_INFO IS NULL )',
'			   )		 ',
'	AND    ( ( :P50_QUEUE_NAME IS NOT NULL',
'				AND EXISTS ( SELECT 1',
'							 FROM   spi_work_queue_details swqd',
'							 WHERE  swqd.work_queue_header_id = :P50_QUEUE_NAME',
'							 AND    swqd.request_id IS NULL',
'							 AND    swqd.property_id = sts.property_id',
'							 UNION',
'							 SELECT 1',
'							 FROM   spi_work_queue_details swqd',
'								   ,spi_matching_requests mmr',
'							 WHERE  swqd.work_queue_header_id = :P50_QUEUE_NAME',
'							 AND    swqd.property_id IS NULL',
'							 AND    mmr.request_id = swqd.request_id',
'							 AND    mmr.product_number = sts.submitter_work_id',
'						   )',
'			  )',
'			  OR',
'			  :P50_QUEUE_NAME IS NULL',
'			 )',
'	--ORDER BY CASE WHEN :P50_DATE_SORT_ORDER = ''O'' THEN sts.creation_date ELSE NULL END ASC',
'		--	,CASE WHEN :P50_DATE_SORT_ORDER = ''N'' THEN sts.creation_date ELSE NULL END DESC',
')',
'where :P50_QUERY = ''Y''',
'and (jingle_flag = :P50_JINGLE_INDICATOR OR :P50_JINGLE_INDICATOR IS NULL) )',
'loop ',
'APEX_COLLECTION.ADD_MEMBER (''REVALUATE_STATUS'',rec1.transaction_status_id,rec1.submission_date,rec1.source_id,rec1.file_name,',
'                            rec1.submission_id,rec1.submitter_work_id,rec1.submitting_publisher_number',
'		                   ,rec1.title,rec1.transaction_status,rec1.status,rec1.property_number,rec1.property_id,rec1.sesac_property_id,',
'                            rec1.SESAC_PROPERTY,rec1.Remarks,rec1.jingle_flag,rec1.HFA_Song_Code);',
'END LOOP;',
'APEX_COLLECTION.CREATE_OR_TRUNCATE_COLLECTION (p_collection_name=>''REVALUATE_STATUS_1'');',
'',
'',
'END;'))
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
,p_process_when_type=>'NEVER'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(5380770017599126)
,p_process_sequence=>40
,p_process_point=>'BEFORE_HEADER'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'CREATECOLL_1'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'BEGIN',
'--RAISE_APPLICATION_ERROR(-20010,''Please enter a search criteria.'');',
'IF :P50_CONFLICT_FLAG = ''N'' THEN',
'IF (APEX_COLLECTION.COLLECTION_EXISTS (p_collection_name => ''REVALUATE_STATUS'') ) THEN',
'APEX_COLLECTION.DELETE_COLLECTION(',
'        p_collection_name => ''REVALUATE_STATUS'');',
'',
'END IF;',
'IF (APEX_COLLECTION.COLLECTION_EXISTS (p_collection_name => ''REVALUATE_STATUS_1'') ) THEN',
'APEX_COLLECTION.DELETE_COLLECTION(',
'        p_collection_name => ''REVALUATE_STATUS_1'');',
'',
'END IF;',
'',
'IF(:P50_SPI_TRANSACTION_STATUS_ID	IS NULL AND :P50_PUB_SRC IS NULL AND :P50_SUB_PUBLISHER_NUMBER	IS NULL AND :P50_FILE_NAME	IS NULL AND :P50_SUBMISSION_ID IS NULL AND :P50_SUBMISSION_DATE_FROM IS NULL AND :P50_SUBMISSION_DATE_TO IS NULL AND :P50_SONG'
||'_TITLE IS NULL AND :P50_SUBMITTER_WORK_NO IS NULL AND :P50_PROPERTY_NUMBER IS NULL AND :P50_PARTY_NAME IS NULL AND :P50_PARTY_IPI_NUMBER	IS NULL AND :P50_PARTY_AFFILIATION	IS NULL AND :P50_WRI_PUB_INDICATOR	IS NULL AND :P50_TRANS_TYPE IS NULL ',
'AND :P50_PROP_ID_HFA_CODE IS NULL AND :P50_TRANSACTION_STATUS IS NULL AND :P50_ISRC_REC_INFO IS NULL AND :P50_JINGLE_INDICATOR IS NULL) THEN',
'    NULL;',
'ELSE',
'    spi_screen_inquiry.wq_inquiry(p_spi_transaction_status_id => :P50_SPI_TRANSACTION_STATUS_ID,',
'                        p_pub_src => :P50_PUB_SRC,',
'                        p_submitting_publisher_number => :P50_SUB_PUBLISHER_NUMBER,',
'                        p_file_name => :P50_FILE_NAME,',
'                        p_submission_id => :P50_SUBMISSION_ID,',
'                        p_submission_date_from => :P50_SUBMISSION_DATE_FROM,',
'                        p_submission_date_to => :P50_SUBMISSION_DATE_TO,',
'                        p_song_title => :P50_SONG_TITLE,',
'                        p_submitter_work_no => :P50_SUBMITTER_WORK_NO,',
'                        p_property_number => :P50_PROPERTY_NUMBER,',
'                        p_party_name => :P50_PARTY_NAME,',
'                        p_party_ipi_number => :P50_PARTY_IPI_NUMBER,',
'                        p_party_affiliation => :P50_PARTY_AFFILIATION,',
'                        p_wri_pub_indicator => :P50_WRI_PUB_INDICATOR,',
'                        p_trans_type => :P50_TRANS_TYPE,',
'                        p_prop_id_hfa_code => :P50_PROP_ID_HFA_CODE,',
'                        p_transaction_status => :P50_TRANSACTION_STATUS,',
'                        p_isrc_rec_info => TO_CHAR(:P50_ISRC_REC_INFO),',
'                        p_queue_name => :P50_QUEUE_NAME,',
'                        p_jingle_indicator => :P50_JINGLE_INDICATOR,',
'                        p_query => :P50_QUERY,',
'                        p_priority_flag => :P50_PRIORITY_FLAG,',
'                        p_hold_reason => :P50_HOLD_VALUE);',
'    APEX_COLLECTION.CREATE_OR_TRUNCATE_COLLECTION (p_collection_name=>''REVALUATE_STATUS_1'');',
'END IF;',
'END IF;',
'END;'))
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(86449093885332856)
,p_process_sequence=>40
,p_process_point=>'ON_DEMAND'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'UPDATECHECKBOXVALUE'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'    l_collection_name VARCHAR2(30) := ''REVALUATE_STATUS_1'';',
'    l_perform VARCHAR2(10);',
'',
'BEGIN',
'        l_perform := wwv_flow.g_x01; ',
'    ',
'        IF apex_collection.collection_exists(p_collection_name => l_collection_name) THEN',
'            NULL;',
'        ELSE',
'            APEX_COLLECTION.CREATE_COLLECTION(p_collection_name => l_collection_name);',
'        END IF;',
'        ',
'        IF(l_perform = ''insert'') THEN',
'            apex_collection.create_or_truncate_collection(p_collection_name => ''REVALUATE_STATUS_1'' );',
'            FOR rec IN(select c001,c003,c006,c013 from apex_collections ac1 where ac1.collection_name=''REVALUATE_STATUS'')',
'            ',
'            LOOP',
'            apex_collection.add_member(p_collection_name=>''REVALUATE_STATUS_1'',p_c001=>rec.c001,p_c002=>rec.c003,p_c003=>rec.c006,p_c004=>rec.c013);',
'                ',
'            END LOOP;',
'        ELSif (l_perform = ''delete'') THEN',
'            apex_collection.create_or_truncate_collection(p_collection_name => ''REVALUATE_STATUS_1'' );',
'        END IF;',
'        ',
'        ',
'        ',
'    ',
'END;'))
,p_process_error_message=>'ERROR# PROCESS- UpdateCheckboxValue#SQLERRM#'
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(19713476057749018)
,p_process_sequence=>20
,p_process_point=>'ON_SUBMIT_BEFORE_COMPUTATION'
,p_process_type=>'NATIVE_RESET_PAGINATION'
,p_process_name=>'Reset Pagination'
,p_attribute_01=>'THIS_PAGE'
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(5381770335599136)
,p_process_sequence=>40
,p_process_point=>'ON_SUBMIT_BEFORE_COMPUTATION'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'onClick_Revaluation'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'    m_child_party_id    NUMBER;       ',
'    m_parent_party_id   NUMBER;',
'    m_relate_out        CHAR(1);',
'BEGIN',
'    FOR i in ( ',
'                SELECT * FROM spi_transaction_status WHERE transaction_status_id IN ',
'                (',
'                    select to_number(c001) from apex_collections where collection_name=''REVALUATE_STATUS_1''',
'                ) and transaction_status < 70',
'        )',
'     LOOP',
'         -- Post nowing update',
'         FOR cur_psc IN(SELECT DISTINCT psc.collector_id, psa.admin_id ',
'                        FROM prop_share_collectors psc, prop_share_admins psa ',
'                        WHERE psc.share_collector_id = psa.share_collector_id',
'                        AND psc.property_id = i.property_id',
'                        AND psc.right_type_id = 2',
'                        AND TRUNC(NVL(psc.end_date_active,SYSDATE+1)) > TRUNC(SYSDATE)',
'                        AND TRUNC(NVL(psa.end_date_active,SYSDATE+1)) > TRUNC(SYSDATE)) ',
'         LOOP',
'             spi_get_mapped_hierarchy(cur_psc.collector_id,cur_psc.admin_id,m_child_party_id,m_parent_party_id,m_relate_out);',
'             ',
'             IF(cur_psc.collector_id = m_child_party_id AND cur_psc.admin_id = m_parent_party_id) THEN',
'                NULL;',
'             ELSE',
'                DEBUG_WRITE(''Owner = ''||cur_psc.collector_id||'' admin_id = ''||cur_psc.admin_id, ''POST-NOW''); ',
'                spi_work_queue_procs.post_nowing_update_owner_admin(cur_psc.collector_id,cur_psc.admin_id,m_child_party_id,m_parent_party_id);',
'                DEBUG_WRITE(''m_child_party_id = ''||m_child_party_id||'' m_parent_party_id = ''||m_parent_party_id, ''POST-NOW''); ',
'             END IF;',
'         END LOOP;',
'     ',
'         IF i.transaction_status <> 60 THEN ',
'         --SPI-1969 - If condition added by Saurav on 29-MAR-2018',
'             Prop_ingestion.set_spi_status(i.source_id,i.submitter_work_id,''M'',i.selected_sesac_property_id);',
'         END IF;',
'    END LOOP;',
'END;'))
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
,p_process_when_button_id=>wwv_flow_api.id(34976895512886030)
);
end;
/
begin
wwv_flow_api.import_end(p_auto_install_sup_obj => nvl(wwv_flow_application_install.get_auto_install_sup_obj, false));
commit;
end;
/
set verify on feedback on define on
prompt  ...done
