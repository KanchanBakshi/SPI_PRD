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
--   Date and Time:   03:25 Tuesday November 16, 2021
--   Exported By:     SCHOPRA
--   Flashback:       0
--   Export Type:     Page Export
--   Version:         18.2.0.00.12
--   Instance ID:     69431613733717
--

prompt --application/pages/delete_07240
begin
wwv_flow_api.remove_page (p_flow_id=>wwv_flow.g_flow_id, p_page_id=>7240);
end;
/
prompt --application/pages/page_07240
begin
wwv_flow_api.create_page(
 p_id=>7240
,p_user_interface_id=>wwv_flow_api.id(51875501499240850)
,p_name=>'Membership Agreements'
,p_page_mode=>'MODAL'
,p_step_title=>'Membership Agreements'
,p_reload_on_submit=>'A'
,p_warn_on_unsaved_changes=>'N'
,p_step_sub_title=>'Membership Agreements'
,p_step_sub_title_type=>'TEXT_WITH_SUBSTITUTIONS'
,p_autocomplete_on_off=>'ON'
,p_group_id=>wwv_flow_api.id(68038105554099950)
,p_html_page_header=>'  '
,p_javascript_code=>wwv_flow_string.join(wwv_flow_t_varchar2(
'//var htmldb_delete_message=''"DELETE_CONFIRM_MSG"'';',
'var htmldb_delete_message=''Are you sure you want to delete this membership agreement?'';',
'',
'function selectAll(pthis)',
'{',
'	if(pthis.checked)',
'		$(''input[name="f02"]'').attr(''checked'',true);',
'	else',
'		$(''input[name="f02"]'').attr(''checked'',false);',
'}'))
,p_page_template_options=>'#DEFAULT#:ui-dialog--stretch'
,p_required_role=>wwv_flow_api.id(69303254223531622)
,p_help_text=>'No help is available for this page.'
,p_last_updated_by=>'ADMIN'
,p_last_upd_yyyymmddhh24miss=>'20210316064941'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(65942970664375075)
,p_plug_name=>'Membership Agreements'
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>20
,p_plug_display_point=>'BODY'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_plug_display_condition_type=>'ITEM_IS_NOT_NULL'
,p_plug_display_when_condition=>'P7240_MEMBERSHIP_AGR_ID'
,p_attribute_01=>'N'
,p_attribute_02=>'HTML'
,p_attribute_03=>'N'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(65953765736375082)
,p_name=>'Territories'
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>30
,p_region_template_options=>'#DEFAULT#:t-Region--hiddenOverflow'
,p_component_template_options=>'#DEFAULT#'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_TABFORM'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select ',
'"MAT_ID",',
'"MAT_ID" MAT_ID_DISPLAY,',
'"MEMBERSHIP_AGREEMENT_ID",',
'"TERRITORY_ID",',
'"INCLUDE_EXCLUDE_FLAG",',
'"CREATED_BY",',
'"CREATION_DATE",',
'"LAST_UPDATED_BY",',
'"LAST_UPDATE_DATE"',
'from "#OWNER#"."AFF_MA_TERRITORIES"',
'WHERE MEMBERSHIP_AGREEMENT_ID = :P7240_MEMBERSHIP_AGR_ID_1',
''))
,p_display_when_condition=>'P7240_MEMBERSHIP_AGR_ID_1'
,p_display_condition_type=>'ITEM_IS_NOT_NULL'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>10
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>'(null)'
,p_query_break_cols=>'0'
,p_query_no_data_found=>'No data found.'
,p_query_num_rows_type=>'ROW_RANGES_IN_SELECT_LIST'
,p_query_row_count_max=>500
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_break_type_flag=>'DEFAULT_BREAK_FORMATTING'
,p_csv_output=>'N'
,p_query_asc_image=>'apex/builder/dup.gif'
,p_query_asc_image_attr=>'width="16" height="16" alt="" '
,p_query_desc_image=>'apex/builder/ddown.gif'
,p_query_desc_image_attr=>'width="16" height="16" alt="" '
,p_plug_query_strip_html=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65954190572375083)
,p_query_column_id=>1
,p_column_alias=>'CHECK$01'
,p_column_display_sequence=>1
,p_column_heading=>'Select Row'
,p_report_column_required_role=>wwv_flow_api.id(17200066639506016)
,p_display_as=>'CHECKBOX'
,p_derived_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65954529428375083)
,p_query_column_id=>2
,p_column_alias=>'MAT_ID'
,p_column_display_sequence=>2
,p_column_heading=>'Mat Id'
,p_disable_sort_column=>'N'
,p_hidden_column=>'Y'
,p_display_as=>'HIDDEN'
,p_column_width=>16
,p_pk_col_source_type=>'S'
,p_pk_col_source=>'AFF_MAT_S'
,p_ref_schema=>'APCTS'
,p_ref_table_name=>'AFF_MA_TERRITORIES'
,p_ref_column_name=>'MAT_ID'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65954966592375084)
,p_query_column_id=>3
,p_column_alias=>'MAT_ID_DISPLAY'
,p_column_display_sequence=>3
,p_column_heading=>'Mat Id'
,p_disable_sort_column=>'N'
,p_hidden_column=>'Y'
,p_column_width=>16
,p_ref_schema=>'APCTS'
,p_ref_table_name=>'AFF_MA_TERRITORIES'
,p_ref_column_name=>'MAT_ID_DISPLAY'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65955369644375084)
,p_query_column_id=>4
,p_column_alias=>'MEMBERSHIP_AGREEMENT_ID'
,p_column_display_sequence=>4
,p_column_heading=>'Membership Agreement Id'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_display_as=>'TEXT'
,p_lov_show_nulls=>'NO'
,p_column_width=>16
,p_column_default=>'P7240_MEMBERSHIP_AGR_ID_1'
,p_column_default_type=>'ITEM'
,p_include_in_export=>'Y'
,p_ref_schema=>'APCTS'
,p_ref_table_name=>'AFF_MA_TERRITORIES'
,p_ref_column_name=>'MEMBERSHIP_AGREEMENT_ID'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65955739543375084)
,p_query_column_id=>5
,p_column_alias=>'TERRITORY_ID'
,p_column_display_sequence=>5
,p_column_heading=>'Territory'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_display_as=>'SELECT_LIST_FROM_LOV'
,p_named_lov=>wwv_flow_api.id(62955684789980410)
,p_lov_show_nulls=>'NO'
,p_column_width=>16
,p_include_in_export=>'Y'
,p_ref_schema=>'APCTS'
,p_ref_table_name=>'AFF_MA_TERRITORIES'
,p_ref_column_name=>'TERRITORY_ID'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65956151884375085)
,p_query_column_id=>6
,p_column_alias=>'INCLUDE_EXCLUDE_FLAG'
,p_column_display_sequence=>6
,p_column_heading=>'Include Exclude Flag'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_display_as=>'SELECT_LIST_FROM_LOV'
,p_named_lov=>wwv_flow_api.id(64835492851304317)
,p_lov_show_nulls=>'NO'
,p_column_width=>12
,p_column_default=>'''I'''
,p_column_default_type=>'FUNCTION'
,p_include_in_export=>'Y'
,p_ref_schema=>'APCTS'
,p_ref_table_name=>'AFF_MA_TERRITORIES'
,p_ref_column_name=>'INCLUDE_EXCLUDE_FLAG'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65956581124375085)
,p_query_column_id=>7
,p_column_alias=>'CREATED_BY'
,p_column_display_sequence=>7
,p_column_heading=>'Created By'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_hidden_column=>'Y'
,p_display_as=>'TEXT'
,p_lov_show_nulls=>'NO'
,p_column_width=>16
,p_column_default=>':APP_USER'
,p_column_default_type=>'FUNCTION'
,p_include_in_export=>'Y'
,p_ref_schema=>'APCTS'
,p_ref_table_name=>'AFF_MA_TERRITORIES'
,p_ref_column_name=>'CREATED_BY'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65956996779375085)
,p_query_column_id=>8
,p_column_alias=>'CREATION_DATE'
,p_column_display_sequence=>8
,p_column_heading=>'Creation Date'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_hidden_column=>'Y'
,p_display_as=>'DATE_PICKER'
,p_lov_show_nulls=>'NO'
,p_column_width=>12
,p_column_default=>'SYSDATE'
,p_column_default_type=>'FUNCTION'
,p_include_in_export=>'Y'
,p_ref_schema=>'APCTS'
,p_ref_table_name=>'AFF_MA_TERRITORIES'
,p_ref_column_name=>'CREATION_DATE'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65957378322375085)
,p_query_column_id=>9
,p_column_alias=>'LAST_UPDATED_BY'
,p_column_display_sequence=>9
,p_column_heading=>'Last Updated By'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_hidden_column=>'Y'
,p_display_as=>'TEXT'
,p_lov_show_nulls=>'NO'
,p_column_width=>16
,p_column_default=>':APP_USER'
,p_column_default_type=>'FUNCTION'
,p_include_in_export=>'Y'
,p_ref_schema=>'APCTS'
,p_ref_table_name=>'AFF_MA_TERRITORIES'
,p_ref_column_name=>'LAST_UPDATED_BY'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65957757521375086)
,p_query_column_id=>10
,p_column_alias=>'LAST_UPDATE_DATE'
,p_column_display_sequence=>10
,p_column_heading=>'Last Update Date'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_hidden_column=>'Y'
,p_display_as=>'DATE_PICKER'
,p_lov_show_nulls=>'NO'
,p_column_width=>12
,p_column_default=>'SYSDATE'
,p_column_default_type=>'FUNCTION'
,p_include_in_export=>'Y'
,p_ref_schema=>'APCTS'
,p_ref_table_name=>'AFF_MA_TERRITORIES'
,p_ref_column_name=>'LAST_UPDATE_DATE'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(65963358214375089)
,p_plug_name=>'Songs'
,p_region_template_options=>'js-showMaximizeButton:t-Region--hiddenOverflow'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>50
,p_plug_display_point=>'BODY'
,p_plug_query_headings_type=>'QUERY_COLUMNS'
,p_plug_query_num_rows_type=>'NEXT_PREVIOUS_LINKS'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_plug_query_show_nulls_as=>' - '
,p_plug_display_condition_type=>'SQL_EXPRESSION'
,p_plug_display_when_condition=>':P7240_MEMBERSHIP_AGR_ID_1 is not null and :P7240_RIGHT_TYPE_1 !=2'
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_attribute_01=>'N'
,p_attribute_02=>'HTML'
,p_attribute_03=>'N'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(65964128192375090)
,p_name=>'Songs included in this agreement'
,p_region_name=>'songs_inc'
,p_parent_plug_id=>wwv_flow_api.id(65963358214375089)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>170
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--hiddenOverflow'
,p_component_template_options=>'#DEFAULT#'
,p_region_attributes=>'style="float:right ; width:645px" '
,p_new_grid_row=>false
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT /*DISTINCT APEX_ITEM.CHECKBOX2(1,pgp.group_prop_id,''onSubmit="doInsert(this);" '',''UNCHECKED'',null,''f01_#ROWNUM#'') "Select Row"',
'      ,APEX_ITEM.DISPLAY_AND_SAVE(3,pp.title)||APEX_ITEM.HIDDEN(2,pp.property_id) property_title',
'      ,APEX_ITEM.DISPLAY_AND_SAVE(4,property_number)||APEX_ITEM.HIDDEN(5,pgp.group_prop_id) property_number',
'      ,APEX_ITEM.DISPLAY_AND_SAVE(7,pp.description)||APEX_ITEM.HIDDEN(6,pg.group_id) description*/',
'	   APEX_ITEM.CHECKBOX(02,pgp.group_prop_id) "Select Row"',
'      ,pp.title property_title',
'      ,property_number',
'      ,pp.description description',
'      ,pp.created_by',
'      ,pp.creation_date',
'      ,pp.last_updated_by',
'      ,pp.last_update_date',
'      ,pp.property_id',
'FROM   aff_membership_agreements pca',
'      ,prop_properties pp',
'      ,prop_groups pg',
'      ,prop_group_props pgp',
'/* SPI-2452 Data is incosistently not populated in pca.group_id column,',
'so removed the conition)',
'where  pca.group_id = pg.group_id',
'*/',
'WHERE  pca.membership_agreement_id = pg.membership_agreement_id',
'AND    pg.group_id = pgp.group_id',
'AND    NVL(pg.group_type_id, 1) = 1 -- CATALOG',
'AND    pgp.property_id = pp.property_id',
'AND    pp.property_type_id = 1',
'and    pp.source_id = 7 -- SESAC',
'and    pp.status = ''A''',
'AND    ( ip_id  = :P7240_INTERESTED_PARTY_1',
'         /*OR',
'         ip_id      = :P7240_INTERESTED_PARTY*/',
'       )',
'--AND    pca.membership_agreement_id = NVL(:P7240_MEMBERSHIP_AGR_ID_1,pca.membership_agreement_id)',
'AND    pca.membership_agreement_id = :P7240_MEMBERSHIP_AGR_ID_1',
'/*AND ( (instr(upper("TITLE"),upper(NVL(trim(:P7240_TITLE),''ZZZZ''))) > 0',
'OR trim(:P7240_TITLE) IS NULL) )*/',
'AND   ( UPPER(pp.title) LIKE ''%''||UPPER(:P7240_TITLE)||''%''',
'        OR',
'        property_number LIKE ''%''||:P7240_TITLE||''%''',
'        OR',
'        :P7240_TITLE IS NULL',
'      )'))
,p_display_when_condition=>'P7240_MEMBERSHIP_AGR_ID_1'
,p_display_condition_type=>'ITEM_IS_NOT_NULL'
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>50
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>' - '
,p_query_no_data_found=>'no data found'
,p_query_num_rows_type=>'ROW_RANGES_IN_SELECT_LIST'
,p_query_row_count_max=>50000
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_csv_output=>'Y'
,p_csv_output_link_text=>'Excel'
,p_prn_output=>'N'
,p_sort_null=>'L'
,p_query_asc_image=>'apex/builder/dup.gif'
,p_query_asc_image_attr=>'width="16" height="16" alt="" '
,p_query_desc_image=>'apex/builder/ddown.gif'
,p_query_desc_image_attr=>'width="16" height="16" alt="" '
,p_plug_query_strip_html=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65964503045375090)
,p_query_column_id=>1
,p_column_alias=>'Select Row'
,p_column_display_sequence=>1
,p_column_heading=>'<input type="checkbox" value="" name="CHKALL" id="selectunselectall";" >'
,p_use_as_row_header=>'N'
,p_report_column_required_role=>wwv_flow_api.id(17200066639506016)
,p_display_as=>'WITHOUT_MODIFICATION'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65964978417375091)
,p_query_column_id=>2
,p_column_alias=>'PROPERTY_TITLE'
,p_column_display_sequence=>3
,p_column_heading=>'Property Title'
,p_column_link=>'f?p=&APP_ID.:310:&SESSION.::&DEBUG.:RP:P310_PROP_ID,P310_CALLING_PAGE_ID:#PROPERTY_ID#,&APP_PAGE_ID.'
,p_column_linktext=>'#PROPERTY_TITLE#'
,p_default_sort_column_sequence=>1
,p_disable_sort_column=>'N'
,p_display_as=>'WITHOUT_MODIFICATION'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65965334277375091)
,p_query_column_id=>3
,p_column_alias=>'PROPERTY_NUMBER'
,p_column_display_sequence=>2
,p_column_heading=>'Property Number'
,p_use_as_row_header=>'N'
,p_default_sort_column_sequence=>2
,p_disable_sort_column=>'N'
,p_display_as=>'WITHOUT_MODIFICATION'
,p_lov_show_nulls=>'NO'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65965770475375091)
,p_query_column_id=>4
,p_column_alias=>'DESCRIPTION'
,p_column_display_sequence=>4
,p_column_heading=>'Description'
,p_use_as_row_header=>'N'
,p_hidden_column=>'Y'
,p_display_as=>'WITHOUT_MODIFICATION'
,p_lov_show_nulls=>'NO'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(19410666790337146)
,p_query_column_id=>5
,p_column_alias=>'CREATED_BY'
,p_column_display_sequence=>5
,p_column_heading=>'Created By'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(19410752972337147)
,p_query_column_id=>6
,p_column_alias=>'CREATION_DATE'
,p_column_display_sequence=>6
,p_column_heading=>'Creation Date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(19410873898337148)
,p_query_column_id=>7
,p_column_alias=>'LAST_UPDATED_BY'
,p_column_display_sequence=>7
,p_column_heading=>'Last Updated By'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(19410962032337149)
,p_query_column_id=>8
,p_column_alias=>'LAST_UPDATE_DATE'
,p_column_display_sequence=>8
,p_column_heading=>'Last Update Date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(19411077208337150)
,p_query_column_id=>9
,p_column_alias=>'PROPERTY_ID'
,p_column_display_sequence=>9
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(65968106790375093)
,p_name=>'Songs for this party not in this agreement'
,p_region_name=>'songs_exc'
,p_parent_plug_id=>wwv_flow_api.id(65963358214375089)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>160
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#'
,p_region_attributes=>'style="float:left ; width:645px" '
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT APEX_ITEM.CHECKBOX (01, property_id) "Select Row",',
'  property_id,',
'  title property_title,',
'  property_number,',
'  CASE',
'    WHEN ',
'      (SELECT COUNT (*)',
'        FROM GET_SONGS_ENGAGE_MUL_MEM_AGMT T1',
'        WHERE T1.property_id         = pp1.property_id',
'        AND IP_ID                    = :P7240_INTERESTED_PARTY_1',
'        AND MEMBERSHIP_AGREEMENT_ID <> :P7240_MEMBERSHIP_AGR_ID_1) >=1',
'            THEN ''Y''',
'            ELSE ''N''',
'  END indicator_flag ,',
'  pp1.created_by ,',
'  pp1.creation_date ,',
'  pp1.last_updated_by ,',
'  pp1.last_update_date',
'FROM prop_properties pp1',
'WHERE property_type_id = 1',
'AND source_id          = 7',
'AND status             = ''A''',
'AND ( UPPER (title) LIKE ''%'' || UPPER (:P7240_PROPERTY) || ''%'' OR property_number LIKE ''%'' || :P7240_PROPERTY || ''%'' OR :P7240_PROPERTY  IS NULL)',
'AND pp1.property_id IN (SELECT property_id',
'                        FROM prop_share_collectors psc',
'    /*replcaed by Saurav on 22-JAN-2018*/',
'                         WHERE ( psc.collector_id = :P7240_INTERESTED_PARTY_1',
'                        AND SYSDATE BETWEEN NVL(start_date_active,sysdate) AND NVL(end_date_active, SYSDATE)',
'                        AND  psc.Right_type_id            = :P7240_RIGHT_TYPE_1  )',
'  ',
'  UNION ALL',
'  ',
'  SELECT pgp.property_id',
'  FROM aff_membership_agreements ama,',
'    prop_group_props pgp, prop_groups pg',
'  WHERE ama.ip_id                  = :P7240_INTERESTED_PARTY_1',
'  AND ama.membership_agreement_id  = pg.membership_agreement_id',
'  AND pg.group_id                  = pgp.group_id',
'  AND ama.membership_agreement_id != :P7240_MEMBERSHIP_AGR_ID_1',
'  AND ama.Right_type_id            = :P7240_RIGHT_TYPE_1  ',
'  )',
'AND property_id NOT IN',
'                      (SELECT pp.property_id',
'                      FROM aff_membership_agreements pca,',
'                        prop_properties pp,',
'                        prop_groups pg,',
'                        prop_group_props pgp',
'                      WHERE pca.membership_agreement_id  = pg.membership_agreement_id',
'                      AND pg.GROUP_ID              = pgp.GROUP_ID',
'                      AND pgp.property_id          = pp.property_id',
'                      AND pp.property_type_id      = 1',
'                      AND NVL(pg.group_type_id, 1) = 1 -- CATALOG',
'                      AND (ip_id                   = :P7240_INTERESTED_PARTY_1',
'                  /*OR',
'    ip_id      = :P7240_INTERESTED_PARTY*/',
'    )',
'    --AND    pca.membership_agreement_id = NVL(:P7240_MEMBERSHIP_AGR_ID_1,pca.membership_agreement_id)',
'                      AND pca.membership_agreement_id = :P7240_MEMBERSHIP_AGR_ID_1',
'  )'))
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>50
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>' - '
,p_query_no_data_found=>'no data found'
,p_query_num_rows_type=>'ROW_RANGES_IN_SELECT_LIST'
,p_query_row_count_max=>50000
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_csv_output=>'Y'
,p_csv_output_link_text=>'Excel'
,p_prn_output=>'N'
,p_sort_null=>'L'
,p_query_asc_image=>'apex/builder/dup.gif'
,p_query_asc_image_attr=>'width="16" height="16" alt="" '
,p_query_desc_image=>'apex/builder/ddown.gif'
,p_query_desc_image_attr=>'width="16" height="16" alt="" '
,p_plug_query_strip_html=>'Y'
,p_comment=>wwv_flow_string.join(wwv_flow_t_varchar2(
'<script type="text/javascript">',
'function doInsert(pThis) {',
'  if (pThis.checked =  true) {',
'    apex.submit(''INSERT''); //INSERT is the Request, change as required',
'  }',
'}',
'',
' </script>'))
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65968595816375093)
,p_query_column_id=>1
,p_column_alias=>'Select Row'
,p_column_display_sequence=>1
,p_column_heading=>'<input type="checkbox" value="" name="CHKALL" id="selectunselectall";" >'
,p_use_as_row_header=>'N'
,p_report_column_required_role=>wwv_flow_api.id(17200066639506016)
,p_display_as=>'WITHOUT_MODIFICATION'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65968927354375094)
,p_query_column_id=>2
,p_column_alias=>'PROPERTY_ID'
,p_column_display_sequence=>4
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65969325761375094)
,p_query_column_id=>3
,p_column_alias=>'PROPERTY_TITLE'
,p_column_display_sequence=>3
,p_column_heading=>'Property Title'
,p_column_link=>'f?p=&APP_ID.:310:&SESSION.::&DEBUG.:RP:P310_PROP_ID,P310_CALLING_PAGE_ID:#PROPERTY_ID#,&APP_PAGE_ID.'
,p_column_linktext=>'#PROPERTY_TITLE#'
,p_default_sort_column_sequence=>1
,p_disable_sort_column=>'N'
,p_display_as=>'WITHOUT_MODIFICATION'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65969745700375095)
,p_query_column_id=>4
,p_column_alias=>'PROPERTY_NUMBER'
,p_column_display_sequence=>2
,p_column_heading=>'Property Number'
,p_use_as_row_header=>'N'
,p_default_sort_column_sequence=>2
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65970184402375095)
,p_query_column_id=>5
,p_column_alias=>'INDICATOR_FLAG'
,p_column_display_sequence=>5
,p_column_heading=>'Exists In Another Agreement'
,p_column_alignment=>'CENTER'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(19410249025337142)
,p_query_column_id=>6
,p_column_alias=>'CREATED_BY'
,p_column_display_sequence=>6
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(19410317498337143)
,p_query_column_id=>7
,p_column_alias=>'CREATION_DATE'
,p_column_display_sequence=>7
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(19410457430337144)
,p_query_column_id=>8
,p_column_alias=>'LAST_UPDATED_BY'
,p_column_display_sequence=>8
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(19410590615337145)
,p_query_column_id=>9
,p_column_alias=>'LAST_UPDATE_DATE'
,p_column_display_sequence=>9
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(65972539728375097)
,p_plug_name=>'Membership Agreements'
,p_region_template_options=>'#DEFAULT#:t-Region--noPadding:t-Region--scrollBody:t-Form--slimPadding'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>10
,p_plug_display_point=>'BODY'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_plug_display_condition_type=>'ITEM_IS_NULL'
,p_plug_display_when_condition=>'P7240_MEMBERSHIP_AGR_ID'
,p_attribute_01=>'N'
,p_attribute_02=>'HTML'
,p_attribute_03=>'N'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(65983375999375105)
,p_name=>'IP Agreements'
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>40
,p_region_template_options=>'#DEFAULT#:t-Region--hiddenOverflow'
,p_component_template_options=>'#DEFAULT#'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT membership_agreement_id,',
'  valid_from_date ,',
'  valid_to_date ,',
'  effective_date ,',
'  signature_date ,',
'  membership_share ,',
'  ammendment_date ,',
'  ip_id ,',
'  (SELECT SOCIETY_NAME||'' - ''||SOCIETY_COUNTRY ',
'FROM AFF_SOCIETIES',
'WHERE SOCIETY_ID = ama.society_id) Society  ,',
'  (SELECT creation_class_code',
'  ||'' - ''',
'  ||creation_class_desc',
'FROM AFF_CREATION_CLASSES',
'WHERE creation_class_id = ama.creation_class_id ) creation_class ,',
' (SELECT role_desc ',
'FROM aff_roles ',
'WHERE role_id = ama.role_id)Role ,',
' (SELECT right_description ',
'FROM aff_right_types ',
'WHERE right_type_id = ama.right_type_id) right_type ,',
'  renew_allowed_flag ,',
'  renewal_term_months ,',
'  status ,',
'  created_by ,',
'  creation_date ,',
'  last_updated_by ,',
'  last_update_date ,',
'  alert_date ,',
'  domestic_pay_thru_date ,',
'  group_id',
'FROM aff_membership_agreements ama',
'WHERE ip_id = :P7240_INTERESTED_PARTY'))
,p_display_when_condition=>'P7240_MEMBERSHIP_AGR_ID'
,p_display_condition_type=>'ITEM_IS_NULL'
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>' - '
,p_query_break_cols=>'0'
,p_query_no_data_found=>'no data found'
,p_query_num_rows_type=>'NEXT_PREVIOUS_LINKS'
,p_query_row_count_max=>500
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_break_type_flag=>'DEFAULT_BREAK_FORMATTING'
,p_csv_output=>'N'
,p_query_asc_image=>'apex/builder/dup.gif'
,p_query_asc_image_attr=>'width="16" height="16" alt="" '
,p_query_desc_image=>'apex/builder/ddown.gif'
,p_query_desc_image_attr=>'width="16" height="16" alt="" '
,p_plug_query_strip_html=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65983763073375105)
,p_query_column_id=>1
,p_column_alias=>'MEMBERSHIP_AGREEMENT_ID'
,p_column_display_sequence=>1
,p_column_heading=>'Membership Agreement ID'
,p_use_as_row_header=>'N'
,p_column_link=>'f?p=&APP_ID.:7240:&SESSION.::&DEBUG.:RP,7240:P7240_MEMBERSHIP_AGR_ID:#MEMBERSHIP_AGREEMENT_ID#'
,p_column_linktext=>'#MEMBERSHIP_AGREEMENT_ID#'
,p_heading_alignment=>'LEFT'
,p_display_as=>'WITHOUT_MODIFICATION'
,p_lov_show_nulls=>'NO'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65984126225375105)
,p_query_column_id=>2
,p_column_alias=>'VALID_FROM_DATE'
,p_column_display_sequence=>2
,p_column_heading=>'Valid From Date'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65984530660375106)
,p_query_column_id=>3
,p_column_alias=>'VALID_TO_DATE'
,p_column_display_sequence=>3
,p_column_heading=>'Valid To Date'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65984915534375106)
,p_query_column_id=>4
,p_column_alias=>'EFFECTIVE_DATE'
,p_column_display_sequence=>4
,p_column_heading=>'Effective Date'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65985315927375107)
,p_query_column_id=>5
,p_column_alias=>'SIGNATURE_DATE'
,p_column_display_sequence=>5
,p_column_heading=>'Signature Date'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65985735068375107)
,p_query_column_id=>6
,p_column_alias=>'MEMBERSHIP_SHARE'
,p_column_display_sequence=>6
,p_column_heading=>'Membership Share'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65986148047375107)
,p_query_column_id=>7
,p_column_alias=>'AMMENDMENT_DATE'
,p_column_display_sequence=>12
,p_column_heading=>'Ammendment Date'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65986549950375108)
,p_query_column_id=>8
,p_column_alias=>'IP_ID'
,p_column_display_sequence=>7
,p_column_heading=>'Ip Id'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65986941141375108)
,p_query_column_id=>9
,p_column_alias=>'SOCIETY'
,p_column_display_sequence=>8
,p_column_heading=>'Society'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65987302113375108)
,p_query_column_id=>10
,p_column_alias=>'CREATION_CLASS'
,p_column_display_sequence=>11
,p_column_heading=>'Creation Class'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65987761979375108)
,p_query_column_id=>11
,p_column_alias=>'ROLE'
,p_column_display_sequence=>10
,p_column_heading=>'Role'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65988104964375109)
,p_query_column_id=>12
,p_column_alias=>'RIGHT_TYPE'
,p_column_display_sequence=>9
,p_column_heading=>'Right Type'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65988507187375109)
,p_query_column_id=>13
,p_column_alias=>'RENEW_ALLOWED_FLAG'
,p_column_display_sequence=>13
,p_column_heading=>'Renew Allowed Flag'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65988902682375109)
,p_query_column_id=>14
,p_column_alias=>'RENEWAL_TERM_MONTHS'
,p_column_display_sequence=>14
,p_column_heading=>'Renewal Term Months'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65989346609375110)
,p_query_column_id=>15
,p_column_alias=>'STATUS'
,p_column_display_sequence=>15
,p_column_heading=>'Status'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65989778885375110)
,p_query_column_id=>16
,p_column_alias=>'CREATED_BY'
,p_column_display_sequence=>16
,p_column_heading=>'Created By'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65990190689375110)
,p_query_column_id=>17
,p_column_alias=>'CREATION_DATE'
,p_column_display_sequence=>17
,p_column_heading=>'Creation Date'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65990526696375111)
,p_query_column_id=>18
,p_column_alias=>'LAST_UPDATED_BY'
,p_column_display_sequence=>18
,p_column_heading=>'Last Updated By'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65990996973375111)
,p_query_column_id=>19
,p_column_alias=>'LAST_UPDATE_DATE'
,p_column_display_sequence=>19
,p_column_heading=>'Last Update Date'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65991335923375111)
,p_query_column_id=>20
,p_column_alias=>'ALERT_DATE'
,p_column_display_sequence=>20
,p_column_heading=>'Alert Date'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65991762990375111)
,p_query_column_id=>21
,p_column_alias=>'DOMESTIC_PAY_THRU_DATE'
,p_column_display_sequence=>22
,p_column_heading=>'Domestic Pay Thru Date'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(65992171032375112)
,p_query_column_id=>22
,p_column_alias=>'GROUP_ID'
,p_column_display_sequence=>21
,p_column_heading=>'Group Id'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(66395358722501770)
,p_plug_name=>'Breadcrumb'
,p_region_template_options=>'#DEFAULT#:t-BreadcrumbRegion--useBreadcrumbTitle'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51852637782240827)
,p_plug_display_sequence=>1
,p_plug_display_point=>'REGION_POSITION_01'
,p_menu_id=>wwv_flow_api.id(51876846391240858)
,p_plug_source_type=>'NATIVE_BREADCRUMB'
,p_menu_template_id=>wwv_flow_api.id(51870806884240842)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(65958121902375086)
,p_button_sequence=>10
,p_button_plug_id=>wwv_flow_api.id(65953765736375082)
,p_button_name=>'ADD'
,p_button_action=>'REDIRECT_URL'
,p_button_template_options=>'#DEFAULT#:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Add Row'
,p_button_position=>'BELOW_BOX'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'javascript:apex.widget.tabular.addRow();'
,p_button_execute_validations=>'N'
,p_icon_css_classes=>'fa-plus'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(17200066639506016)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(65973731767375098)
,p_button_sequence=>10
,p_button_plug_id=>wwv_flow_api.id(65972539728375097)
,p_button_name=>'CANCEL2'
,p_button_action=>'SUBMIT'
,p_button_template_options=>'#DEFAULT#:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Cancel'
,p_button_position=>'BELOW_BOX'
,p_button_alignment=>'LEFT'
,p_icon_css_classes=>'fa-rotate-left'
,p_grid_new_grid=>false
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(65959726263375087)
,p_button_sequence=>20
,p_button_plug_id=>wwv_flow_api.id(65953765736375082)
,p_button_name=>'APPLY_CHANGES_Terr'
,p_button_action=>'SUBMIT'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Apply Changes'
,p_button_position=>'BELOW_BOX'
,p_button_alignment=>'LEFT'
,p_icon_css_classes=>'fa-save'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(17200066639506016)
);
end;
/
begin
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(65972920984375098)
,p_button_sequence=>20
,p_button_plug_id=>wwv_flow_api.id(65972539728375097)
,p_button_name=>'RESET'
,p_button_action=>'SUBMIT'
,p_button_template_options=>'#DEFAULT#:t-Button--iconRight'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Reset'
,p_button_position=>'BELOW_BOX'
,p_button_alignment=>'LEFT'
,p_button_condition_type=>'NEVER'
,p_icon_css_classes=>'fa-eraser'
,p_grid_new_grid=>false
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(65973346971375098)
,p_button_sequence=>30
,p_button_plug_id=>wwv_flow_api.id(65972539728375097)
,p_button_name=>'Create'
,p_button_action=>'SUBMIT'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft:t-Button--gapLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Create'
,p_button_position=>'BELOW_BOX'
,p_button_alignment=>'LEFT'
,p_icon_css_classes=>'fa-plus'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(17200066639506016)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(65959380700375087)
,p_button_sequence=>40
,p_button_plug_id=>wwv_flow_api.id(65953765736375082)
,p_button_name=>'MULTI_ROW_DELETE'
,p_button_action=>'REDIRECT_URL'
,p_button_template_options=>'#DEFAULT#:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_is_hot=>'Y'
,p_button_image_alt=>'Delete'
,p_button_position=>'BELOW_BOX'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'javascript:apex.confirm(htmldb_delete_message,''MULTI_ROW_DELETE'');'
,p_button_execute_validations=>'N'
,p_icon_css_classes=>'fa-remove'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(17200066639506016)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(65944142406375075)
,p_button_sequence=>10
,p_button_plug_id=>wwv_flow_api.id(65942970664375075)
,p_button_name=>'CANCEL1'
,p_button_action=>'SUBMIT'
,p_button_template_options=>'#DEFAULT#:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Cancel'
,p_button_position=>'BOTTOM'
,p_button_alignment=>'LEFT'
,p_icon_css_classes=>'fa-rotate-left'
,p_grid_new_grid=>false
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(65944917012375076)
,p_button_sequence=>20
,p_button_plug_id=>wwv_flow_api.id(65942970664375075)
,p_button_name=>'DELETE'
,p_button_action=>'REDIRECT_URL'
,p_button_template_options=>'#DEFAULT#:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Delete'
,p_button_position=>'BOTTOM'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'javascript:apex.confirm(htmldb_delete_message,''DELETE'');'
,p_button_execute_validations=>'N'
,p_button_condition=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'',
'  v_songs_count NUMBER;',
'',
'BEGIN',
'',
'  SELECT COUNT(1)',
'  INTO   v_songs_count',
'    from prop_collector_agreements pca,',
'    prop_groups pg,',
'    prop_group_props pgp',
'    where pca.COLLECTOR_AGREEMENT_ID=pg.COLLECTOR_AGREEMENT_ID',
'    and pg.group_id=pgp.group_id',
'    and pca.collects_on_agreement_id in ',
'          (select collector_agreement_id from prop_collector_agreements where collector_id=:P7240_INTERESTED_PARTY_1)',
'    and pca.right_type_id=:P7240_RIGHT_TYPE_1;',
'',
'  IF v_songs_count > 0 or  :P7240_MEMBERSHIP_AGR_ID_1 is null ',
'  THEN',
'    RETURN FALSE;',
'  ELSE',
'    RETURN TRUE;',
'  END IF;',
'END;'))
,p_button_condition_type=>'FUNCTION_BODY'
,p_icon_css_classes=>'fa-close'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(17200066639506016)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(65943395053375075)
,p_button_sequence=>40
,p_button_plug_id=>wwv_flow_api.id(65942970664375075)
,p_button_name=>'PAYABLE_CATEGORY'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Payable Category'
,p_button_position=>'BOTTOM'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:7265:&SESSION.::&DEBUG.:7265:P7265_MEM_AGR_ID:&P7240_MEMBERSHIP_AGR_ID_1.'
,p_icon_css_classes=>'fa-money'
,p_grid_new_grid=>false
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(65943704647375075)
,p_button_sequence=>50
,p_button_plug_id=>wwv_flow_api.id(65942970664375075)
,p_button_name=>'APPLY_CHANGES_UPD'
,p_button_action=>'SUBMIT'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Apply Changes'
,p_button_position=>'BOTTOM'
,p_button_alignment=>'LEFT'
,p_icon_css_classes=>'fa-save'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(17200066639506016)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(65958550538375086)
,p_button_sequence=>50
,p_button_plug_id=>wwv_flow_api.id(65953765736375082)
,p_button_name=>'SUBMIT'
,p_button_action=>'SUBMIT'
,p_button_template_options=>'#DEFAULT#'
,p_button_template_id=>wwv_flow_api.id(51870385911240841)
,p_button_image_alt=>'Apply Changes'
,p_button_position=>'REGION_TEMPLATE_CHANGE'
,p_button_condition_type=>'NEVER'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(17200066639506016)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(65958966665375086)
,p_button_sequence=>30
,p_button_plug_id=>wwv_flow_api.id(65953765736375082)
,p_button_name=>'CANCEL'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#'
,p_button_template_id=>wwv_flow_api.id(51870385911240841)
,p_button_image_alt=>'Cancel'
,p_button_position=>'REGION_TEMPLATE_CLOSE'
,p_button_redirect_url=>'f?p=&APP_ID.:1:&SESSION.::&DEBUG.:::'
,p_button_condition_type=>'NEVER'
,p_grid_new_grid=>false
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(65970507677375096)
,p_button_sequence=>50
,p_button_plug_id=>wwv_flow_api.id(65968106790375093)
,p_button_name=>'SEARCH'
,p_button_static_id=>'SEARCH'
,p_button_action=>'SUBMIT'
,p_button_template_options=>'#DEFAULT#:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Search'
,p_button_position=>'TOP'
,p_button_alignment=>'LEFT'
,p_icon_css_classes=>'fa-filter'
,p_grid_new_grid=>false
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(65970931821375096)
,p_button_sequence=>60
,p_button_plug_id=>wwv_flow_api.id(65968106790375093)
,p_button_name=>'ADD_SONG'
,p_button_action=>'SUBMIT'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Add Song'
,p_button_position=>'TOP'
,p_button_alignment=>'LEFT'
,p_icon_css_classes=>'fa-plus'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(17200066639506016)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(65971345747375096)
,p_button_sequence=>70
,p_button_plug_id=>wwv_flow_api.id(65968106790375093)
,p_button_name=>'ADD_ALL_SONGS'
,p_button_action=>'SUBMIT'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Add All Songs'
,p_button_position=>'TOP'
,p_button_alignment=>'LEFT'
,p_icon_css_classes=>'fa-plus'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(17200066639506016)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(65971716942375097)
,p_button_sequence=>150
,p_button_plug_id=>wwv_flow_api.id(65968106790375093)
,p_button_name=>'RESET_TITLE'
,p_button_action=>'SUBMIT'
,p_button_template_options=>'#DEFAULT#:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Reset'
,p_button_position=>'TOP'
,p_button_alignment=>'LEFT'
,p_icon_css_classes=>'fa-eraser'
,p_grid_new_grid=>false
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(65966129459375091)
,p_button_sequence=>180
,p_button_plug_id=>wwv_flow_api.id(65964128192375090)
,p_button_name=>'SEARCH'
,p_button_static_id=>'SEARCH'
,p_button_action=>'SUBMIT'
,p_button_template_options=>'#DEFAULT#:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Search'
,p_button_position=>'TOP'
,p_button_alignment=>'LEFT'
,p_icon_css_classes=>'fa-filter'
,p_grid_new_grid=>false
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(65966534371375092)
,p_button_sequence=>190
,p_button_plug_id=>wwv_flow_api.id(65964128192375090)
,p_button_name=>'DELETE_SONGS'
,p_button_action=>'SUBMIT'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Delete Songs'
,p_button_position=>'TOP'
,p_button_alignment=>'LEFT'
,p_icon_css_classes=>'fa-close'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(17200066639506016)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(65966990917375092)
,p_button_sequence=>200
,p_button_plug_id=>wwv_flow_api.id(65964128192375090)
,p_button_name=>'DELETE_ALL_SONGS'
,p_button_action=>'SUBMIT'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Delete All Songs'
,p_button_position=>'TOP'
,p_button_alignment=>'LEFT'
,p_icon_css_classes=>'fa-close'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(17200066639506016)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(65967333858375092)
,p_button_sequence=>240
,p_button_plug_id=>wwv_flow_api.id(65964128192375090)
,p_button_name=>'RESET_Property'
,p_button_action=>'SUBMIT'
,p_button_template_options=>'#DEFAULT#:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Reset'
,p_button_position=>'TOP'
,p_button_alignment=>'LEFT'
,p_icon_css_classes=>'fa-eraser'
,p_grid_new_grid=>false
);
wwv_flow_api.create_page_branch(
 p_id=>wwv_flow_api.id(66001270946375120)
,p_branch_name=>'Go To Page 7240'
,p_branch_action=>'f?p=&APP_ID.:7240:&SESSION.&success_msg=#SUCCESS_MSG#'
,p_branch_point=>'AFTER_PROCESSING'
,p_branch_type=>'REDIRECT_URL'
,p_branch_sequence=>1
,p_branch_condition_type=>'NEVER'
);
wwv_flow_api.create_page_branch(
 p_id=>wwv_flow_api.id(66001660519375120)
,p_branch_name=>'Go To Page 7240'
,p_branch_action=>'f?p=&APP_ID.:7240:&SESSION.&success_msg=#SUCCESS_MSG#'
,p_branch_point=>'AFTER_PROCESSING'
,p_branch_type=>'REDIRECT_URL'
,p_branch_sequence=>1
,p_branch_condition_type=>'NEVER'
);
wwv_flow_api.create_page_branch(
 p_id=>wwv_flow_api.id(66002404221375121)
,p_branch_name=>'Go To Page 7240'
,p_branch_action=>'f?p=&APP_ID.:7240:&SESSION.&success_msg=#SUCCESS_MSG#'
,p_branch_point=>'AFTER_PROCESSING'
,p_branch_type=>'REDIRECT_URL'
,p_branch_sequence=>1
,p_branch_condition_type=>'NEVER'
);
wwv_flow_api.create_page_branch(
 p_id=>wwv_flow_api.id(66002066682375120)
,p_branch_name=>'Go To Page &P7240_CALLING_PAGE_ID1.'
,p_branch_action=>'f?p=&APP_ID.:&P7240_CALLING_PAGE_ID1.:&SESSION.::&DEBUG.:::'
,p_branch_point=>'AFTER_PROCESSING'
,p_branch_type=>'REDIRECT_URL'
,p_branch_when_button_id=>wwv_flow_api.id(4575830568943055)
,p_branch_sequence=>21
,p_branch_condition_type=>'NEVER'
);
wwv_flow_api.create_page_branch(
 p_id=>wwv_flow_api.id(23242302534825325)
,p_branch_name=>'Go To Page 7240'
,p_branch_action=>'f?p=&APP_ID.:7240:&SESSION.:SEARCH1:&DEBUG.:RP::&success_msg=#SUCCESS_MSG#'
,p_branch_point=>'AFTER_PROCESSING'
,p_branch_type=>'REDIRECT_URL'
,p_branch_when_button_id=>wwv_flow_api.id(65966129459375091)
,p_branch_sequence=>31
);
wwv_flow_api.create_page_branch(
 p_id=>wwv_flow_api.id(23242425265825326)
,p_branch_name=>'Go To Page 7240'
,p_branch_action=>'f?p=&APP_ID.:7240:&SESSION.:SEARCH2:&DEBUG.:RP::&success_msg=#SUCCESS_MSG#'
,p_branch_point=>'AFTER_PROCESSING'
,p_branch_type=>'REDIRECT_URL'
,p_branch_when_button_id=>wwv_flow_api.id(65970507677375096)
,p_branch_sequence=>41
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(18984949607748850)
,p_name=>'P7240_IPI_START_DATE_1'
,p_item_sequence=>140
,p_item_plug_id=>wwv_flow_api.id(65942970664375075)
,p_prompt=>'IPI Start Date'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_colspan=>4
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(19388012046587501)
,p_name=>'P7240_IPI_END_DATE_1'
,p_item_sequence=>150
,p_item_plug_id=>wwv_flow_api.id(65942970664375075)
,p_prompt=>'IPI End Date'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(19388529704587506)
,p_name=>'P7240_IPI_START_DATE'
,p_item_sequence=>120
,p_item_plug_id=>wwv_flow_api.id(65972539728375097)
,p_prompt=>'IPI Start Date'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_colspan=>5
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(19388611236587507)
,p_name=>'P7240_IPI_END_DATE'
,p_item_sequence=>130
,p_item_plug_id=>wwv_flow_api.id(65972539728375097)
,p_prompt=>'IPI End Date'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_begin_on_new_line=>'N'
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(22293245141322425)
,p_name=>'P7240_IP_DISPALY_1'
,p_item_sequence=>50
,p_item_plug_id=>wwv_flow_api.id(65942970664375075)
,p_prompt=>'Interested Party'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT   p.ip_number || ''|'' || P.hfa_number || ''|'' || N.full_name AS REC',
'FROM   aff_ip_names N,',
'       aff_interested_parties P',
'WHERE  P.ip_id              =  :P7240_INTERESTED_PARTY_1 ',
' AND  P.status_code_id      = 120',
' AND P.ip_id               = N.ip_id',
' AND N.name_type_id        = 6',
' --AND N.full_name           NOT LIKE ''*%''',
' AND P.ip_number           IS NOT NULL;'))
,p_source_type=>'QUERY'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_colspan=>4
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'Y'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65945394295375076)
,p_name=>'P7240_INTERESTED_PARTY_1'
,p_item_sequence=>10
,p_item_plug_id=>wwv_flow_api.id(65942970664375075)
,p_display_as=>'NATIVE_HIDDEN'
,p_help_text=>'Pro Tip: Enter search string and press Tab to get the filtered results'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65945767502375077)
,p_name=>'P7240_SOCIETY_1'
,p_item_sequence=>60
,p_item_plug_id=>wwv_flow_api.id(65942970664375075)
,p_prompt=>'Society'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_named_lov=>'SOCIETY'
,p_lov=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select society_long_name d, society_id r',
'from   aff_societies_v',
'order by 1'))
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
 p_id=>wwv_flow_api.id(65946153149375077)
,p_name=>'P7240_MEMBERSHIP_AGR_ID_1'
,p_item_sequence=>70
,p_item_plug_id=>wwv_flow_api.id(65942970664375075)
,p_prompt=>'Membership Agreement ID'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65946560108375077)
,p_name=>'P7240_AGREEMENT_TYPE_1'
,p_item_sequence=>80
,p_item_plug_id=>wwv_flow_api.id(65942970664375075)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65946975447375077)
,p_name=>'P7240_TERRITORIES_1'
,p_item_sequence=>90
,p_item_plug_id=>wwv_flow_api.id(65942970664375075)
,p_prompt=>'Territories'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_named_lov=>'AFF_TERRITORIES'
,p_lov=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select TISAN d, territory_id r',
'from   aff_territories ',
'order by 1 desc'))
,p_lov_display_null=>'YES'
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
 p_id=>wwv_flow_api.id(65947371055375078)
,p_name=>'P7240_RIGHT_TYPE_1'
,p_item_sequence=>100
,p_item_plug_id=>wwv_flow_api.id(65942970664375075)
,p_prompt=>'Right Type'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_lov=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT right_description d',
'      ,right_type_id r',
'FROM   aff_right_types',
'ORDER BY 2'))
,p_lov_display_null=>'YES'
,p_cHeight=>1
,p_colspan=>4
,p_grid_label_column_span=>1
,p_read_only_when_type=>'ALWAYS'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'NO'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65947787715375078)
,p_name=>'P7240_ROLE_1'
,p_item_sequence=>120
,p_item_plug_id=>wwv_flow_api.id(65942970664375075)
,p_prompt=>'Role'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_named_lov=>'ROLES'
,p_lov=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select role_desc d, role_id r',
'from   aff_roles',
'order by 1'))
,p_lov_display_null=>'YES'
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
 p_id=>wwv_flow_api.id(65948112925375078)
,p_name=>'P7240_CREATION_CLASS_1'
,p_item_sequence=>130
,p_item_plug_id=>wwv_flow_api.id(65942970664375075)
,p_prompt=>'Creation Class'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_named_lov=>'CREATION_CLASS'
,p_lov=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select creation_class_code||'' - ''||creation_class_desc d, creation_class_id r ',
'from AFF_CREATION_CLASSES',
'order by 1'))
,p_lov_display_null=>'YES'
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
 p_id=>wwv_flow_api.id(65948530828375079)
,p_name=>'P7240_STATUS_1'
,p_item_sequence=>110
,p_item_plug_id=>wwv_flow_api.id(65942970664375075)
,p_prompt=>'Status'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_named_lov=>'PROP_STATUS'
,p_lov=>'.'||wwv_flow_api.id(62613490312062601)||'.'
,p_lov_display_null=>'YES'
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
 p_id=>wwv_flow_api.id(65948958837375079)
,p_name=>'P7240_VALID_FROM_DATE_1'
,p_item_sequence=>160
,p_item_plug_id=>wwv_flow_api.id(65942970664375075)
,p_prompt=>'Valid From Date'
,p_format_mask=>'YYYY-MM-DD'
,p_display_as=>'NATIVE_DATE_PICKER'
,p_cSize=>12
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
 p_id=>wwv_flow_api.id(65949349714375079)
,p_name=>'P7240_VALID_TO_DATE_1'
,p_item_sequence=>170
,p_item_plug_id=>wwv_flow_api.id(65942970664375075)
,p_prompt=>'Valid To Date'
,p_format_mask=>'YYYY-MM-DD'
,p_display_as=>'NATIVE_DATE_PICKER'
,p_cSize=>12
,p_cMaxlength=>4000
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_04=>'button'
,p_attribute_05=>'N'
,p_attribute_07=>'NONE'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65950173203375080)
,p_name=>'P7240_EFFECTIVE_DATE_1'
,p_item_sequence=>190
,p_item_plug_id=>wwv_flow_api.id(65942970664375075)
,p_prompt=>'Effective Date'
,p_format_mask=>'YYYY-MM-DD'
,p_display_as=>'NATIVE_DATE_PICKER'
,p_cSize=>12
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
 p_id=>wwv_flow_api.id(65950508536375080)
,p_name=>'P7240_SIGNATURE_DATE_1'
,p_item_sequence=>200
,p_item_plug_id=>wwv_flow_api.id(65942970664375075)
,p_prompt=>'Signature Date'
,p_format_mask=>'YYYY-MM-DD'
,p_display_as=>'NATIVE_DATE_PICKER'
,p_cSize=>12
,p_cMaxlength=>4000
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_04=>'button'
,p_attribute_05=>'N'
,p_attribute_07=>'NONE'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65950983145375080)
,p_name=>'P7240_AMMENDMENT_DATE_1'
,p_item_sequence=>210
,p_item_plug_id=>wwv_flow_api.id(65942970664375075)
,p_prompt=>'Ammendment Date'
,p_format_mask=>'YYYY-MM-DD'
,p_display_as=>'NATIVE_DATE_PICKER'
,p_cSize=>12
,p_cMaxlength=>4000
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_04=>'button'
,p_attribute_05=>'N'
,p_attribute_07=>'NONE'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65951320933375080)
,p_name=>'P7240_RENEW_ALLOWED_FLAG_1'
,p_item_sequence=>220
,p_item_plug_id=>wwv_flow_api.id(65942970664375075)
,p_prompt=>'Renew Allowed Flag'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_named_lov=>'MEMAGR_RENEWAL_CODES'
,p_lov=>'.'||wwv_flow_api.id(66009685797375127)||'.'
,p_lov_display_null=>'YES'
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
 p_id=>wwv_flow_api.id(65951790897375081)
,p_name=>'P7240_RENEWAL_TERM_MONTHS_1'
,p_item_sequence=>230
,p_item_plug_id=>wwv_flow_api.id(65942970664375075)
,p_prompt=>'Renewal Term Months'
,p_display_as=>'NATIVE_NUMBER_FIELD'
,p_cSize=>10
,p_cMaxlength=>4000
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_03=>'right'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65952109104375081)
,p_name=>'P7240_ALERT_DATE_1'
,p_item_sequence=>240
,p_item_plug_id=>wwv_flow_api.id(65942970664375075)
,p_prompt=>'Alert Date'
,p_format_mask=>'YYYY-MM-DD'
,p_display_as=>'NATIVE_DATE_PICKER'
,p_cSize=>12
,p_cMaxlength=>4000
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_04=>'button'
,p_attribute_05=>'N'
,p_attribute_07=>'NONE'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65952559103375081)
,p_name=>'P7240_MEMBERSHIP_SHARE_1'
,p_item_sequence=>250
,p_item_plug_id=>wwv_flow_api.id(65942970664375075)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65952976347375082)
,p_name=>'P7240_CALLING_PAGE_ID1'
,p_item_sequence=>260
,p_item_plug_id=>wwv_flow_api.id(65942970664375075)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65953351058375082)
,p_name=>'P7240_SPECIAL_HANDLING_1'
,p_item_sequence=>270
,p_item_plug_id=>wwv_flow_api.id(65942970664375075)
,p_prompt=>'Special Handling'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_named_lov=>'AFF_LOOKUPS_SPECIAL_HANDLING'
,p_lov=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT DESCRIPTION D, LOOKUP_CODE R',
'FROM AFF_LOOKUPS',
'WHERE LOOKUP_TYPE = ''SPECIAL_HANDLING''',
'AND ENABLED_FLAG  = ''Y''',
'ORDER BY 1;'))
,p_lov_display_null=>'YES'
,p_lov_null_text=>'-select-'
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
 p_id=>wwv_flow_api.id(65963761331375089)
,p_name=>'P7240_VALIDATION_MESSAGE'
,p_item_sequence=>990
,p_item_plug_id=>wwv_flow_api.id(65963358214375089)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65967762939375092)
,p_name=>'P7240_TITLE'
,p_item_sequence=>980
,p_item_plug_id=>wwv_flow_api.id(65964128192375090)
,p_prompt=>'Title'
,p_display_as=>'NATIVE_TEXT_FIELD'
,p_cSize=>30
,p_cMaxlength=>4000
,p_begin_on_new_line=>'N'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'N'
,p_attribute_04=>'TEXT'
,p_attribute_05=>'NONE'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65972194492375097)
,p_name=>'P7240_PROPERTY'
,p_item_sequence=>740
,p_item_plug_id=>wwv_flow_api.id(65968106790375093)
,p_prompt=>'Title'
,p_display_as=>'NATIVE_TEXT_FIELD'
,p_cSize=>30
,p_cMaxlength=>4000
,p_begin_on_new_line=>'N'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'N'
,p_attribute_04=>'TEXT'
,p_attribute_05=>'NONE'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65974120110375098)
,p_name=>'P7240_INTERESTED_PARTY'
,p_item_sequence=>10
,p_item_plug_id=>wwv_flow_api.id(65972539728375097)
,p_prompt=>'Interested Party'
,p_display_as=>'PLUGIN_COM_SKILLBUILDERS_SUPER_LOV'
,p_lov=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT  TO_NUMBER(ip_number) "IP NUMBER"',
'     ,hfa_number AS "HFA NUMBER"',
'     ,full_name AS "FULL NAME"',
'     ,song_count AS "SONG COUNT"',
'     ,ip_description AS Type  ',
'     ,ip_id',
'     ,ip_number || '' | '' || hfa_number || '' | '' || full_name  as RECORD',
'FROM SPI_SONG_COUNT_V sc, aff_ip_types at',
'WHERE sc.ip_type_id = at.ip_type_id',
'AND at.ebs_type = ''V''',
'AND status_code_id = 120'))
,p_cSize=>25
,p_colspan=>5
,p_grid_label_column_span=>1
,p_read_only_when=>'P7240_INTERESTED_PARTY'
,p_read_only_when_type=>'ITEM_IS_NOT_NULL'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_help_text=>'Pro Tip: Enter search string and press Tab to get the filtered results'
,p_attribute_01=>'N'
,p_attribute_03=>'7,6'
,p_attribute_04=>'1,2,3'
,p_attribute_05=>'6'
,p_attribute_08=>'ENTERABLE_RESTRICTED'
,p_attribute_09=>'15'
,p_attribute_10=>'&nbsp;'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65974579500375099)
,p_name=>'P7240_MEMBERSHIP_AGR_ID'
,p_item_sequence=>30
,p_item_plug_id=>wwv_flow_api.id(65972539728375097)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65974936292375099)
,p_name=>'P7240_SOCIETY'
,p_item_sequence=>40
,p_item_plug_id=>wwv_flow_api.id(65972539728375097)
,p_item_default=>'72'
,p_prompt=>'Current Society'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_named_lov=>'SOCIETY'
,p_lov=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select society_long_name d, society_id r',
'from   aff_societies_v',
'order by 1'))
,p_lov_display_null=>'YES'
,p_cHeight=>1
,p_begin_on_new_line=>'N'
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65975377978375099)
,p_name=>'P7240_AGREEMENT_TYPE'
,p_item_sequence=>50
,p_item_plug_id=>wwv_flow_api.id(65972539728375097)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65975752033375099)
,p_name=>'P7240_PRIOR_SOCIETY'
,p_item_sequence=>60
,p_item_plug_id=>wwv_flow_api.id(65972539728375097)
,p_prompt=>'Prior Society'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_named_lov=>'SOCIETY WITHOUT SESAC'
,p_lov=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select society_long_name d, society_id r',
'from   aff_societies_v',
'WHERE society_name <> ''SESAC''',
'order by 1'))
,p_lov_display_null=>'YES'
,p_lov_cascade_parent_items=>'P7240_INTERESTED_PARTY'
,p_ajax_items_to_submit=>'P7240_PRIOR_SOCIETY'
,p_ajax_optimize_refresh=>'Y'
,p_cHeight=>1
,p_begin_on_new_line=>'N'
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65976127294375100)
,p_name=>'P7240_TERRITORIES'
,p_item_sequence=>70
,p_item_plug_id=>wwv_flow_api.id(65972539728375097)
,p_item_default=>'249'
,p_prompt=>'Territories'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_named_lov=>'TERRITORY'
,p_lov=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select tisan d, territory_id r',
'from   aff_territories_v',
'order by 1',
'',
''))
,p_cHeight=>6
,p_colspan=>5
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51870006101240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'NONE'
,p_attribute_02=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65976532339375100)
,p_name=>'P7240_RIGHT_TYPE'
,p_item_sequence=>80
,p_item_plug_id=>wwv_flow_api.id(65972539728375097)
,p_prompt=>'Right Type'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_lov=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT right_description d',
'      ,right_type_id r',
'FROM   aff_right_types',
'ORDER BY 2'))
,p_cHeight=>1
,p_begin_on_new_line=>'N'
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51870006101240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'NO'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65976948631375100)
,p_name=>'P7240_ROLE'
,p_item_sequence=>90
,p_item_plug_id=>wwv_flow_api.id(65972539728375097)
,p_prompt=>'Role'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_named_lov=>'ROLES'
,p_lov=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select role_desc d, role_id r',
'from   aff_roles',
'order by 1'))
,p_lov_display_null=>'YES'
,p_cHeight=>1
,p_colspan=>5
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51870006101240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65977354362375100)
,p_name=>'P7240_CREATION_CLASS'
,p_item_sequence=>100
,p_item_plug_id=>wwv_flow_api.id(65972539728375097)
,p_item_default=>'15'
,p_prompt=>'Creation Class'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_named_lov=>'CREATION_CLASS'
,p_lov=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select creation_class_code||'' - ''||creation_class_desc d, creation_class_id r ',
'from AFF_CREATION_CLASSES',
'order by 1'))
,p_lov_display_null=>'YES'
,p_cHeight=>1
,p_begin_on_new_line=>'N'
,p_grid_label_column_span=>1
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
 p_id=>wwv_flow_api.id(65977771791375101)
,p_name=>'P7240_STATUS'
,p_item_sequence=>110
,p_item_plug_id=>wwv_flow_api.id(65972539728375097)
,p_item_default=>'A'
,p_prompt=>'Status'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_named_lov=>'PROP_STATUS'
,p_lov=>'.'||wwv_flow_api.id(62613490312062601)||'.'
,p_lov_display_null=>'YES'
,p_cHeight=>1
,p_begin_on_new_line=>'N'
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65978146024375101)
,p_name=>'P7240_VALID_FROM_DATE'
,p_item_sequence=>140
,p_item_plug_id=>wwv_flow_api.id(65972539728375097)
,p_prompt=>'Start Date'
,p_format_mask=>'YYYY-MM-DD'
,p_display_as=>'NATIVE_DATE_PICKER'
,p_cSize=>12
,p_cMaxlength=>4000
,p_colspan=>5
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_04=>'button'
,p_attribute_05=>'N'
,p_attribute_07=>'NONE'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65978513679375101)
,p_name=>'P7240_VALID_TO_DATE'
,p_item_sequence=>150
,p_item_plug_id=>wwv_flow_api.id(65972539728375097)
,p_prompt=>'End Date'
,p_format_mask=>'YYYY-MM-DD'
,p_display_as=>'NATIVE_DATE_PICKER'
,p_cSize=>12
,p_cMaxlength=>4000
,p_begin_on_new_line=>'N'
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_04=>'button'
,p_attribute_05=>'N'
,p_attribute_07=>'NONE'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65978973274375102)
,p_name=>'P7240_DOM_PAY_THRU_DATE'
,p_item_sequence=>160
,p_item_plug_id=>wwv_flow_api.id(65972539728375097)
,p_prompt=>'Domestic Pay Thru Date'
,p_format_mask=>'YYYY-MM-DD'
,p_display_as=>'NATIVE_DATE_PICKER'
,p_cSize=>12
,p_cMaxlength=>400
,p_begin_on_new_line=>'N'
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_04=>'button'
,p_attribute_05=>'N'
,p_attribute_07=>'NONE'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65979325086375102)
,p_name=>'P7240_EFFECTIVE_DATE'
,p_item_sequence=>170
,p_item_plug_id=>wwv_flow_api.id(65972539728375097)
,p_prompt=>'Effective Date'
,p_format_mask=>'YYYY-MM-DD'
,p_display_as=>'NATIVE_DATE_PICKER'
,p_cSize=>12
,p_cMaxlength=>4000
,p_colspan=>5
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_04=>'button'
,p_attribute_05=>'N'
,p_attribute_07=>'NONE'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65979702715375102)
,p_name=>'P7240_SIGNATURE_DATE'
,p_item_sequence=>180
,p_item_plug_id=>wwv_flow_api.id(65972539728375097)
,p_prompt=>'Signature Date'
,p_format_mask=>'YYYY-MM-DD'
,p_display_as=>'NATIVE_DATE_PICKER'
,p_cSize=>12
,p_cMaxlength=>4000
,p_begin_on_new_line=>'N'
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_04=>'button'
,p_attribute_05=>'N'
,p_attribute_07=>'NONE'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65980111186375102)
,p_name=>'P7240_AMMENDMENT_DATE'
,p_item_sequence=>190
,p_item_plug_id=>wwv_flow_api.id(65972539728375097)
,p_prompt=>'Ammendment Date'
,p_format_mask=>'YYYY-MM-DD'
,p_display_as=>'NATIVE_DATE_PICKER'
,p_cSize=>12
,p_cMaxlength=>4000
,p_begin_on_new_line=>'N'
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_04=>'button'
,p_attribute_05=>'N'
,p_attribute_07=>'NONE'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65980540206375103)
,p_name=>'P7240_RENEW_ALLOWED_FLAG'
,p_item_sequence=>200
,p_item_plug_id=>wwv_flow_api.id(65972539728375097)
,p_prompt=>'Renew Allowed Flag'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_named_lov=>'MEMAGR_RENEWAL_CODES'
,p_lov=>'.'||wwv_flow_api.id(66009685797375127)||'.'
,p_lov_display_null=>'YES'
,p_cHeight=>1
,p_colspan=>5
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65980997514375103)
,p_name=>'P7240_RENEWAL_TERM_MONTHS'
,p_item_sequence=>210
,p_item_plug_id=>wwv_flow_api.id(65972539728375097)
,p_prompt=>'Renewal Term Months'
,p_display_as=>'NATIVE_NUMBER_FIELD'
,p_cSize=>10
,p_cMaxlength=>4000
,p_begin_on_new_line=>'N'
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_03=>'right'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65981309395375103)
,p_name=>'P7240_ALERT_DATE'
,p_item_sequence=>220
,p_item_plug_id=>wwv_flow_api.id(65972539728375097)
,p_prompt=>'Alert Date'
,p_format_mask=>'YYYY-MM-DD'
,p_display_as=>'NATIVE_DATE_PICKER'
,p_cSize=>12
,p_cMaxlength=>4000
,p_begin_on_new_line=>'N'
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_04=>'button'
,p_attribute_05=>'N'
,p_attribute_07=>'NONE'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65981771153375104)
,p_name=>'P7240_MEMBERSHIP_SHARE'
,p_item_sequence=>230
,p_item_plug_id=>wwv_flow_api.id(65972539728375097)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65982142715375104)
,p_name=>'P7240_CALLING_PAGE_ID'
,p_item_sequence=>260
,p_item_plug_id=>wwv_flow_api.id(65972539728375097)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65982513898375104)
,p_name=>'P7240_REQUEST'
,p_item_sequence=>240
,p_item_plug_id=>wwv_flow_api.id(65972539728375097)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65982988538375104)
,p_name=>'P7240_SPECIAL_HANDLING'
,p_item_sequence=>250
,p_item_plug_id=>wwv_flow_api.id(65972539728375097)
,p_prompt=>'Special Handling'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_named_lov=>'AFF_LOOKUPS_SPECIAL_HANDLING'
,p_lov=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT DESCRIPTION D, LOOKUP_CODE R',
'FROM AFF_LOOKUPS',
'WHERE LOOKUP_TYPE = ''SPECIAL_HANDLING''',
'AND ENABLED_FLAG  = ''Y''',
'ORDER BY 1;'))
,p_lov_display_null=>'YES'
,p_lov_null_text=>'-select-'
,p_cHeight=>1
,p_colspan=>5
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(67960870718740107)
,p_name=>'P7240_DOM_PAY_THRU_DATE_1'
,p_item_sequence=>180
,p_item_plug_id=>wwv_flow_api.id(65942970664375075)
,p_prompt=>'Dom Pay Thru Date'
,p_display_as=>'NATIVE_DATE_PICKER'
,p_cSize=>12
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_04=>'button'
,p_attribute_05=>'N'
,p_attribute_07=>'NONE'
);
wwv_flow_api.create_page_validation(
 p_id=>wwv_flow_api.id(65960234247375087)
,p_tabular_form_region_id=>wwv_flow_api.id(65953765736375082)
,p_validation_name=>'MEMBERSHIP_AGREEMENT_ID not null'
,p_validation_sequence=>30
,p_validation=>'MEMBERSHIP_AGREEMENT_ID'
,p_validation_type=>'ITEM_NOT_NULL'
,p_error_message=>'#COLUMN_HEADER# must have a value.'
,p_when_button_pressed=>wwv_flow_api.id(65958550538375086)
,p_associated_column=>'MEMBERSHIP_AGREEMENT_ID'
,p_error_display_location=>'INLINE_WITH_FIELD_AND_NOTIFICATION'
);
wwv_flow_api.create_page_validation(
 p_id=>wwv_flow_api.id(65960630045375088)
,p_tabular_form_region_id=>wwv_flow_api.id(65953765736375082)
,p_validation_name=>'MEMBERSHIP_AGREEMENT_ID must be numeric'
,p_validation_sequence=>30
,p_validation=>'MEMBERSHIP_AGREEMENT_ID'
,p_validation_type=>'ITEM_IS_NUMERIC'
,p_error_message=>'#COLUMN_HEADER# must be numeric.'
,p_when_button_pressed=>wwv_flow_api.id(65958550538375086)
,p_associated_column=>'MEMBERSHIP_AGREEMENT_ID'
,p_error_display_location=>'INLINE_WITH_FIELD_AND_NOTIFICATION'
);
wwv_flow_api.create_page_validation(
 p_id=>wwv_flow_api.id(65961044690375088)
,p_tabular_form_region_id=>wwv_flow_api.id(65953765736375082)
,p_validation_name=>'TERRITORY_ID not null'
,p_validation_sequence=>40
,p_validation=>'TERRITORY_ID'
,p_validation_type=>'ITEM_NOT_NULL'
,p_error_message=>'#COLUMN_HEADER# must have a value.'
,p_when_button_pressed=>wwv_flow_api.id(65958550538375086)
,p_associated_column=>'TERRITORY_ID'
,p_error_display_location=>'INLINE_WITH_FIELD_AND_NOTIFICATION'
);
wwv_flow_api.create_page_validation(
 p_id=>wwv_flow_api.id(65961407425375088)
,p_tabular_form_region_id=>wwv_flow_api.id(65953765736375082)
,p_validation_name=>'TERRITORY_ID must be numeric'
,p_validation_sequence=>60
,p_validation=>'TERRITORY_ID'
,p_validation_type=>'ITEM_IS_NUMERIC'
,p_error_message=>'#COLUMN_HEADER# must be numeric.'
,p_when_button_pressed=>wwv_flow_api.id(65958550538375086)
,p_associated_column=>'TERRITORY_ID'
,p_error_display_location=>'INLINE_WITH_FIELD_AND_NOTIFICATION'
);
wwv_flow_api.create_page_validation(
 p_id=>wwv_flow_api.id(65961848243375088)
,p_tabular_form_region_id=>wwv_flow_api.id(65953765736375082)
,p_validation_name=>'CREATION_DATE must be a valid date'
,p_validation_sequence=>70
,p_validation=>'CREATION_DATE'
,p_validation_type=>'ITEM_IS_DATE'
,p_error_message=>'#COLUMN_HEADER# must be a valid date.'
,p_when_button_pressed=>wwv_flow_api.id(65958550538375086)
,p_associated_column=>'CREATION_DATE'
,p_error_display_location=>'INLINE_WITH_FIELD_AND_NOTIFICATION'
);
wwv_flow_api.create_page_validation(
 p_id=>wwv_flow_api.id(65962218553375088)
,p_tabular_form_region_id=>wwv_flow_api.id(65953765736375082)
,p_validation_name=>'LAST_UPDATE_DATE must be a valid date'
,p_validation_sequence=>80
,p_validation=>'LAST_UPDATE_DATE'
,p_validation_type=>'ITEM_IS_DATE'
,p_error_message=>'#COLUMN_HEADER# must be a valid date.'
,p_when_button_pressed=>wwv_flow_api.id(65958550538375086)
,p_associated_column=>'LAST_UPDATE_DATE'
,p_error_display_location=>'INLINE_WITH_FIELD_AND_NOTIFICATION'
);
wwv_flow_api.create_page_validation(
 p_id=>wwv_flow_api.id(23241403884825316)
,p_validation_name=>'CheckDuplicates'
,p_validation_sequence=>90
,p_validation=>wwv_flow_string.join(wwv_flow_t_varchar2(
'declare',
'v_count number;',
'begin',
'',
'select count(1) into v_count',
'from aff_membership_agreements',
'where',
'nvl(status,''A'')!=''I'' ',
'and sysdate between nvl(valid_from_date,sysdate) and nvl(valid_to_date, sysdate) ',
'and SOCIETY_ID=:P7240_SOCIETY',
'and ip_id=:p7240_interested_party',
'and (:p7240_membership_agr_id is null or  membership_agreement_id!=:p7240_membership_agr_id);',
'',
'if v_count >0 then',
'return ''An Active Membership Agreement already exists for the same combination of Party and Society!'';',
'end if;',
'end;'))
,p_validation_type=>'FUNC_BODY_RETURNING_ERR_TEXT'
,p_when_button_pressed=>wwv_flow_api.id(65973346971375098)
,p_error_display_location=>'INLINE_WITH_FIELD_AND_NOTIFICATION'
);
wwv_flow_api.create_page_validation(
 p_id=>wwv_flow_api.id(23242296069825324)
,p_validation_name=>'CheckDuplicates_1'
,p_validation_sequence=>100
,p_validation=>wwv_flow_string.join(wwv_flow_t_varchar2(
'declare',
'v_count number;',
'begin',
'',
'select count(1) into v_count',
'from aff_membership_agreements',
'where',
'nvl(status,''A'')!=''I'' ',
'and sysdate between nvl(valid_from_date,sysdate) and nvl(valid_to_date, sysdate) ',
'and SOCIETY_ID=:P7240_SOCIETY_1',
'and ip_id=:p7240_interested_party_1',
'and (:p7240_membership_agr_id_1 is null or  membership_agreement_id!=:p7240_membership_agr_id_1);',
'',
'if v_count >0 then',
'return ''An Active Membership Agreement already exists for the same combination of Party and Society!'';',
'end if;',
'end;'))
,p_validation_type=>'FUNC_BODY_RETURNING_ERR_TEXT'
,p_when_button_pressed=>wwv_flow_api.id(65943704647375075)
,p_error_display_location=>'INLINE_WITH_FIELD_AND_NOTIFICATION'
);
wwv_flow_api.create_page_validation(
 p_id=>wwv_flow_api.id(23242801435825330)
,p_validation_name=>'CHKSONGINANOTHERAGREE'
,p_validation_sequence=>110
,p_validation=>wwv_flow_string.join(wwv_flow_t_varchar2(
'declare',
'v_no_of_rows            number;',
'vrow                    BINARY_INTEGER;',
'v_count                 number :=0;',
'v_property_number       number :=0;',
'',
'begin',
'',
'',
'',
'v_no_of_rows := apex_application.G_F01.COUNT;',
'',
'',
'    BEGIN    ',
'    FOR k IN 1 .. v_no_of_rows',
'    LOOP',
'      vrow := apex_application.g_f01(k);',
'      ',
'          SELECT COUNT (*) into v_count',
'          FROM GET_SONGS_ENGAGE_MUL_MEM_AGMT T1',
'          WHERE     T1.property_id = vrow',
'               AND IP_ID = :P7240_INTERESTED_PARTY_1',
'               and right_type_id=:P7240_RIGHT_TYPE_1',
'               AND MEMBERSHIP_AGREEMENT_ID <> :P7240_MEMBERSHIP_AGR_ID_1;',
'        ',
'        if v_count>0 then',
'        select property_number into v_property_number',
'        from prop_properties where property_id=vrow and rownum=1;',
'            ',
'        ',
'        return ''Property ''||v_property_number||'' is already present in another Membership Agreement for the same Party!'';',
'        end if;',
'',
'',
'',
'    END LOOP;',
'',
'    END;',
'',
'end;'))
,p_validation_type=>'FUNC_BODY_RETURNING_ERR_TEXT'
,p_when_button_pressed=>wwv_flow_api.id(65970931821375096)
,p_error_display_location=>'INLINE_WITH_FIELD_AND_NOTIFICATION'
);
wwv_flow_api.create_page_validation(
 p_id=>wwv_flow_api.id(23243216804825334)
,p_validation_name=>'CHKSONGINANOTHERAGREE_1'
,p_validation_sequence=>120
,p_validation=>wwv_flow_string.join(wwv_flow_t_varchar2(
'declare',
'',
'v_count                 number :=0;',
'',
'begin',
'',
'',
'',
'',
'',
'select count(*) into v_count from (',
'SELECT property_id,',
'       title                                property_title,',
'       property_number,',
'       case when (SELECT COUNT (*)',
'          FROM GET_SONGS_ENGAGE_MUL_MEM_AGMT T1',
'         WHERE     T1.property_id = pp1.property_id',
'               AND IP_ID = :P7240_INTERESTED_PARTY_1',
'               and right_type_id=:P7240_RIGHT_TYPE_1',
'               AND MEMBERSHIP_AGREEMENT_ID <> :P7240_MEMBERSHIP_AGR_ID_1) >=1 then',
'                       ''Y''',
'                       ELSE',
'                       ''N''',
'       end indicator_flag',
'            ,pp1.created_by',
'            ,pp1.creation_date',
'            ,pp1.last_updated_by',
'            ,pp1.last_update_date  FROM prop_properties pp1',
' WHERE     property_type_id = 1',
'       AND source_id = 7                                              -- SESAC',
'       AND status = ''A''',
'       /*AND ( (instr(upper("TITLE"),upper(NVL(trim(:P7240_PROPERTY),''ZZZZ''))) > 0',
'       OR trim(:P7240_PROPERTY) IS NULL) )*/',
'       AND (   UPPER (title) LIKE ''%'' || UPPER (:P7240_PROPERTY) || ''%''',
'            OR property_number LIKE ''%'' || :P7240_PROPERTY || ''%''',
'            OR :P7240_PROPERTY IS NULL)',
'       AND pp1.property_id in',
'               (SELECT property_id',
'                  FROM prop_share_collectors psc /*replcaed by Saurav on 22-JAN-2018*/',
'                 WHERE (    psc.collector_id = :P7240_INTERESTED_PARTY_1',
'                        AND SYSDATE BETWEEN nvl(start_date_active,sysdate) AND NVL(end_date_active, SYSDATE) /*OR',
'                                                                        owner_id   = :P7240_INTERESTED_PARTY*/',
'                                                                        )',
'               UNION ALL',
'                SELECT pgp.property_id',
'                  FROM aff_membership_agreements ama, prop_group_props pgp, prop_groups pg ',
'                 WHERE ama.ip_id = :P7240_INTERESTED_PARTY_1',
'                 AND ama.membership_agreement_id = pg.membership_agreement_id',
'                 AND pg.group_id = pgp.group_id',
'                 AND ama.membership_agreement_id != :P7240_MEMBERSHIP_AGR_ID_1',
'               )',
'       AND property_id NOT IN',
'               (SELECT pp.property_id',
'                  FROM aff_membership_agreements  pca,',
'                       prop_properties            pp,',
'                       prop_groups                pg,',
'                       prop_group_props           pgp',
'                 WHERE     pca.membership_agreement_id = pg.membership_agreement_id',
'                       AND pg.GROUP_ID = pgp.GROUP_ID',
'                       AND pgp.property_id = pp.property_id',
'                       AND pp.property_type_id = 1',
'                       AND pg.group_type_id = 1 -- CATALOG',
'                       AND (ip_id = :P7240_INTERESTED_PARTY_1/*OR',
'                                                             ip_id      = :P7240_INTERESTED_PARTY*/',
'                                                             )',
'                       --AND    pca.membership_agreement_id = NVL(:P7240_MEMBERSHIP_AGR_ID_1,pca.membership_agreement_id)',
'                       AND pca.membership_agreement_id =',
'                               :P7240_MEMBERSHIP_AGR_ID_1)) where indicator_flag =''Y'';',
'    ',
'if v_count>0 then',
'return ''One or more property is already present in another Membership Agreement for the same Party!'';',
'end if;',
'end;'))
,p_validation_type=>'FUNC_BODY_RETURNING_ERR_TEXT'
,p_when_button_pressed=>wwv_flow_api.id(65971345747375096)
,p_error_display_location=>'INLINE_WITH_FIELD_AND_NOTIFICATION'
);
wwv_flow_api.create_page_validation(
 p_id=>wwv_flow_api.id(65993391197375113)
,p_validation_name=>'Checking The Song Exsist For USA for Same Interested Party on different Membership Agreement'
,p_validation_sequence=>130
,p_validation=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'  lv_society_id   NUMBER;',
'  lv_err_msg      VARCHAR2(5000):=''Song '';',
'  lv_list_of_song VARCHAR2(5000);',
'  lv_valid_flag   VARCHAR2(1):=''P'';',
'  lv_cnt number;',
'  lv_property_number varchar2(1000);',
'BEGIN',
'',
'  /**********************Checking The Membership Agreement belongs to USA For (''SESAC'',''ASCAP'',''BMI'')***********************/',
'proc_debug(''hi2'');',
'proc_debug(''P7240_INTERESTED_PARTY_1-''||:P7240_INTERESTED_PARTY_1);',
'proc_debug(''P7240_MEMBERSHIP_AGR_ID_1-''||:P7240_MEMBERSHIP_AGR_ID_1);',
'proc_debug(''hi2'');',
'  ',
'  SELECT SOCIETY_ID',
'  INTO lv_society_id',
'  FROM aff_SOCIETIES',
'  WHERE SOCIETY_ID =(SELECT SOCIETY_ID',
'    FROM AFF_MEMBERSHIP_AGREEMENTS',
'    WHERE MEMBERSHIP_AGREEMENT_ID=:P7240_MEMBERSHIP_AGR_ID_1)',
'  AND SOCIETY_CODE  in (''071'',''010'',''021'');',
'  ',
'  IF apex_application.G_F01.COUNT > 0 THEN',
'    FOR songs IN 1..apex_application.G_F01.COUNT',
'    LOOP',
'      SELECT COUNT(*)',
'      INTO lv_cnt',
'      FROM prop_groups pg,',
'        prop_group_props pgp,',
'        AFF_MEMBERSHIP_AGREEMENTS ama',
'      WHERE pgp.property_id         =apex_application.G_F01(songs)',
'      AND ama.SOCIETY_ID            IN (SELECT SOCIETY_ID  ',
'          FROM aff_SOCIETIES',
'          WHERE SOCIETY_CODE  in (''071'',''010'',''021'')',
'          )',
'      AND ama.IP_ID                 =:P7240_INTERESTED_PARTY_1',
'      AND pg.group_id               =pgp.group_id',
'      AND pg.membership_agreement_id=ama.membership_agreement_id;',
'      ',
'      IF lv_cnt                     > 0 THEN',
'        SELECT PROPERTY_NUMBER',
'        INTO lv_property_number',
'        FROM PROP_PROPERTIES',
'        WHERE PROPERTY_ID=apex_application.G_F01(songs);',
'         ',
'        lv_list_of_song            :=lv_list_of_song||lv_property_number||'', '';',
'        lv_valid_flag              :=''F'';',
'      END IF;',
'    END LOOP;',
'  END IF;',
'proc_debug(''hi3'');',
'  ',
'  IF lv_valid_flag  =''F'' THEN',
'    lv_list_of_song:=rtrim(lv_list_of_song,'','');',
'    lv_err_msg     :=lv_err_msg||lv_list_of_song ||CHR(10)||''Exist in another Membership Agreement for USA for the Same Interested Party For Society ASCAP/BMI/SESAC.'';',
'    :P7240_VALIDATION_MESSAGE:=lv_err_msg;',
'proc_debug(''hi4'');',
'     RETURN FALSE;',
'  END IF;',
'  ',
'EXCEPTION',
'WHEN NO_DATA_FOUND THEN',
'  RETURN TRUE;',
'END;'))
,p_validation_type=>'FUNC_BODY_RETURNING_BOOLEAN'
,p_error_message=>'&P7240_VALIDATION_MESSAGE.'
,p_validation_condition_type=>'NEVER'
,p_when_button_pressed=>wwv_flow_api.id(65970931821375096)
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
);
wwv_flow_api.create_page_validation(
 p_id=>wwv_flow_api.id(65993713263375113)
,p_validation_name=>'Checking The Song Exsist For USA for Same Interested Party on different Membership Agreement On Click Add All Song'
,p_validation_sequence=>140
,p_validation=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'  lv_society_id   NUMBER;',
'  lv_err_msg      VARCHAR2(5000):=''Song '';',
'  lv_list_of_song VARCHAR2(5000);',
'  lv_valid_flag   VARCHAR2(1):=''P'';',
'  lv_cnt number;',
'  lv_property_number varchar2(1000);',
'BEGIN',
'',
'  /**********************Checking The Membership Agreement belongs to USA For (''SESAC'',''ASCAP'',''BMI'')***********************/',
'  ',
'  SELECT SOCIETY_ID',
'  INTO lv_society_id',
'  FROM aff_SOCIETIES',
'  WHERE SOCIETY_ID =(SELECT SOCIETY_ID',
'    FROM AFF_MEMBERSHIP_AGREEMENTS',
'    WHERE MEMBERSHIP_AGREEMENT_ID=:P7240_MEMBERSHIP_AGR_ID_1)',
'  AND SOCIETY_CODE  in (''071'',''010'',''021'');',
'  ',
'  IF apex_application.G_F01.COUNT > 0 THEN',
'    FOR songs IN 1..apex_application.G_F01.COUNT',
'    LOOP',
'      SELECT COUNT(*)',
'      INTO lv_cnt',
'      FROM prop_groups pg,',
'        prop_group_props pgp,',
'        AFF_MEMBERSHIP_AGREEMENTS ama',
'      WHERE pgp.property_id         =apex_application.G_F01(songs)',
'      AND ama.SOCIETY_ID            IN (SELECT SOCIETY_ID  ',
'          FROM aff_SOCIETIES',
'          WHERE SOCIETY_CODE  in (''071'',''010'',''021'')',
'          )',
'      AND ama.IP_ID                 =:P7240_INTERESTED_PARTY_1',
'      AND pg.group_id               =pgp.group_id',
'      AND pg.membership_agreement_id=ama.membership_agreement_id;',
'      ',
'      IF lv_cnt                     > 0 THEN',
'        SELECT PROPERTY_NUMBER',
'        INTO lv_property_number',
'        FROM PROP_PROPERTIES',
'        WHERE PROPERTY_ID=apex_application.G_F01(songs);',
'         ',
'        lv_list_of_song            :=lv_list_of_song||lv_property_number||'', '';',
'        lv_valid_flag              :=''F'';',
'      END IF;',
'    END LOOP;',
'  END IF;',
'  ',
'  IF lv_valid_flag  =''F'' THEN',
'    lv_list_of_song:=rtrim(lv_list_of_song,'','');',
'    lv_err_msg     :=lv_err_msg||lv_list_of_song ||CHR(10)||''Exist in another Membership Agreement for USA for the Same Interested Party For Society ASCAP/BMI/SESAC.'';',
'    :P7240_VALIDATION_MESSAGE:=lv_err_msg;',
'     RETURN FALSE;',
'  END IF;',
'  ',
'EXCEPTION',
'WHEN NO_DATA_FOUND THEN',
'  RETURN TRUE;',
'END;'))
,p_validation_type=>'FUNC_BODY_RETURNING_BOOLEAN'
,p_error_message=>'&P7240_VALIDATION_MESSAGE.'
,p_when_button_pressed=>wwv_flow_api.id(65971345747375096)
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
);
wwv_flow_api.create_page_validation(
 p_id=>wwv_flow_api.id(21676440296838105)
,p_validation_name=>'Role must have some value'
,p_validation_sequence=>150
,p_validation=>wwv_flow_string.join(wwv_flow_t_varchar2(
'BEGIN',
'    IF :P7240_MEMBERSHIP_AGR_ID IS NULL AND UPPER(NVL(:REQUEST,''X'')) =  ''CREATE'' THEN',
'        IF :P7240_ROLE IS NULL THEN',
'            RETURN ''Role must have some value'';',
'        END IF;',
'    ELSIF :P7240_MEMBERSHIP_AGR_ID IS NOT NULL AND NVL(:REQUEST,''X'') = ''APPLY_CHANGES_UPD'' THEN',
'        IF :P7240_ROLE_1 IS NULL THEN',
'            RETURN ''Role must have some value'';',
'        END IF;',
'    END IF;',
'END;'))
,p_validation_type=>'FUNC_BODY_RETURNING_ERR_TEXT'
,p_error_display_location=>'INLINE_WITH_FIELD_AND_NOTIFICATION'
);
wwv_flow_api.create_page_validation(
 p_id=>wwv_flow_api.id(21676522305838106)
,p_validation_name=>'Right Type must have some value'
,p_validation_sequence=>160
,p_validation=>wwv_flow_string.join(wwv_flow_t_varchar2(
'BEGIN',
'    IF :P7240_MEMBERSHIP_AGR_ID IS NULL AND UPPER(NVL(:REQUEST,''X'')) =  ''CREATE'' THEN',
'        IF :P7240_RIGHT_TYPE IS NULL THEN',
'            RETURN ''Right Type must have some value'';',
'        END IF;',
'    ELSIF :P7240_MEMBERSHIP_AGR_ID IS NOT NULL AND NVL(:REQUEST,''X'') = ''APPLY_CHANGES_UPD'' THEN',
'        IF :P7240_RIGHT_TYPE_1 IS NULL THEN',
'            RETURN ''Right Type must have some value'';',
'        END IF;',
'    END IF;',
'END;'))
,p_validation_type=>'FUNC_BODY_RETURNING_ERR_TEXT'
,p_error_display_location=>'INLINE_WITH_FIELD_AND_NOTIFICATION'
);
wwv_flow_api.create_page_validation(
 p_id=>wwv_flow_api.id(17288709374721913)
,p_validation_name=>'ValidateValidToDate1'
,p_validation_sequence=>170
,p_validation=>wwv_flow_string.join(wwv_flow_t_varchar2(
'if last_day(add_months(trunc(to_date(:P7240_VALID_TO_DATE,''YYYY-MM-DD'')-1, ''Q''),2))!=to_date(:P7240_VALID_TO_DATE,''YYYY-MM-DD'') and :P7240_VALID_TO_DATE is not null ',
'and :P7240_RIGHT_TYPE=1 then',
'return ''Valid To Date must be the last date of a Quarter.'';',
'end if;'))
,p_validation_type=>'FUNC_BODY_RETURNING_ERR_TEXT'
,p_when_button_pressed=>wwv_flow_api.id(65973346971375098)
,p_associated_item=>wwv_flow_api.id(65978513679375101)
,p_error_display_location=>'INLINE_WITH_FIELD_AND_NOTIFICATION'
);
wwv_flow_api.create_page_validation(
 p_id=>wwv_flow_api.id(17288651734721912)
,p_validation_name=>'ValidateValidToDate2'
,p_validation_sequence=>180
,p_validation=>wwv_flow_string.join(wwv_flow_t_varchar2(
'if last_day(add_months(trunc(to_date(:P7240_VALID_TO_DATE_1,''YYYY-MM-DD'')-1, ''Q''),2))!=to_date(:P7240_VALID_TO_DATE_1,''YYYY-MM-DD'') and :P7240_VALID_TO_DATE_1 is not null ',
'and :P7240_RIGHT_TYPE_1 =1 then',
'return ''Valid To Date must be the last date of a Quarter.'';',
'end if;'))
,p_validation_type=>'FUNC_BODY_RETURNING_ERR_TEXT'
,p_when_button_pressed=>wwv_flow_api.id(65943704647375075)
,p_associated_item=>wwv_flow_api.id(65949349714375079)
,p_error_display_location=>'INLINE_WITH_FIELD_AND_NOTIFICATION'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(65998850069375118)
,p_name=>'Get IP Songs '
,p_event_sequence=>10
,p_triggering_element_type=>'ITEM'
,p_triggering_element=>'P7240_INTERESTED_PARTY'
,p_condition_element=>'P7240_INTERESTED_PARTY'
,p_triggering_condition_type=>'NOT_NULL'
,p_bind_type=>'bind'
,p_bind_event_type=>'change'
,p_display_when_type=>'NEVER'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(65999353798375119)
,p_event_id=>wwv_flow_api.id(65998850069375118)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_SUBMIT_PAGE'
,p_attribute_02=>'Y'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(65999785144375119)
,p_name=>'Get Songs report'
,p_event_sequence=>20
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(66395358722501770)
,p_condition_element=>'P7240_INTERESTED_PARTY'
,p_triggering_condition_type=>'NOT_NULL'
,p_bind_type=>'bind'
,p_bind_event_type=>'change'
,p_display_when_type=>'NEVER'
,p_da_event_comment=>'This Dynamic action needs to be verified, It is invalid in 118 app as well.'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(66000235458375119)
,p_event_id=>wwv_flow_api.id(65999785144375119)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_SET_VALUE'
,p_affected_elements_type=>'REGION'
,p_attribute_01=>'SQL_STATEMENT'
,p_attribute_03=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT',
'APEX_ITEM.CHECKBOX2(1,property_id,''onSubmit="doInsert(this);" '',''UNCHECKED'',null,''f01_#ROWNUM#'') "Select Row",',
'APEX_ITEM.DISPLAY_AND_SAVE(3,title)||APEX_ITEM.HIDDEN(2,property_id) property_title,',
'property_number',
'from prop_properties pp1',
'where property_type_id = 1',
'AND property_id IN',
'(SELECT distinct property_id FROM prop_share_owners',
'WHERE ( owner_id  = :P7240_INTERESTED_PARTY_1',
'				  OR  owner_id   = :P7240_INTERESTED_PARTY))',
'AND NOT EXISTS',
'(SELECT  1  ',
'FROM ',
'aff_membership_agreements pca,prop_properties pp,prop_groups pg,prop_group_props pgp',
'where pca.membership_agreement_id = pg.membership_agreement_id',
'AND pg.group_id = pgp.group_id',
'AND pgp.property_id = pp.property_id',
'AND pp.property_type_id = 1',
'AND  (ip_id  = :P7240_INTERESTED_PARTY_1',
'OR ip_id      = :P7240_INTERESTED_PARTY))'))
,p_attribute_07=>'P7240_INTERESTED_PARTY'
,p_attribute_08=>'Y'
,p_attribute_09=>'N'
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(66000795011375120)
,p_event_id=>wwv_flow_api.id(65999785144375119)
,p_event_result=>'FALSE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_CANCEL_EVENT'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(18984753414748848)
,p_name=>'Set Role'
,p_event_sequence=>30
,p_triggering_element_type=>'ITEM'
,p_triggering_element=>'P7240_SOCIETY'
,p_bind_type=>'bind'
,p_bind_event_type=>'change'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(18984869768748849)
,p_event_id=>wwv_flow_api.id(18984753414748848)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_SET_VALUE'
,p_affected_elements_type=>'ITEM'
,p_affected_elements=>'P7240_ROLE'
,p_attribute_01=>'FUNCTION_BODY'
,p_attribute_06=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'm_hfa_society_id NUMBER;',
'm_role_id NUMBER;',
'BEGIN',
'    select role_id INTO m_role_id',
'    from   aff_roles',
'    where UPPER(role_desc) = ''ORIGINAL PUBLISHER'';',
'',
'    SELECT society_id INTO m_hfa_society_id',
'    FROM   aff_societies_v',
'    WHERE UPPER(society_long_name) LIKE''HARRY FOX AGENCY-034-USA'';',
'    IF(:P7240_SOCIETY = m_hfa_society_id) THEN',
'        RETURN(m_role_id);',
'    END IF;',
'END;'))
,p_attribute_07=>'P7240_SOCIETY'
,p_attribute_08=>'Y'
,p_attribute_09=>'N'
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(21093023221189719)
,p_name=>'Set Role_upd'
,p_event_sequence=>40
,p_triggering_element_type=>'ITEM'
,p_triggering_element=>'P7240_SOCIETY_1'
,p_bind_type=>'bind'
,p_bind_event_type=>'change'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(21093154289189720)
,p_event_id=>wwv_flow_api.id(21093023221189719)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_SET_VALUE'
,p_affected_elements_type=>'ITEM'
,p_affected_elements=>'P7240_ROLE_1'
,p_attribute_01=>'FUNCTION_BODY'
,p_attribute_06=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'm_hfa_society_id NUMBER;',
'm_role_id NUMBER;',
'BEGIN',
'    select role_id INTO m_role_id',
'    from   aff_roles',
'    where UPPER(role_desc) = ''ORIGINAL PUBLISHER'';',
'',
'    SELECT society_id INTO m_hfa_society_id',
'    FROM   aff_societies_v',
'    WHERE UPPER(society_long_name) LIKE''HARRY FOX AGENCY-034-USA'';',
'    IF(:P7240_SOCIETY = m_hfa_society_id) THEN',
'        RETURN(m_role_id);',
'    END IF;',
'END;'))
,p_attribute_07=>'P7240_SOCIETY_1'
,p_attribute_08=>'Y'
,p_attribute_09=>'N'
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(19388778637587508)
,p_name=>'Set IPI Dates'
,p_event_sequence=>50
,p_triggering_element_type=>'ITEM'
,p_triggering_element=>'P7240_ROLE,P7240_SOCIETY,P7240_INTERESTED_PARTY'
,p_bind_type=>'bind'
,p_bind_event_type=>'change'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(19388853072587509)
,p_event_id=>wwv_flow_api.id(19388778637587508)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_SET_VALUE'
,p_affected_elements_type=>'ITEM'
,p_affected_elements=>'P7240_IPI_START_DATE'
,p_attribute_01=>'FUNCTION_BODY'
,p_attribute_06=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'    l_ipi_valid_from_date    DATE;',
'    l_ipi_valid_to_date      DATE;',
'    l_ipi_base_no            AFF_INTERESTED_PARTIES.IPI_BASE_NUMBER%TYPE;',
'BEGIN',
'    SELECT ipi_base_number ',
'           INTO l_ipi_base_no',
'      FROM aff_interested_parties',
'     WHERE ip_id = :P7240_interested_party;',
'    ',
'    SELECT MAX(m.valid_from_date)',
'           INTO ',
'           l_ipi_valid_from_date',
'    FROM ipi_membership_agreement m, ipi_interested_party p',
'    WHERE m.interested_party_id  = p.interested_party_id',
'    AND p.base_number            = l_ipi_base_no   ',
'    AND society_code_id          = :P7240_SOCIETY',
'    AND m.right_code_id          = DECODE(:P7240_SOCIETY,72,10,35,6)',
'    AND m.role_code_id  		 = DECODE(:P7240_role,4,18,11,25);',
'                                   --Below maping between Sesac role code id and IPI role code id',
'                                   --4 = Original Publisher, 18 = MUSIC PUBLISHER ',
'                                   --11  = Composer/Author,  25 = MUSICAL CREATOR (COMPOSER)',
'    ',
'    RETURN l_ipi_valid_from_date;',
'EXCEPTION WHEN OTHERS THEN ',
'    NULL;',
'END;'))
,p_attribute_07=>'P7240_INTERESTED_PARTY,P7240_SOCIETY,P7240_ROLE'
,p_attribute_08=>'Y'
,p_attribute_09=>'N'
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
end;
/
begin
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(19388952801587510)
,p_event_id=>wwv_flow_api.id(19388778637587508)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_SET_VALUE'
,p_affected_elements_type=>'ITEM'
,p_affected_elements=>'P7240_IPI_END_DATE'
,p_attribute_01=>'FUNCTION_BODY'
,p_attribute_06=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'    l_ipi_valid_from_date    DATE;',
'    l_ipi_valid_to_date      DATE;',
'    l_ipi_base_no            AFF_INTERESTED_PARTIES.IPI_BASE_NUMBER%TYPE;',
'BEGIN',
'    SELECT ipi_base_number ',
'           INTO l_ipi_base_no',
'      FROM aff_interested_parties',
'     WHERE ip_id = :P7240_interested_party;',
'    ',
'    SELECT MAX(m.valid_to_date)',
'           INTO ',
'           l_ipi_valid_to_date',
'    FROM ipi_membership_agreement m, ipi_interested_party p',
'    WHERE m.interested_party_id  = p.interested_party_id',
'    AND p.base_number            = l_ipi_base_no   ',
'    AND society_code_id          = :P7240_SOCIETY',
'    AND m.right_code_id          = DECODE(:P7240_SOCIETY,72,10,35,6)',
'    AND m.role_code_id  		 = DECODE(:P7240_role,4,18,11,25);',
'                                   --Below maping between Sesac role code id and IPI role code id',
'                                   --4 = Original Publisher, 18 = MUSIC PUBLISHER ',
'                                   --11  = Composer/Author,  25 = MUSICAL CREATOR (COMPOSER)',
'    ',
'    RETURN l_ipi_valid_to_date;',
'EXCEPTION WHEN OTHERS THEN ',
'    NULL;',
'END;'))
,p_attribute_07=>'P7240_INTERESTED_PARTY,P7240_SOCIETY,P7240_ROLE'
,p_attribute_08=>'Y'
,p_attribute_09=>'N'
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(25109701494958235)
,p_name=>'Set IPI Dates_Upd'
,p_event_sequence=>60
,p_triggering_element_type=>'ITEM'
,p_triggering_element=>'P7240_INTERESTED_PARTY_1,P7240_SOCIETY_1,P7240_ROLE_1'
,p_bind_type=>'bind'
,p_bind_event_type=>'change'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(25109818465958236)
,p_event_id=>wwv_flow_api.id(25109701494958235)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_SET_VALUE'
,p_affected_elements_type=>'ITEM'
,p_affected_elements=>'P7240_IPI_START_DATE_1'
,p_attribute_01=>'FUNCTION_BODY'
,p_attribute_06=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'    l_ipi_valid_from_date    DATE;',
'    l_ipi_valid_to_date      DATE;',
'    l_ipi_base_no            AFF_INTERESTED_PARTIES.IPI_BASE_NUMBER%TYPE;',
'BEGIN',
'    SELECT ipi_base_number ',
'           INTO l_ipi_base_no',
'      FROM aff_interested_parties',
'     WHERE ip_id = :P7240_INTERESTED_PARTY_1;',
'    ',
'    SELECT MAX(m.valid_from_date)',
'           INTO ',
'           l_ipi_valid_from_date',
'    FROM ipi_membership_agreement m, ipi_interested_party p',
'    WHERE m.interested_party_id  = p.interested_party_id',
'    AND p.base_number            = l_ipi_base_no   ',
'    AND society_code_id          = :P7240_SOCIETY_1',
'    AND m.right_code_id          = DECODE(:P7240_SOCIETY_1,72,10,35,6)',
'    AND m.role_code_id  		 = DECODE(:P7240_ROLE_1,4,18,11,25);',
'                                   --Below maping between Sesac role code id and IPI role code id',
'                                   --4 = Original Publisher, 18 = MUSIC PUBLISHER ',
'                                   --11  = Composer/Author,  25 = MUSICAL CREATOR (COMPOSER)',
'    ',
'    RETURN l_ipi_valid_from_date;',
'EXCEPTION WHEN OTHERS THEN ',
'    NULL;',
'END;'))
,p_attribute_07=>'P7240_INTERESTED_PARTY_1,P7240_SOCIETY_1,P7240_ROLE_1'
,p_attribute_08=>'Y'
,p_attribute_09=>'N'
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(25109968834958237)
,p_event_id=>wwv_flow_api.id(25109701494958235)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_SET_VALUE'
,p_affected_elements_type=>'ITEM'
,p_affected_elements=>'P7240_IPI_END_DATE_1'
,p_attribute_01=>'FUNCTION_BODY'
,p_attribute_06=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'    l_ipi_valid_from_date    DATE;',
'    l_ipi_valid_to_date      DATE;',
'    l_ipi_base_no            AFF_INTERESTED_PARTIES.IPI_BASE_NUMBER%TYPE;',
'BEGIN',
'    SELECT ipi_base_number ',
'           INTO l_ipi_base_no',
'      FROM aff_interested_parties',
'     WHERE ip_id = :P7240_interested_party_1;',
'    ',
'    SELECT MAX(m.valid_to_date)',
'           INTO ',
'           l_ipi_valid_to_date',
'    FROM ipi_membership_agreement m, ipi_interested_party p',
'    WHERE m.interested_party_id  = p.interested_party_id',
'    AND p.base_number            = l_ipi_base_no   ',
'    AND society_code_id          = :P7240_SOCIETY_1',
'    AND m.right_code_id          = DECODE(:P7240_SOCIETY_1,72,10,35,6)',
'    AND m.role_code_id  		 = DECODE(:P7240_role_1,4,18,11,25);',
'                                   --Below maping between Sesac role code id and IPI role code id',
'                                   --4 = Original Publisher, 18 = MUSIC PUBLISHER ',
'                                   --11  = Composer/Author,  25 = MUSICAL CREATOR (COMPOSER)',
'    ',
'    RETURN l_ipi_valid_to_date;',
'EXCEPTION WHEN OTHERS THEN ',
'    NULL;',
'END;'))
,p_attribute_07=>'P7240_INTERESTED_PARTY_1,P7240_SOCIETY_1,P7240_ROLE_1'
,p_attribute_08=>'Y'
,p_attribute_09=>'N'
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(34937957941306528)
,p_name=>'Set IPI Dates Prev Society'
,p_event_sequence=>70
,p_triggering_element_type=>'ITEM'
,p_triggering_element=>'P7240_PRIOR_SOCIETY,P7240_INTERESTED_PARTY,P7240_ROLE'
,p_condition_element=>'P7240_PRIOR_SOCIETY'
,p_triggering_condition_type=>'NOT_NULL'
,p_bind_type=>'bind'
,p_bind_event_type=>'change'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(34938040664306529)
,p_event_id=>wwv_flow_api.id(34937957941306528)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_SET_VALUE'
,p_affected_elements_type=>'ITEM'
,p_affected_elements=>'P7240_IPI_START_DATE'
,p_attribute_01=>'FUNCTION_BODY'
,p_attribute_06=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'    l_ipi_valid_from_date    DATE;',
'    l_ipi_valid_to_date      DATE;',
'    l_ipi_base_no            AFF_INTERESTED_PARTIES.IPI_BASE_NUMBER%TYPE;',
'BEGIN',
'    SELECT ipi_base_number ',
'           INTO l_ipi_base_no',
'      FROM aff_interested_parties',
'     WHERE ip_id = :P7240_interested_party;',
'    ',
'    SELECT MAX(m.valid_from_date)',
'           INTO ',
'           l_ipi_valid_from_date',
'    FROM ipi_membership_agreement m, ipi_interested_party p',
'    WHERE m.interested_party_id  = p.interested_party_id',
'    AND p.base_number            = l_ipi_base_no   ',
'    AND society_code_id          = :P7240_PRIOR_SOCIETY',
'    AND m.right_code_id          = DECODE(:P7240_PRIOR_SOCIETY,72,10,35,6)',
'    AND m.role_code_id  		 = DECODE(:P7240_role,4,18,11,25);',
'                                   --Below maping between Sesac role code id and IPI role code id',
'                                   --4 = Original Publisher, 18 = MUSIC PUBLISHER ',
'                                   --11  = Composer/Author,  25 = MUSICAL CREATOR (COMPOSER)',
'    ',
'    RETURN l_ipi_valid_from_date;',
'EXCEPTION WHEN OTHERS THEN ',
'    NULL;',
'END;'))
,p_attribute_07=>'P7240_PRIOR_SOCIETY,P7240_INTERESTED_PARTY,P7240_ROLE'
,p_attribute_08=>'Y'
,p_attribute_09=>'N'
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(34938136276306530)
,p_event_id=>wwv_flow_api.id(34937957941306528)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_SET_VALUE'
,p_affected_elements_type=>'ITEM'
,p_affected_elements=>'P7240_IPI_END_DATE'
,p_attribute_01=>'FUNCTION_BODY'
,p_attribute_06=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'    l_ipi_valid_from_date    DATE;',
'    l_ipi_valid_to_date      DATE;',
'    l_ipi_base_no            AFF_INTERESTED_PARTIES.IPI_BASE_NUMBER%TYPE;',
'BEGIN',
'    SELECT ipi_base_number ',
'           INTO l_ipi_base_no',
'      FROM aff_interested_parties',
'     WHERE ip_id = :P7240_interested_party;',
'    ',
'    SELECT MAX(m.valid_to_date)',
'           INTO ',
'           l_ipi_valid_to_date',
'    FROM ipi_membership_agreement m, ipi_interested_party p',
'    WHERE m.interested_party_id  = p.interested_party_id',
'    AND p.base_number            = l_ipi_base_no   ',
'    AND society_code_id          = :P7240_PRIOR_SOCIETY',
'    AND m.right_code_id          = DECODE(:P7240_PRIOR_SOCIETY,72,10,35,6)',
'    AND m.role_code_id  		 = DECODE(:P7240_role,4,18,11,25);',
'                                   --Below maping between Sesac role code id and IPI role code id',
'                                   --4 = Original Publisher, 18 = MUSIC PUBLISHER ',
'                                   --11  = Composer/Author,  25 = MUSICAL CREATOR (COMPOSER)',
'    ',
'    RETURN l_ipi_valid_to_date;',
'EXCEPTION WHEN OTHERS THEN ',
'    NULL;',
'END;'))
,p_attribute_07=>'P7240_PRIOR_SOCIETY,P7240_INTERESTED_PARTY,P7240_ROLE'
,p_attribute_08=>'Y'
,p_attribute_09=>'N'
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(23989770217664646)
,p_name=>'On Press Enter Trigger Click On Search Button'
,p_event_sequence=>80
,p_bind_type=>'bind'
,p_bind_event_type=>'ready'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(23989841149664647)
,p_event_id=>wwv_flow_api.id(23989770217664646)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
'$(''input'').keyup(function(e) {',
'    if (e.keyCode == ''13'')',
'    $(''#SEARCH'').trigger(''click'');',
'});'))
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(31794247707484809)
,p_name=>'Check Uncheck Songs Inc'
,p_event_sequence=>90
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'#selectunselectall'
,p_bind_type=>'live'
,p_bind_delegate_to_selector=>'#songs_inc'
,p_bind_event_type=>'change'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(31794315623484810)
,p_event_id=>wwv_flow_api.id(31794247707484809)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
'if ($(''#songs_inc #selectunselectall'' ).is('':checked'') ) {',
'  $(''#songs_inc input[type=checkbox][name=f02]'').prop(''checked'',true);',
'} else {',
'  $(''#songs_inc input[type=checkbox][name=f02]'').prop(''checked'',false);',
'}'))
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(31794418199484811)
,p_name=>'Check Uncheck Songs Exc'
,p_event_sequence=>100
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'#selectunselectall'
,p_bind_type=>'live'
,p_bind_delegate_to_selector=>'#songs_exc'
,p_bind_event_type=>'change'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(31794545922484812)
,p_event_id=>wwv_flow_api.id(31794418199484811)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
'if ($(''#songs_exc #selectunselectall'' ).is('':checked'') ) {',
'  $(''#songs_exc input[type=checkbox][name=f01]'').prop(''checked'',true);',
'} else {',
'  $(''#songs_exc input[type=checkbox][name=f01]'').prop(''checked'',false);',
'}'))
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(32121439525480331)
,p_name=>'closeDialog'
,p_event_sequence=>110
,p_triggering_element_type=>'BUTTON'
,p_triggering_button_id=>wwv_flow_api.id(65944142406375075)
,p_bind_type=>'bind'
,p_bind_event_type=>'apexafterclosedialog'
,p_display_when_type=>'NEVER'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(32121566534480332)
,p_event_id=>wwv_flow_api.id(32121439525480331)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_DIALOG_CLOSE'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(65962559439375089)
,p_process_sequence=>10
,p_process_point=>'AFTER_SUBMIT'
,p_region_id=>wwv_flow_api.id(65953765736375082)
,p_process_type=>'NATIVE_TABFORM_UPDATE'
,p_process_name=>'ApplyMRU'
,p_attribute_02=>'AFF_MA_TERRITORIES'
,p_attribute_03=>'MAT_ID'
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
,p_process_when_button_id=>wwv_flow_api.id(65958550538375086)
,p_process_success_message=>'#MRU_COUNT# row(s) updated, #MRI_COUNT# row(s) inserted.'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(65962906135375089)
,p_process_sequence=>20
,p_process_point=>'AFTER_SUBMIT'
,p_region_id=>wwv_flow_api.id(65953765736375082)
,p_process_type=>'NATIVE_TABFORM_DELETE'
,p_process_name=>'ApplyMRD'
,p_attribute_02=>'AFF_MA_TERRITORIES'
,p_attribute_03=>'MAT_ID'
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
,p_process_when=>'MULTI_ROW_DELETE'
,p_process_when_type=>'REQUEST_EQUALS_CONDITION'
,p_process_success_message=>'#MRD_COUNT# row(s) deleted.'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(65997249285375116)
,p_process_sequence=>30
,p_process_point=>'AFTER_SUBMIT'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'Create Membership Agreement'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'  Fr_Ma Aff_Membership_Agreements%ROWTYPE;',
'  fr_MAT AFF_MA_TERRITORIES%rowtype;',
'  Fn_Rows_Before_Insert PLS_INTEGER := 1;',
'  Fv_Commit                 VARCHAR2(1)             := ''N'';',
'  l_society_id              NUMBER;',
'  l_membership_agreement_id NUMBER;',
'  l_mat_id                  NUMBER;',
'l_group_id                  NUMBER;',
'l_row_count			NUMBER ;',
'l_row_count1			NUMBER := 0 ;',
'l_valid_from_date		DATE;',
'l_valid_to_date			DATE;',
'--l_ipi_valid_from_date    DATE;',
'--l_ipi_valid_to_date      DATE;',
'--l_ipi_base_no            AFF_INTERESTED_PARTIES.IPI_BASE_NUMBER%TYPE;',
'TYPE right_type_rec',
'IS',
'  RECORD',
'  (',
'    right_type VARCHAR2(2000));',
'TYPE right_tab_typ',
'IS',
'  TABLE OF right_type_rec;',
'  f_right_tab right_tab_typ;',
'TYPE territory_rec',
'IS',
'  RECORD',
'  (',
'    territory VARCHAR2(2000));',
'TYPE territory_tab_typ',
'IS',
'  TABLE OF territory_rec;',
'  f_territory_tab territory_tab_typ;',
' v_cnt NUMBER := 0;',
'BEGIN',
'  htp.prn(''Right_type values are''||'' ''||:P7240_right_type);',
'  htp.prn(''Territories values are''||'' ''||:P7240_TERRITORIES);',
'  IF :P7240_MEMBERSHIP_AGR_ID IS NULL THEN',
'',
'	IF :P7240_PRIOR_SOCIETY IS NOT NULL THEN',
'',
'	      l_row_count := 2;',
'	ELSE',
'		l_row_count := 1;',
'	END IF;',
'',
'    /*BEGIN',
'      SELECT society_id',
'      INTO l_society_id',
'      FROM aff_societies',
'      WHERE society_name =''SESAC'' ;',
'    EXCEPTION',
'    WHEN OTHERS THEN',
'      l_society_id := 72;',
'    END;*/',
'    FOR l_row_count1 IN 1..l_row_count LOOP ',
'',
'    	',
'		IF l_row_count1 = 1 THEN',
'			 l_society_id := :P7240_SOCIETY;',
'			 l_valid_from_date := to_date(:P7240_valid_from_date,:APP_NLS_DATE_FORMAT) ;',
'			 l_valid_to_date   := to_date(:P7240_valid_to_date,:APP_NLS_DATE_FORMAT) ;',
'		ELSIF l_row_count1 = 2 THEN',
'		    l_society_id := :P7240_PRIOR_SOCIETY;',
'		    l_valid_from_date :=  to_date(''2000-01-01'',:APP_NLS_DATE_FORMAT);',
'		    --l_valid_to_date   := :P7240_valid_from_date -1 ;',
'		    l_valid_to_date := to_date(:P7240_valid_from_date,:APP_NLS_DATE_FORMAT)  -1;',
'',
'				',
'		END IF;',
'			    IF :P7240_RIGHT_TYPE IS NOT NULL THEN',
'			      BEGIN',
'				SELECT REGEXP_SUBSTR(:P7240_RIGHT_TYPE, ''[[:alnum:]]+'', 1, level) BULK COLLECT',
'				INTO f_right_tab',
'				FROM DUAL',
'				  CONNECT BY LEVEL <= REGEXP_COUNT(:P7240_RIGHT_TYPE, ''[[:alnum:]]+'');',
'			      EXCEPTION',
'			      WHEN OTHERS THEN',
'				NULL;',
'			      END;',
'			      IF NVL(f_right_tab.count,0) <> 0 THEN',
'				FOR idx IN 1 .. f_right_tab.COUNT',
'				LOOP ',
'					v_cnt := v_cnt + 1;',
'					  SELECT AFF_MA_S.nextval ',
'					  INTO l_membership_agreement_id ',
'					  FROM DUAL;',
'',
'					  fr_ma.membership_agreement_id := l_membership_agreement_id ;',
'					  fr_ma.valid_from_date         := l_valid_from_date ;',
'					  fr_ma.valid_to_date           := l_valid_to_date ;',
'					  fr_ma.effective_date          := CASE WHEN l_row_count1=1 THEN :P7240_effective_date END; --We should not populate this field when we have previous society.',
'					  fr_ma.signature_date          := CASE WHEN l_row_count1=1 THEN :P7240_signature_date END; --We should not populate this field when we have previous society.',
'					  fr_ma.membership_share        := NVL(:P7240_membership_share,100) ;',
'					  fr_ma.ammendment_date         := :P7240_ammendment_date ;',
'					  fr_ma.ip_id                   := :P7240_interested_party ;',
'					  fr_ma.society_id              := l_society_id ;',
'					  fr_ma.creation_class_id       := :P7240_creation_class ;',
'					  fr_ma.role_id                 := :P7240_role ;',
'					  fr_ma.right_type_id           := to_number(trim(f_right_tab(idx).right_type)) ;',
'					  fr_ma.renew_allowed_flag      := CASE WHEN l_row_count1=1 THEN :P7240_renew_allowed_flag END;  --We should not populate this field when we have previous society.',
'					  fr_ma.renewal_term_months     := CASE WHEN l_row_count1=1 THEN :P7240_renewal_term_months END; --We should not populate this field when we have previous society.',
'					  fr_ma.status                  := :P7240_status ;',
'					  fr_ma.created_by              := V(''APP_USER'') ;',
'					  fr_ma.creation_date           := SYSDATE ;',
'					  fr_ma.last_updated_by         := V(''APP_USER'') ;',
'					  fr_ma.last_update_date        := SYSDATE ;',
'					  fr_ma.alert_date              := :P7240_alert_date ;',
'					  fr_ma.domestic_pay_thru_date  := :P7240_dom_pay_thru_date ;',
'					  fr_ma.group_id                := NULL ;',
'                      fr_ma.SPECIAL_HANDLING        := :P7240_SPECIAL_HANDLING;',
'  ',
'  /* The below code is commented on 21-Jun as these IPI dates cols are only for display purpose and not for storing into table.',
'                     -- Sachin, changes part of SPI-25 ',
'                    BEGIN',
'                        SELECT ipi_base_number ',
'                               INTO l_ipi_base_no',
'                         FROM  aff_interested_parties',
'                        WHERE  ip_id = :P7240_interested_party;',
'',
'                        IF l_ipi_base_no IS NOT NULL AND l_row_count1 = 1    --This check is for current society.',
'                        THEN',
'                            SELECT  MAX(m.valid_from_date),',
'                                    MAX(m.valid_to_date)',
'                                    INTO ',
'                                    l_ipi_valid_from_date,',
'                                    l_ipi_valid_to_date',
'                            FROM ipi_membership_agreement m, ipi_interested_party p',
'                            WHERE m.interested_party_id    = p.interested_party_id',
'                            AND p.base_number            = l_ipi_base_no   ',
'                            AND society_code_id          = l_society_id',
'                            AND m.right_code_id          = DECODE(l_society_id,72,10,35,6)',
'                            AND m.role_code_id           = DECODE(:P7240_role,4,18,11,25);',
'                                                        --Sesac role code         | IPI role code',
'                                                        --4  = Original Publisher | 18 = MUSIC PUBLISHER ',
'                                                        --11 = Composer/Author    | 25 = MUSICAL CREATOR (COMPOSER) ',
'                        ELSIF l_ipi_base_no IS NOT NULL AND l_row_count1 = 2    --This check is for previous society. ',
'                        THEN',
'                            SELECT  MAX(m.valid_from_date),',
'                                    MAX(m.valid_to_date)',
'                                    INTO ',
'                                    l_ipi_valid_from_date,',
'                                    l_ipi_valid_to_date',
'                            FROM ipi_membership_agreement m, ipi_interested_party p',
'                            WHERE m.interested_party_id    = p.interested_party_id',
'                            AND p.base_number            = l_ipi_base_no   ',
'                            AND society_code_id          = :P7240_PRIOR_SOCIETY',
'                            AND m.right_code_id          = DECODE(l_society_id,72,10,35,6)',
'                            AND m.role_code_id           = DECODE(:P7240_role,4,18,11,25);',
'                                                        --Sesac role code         | IPI role code',
'                                                        --4  = Original Publisher | 18 = MUSIC PUBLISHER ',
'                                                        --11 = Composer/Author    | 25 = MUSICAL CREATOR (COMPOSER) ',
'                        END IF;',
'                    EXCEPTION WHEN OTHERS THEN',
'                        NULL;',
'                    END;',
'                    fr_ma.ipi_valid_from_date := l_ipi_valid_from_date;',
'                    fr_ma.ipi_valid_to_date   := l_ipi_valid_to_date; ',
'                    ',
'                    l_ipi_valid_from_date := '''';',
'                    l_ipi_valid_to_date   := '''';',
'   */',
'   ',
'                    /* END */',
'					  aff_api.Ma_Create (Fr_Ma , ',
'								Fn_Rows_Before_Insert, ',
'								Fv_Commit);',
'					',
'							  IF :P7240_TERRITORIES IS NOT NULL THEN',
'								    BEGIN',
'								      SELECT REGEXP_SUBSTR(:P7240_TERRITORIES, ''[[:alnum:]]+'', 1, level) BULK COLLECT',
'								      INTO f_territory_tab',
'								      FROM DUAL',
'									CONNECT BY LEVEL <= REGEXP_COUNT(:P7240_TERRITORIES, ''[[:alnum:]]+'');',
'								    EXCEPTION',
'								    WHEN OTHERS THEN',
'								      NULL;',
'								    END;',
'							    IF NVL(f_territory_tab.count,0) <> 0 THEN',
'							      FOR idx IN 1 .. f_territory_tab.COUNT',
'							      LOOP',
'								--MA Territories',
'								SELECT AFF_MAT_S.nextval INTO l_mat_id FROM DUAL;',
'								fr_MAT.mat_id                  := l_mat_id ;',
'								fr_MAT.membership_agreement_id := l_membership_agreement_id ;',
'								fr_MAT.territory_id            := to_number(trim(f_territory_tab(idx).territory)) ;',
'								fr_MAT.include_exclude_flag    := ''I'' ;',
'								fr_MAT.created_by              := V(''APP_USER'') ;',
'								fr_MAT.creation_date           := SYSDATE ;',
'								fr_MAT.last_updated_by         := V(''APP_USER'') ;',
'								fr_MAT.last_update_date        := SYSDATE ;',
'								aff_api.MAT_Create(fr_MAT, fn_rows_before_insert, fv_commit);',
'							      END LOOP;',
'							    END IF;',
'							END IF;',
'						IF v_cnt = 1 AND l_row_count1 = 1 THEN ',
'							:P7240_MEMBERSHIP_AGR_ID := l_membership_agreement_id;',
'						END IF;',
'				END LOOP;',
'					IF NVL(l_membership_agreement_id,0) <> 0 THEN',
'',
'						   BEGIN',
'							  SELECT group_id',
'							    INTO l_group_id',
'							   FROM aff_membership_agreements',
'							   WHERE  (ip_id = :P7240_INTERESTED_PARTY_1 OR ',
'							ip_id = :P7240_INTERESTED_PARTY)',
'							AND society_id = l_society_id',
'							     AND group_id is not null',
'							     AND rownum = 1;',
'							    UPDATE aff_membership_agreements',
'							    SET group_id = l_group_id',
'							      WHERE  (ip_id = :P7240_INTERESTED_PARTY_1 OR ',
'								 ip_id = :P7240_INTERESTED_PARTY)',
'							     AND group_id is null;',
'						   EXCEPTION',
'							     WHEN others THEN',
'							      l_group_id := 0;',
'						   END;',
'',
'							  ',
'					END IF;',
'					v_cnt := 0;',
'			      END IF;',
'			    END IF;',
'		END LOOP;',
'   END IF;',
'  COMMIT;',
'',
'exception',
' when others then',
'  htp.prn(''Exceptions are ''||'' ''||sqlerrm(sqlcode));',
'END;'))
,p_process_error_message=>'Create process failed to create agreements'
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
,p_process_when_button_id=>wwv_flow_api.id(65973346971375098)
,p_process_success_message=>'Agreements created successfully'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(65997675090375117)
,p_process_sequence=>40
,p_process_point=>'AFTER_SUBMIT'
,p_process_type=>'NATIVE_SESSION_STATE'
,p_process_name=>'Reset page items'
,p_attribute_01=>'CLEAR_CACHE_CURRENT_PAGE'
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
,p_process_when_button_id=>wwv_flow_api.id(65972920984375098)
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(65994423373375114)
,p_process_sequence=>50
,p_process_point=>'AFTER_SUBMIT'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'Update Agreements'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'CURSOR c1 IS ',
'SELECT membership_agreement_id from aff_membership_agreements',
'WHERE (ip_id,society_id) IN ( ',
'SELECT ip_id,society_id ',
'FROM aff_membership_agreements',
'WHERE membership_agreement_id = :P7240_MEMBERSHIP_AGR_ID);',
'v_no_of_rows NUMBER;',
'vrow BINARY_INTEGER;',
'BEGIN',
'htp.prn(''Agr Id is''||'' ''||:P7240_MEMBERSHIP_AGR_ID);',
'',
'  IF :P7240_MEMBERSHIP_AGR_ID IS NOT NULL THEN',
'	htp.prn(''Agr id after if ''||'' ''||:P7240_MEMBERSHIP_AGR_ID);',
'	  FOR c1rec IN c1 LOOP',
'',
'		    UPDATE aff_membership_agreements',
'		    SET valid_from_date           = :p7240_valid_from_date_1,',
'		      valid_to_date               = :p7240_valid_to_date_1,',
'		      effective_date              = :p7240_effective_date_1,',
'		      signature_date              = :p7240_signature_date_1,',
'		      membership_share            = :p7240_membership_share_1,',
'		      ammendment_date             = :p7240_ammendment_date_1,',
'		      ip_id                       = :P7240_interested_party_1,',
'		      society_id                  = :P7240_society_1,',
'		      creation_class_id           = :P7240_creation_class_1 ,',
'		      role_id                     = :P7240_role_1 ,',
'		      right_type_id               = :P7240_right_type_1 ,',
'		      renew_allowed_flag          = :p7240_renew_allowed_flag_1,',
'		      renewal_term_months         = :p7240_renewal_term_months_1,',
'		      status                      = :P7240_status_1 ,',
'		      last_updated_by             = V(''APP_USER''),',
'		      last_update_date            = SYSDATE,',
'		      alert_date                  = :p7240_alert_date_1,',
'		      domestic_pay_thru_date      = :P7240_DOM_PAY_THRU_DATE_1,',
'              SPECIAL_HANDLING            = :P7240_SPECIAL_HANDLING_1',
'		    WHERE membership_agreement_id = c1rec.membership_agreement_id;',
'',
'		   ',
'	END LOOP;',
'		v_no_of_rows := apex_application.G_F01.count;',
'		--htp.prn(''Rows ''||'' ''||v_no_of_rows);',
'',
'		  FOR k IN 1 .. v_no_of_rows',
'		  LOOP',
'		      /* htp.prn(''f2 ''||'' ''||apex_application.g_f02 (k));',
'                       htp.prn('' f5 ''||'' ''||apex_application.g_f05 (k));',
'		      htp.prn('' f6 ''||'' ''||apex_application.g_f06 (k));',
'		      htp.prn('' f10 ''||'' ''||apex_application.g_f10 (k));',
'		   */',
'',
'		    UPDATE aff_ma_territories',
'		    SET territory_id              = apex_application.g_f06 (k),',
'		    INCLUDE_EXCLUDE_FLAG = apex_application.g_f07 (k)',
'		    WHERE mat_id =  apex_application.g_f10 (k);',
'',
'		 END LOOP;',
'  END IF;',
'  COMMIT;',
'END;'))
,p_process_error_message=>'Failed to Update Agreements data'
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
,p_process_when_button_id=>wwv_flow_api.id(65943704647375075)
,p_process_success_message=>'Updated Agreements data'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(65996499178375115)
,p_process_sequence=>60
,p_process_point=>'AFTER_SUBMIT'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'Add IP Song'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'  v_no_of_rows  NUMBER;',
'  v_no_of_rows2 NUMBER;',
'  v_seq         NUMBER;',
'  vrow BINARY_INTEGER;',
'  v_cnt      NUMBER := 0;',
'  v_cnt1     NUMBER := 0;',
'  v_group_id NUMBER := 0;',
'',
'BEGIN',
'',
'  IF NVL(:P7240_MEMBERSHIP_AGR_ID_1,0) <> 0',
'  THEN',
'',
'    BEGIN',
'      SELECT group_id',
'      INTO   v_group_id',
'      FROM   prop_groups',
'      WHERE  membership_agreement_id = :P7240_MEMBERSHIP_AGR_ID_1',
'      AND rownum = 1;',
'	  /*SPI-2468',
'      SELECT group_id',
'      INTO   v_group_id',
'      FROM   aff_membership_agreements',
'      WHERE  membership_agreement_id = :P7240_MEMBERSHIP_AGR_ID_1;',
'      */',
'    EXCEPTION',
'      WHEN OTHERS',
'      THEN',
'        v_group_id := 0;',
'    END;',
'',
'    IF NVL(v_group_id,0) = 0',
'    THEN',
'',
'      htp.prn(''v_group_id ''||'' ''||v_group_id);',
'',
'      SELECT PROP_GROUP_S.nextval',
'      INTO   v_group_id',
'      FROM   DUAL;',
'',
'      INSERT INTO prop_groups( group_id',
'                              ,group_type_id',
'                              ,group_name',
'                              ,society_id',
'                              ,collector_agreement_id',
'                              ,share_owner_id',
'                              ,start_date',
'                              ,end_date',
'                              ,created_by',
'                              ,creation_date',
'                              ,last_updated_by',
'                              ,last_update_date',
'                              ,membership_agreement_id',
'                             )',
'                       VALUES( v_group_id',
'                              ,1',
'                              ,''CATALOG''',
'                              ,NULL',
'                              ,NULL',
'                              ,NULL',
'                              ,:P7210_START_DATE',
'                              ,:P7210_END_DATE',
'                              ,v(''APP_USER'')',
'                              ,SYSDATE',
'                              ,v(''APP_USER'')',
'                              ,SYSDATE',
'                              ,:P7240_MEMBERSHIP_AGR_ID_1',
'                             );',
'',
'       UPDATE aff_membership_agreements',
'       SET    group_id = v_group_id',
'       WHERE  membership_agreement_id = :P7240_MEMBERSHIP_AGR_ID_1;',
'',
'       COMMIT;',
'',
'    END IF;',
'',
'    v_no_of_rows := apex_application.G_F01.COUNT;',
'',
'    FOR k IN 1 .. v_no_of_rows',
'    LOOP',
'',
'      vrow := apex_application.g_f01(k);',
'      htp.prn(''vrow  ''||'' ''||vrow );',
'',
'      BEGIN',
'        SELECT 1',
'        INTO   v_cnt',
'        FROM   prop_group_props',
'        WHERE  property_id = vrow',
'        AND    group_id = v_group_id;',
'      EXCEPTION',
'        WHEN OTHERS',
'        THEN',
'          v_cnt := 0;',
'      END;',
'',
'      IF NVL(v_cnt,0) = 0',
'      THEN',
'',
'        BEGIN',
'          INSERT INTO prop_group_props ( group_prop_id',
'                                        ,group_id',
'                                        ,property_id',
'                                        ,created_by',
'                                        ,creation_date',
'                                        ,last_updated_by',
'                                        ,last_update_date',
'                                       )',
'                                VALUES ( prop_group_prop_s.nextval',
'                                        ,v_group_id',
'                                        ,apex_application.g_f01(k)',
'                                        ,v(''APP_USER'')',
'                                        ,SYSDATE',
'                                        ,v(''APP_USER'')',
'                                        ,SYSDATE',
'                                       );',
'        EXCEPTION',
'          WHEN OTHERS',
'          THEN',
'            htp.p(''Exception'');',
'',
'        END;',
'',
'      END IF;',
'    END LOOP;',
'',
'  END IF;',
'',
'EXCEPTION',
'  WHEN OTHERS',
'  THEN',
'    htp.prn(''Exceptions are ''||'' ''||SQLERRM(SQLCODE));',
'END;'))
,p_process_error_message=>'Failed to add the song to the agreements'
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
,p_process_when_button_id=>wwv_flow_api.id(65970931821375096)
,p_process_success_message=>'Successfully added the song to the agreements'
);
end;
/
begin
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(65996899158375116)
,p_process_sequence=>70
,p_process_point=>'AFTER_SUBMIT'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'Delete Song'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'  v_no_of_rows  NUMBER;',
'  v_no_of_rows2 NUMBER;',
'  v_seq         NUMBER;',
'  vrow BINARY_INTEGER;',
'  v_cnt      NUMBER := 0;',
'  v_group_prop_id    NUMBER := 0;',
'  v_group_id NUMBER := 0;',
'',
'BEGIN',
'  htp.prn(''Starting ''||'' ''||:P7240_INTERESTED_PARTY_1);',
'  -- htp.prn(''Starting ''||'' ''||:P7240_INTERESTED_PARTY);',
'',
'  v_no_of_rows := apex_application.G_F02.COUNT;',
'',
'  htp.prn(''v_no_of_rows''||'' ''||v_no_of_rows);',
'',
'  FOR k IN 1 .. v_no_of_rows',
'  LOOP',
'    -- vrow := apex_application.g_f01 (k);',
'    --  htp.prn(''vrow''||'' ''||vrow);',
'             /*       htp.prn(''f1''||'' ''||apex_application.g_f01 (k));',
'               htp.prn(''f2''||'' ''||apex_application.g_f02 (k));',
'               htp.prn(''f3''||'' ''||apex_application.g_f03 (k));',
'    htp.prn(''f4''||'' ''||apex_application.g_f04 (k));',
'    htp.prn(''f5''||'' ''||apex_application.g_f05 (k));*/',
'    BEGIN',
'',
'      DELETE FROM prop_group_props',
'      WHERE  group_prop_id = apex_application.g_f02(k);',
'',
'    EXCEPTION',
'      WHEN OTHERS',
'      THEN',
'        NULL;',
'    END;',
'',
'  END LOOP;',
'',
'  COMMIT;',
'',
'EXCEPTION',
'  WHEN OTHERS',
'  THEN',
'    htp.prn(''Exceptions are ''||'' ''||SQLERRM(SQLCODE));',
'END;'))
,p_process_error_message=>'Failed to delete songs from the agreement'
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
,p_process_when_button_id=>wwv_flow_api.id(65966534371375092)
,p_process_success_message=>'Songs deleted successfully from the agreement'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(65994824189375114)
,p_process_sequence=>80
,p_process_point=>'AFTER_SUBMIT'
,p_process_type=>'NATIVE_SESSION_STATE'
,p_process_name=>'Reset Title'
,p_attribute_01=>'CLEAR_CACHE_FOR_ITEMS'
,p_attribute_03=>'P7240_TITLE'
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
,p_process_when_button_id=>wwv_flow_api.id(65971716942375097)
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(65995201081375114)
,p_process_sequence=>90
,p_process_point=>'AFTER_SUBMIT'
,p_process_type=>'NATIVE_SESSION_STATE'
,p_process_name=>'Reset Property'
,p_attribute_01=>'CLEAR_CACHE_FOR_ITEMS'
,p_attribute_03=>'P7240_PROPERTY'
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
,p_process_when_button_id=>wwv_flow_api.id(65967333858375092)
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(65996048612375115)
,p_process_sequence=>100
,p_process_point=>'AFTER_SUBMIT'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'Territory Insert or Update'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'declare',
' v_no_of_rows NUMBER;',
'  fr_mat AFF_MA_TERRITORIES%ROWTYPE;',
'  Fn_Rows_Before_Insert PLS_INTEGER := 1;',
'  Fv_Commit            VARCHAR2(1)             := ''N'';',
'l_mat_id                 NUMBER;',
'begin',
'htp.prn(''Start'');',
'        v_no_of_rows := apex_application.G_F04.count;',
'htp.prn(''No of rows''||'' ''|| v_no_of_rows);',
'',
'		  FOR k IN 1 .. v_no_of_rows',
'		  LOOP',
'htp.prn(''Territory Id is ''||'' ''||apex_application.g_f02 (k));',
'',
'                        /* htp.prn(''f2''||'' ''||apex_application.g_f02 (k));',
'			htp.prn(''f3''||'' ''||apex_application.g_f03 (k));',
'			htp.prn(''f4''||'' ''||apex_application.g_f04 (k));',
'			htp.prn(''f5''||'' ''||apex_application.g_f05 (k));',
'			htp.prn(''f6''||'' ''||apex_application.g_f06 (k));',
'			htp.prn(''f7''||'' ''||apex_application.g_f07 (k));',
'			htp.prn(''f8''||'' ''||apex_application.g_f08 (k));',
'			;*/',
'			IF apex_application.g_f02 (k) IS NULL THEN',
'			htp.prn(''Inside loop'');',
'',
'				SELECT AFF_MAT_S.nextval INTO l_mat_id FROM DUAL;',
'				  fr_mat.mat_id                 := l_mat_id ;',
'				  fr_mat.membership_agreement_id := apex_application.g_f03 (k) ;',
'				  fr_mat.territory_id           := apex_application.g_f04 (k) ;',
'				  fr_mat.include_exclude_flag   := apex_application.g_f05 (k);',
'				  fr_mat.created_by             := apex_application.g_f06 (k);',
'				  fr_mat.creation_date          := apex_application.g_f07 (k) ;',
'				  fr_mat.last_updated_by        := apex_application.g_f08 (k) ;',
'				  fr_mat.last_update_date       := apex_application.g_f09 (k) ;',
'				    aff_api.MAT_Create(fr_MAT, fn_rows_before_insert, fv_commit);',
'',
'			ELSE',
'				UPDATE AFF_MA_TERRITORIES',
'				SET territory_id = apex_application.g_f04 (k),',
'				    include_exclude_flag = apex_application.g_f05 (k),',
'					last_updated_by = v(''APP_USER''),',
'					last_update_date = sysdate',
'				WHERE mat_id = apex_application.g_f02 (k);',
'				',
'			END IF;',
'                  END LOOP;',
'',
'COMMIT;',
'EXCEPTION',
'WHEN others THEN',
'htp.prn(''Exception is ''||sqlerrm(sqlcode));',
'END;'))
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
,p_process_when_button_id=>wwv_flow_api.id(65959726263375087)
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(65998065571375118)
,p_process_sequence=>110
,p_process_point=>'AFTER_SUBMIT'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'Add ALL Songs'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'',
'  v_no_of_rows           NUMBER;',
'  v_no_of_rows2          NUMBER;',
'  v_seq                  NUMBER;',
'  vrow                   BINARY_INTEGER;',
'  v_cnt                  NUMBER := 0;',
'  v_cnt1                 NUMBER := 0;',
'  v_group_id             NUMBER := 0;',
'  Fr_Group               PROP_GROUPS%ROWTYPE;',
'  Fr_Grprop              PROP_GROUP_PROPS%ROWTYPE;',
'  Fn_Rows_Before_Insert  PLS_INTEGER := 1;',
'  Fv_Commit              VARCHAR2(1) := ''N'';',
'',
'  CURSOR c_song_rec',
'  IS',
'  /*SELECT',
'property_id,',
'title,',
'property_number',
'from prop_properties pp1',
'where property_type_id = 1',
'and status = ''A''',
'AND   ( UPPER(title) LIKE ''%''||UPPER(:P7240_PROPERTY)||''%''',
'        OR',
'        property_number LIKE ''%''||:P7240_PROPERTY||''%''',
'      )',
'--AND property_id = NVL(:P7240_PROPERTY,property_id)',
'AND property_id IN',
'(SELECT distinct property_id FROM prop_share_owners',
'WHERE ( owner_id  = :P7240_INTERESTED_PARTY_1',
'                  OR  owner_id   = :P7240_INTERESTED_PARTY))',
'AND  property_id NOT IN',
'(SELECT distinct pp.property_id',
'FROM',
'aff_membership_agreements pca,prop_properties pp,prop_groups pg,prop_group_props pgp',
'where pca.group_id = pg.group_id',
'AND pg.group_id = pgp.group_id',
'AND pgp.property_id = pp.property_id',
'AND pp.property_type_id = 1',
'AND  (ip_id  = :P7240_INTERESTED_PARTY_1',
'OR ip_id      = :P7240_INTERESTED_PARTY)',
'AND pca.membership_agreement_id = :P7240_MEMBERSHIP_AGR_ID_1);*/',
'  SELECT property_id',
'        ,title',
'        ,property_number',
'  FROM   prop_properties pp1',
'  WHERE  property_type_id = 1',
'  AND    status = ''A''',
'  AND    ( UPPER(title) LIKE ''%''||UPPER(:P7240_PROPERTY)||''%''',
'           OR',
'           property_number LIKE ''%''||:P7240_PROPERTY||''%''',
'           OR',
'           :P7240_PROPERTY IS NULL',
'         )',
'  --AND property_id = NVL(:P7240_PROPERTY,property_id)',
'  AND property_id IN ( SELECT DISTINCT property_id',
'                       FROM   prop_share_collectors',
'                       WHERE  ( collector_id  = :P7240_INTERESTED_PARTY_1',
'                            AND SYSDATE BETWEEN nvl(start_date_active,sysdate) AND NVL(end_date_active, SYSDATE)',
'                                /*OR',
'                                owner_id   = :P7240_INTERESTED_PARTY*/',
'                              )',
'                     )',
'  AND  property_id NOT IN ( SELECT DISTINCT pp.property_id',
'                            FROM   aff_membership_agreements pca',
'                                  ,prop_properties pp',
'                                  ,prop_groups pg',
'                                  ,prop_group_props pgp',
'                            WHERE  pca.membership_agreement_id = pg.membership_agreement_id',
'                            AND    pg.group_id = pgp.group_id',
'                            AND    pgp.property_id = pp.property_id',
'                            AND    pp.property_type_id = 1',
'                            AND    ( ip_id  = :P7240_INTERESTED_PARTY_1',
'                                     /*OR',
'                                     ip_id  = :P7240_INTERESTED_PARTY*/',
'                                   )',
'                            AND    pca.membership_agreement_id = :P7240_MEMBERSHIP_AGR_ID_1',
'                         );',
'',
'BEGIN',
'',
'  BEGIN',
'    /*SELECT group_id',
'    INTO   v_group_id',
'    FROM   prop_groups',
'    WHERE  membership_agreement_id = :P7240_MEMBERSHIP_AGR_ID_1;*/',
'    ',
'	SELECT group_id',
'    INTO   v_group_id',
'    FROM   aff_membership_agreements',
'    WHERE  membership_agreement_id = :P7240_MEMBERSHIP_AGR_ID_1;',
'	',
'  EXCEPTION',
'  WHEN OTHERS',
'  THEN',
'    v_group_id := 0;',
'  END;',
'',
'  IF NVL(v_group_id,0) = 0',
'  THEN',
'',
'     SELECT PROP_GROUP_S.nextval',
'        INTO   v_group_id',
'        FROM   DUAL;',
'',
'        Fr_Group.group_id                 := v_group_id ;',
'        Fr_Group.group_type_id            := 1;',
'        Fr_Group.group_name               := ''CATALOG'';',
'        Fr_Group.society_id               := :P7240_SOCIETY_1;',
'        Fr_Group.collector_agreement_id   := NULL;',
'        Fr_Group.share_owner_id           := NULL;',
'        Fr_Group.start_date               := :P7240_VALID_FROM_DATE_1;',
'        Fr_Group.end_date                 := :P7240_VALID_TO_DATE_1;',
'        Fr_Group.created_by               := v(''APP_USER'');',
'        Fr_Group.creation_date            := SYSDATE;',
'        Fr_Group.last_updated_by          := v(''APP_USER'');',
'        Fr_Group.last_update_date         := SYSDATE;',
'        Fr_Group.membership_agreement_id  := :P7240_MEMBERSHIP_AGR_ID_1;',
'        ',
'        UPDATE aff_membership_agreements',
'        SET    group_id = v_group_id',
'        WHERE  membership_agreement_id = :P7240_MEMBERSHIP_AGR_ID_1;',
'        ',
'        ',
'        PROP_API.GROUP_CREATE( Fr_Group',
'                      ,Fn_Rows_Before_Insert',
'                      ,Fv_Commit',
'                     );',
'  END IF;',
'    FOR r IN c_song_rec',
'    LOOP',
'',
'      BEGIN',
'        Fr_Grprop.group_prop_id     := prop_group_prop_s.nextval;',
'        Fr_Grprop.group_id          := v_group_id ;',
'        Fr_Grprop.property_id       := r.property_id;',
'        Fr_Grprop.created_by        := v(''APP_USER'');',
'        Fr_Grprop.creation_date     := SYSDATE;',
'        Fr_Grprop.last_updated_by   := v(''APP_USER'');',
'        Fr_Grprop.last_update_date  := SYSDATE;',
'',
'        PROP_API.GRPROP_CREATE( Fr_Grprop',
'                               ,Fn_Rows_Before_Insert',
'                               ,Fv_Commit',
'                              );',
'',
'      EXCEPTION',
'        WHEN OTHERS',
'        THEN',
'          NULL;',
'      END;',
'',
'    END LOOP;',
'',
'  --END IF;',
'',
'',
'  COMMIT;',
'',
'END;'))
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
,p_process_when_button_id=>wwv_flow_api.id(65971345747375096)
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(65998446221375118)
,p_process_sequence=>120
,p_process_point=>'AFTER_SUBMIT'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'Delete ALL Songs'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'',
'  v_no_of_rows  NUMBER;',
'  v_no_of_rows2 NUMBER;',
'  v_seq         NUMBER;',
'  vrow BINARY_INTEGER;',
'  v_cnt      NUMBER := 0;',
'  v_group_prop_id    NUMBER := 0;',
'  v_group_id NUMBER := 0;',
'',
'  CURSOR c1',
'  IS',
'  SELECT DISTINCT pgp.group_prop_id',
'        ,pp.title',
'        ,pp.property_id',
'        ,property_number',
'        ,pg.group_id',
'  FROM   aff_membership_agreements pca',
'        ,prop_properties pp',
'        ,prop_groups pg',
'        ,prop_group_props pgp',
'  WHERE  pca.membership_agreement_id = pg.membership_agreement_id',
'  AND    pg.group_id = pgp.group_id',
'  AND    pgp.property_id = pp.property_id',
'  AND    pp.property_type_id = 1',
'  and    pp.status = ''A''',
'  AND    ( ip_id  = :P7240_INTERESTED_PARTY_1',
'           /*OR',
'           ip_id      = :P7240_INTERESTED_PARTY*/',
'         )',
'  AND    pca.membership_agreement_id = :P7240_MEMBERSHIP_AGR_ID_1',
'  --AND    pp.property_id = NVL(:P7240_TITLE,pp.property_id)',
'  AND   ( UPPER(title) LIKE ''%''||UPPER(:P7240_TITLE)||''%''',
'          OR',
'          property_number LIKE ''%''||:P7240_TITLE||''%''',
'          OR',
'          :P7240_TITLE IS NULL',
'        );',
'',
'BEGIN',
'',
'    htp.prn(''Starting ''||'' ''||:P7240_INTERESTED_PARTY_1);',
'    -- htp.prn(''Starting ''||'' ''||:P7240_INTERESTED_PARTY);',
'',
'    FOR c1rec IN c1',
'    LOOP',
'',
'      BEGIN',
'        DELETE FROM prop_group_props',
'        WHERE  group_prop_id = c1rec.group_prop_id;',
'',
'      EXCEPTION',
'        WHEN OTHERS',
'        THEN',
'          NULL;',
'      END;',
'',
'    END LOOP;',
'',
'    COMMIT;',
'',
'EXCEPTION',
'  WHEN OTHERS',
'  THEN',
'    htp.prn(''Exceptions are ''||'' ''||SQLERRM(SQLCODE));',
'END;'))
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
,p_process_when_button_id=>wwv_flow_api.id(65966990917375092)
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(65994041296375113)
,p_process_sequence=>130
,p_process_point=>'AFTER_SUBMIT'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'Delete Agreement'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'',
'  v_group_id NUMBER;',
'',
'BEGIN',
'',
'  IF :P7240_MEMBERSHIP_AGR_ID IS NOT NULL',
'  THEN',
'',
'    DELETE FROM prop_group_props',
'    WHERE  group_id in (select group_id from prop_groups where membership_agreement_id = :P7240_MEMBERSHIP_AGR_ID);',
'    ',
'    DELETE FROM prop_groups',
'    WHERE  membership_agreement_id = :P7240_MEMBERSHIP_AGR_ID;',
'',
'    DELETE FROM aff_ma_territories',
'    WHERE  membership_agreement_id = :P7240_MEMBERSHIP_AGR_ID;',
'',
'    DELETE FROM aff_membership_agreements',
'    WHERE  membership_agreement_id = :P7240_MEMBERSHIP_AGR_ID;',
'',
'  END IF;',
'',
'  COMMIT;',
'',
'END;'))
,p_process_error_message=>'Failed to delete Membership Agreement.'
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
,p_process_when_button_id=>wwv_flow_api.id(65944917012375076)
,p_process_success_message=>'Membership Agreement is deleted successfully.'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(32121886822480335)
,p_process_sequence=>140
,p_process_point=>'AFTER_SUBMIT'
,p_process_type=>'NATIVE_CLOSE_WINDOW'
,p_process_name=>'CloseDialog_1'
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
,p_process_when_button_id=>wwv_flow_api.id(65944917012375076)
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(32121683046480333)
,p_process_sequence=>150
,p_process_point=>'AFTER_SUBMIT'
,p_process_type=>'NATIVE_CLOSE_WINDOW'
,p_process_name=>'CloseDialog'
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
,p_process_when_button_id=>wwv_flow_api.id(65944142406375075)
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(20920564864266304)
,p_process_sequence=>160
,p_process_point=>'AFTER_SUBMIT'
,p_process_type=>'NATIVE_CLOSE_WINDOW'
,p_process_name=>'CloseDialog_2'
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
,p_process_when_button_id=>wwv_flow_api.id(65973731767375098)
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(35501182691546848)
,p_process_sequence=>170
,p_process_point=>'AFTER_SUBMIT'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'Update Derived Status'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'declare',
'v_aff_category varchar2(50);',
'BEGIN',
'IF :REQUEST =''Create''',
'THEN',
'    FOR I IN',
'    ( ',
'        SELECT Aff_ip.Aff_status_func(:P7240_interested_party) a_category,',
'        Aff_api.Aff_status_func(:P7240_interested_party, ''HFA'') a_category_hfa',
'        FROM DUAL',
'    ) LOOP',
'        UPDATE rep_affiliates SET ',
'            aff_category    = i.a_category, ',
'            aff_category_hfa = i.a_category_hfa',
'         WHERE ip_id = :P7240_interested_party;',
'      END LOOP;',
'      ',
'sf_outbound_api.send_account_details(:P7240_interested_party);',
'',
'ELSIF :REQUEST IN(''APPLY_CHANGES_UPD'',''APPLY_CHANGES_Terr'')',
'THEN',
'     FOR I IN',
'        ( ',
'            SELECT Aff_ip.Aff_status_func(:P7240_INTERESTED_PARTY_1) a_category,',
'            Aff_api.Aff_status_func(:P7240_INTERESTED_PARTY_1, ''HFA'') a_category_hfa',
'            FROM DUAL',
'        ) LOOP',
'            select aff_category    ',
'            into v_aff_category',
'            from rep_affiliates',
'            WHERE ip_id = :P7240_INTERESTED_PARTY_1;',
'            ',
'            ',
'            UPDATE rep_affiliates SET ',
'                aff_category    = i.a_category, ',
'                aff_category_hfa = i.a_category_hfa',
'            WHERE ip_id = :P7240_INTERESTED_PARTY_1;',
'            ',
'            if v_aff_category!=''NON-SESAC'' or i.a_category!=''NON-SESAC'' then',
'            sf_outbound_api.send_account_details(:P7240_interested_party_1,''Y'');',
'            end if;',
'            ',
'        END LOOP;',
' END IF;',
'',
'END;',
''))
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
,p_process_when=>'Create,APPLY_CHANGES_UPD,APPLY_CHANGES_Terr'
,p_process_when_type=>'REQUEST_IN_CONDITION'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(22293047285322423)
,p_process_sequence=>180
,p_process_point=>'AFTER_SUBMIT'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'REFRESH_COLLECTION_PG_250'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'  v_derived_status VARCHAR2(200);',
'  v_seq_id         NUMBER;',
'  v_ws_id          NUMBER;',
'  v_app_id         NUMBER := :APP_ID;',
'  v_session_id     NUMBER := :SESSION;',
'',
'BEGIN',
'',
' --Setting apex session',
' SELECT workspace_id INTO v_ws_id FROM apex_workspaces WHERE workspace = ''SESAC'';',
' wwv_flow_api.set_security_group_id(v_ws_id);',
' wwv_flow.g_flow_id  := v_app_id;',
' wwv_flow.g_instance := v_session_id;',
' ',
'IF :REQUEST =''Create''',
'THEN ',
'',
'    --Retrieving lastest derived status to update in collection of page 250',
'    SELECT NVL2(ra.aff_category_hfa, ra.aff_category||''/''||ra.aff_category_hfa, ra.aff_category) ',
'    INTO v_derived_status',
'      FROM rep_affiliates ra',
'     WHERE ra.ip_id = :P7240_INTERESTED_PARTY;  ',
'',
'    --Retrieving sequence number or PKey of apex collection.',
'    SELECT seq_id ',
'           INTO v_seq_id ',
'      FROM apex_collections  ',
'     WHERE collection_name = ''IP_INQUIRY_COLLECTION_PG250'' ',
'       AND C001 = :P7240_INTERESTED_PARTY;',
'',
'ELSIF :REQUEST IN(''APPLY_CHANGES_UPD'',''APPLY_CHANGES_Terr'')',
'THEN',
'    ',
'    --Retrieving lastest derived status to update in collection of page 250',
'    SELECT NVL2(ra.aff_category_hfa, ra.aff_category||''/''||ra.aff_category_hfa, ra.aff_category) ',
'    INTO v_derived_status',
'      FROM rep_affiliates ra',
'     WHERE ra.ip_id = :P7240_INTERESTED_PARTY_1;  ',
'',
'    --Retrieving sequence number or PKey of apex collection.',
'    SELECT seq_id ',
'           INTO v_seq_id ',
'      FROM apex_collections  ',
'     WHERE collection_name = ''IP_INQUIRY_COLLECTION_PG250'' ',
'       AND C001 = :P7240_INTERESTED_PARTY_1;',
'END IF;',
'',
' --Updating collection of page 250',
' APEX_COLLECTION.UPDATE_MEMBER_ATTRIBUTE (',
'                                          p_collection_name => ''IP_INQUIRY_COLLECTION_PG250'',',
'                                          p_seq             => v_seq_id,',
'                                          p_attr_number     => 14,',
'                                          p_attr_value      => v_derived_status',
'                                         );',
'',
'EXCEPTION',
'    WHEN OTHERS THEN',
'    NULL;',
'END;'))
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
,p_process_when=>'Create,APPLY_CHANGES_UPD,APPLY_CHANGES_Terr'
,p_process_when_type=>'REQUEST_IN_CONDITION'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(65995630594375115)
,p_process_sequence=>50
,p_process_point=>'BEFORE_BOX_BODY'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'Populate Agreement Items'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'BEGIN',
'IF :P7240_MEMBERSHIP_AGR_ID IS NOT NULL THEN',
'   SELECT valid_from_date ,',
'    valid_to_date ,',
'    effective_date ,',
'    signature_date ,',
'    membership_share ,',
'    ammendment_date ,',
'    ip_id ,',
'    society_id ,',
'    creation_class_id ,',
'    role_id ,',
'    right_type_id ,',
'    renew_allowed_flag ,',
'    renewal_term_months ,',
'    status ,',
'    alert_date ,',
'    domestic_pay_thru_date ,',
'    ama.membership_agreement_id,',
'    ama.SPECIAL_HANDLING',
'    --ama.ipi_valid_from_date,',
'    --ama.ipi_valid_to_date',
'  INTO :P7240_valid_from_date_1,',
'    :P7240_valid_to_date_1 ,',
'    :P7240_effective_date_1,',
'    :P7240_signature_date_1 ,',
'    :P7240_membership_share_1,',
'    :P7240_ammendment_date_1,',
'    :P7240_interested_party_1,',
'    :P7240_society_1 ,',
'    :P7240_creation_class_1 ,',
'    :P7240_role_1 ,',
'    :P7240_right_type_1 ,',
'    :P7240_renew_allowed_flag_1,',
'    :P7240_renewal_term_months_1,',
'    :P7240_status_1,',
'    :P7240_alert_date_1 ,',
'    :P7240_dom_pay_thru_date_1,',
'    :P7240_MEMBERSHIP_AGR_ID_1,',
'    :P7240_SPECIAL_HANDLING_1',
'    --:P7240_IPI_START_DATE_1,',
'    --:P7240_IPI_END_DATE_1',
'  FROM aff_membership_agreements ama',
'  WHERE ama.membership_agreement_id = :P7240_MEMBERSHIP_AGR_ID;',
'END IF;',
'',
'IF :REQUEST not in (''SEARCH1'',''SEARCH2'') THEN',
':P7240_PROPERTY:=NULL;',
':P7240_TITLE:=NULL;',
'END IF;',
'EXCEPTION',
'WHEN OTHERS THEN',
'  NULL;',
'END;',
'',
'',
''))
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
