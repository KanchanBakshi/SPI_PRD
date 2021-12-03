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
--   Date and Time:   06:00 Wednesday November 17, 2021
--   Exported By:     SCHOPRA
--   Flashback:       0
--   Export Type:     Page Export
--   Version:         18.2.0.00.12
--   Instance ID:     69431613733717
--

prompt --application/pages/delete_50500
begin
wwv_flow_api.remove_page (p_flow_id=>wwv_flow.g_flow_id, p_page_id=>50500);
end;
/
prompt --application/pages/page_50500
begin
wwv_flow_api.create_page(
 p_id=>50500
,p_user_interface_id=>wwv_flow_api.id(51875501499240850)
,p_name=>'Non-Renewals'
,p_step_title=>'Non-Renewals'
,p_step_sub_title_type=>'TEXT_WITH_SUBSTITUTIONS'
,p_autocomplete_on_off=>'OFF'
,p_javascript_code=>wwv_flow_string.join(wwv_flow_t_varchar2(
'$(document).ready(function() {',
'  setInterval(function() {',
'   apex.event.trigger( "#DEADWOODJOBSTATUS", "apexrefresh" );',
'  }, 30000);',
'});'))
,p_page_template_options=>'#DEFAULT#'
,p_last_updated_by=>'SMIRLEK'
,p_last_upd_yyyymmddhh24miss=>'20200828012718'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(12357577245497422)
,p_plug_name=>'Search'
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>10
,p_include_in_reg_disp_sel_yn=>'Y'
,p_plug_display_point=>'BODY'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_attribute_01=>'N'
,p_attribute_02=>'HTML'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(12357674549497423)
,p_plug_name=>'Job Status'
,p_region_name=>'DEADWOODJOBSTATUS'
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>20
,p_include_in_reg_disp_sel_yn=>'Y'
,p_plug_display_point=>'BODY'
,p_query_type=>'SQL'
,p_plug_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT srp.schedule_id job_id,',
'  job_name,',
'  status,',
'  MAX (decode(srp.parameter_type , ''EARNINGS_AMOUNT'' ,srp.parameter_value_from )) earnings_amount_threshold,',
'  srp.created_by,',
'  srp.creation_date,',
'  CASE',
'    WHEN status =''COMPLETED''',
'    THEN ''<a href="''',
'      ||apex_util.prepare_url(''f?p=&APP_ID.:50510:&APP_SESSION.::::P50510_EARNINGS_AMOUNT:''',
'      ||srp.parameter_value_from',
'      ||'''')',
'      ||''"><b>View Result</b>''',
'    ELSE NULL',
'  END view_result',
'FROM spi_report_params srp,',
'  job_fileload_schedules jfs',
'WHERE srp.schedule_id=jfs.schedule_id',
'AND report_type      =''DEADWOOD''',
'GROUP BY srp.schedule_id,',
'  job_name,',
'  status,',
'  srp.parameter_value_from,',
'  srp.created_by,',
'  srp.creation_date;'))
,p_plug_source_type=>'NATIVE_IR'
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
,p_plug_header=>'Once the Job Status updated to <b>Completed</b> then you can see the report by clicking on <b>View Result.</b>'
);
wwv_flow_api.create_worksheet(
 p_id=>wwv_flow_api.id(12357772306497424)
,p_max_row_count=>'1000000'
,p_show_nulls_as=>'-'
,p_pagination_type=>'ROWS_X_TO_Y'
,p_pagination_display_pos=>'BOTTOM_RIGHT'
,p_report_list_mode=>'TABS'
,p_show_detail_link=>'N'
,p_show_notify=>'Y'
,p_download_formats=>'CSV:HTML:EMAIL:XLS:PDF:RTF'
,p_owner=>'SMIRLEK'
,p_internal_uid=>12357772306497424
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(12357810836497425)
,p_db_column_name=>'JOB_ID'
,p_display_order=>10
,p_column_identifier=>'A'
,p_column_label=>'Job ID'
,p_column_type=>'NUMBER'
,p_heading_alignment=>'RIGHT'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(12357922791497426)
,p_db_column_name=>'JOB_NAME'
,p_display_order=>20
,p_column_identifier=>'B'
,p_column_label=>'Job Name'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(12358134806497428)
,p_db_column_name=>'CREATED_BY'
,p_display_order=>40
,p_column_identifier=>'D'
,p_column_label=>'Created By'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(12358378213497430)
,p_db_column_name=>'EARNINGS_AMOUNT_THRESHOLD'
,p_display_order=>60
,p_column_identifier=>'F'
,p_column_label=>'Earnings Amount Threshold'
,p_column_type=>'STRING'
,p_heading_alignment=>'RIGHT'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(12358460578497431)
,p_db_column_name=>'VIEW_RESULT'
,p_display_order=>70
,p_column_identifier=>'G'
,p_column_label=>'View Result'
,p_column_link=>'f?p=&APP_ID.:50510:&SESSION.::&DEBUG.:RP:P50510_EARNINGS_AMOUNT:#EARNINGS_AMOUNT_THRESHOLD#'
,p_column_linktext=>'#VIEW_RESULT#'
,p_column_type=>'STRING'
,p_display_text_as=>'WITHOUT_MODIFICATION'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(18525004938206623)
,p_db_column_name=>'STATUS'
,p_display_order=>80
,p_column_identifier=>'H'
,p_column_label=>'Job Status'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(18525133860206624)
,p_db_column_name=>'CREATION_DATE'
,p_display_order=>90
,p_column_identifier=>'I'
,p_column_label=>'Creation Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(18511242914495236)
,p_application_user=>'APXWS_DEFAULT'
,p_report_seq=>10
,p_report_alias=>'185113'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'JOB_ID:JOB_NAME:EARNINGS_AMOUNT_THRESHOLD:STATUS:CREATED_BY:CREATION_DATE:VIEW_RESULT:'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(18505819929551511)
,p_plug_name=>'Breadcrumb'
,p_region_template_options=>'#DEFAULT#:t-BreadcrumbRegion--useBreadcrumbTitle'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51852637782240827)
,p_plug_display_sequence=>10
,p_plug_display_point=>'REGION_POSITION_01'
,p_menu_id=>wwv_flow_api.id(51876846391240858)
,p_plug_source_type=>'NATIVE_BREADCRUMB'
,p_menu_template_id=>wwv_flow_api.id(51870806884240842)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(12358714751497434)
,p_button_sequence=>20
,p_button_plug_id=>wwv_flow_api.id(12357577245497422)
,p_button_name=>'RESET'
,p_button_action=>'SUBMIT'
,p_button_template_options=>'#DEFAULT#:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Clear'
,p_button_position=>'BODY'
,p_icon_css_classes=>'fa-eraser'
,p_grid_new_grid=>false
,p_grid_new_row=>'Y'
,p_grid_column_span=>1
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(12358867461497435)
,p_button_sequence=>30
,p_button_plug_id=>wwv_flow_api.id(12357577245497422)
,p_button_name=>'SEARCH'
,p_button_action=>'SUBMIT'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Submit'
,p_button_position=>'BODY'
,p_icon_css_classes=>'fa-filter'
,p_grid_new_grid=>false
,p_grid_new_row=>'N'
,p_grid_new_column=>'Y'
,p_grid_column_span=>1
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(12358554908497432)
,p_name=>'P50500_EAR_AMOUNT_THRESHOLD'
,p_item_sequence=>10
,p_item_plug_id=>wwv_flow_api.id(12357577245497422)
,p_prompt=>'Earnings Amount Threshold'
,p_display_as=>'NATIVE_TEXT_FIELD'
,p_cSize=>30
,p_colspan=>4
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'Y'
,p_attribute_02=>'N'
,p_attribute_04=>'TEXT'
,p_attribute_05=>'BOTH'
);
wwv_flow_api.create_page_validation(
 p_id=>wwv_flow_api.id(18590371036162301)
,p_validation_name=>'Not null'
,p_validation_sequence=>10
,p_validation=>'P50500_EAR_AMOUNT_THRESHOLD'
,p_validation_type=>'ITEM_NOT_NULL'
,p_error_message=>'Earnings Amount Threshold must have some value..'
,p_when_button_pressed=>wwv_flow_api.id(12358867461497435)
,p_associated_item=>wwv_flow_api.id(12358554908497432)
,p_error_display_location=>'INLINE_WITH_FIELD_AND_NOTIFICATION'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(12358934000497436)
,p_process_sequence=>10
,p_process_point=>'AFTER_SUBMIT'
,p_process_type=>'NATIVE_SESSION_STATE'
,p_process_name=>'Reset Page Items'
,p_attribute_01=>'CLEAR_CACHE_CURRENT_PAGE'
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
,p_process_when_button_id=>wwv_flow_api.id(12358714751497434)
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(12359048488497437)
,p_process_sequence=>20
,p_process_point=>'AFTER_SUBMIT'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'Call_Deadwood_report '
,p_process_sql_clob=>'spi_deadwood_pkg.deadwood(trim(:P50500_EAR_AMOUNT_THRESHOLD));'
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
,p_process_when_button_id=>wwv_flow_api.id(12358867461497435)
,p_process_success_message=>'Process is submitted.'
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
