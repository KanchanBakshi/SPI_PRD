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
--   Date and Time:   08:16 Wednesday November 17, 2021
--   Exported By:     SCHOPRA
--   Flashback:       0
--   Export Type:     Page Export
--   Version:         18.2.0.00.12
--   Instance ID:     69431613733717
--

prompt --application/pages/delete_00010
begin
wwv_flow_api.remove_page (p_flow_id=>wwv_flow.g_flow_id, p_page_id=>10);
end;
/
prompt --application/pages/page_00010
begin
wwv_flow_api.create_page(
 p_id=>10
,p_user_interface_id=>wwv_flow_api.id(51875501499240850)
,p_name=>'Create Queues'
,p_step_title=>'Create Queues'
,p_reload_on_submit=>'A'
,p_warn_on_unsaved_changes=>'N'
,p_step_sub_title=>'Create Queues'
,p_step_sub_title_type=>'TEXT_WITH_SUBSTITUTIONS'
,p_autocomplete_on_off=>'ON'
,p_group_id=>wwv_flow_api.id(68043639130234200)
,p_javascript_code_onload=>'setInterval("jQuery(''#P10_QUEUE_HEADER'').trigger(''apexrefresh'');", 5000);'
,p_page_template_options=>'#DEFAULT#'
,p_required_role=>wwv_flow_api.id(69657211077036510)
,p_help_text=>'No help is available for this page.'
,p_last_updated_by=>'SKUMARAN'
,p_last_upd_yyyymmddhh24miss=>'20200914081455'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(7576849457132024)
,p_plug_name=>'Queue Type Selection'
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>20
,p_plug_display_point=>'BODY'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_attribute_01=>'N'
,p_attribute_02=>'HTML'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(26004152831765303)
,p_name=>'CA Work Queue'
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>50
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#:t-Report--stretch:t-Report--altRowsDefault:t-Report--rowHighlight'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT  pcwq.PROP_COLLECTION_WORK_QUEUE_ID,',
'        aip.ip_id,',
'        aip.ip_number,',
'        ain.full_name, ',
'        pcwq.property_id,',
'        prp.title,',
'        NULL "SESAC Earnings",',
'        NULL "# of HFA Licenses",',
'        NULL "# of linked recordings",',
'        aip.AFFILIATE_REP',
'FROM    aff_interested_parties aip,',
'        PROP_COLLECTION_WORK_QUEUE pcwq,',
'        aff_ip_names ain,',
'        prop_properties prp',
'WHERE   aip.ip_id = pcwq.collector_id',
'AND     aip.ip_id = ain.ip_id',
'AND     ain.name_type_id = 6',
'AND     pcwq.property_id = prp.property_id',
'AND     aip.AFFILIATE_REP = ''*NONE''--:APP_USER'))
,p_display_condition_type=>'NEVER'
,p_ajax_enabled=>'Y'
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
 p_id=>wwv_flow_api.id(26005212449765314)
,p_query_column_id=>1
,p_column_alias=>'PROP_COLLECTION_WORK_QUEUE_ID'
,p_column_display_sequence=>1
,p_column_heading=>'Process'
,p_column_link=>'f?p=&APP_ID.:120:&SESSION.::&DEBUG.:RP,120:P120_CAWQ_ID:#PROP_COLLECTION_WORK_QUEUE_ID#'
,p_column_linktext=>'<i class="fa fa-cogs"></i>'
,p_column_alignment=>'CENTER'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(26004352285765305)
,p_query_column_id=>2
,p_column_alias=>'IP_ID'
,p_column_display_sequence=>2
,p_column_heading=>'Ip id'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(26004710667765309)
,p_query_column_id=>3
,p_column_alias=>'IP_NUMBER'
,p_column_display_sequence=>6
,p_column_heading=>'Ip number'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(26004412484765306)
,p_query_column_id=>4
,p_column_alias=>'FULL_NAME'
,p_column_display_sequence=>3
,p_column_heading=>'Full name'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(26004588593765307)
,p_query_column_id=>5
,p_column_alias=>'PROPERTY_ID'
,p_column_display_sequence=>4
,p_column_heading=>'Property id'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(26004691180765308)
,p_query_column_id=>6
,p_column_alias=>'TITLE'
,p_column_display_sequence=>5
,p_column_heading=>'Title'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(26004840127765310)
,p_query_column_id=>7
,p_column_alias=>'SESAC Earnings'
,p_column_display_sequence=>7
,p_column_heading=>'Sesac earnings'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(26004957115765311)
,p_query_column_id=>8
,p_column_alias=>'# of HFA Licenses'
,p_column_display_sequence=>8
,p_column_heading=>'# of hfa licenses'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(26005034491765312)
,p_query_column_id=>9
,p_column_alias=>'# of linked recordings'
,p_column_display_sequence=>9
,p_column_heading=>'# of linked recordings'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(26005114729765313)
,p_query_column_id=>10
,p_column_alias=>'AFFILIATE_REP'
,p_column_display_sequence=>10
,p_column_heading=>'Affiliate rep'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(62095286994062121)
,p_plug_name=>'Generate Work Queues'
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody:t-Form--slimPadding:t-Form--stretchInputs'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>40
,p_plug_display_point=>'BODY'
,p_plug_query_num_rows=>15
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_attribute_01=>'N'
,p_attribute_02=>'HTML'
,p_attribute_03=>'Y'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(62125154455062144)
,p_name=>'Current Queue'
,p_region_name=>'P10_QUEUE_HEADER'
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>30
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#:t-Report--altRowsDefault:t-Report--rowHighlight'
,p_grid_column_css_classes=>'cl-ir-dynamic-display'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT',
'       swqh.work_queue_header_id',
'       ,TO_NUMBER(swqh.queue_type) "Queue Type ID"',
'       ,DECODE(swqh.queue_type',
'                         ,''1'', ''Property Match''',
'                         ,''2'', ''Party Match''',
'                         ,''3'', ''Property Resolution''',
'                         ,''4'', ''Party Resolution''',
'                         ,''5'', ''Group Resolution''',
'                         ,''6'', ''Collection Agr''',
'                         ,swqh.queue_type)queue_type',
'       ,swqh.status',
'      , ',
'        CASE swqh.status',
'        WHEN 60 THEN',
'            ''<span style="color:#FF0000;"><b>ERROR</b></span>''',
'        ELSE',
'            CASE pl.meaning',
'                WHEN ''BUILDING''         THEN  ''<i class="fa fa-spinner fa-spin fa-3x fa-fw"></i><span class="sr-only">Generating...</span>'' --''Queue Being Generated''',
'                WHEN ''NO_DATA''          THEN  ''<i class="fa fa-ban" aria-hidden="true"></i>''',
'                WHEN ''ANOTHER_QUEUE''    THEN  ''<i class="fa fa-ban" aria-hidden="true"></i>''--''Queue Generated with no data''',
'                WHEN ''SCHEDULED''        THEN  ''<i class="fa fa-spinner fa-spin fa-3x fa-fw"></i><span class="sr-only">Scheduled</span>''',
'                WHEN ''COMPLETED''        THEN NULL',
'            ELSE ',
'                DECODE(queue_type, ''1'', ''<a href=''||''"''||''f?p=&APP_ID.:30:&SESSION.::&DEBUG.:30:P30_HEADER_ID:''||work_queue_header_id||''"''||''>Resume Work Queue</a>''',
'                                 , ''2'', ''<a href=''||''"''||''f?p=&APP_ID.:20:&SESSION.::&DEBUG.:20:P20_HEADER_ID:''||work_queue_header_id||''"''||''>Resume Work Queue</a>''',
'                                 , ''3'', ''<a href=''||''"''||''f?p=&APP_ID.:90:&SESSION.::&DEBUG.:90:P90_HEADER_ID:''||work_queue_header_id||'',:P90_CREATED_FLAG:N''||''"''||''>Resume Work Queue</a>''',
'                                 , ''4'', ''<a href=''||''"''||''f?p=&APP_ID.:100:&SESSION.::&DEBUG.:100:P100_HEADER_ID:''||work_queue_header_id||''"''||''>Resume Work Queue</a>''',
'                                 , ''5'', ''<a href=''||''"''||''f?p=&APP_ID.:110:&SESSION.::&DEBUG.:110:P110_HEADER_ID:''||work_queue_header_id||''"''||''>Resume Work Queue</a>''',
'                      )',
'            END ',
'        END LINK',
'FROM   spi_work_queue_header swqh',
'      ,prop_lookups pl',
'WHERE  swqh.username = v(''APP_USER'')',
'AND    swqh.status <> 40',
'AND    NVL(swqh.active_flag,''Y'') = ''Y''',
'AND    pl.lookup_type = ''SPI_WORK_QUEUE_STATUS''',
'AND    TO_NUMBER(pl.lookup_code(+)) = swqh.status   ',
'/*UNION',
'SELECT',
'       owqh.WQ_HEADER_ID',
'       ,TO_NUMBER(owqh.WQ_TYPE) "Queue Type ID"',
'       ,DECODE(owqh.WQ_TYPE',
'                         ',
'                         ,''6'', ''Collection Agr''',
'                         ,owqh.WQ_TYPE)WQ_TYPE',
'       ,owqh.status',
'      , ',
'        CASE owqh.status',
'        WHEN 60 THEN',
'            ''<span style="color:#FF0000;"><b>ERROR</b></span>''',
'        ELSE',
'            CASE pl.description',
'                WHEN ''Queue Being Generated'' THEN  ''<i class="fa fa-spinner fa-spin fa-3x fa-fw"></i><span class="sr-only">Generating...</span>'' --''Queue Being Generated''',
'                WHEN ''Queue Generated with no data'' THEN  ''<i class="fa fa-ban" aria-hidden="true"></i>'' --''Queue Generated with no data''',
'                WHEN ''Scheduled'' THEN  ''<i class="fa fa-spinner fa-spin fa-3x fa-fw"></i><span class="sr-only">Scheduled</span>''',
'                WHEN ''Queue Completed'' THEN NULL',
'            ELSE ',
'                DECODE(WQ_TYPE, ''6'', ''<a href=''||''"''||''f?p=&APP_ID.:120:&SESSION.::&DEBUG.:120:P120_CAWQ_HEADER_ID:''||WQ_HEADER_ID||''"''||''>Resume Work Queue</a>''',
'                      ',
'                      )',
'            END ',
'        END X',
'FROM   spi_ot_work_queues_header owqh',
'      ,prop_lookups pl',
'WHERE  owqh.username = v(''APP_USER'')',
'AND    owqh.status <> 40',
'AND    NVL(owqh.active_flag,''Y'') = ''Y''',
'AND    pl.lookup_type = ''SPI_WORK_QUEUE_STATUS''',
'AND    TO_NUMBER(pl.lookup_code(+)) = owqh.status',
'--AND    :APP_USER IN (''ATRIPATHI'')*/'))
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>' '
,p_query_no_data_found=>'Currently you do not have any active work queue'
,p_query_row_count_max=>500
,p_csv_output=>'N'
,p_prn_output=>'N'
,p_sort_null=>'L'
,p_plug_query_strip_html=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(62125940260062145)
,p_query_column_id=>1
,p_column_alias=>'WORK_QUEUE_HEADER_ID'
,p_column_display_sequence=>4
,p_column_heading=>'Abandon Work Queue'
,p_column_css_class=>'t-Icon'
,p_column_link=>'f?p=&APP_ID.:5:&SESSION.::&DEBUG.:5:P5_HEADER_ID,P5_QUEUE_TYPE:#WORK_QUEUE_HEADER_ID#,#Queue Type ID#'
,p_column_linktext=>'<span class="fa fa-remove" alt="Abandon Queue" title="Abandon Queue"></span>'
,p_column_alignment=>'CENTER'
,p_report_column_required_role=>wwv_flow_api.id(69657211077036510)
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(34377067863973332)
,p_query_column_id=>2
,p_column_alias=>'Queue Type ID'
,p_column_display_sequence=>5
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(62126343122062145)
,p_query_column_id=>3
,p_column_alias=>'QUEUE_TYPE'
,p_column_display_sequence=>2
,p_column_heading=>'Queue Type'
,p_use_as_row_header=>'N'
,p_column_alignment=>'CENTER'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(62126741046062146)
,p_query_column_id=>4
,p_column_alias=>'STATUS'
,p_column_display_sequence=>3
,p_column_heading=>'Status'
,p_use_as_row_header=>'N'
,p_column_alignment=>'CENTER'
,p_display_as=>'TEXT_FROM_LOV'
,p_named_lov=>wwv_flow_api.id(58558987709691481)
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(40198940914804241)
,p_query_column_id=>5
,p_column_alias=>'LINK'
,p_column_display_sequence=>1
,p_use_as_row_header=>'N'
,p_column_css_class=>'test'
,p_disable_sort_column=>'N'
,p_display_as=>'WITHOUT_MODIFICATION'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(79126398734447688)
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
 p_id=>wwv_flow_api.id(62096801533062122)
,p_button_sequence=>10
,p_button_plug_id=>wwv_flow_api.id(62095286994062121)
,p_button_name=>'CREATE_WORK_QUEUE'
,p_button_action=>'SUBMIT'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Create Work Queue'
,p_button_position=>'TOP_AND_BOTTOM'
,p_button_alignment=>'LEFT'
,p_icon_css_classes=>'fa-plus'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(69657211077036510)
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(11647291994834312)
,p_name=>'P10_PRIORITY_FLAG'
,p_item_sequence=>315
,p_item_plug_id=>wwv_flow_api.id(62095286994062121)
,p_prompt=>'High Priority'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_lov=>'STATIC2:Yes;Y,No;N'
,p_lov_display_null=>'YES'
,p_lov_null_text=>'-Select-'
,p_cHeight=>1
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_column=>5
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(19713737258749021)
,p_name=>'P10_SPI_TRANSACTION_ID'
,p_item_sequence=>55
,p_item_plug_id=>wwv_flow_api.id(62095286994062121)
,p_prompt=>'SPI Reference#'
,p_display_as=>'NATIVE_TEXT_FIELD'
,p_cSize=>30
,p_cMaxlength=>20
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
 p_id=>wwv_flow_api.id(20672022130138347)
,p_name=>'P10_HOLD_VALUE'
,p_item_sequence=>325
,p_item_plug_id=>wwv_flow_api.id(62095286994062121)
,p_prompt=>'Hold Reason'
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
,p_cHeight=>1
,p_begin_on_new_line=>'N'
,p_colspan=>4
,p_grid_column=>1
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'NO'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(34376571312973327)
,p_name=>'P10_AFFILIATE_REP'
,p_item_sequence=>335
,p_item_plug_id=>wwv_flow_api.id(62095286994062121)
,p_prompt=>'Rep Affiliate'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_lov=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT  DISTINCT affiliate_rep D, ',
'        affiliate_rep R ',
'FROM    aff_interested_parties ',
'WHERE   affiliate_rep IS NOT NULL',
'--AND     prospect_flag = ''N''',
'--AND     STATUS_CODE_ID = 120',
'ORDER by 1'))
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
 p_id=>wwv_flow_api.id(36666494755113413)
,p_name=>'P10_TYPE'
,p_item_sequence=>35
,p_item_plug_id=>wwv_flow_api.id(62095286994062121)
,p_prompt=>'Type'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_lov=>'STATIC:Injestion;I,Others;O'
,p_lov_display_null=>'YES'
,p_lov_null_text=>'-select-'
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
 p_id=>wwv_flow_api.id(62094847943062120)
,p_name=>'P10_HEADER_ID'
,p_item_sequence=>20
,p_item_plug_id=>wwv_flow_api.id(7576849457132024)
,p_use_cache_before_default=>'NO'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT work_queue_header_id',
'FROM   spi_work_queue_header',
'WHERE  username = v(''APP_USER'')',
'AND    status <> 40',
'AND    nvl(active_flag,''Y'') = ''Y'''))
,p_source_type=>'QUERY'
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(62097219951062122)
,p_name=>'P10_QUEUE_TYPE'
,p_item_sequence=>10
,p_item_plug_id=>wwv_flow_api.id(7576849457132024)
,p_prompt=>'Queue Type'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_lov=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT ''Current Queue'' d',
'      ,0 r',
'FROM   dual',
'UNION',
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
'UNION',
'SELECT ''Party Resolution'' d',
'      ,4 r',
'FROM   dual',
'/*',
'UNION',
'SELECT ''Collection Agreement'' d',
'      ,6 r',
'FROM   dual',
'',
'UNION',
'SELECT ''Group Resolution'' d',
'      ,5 r',
'FROM   dual*/',
'ORDER BY 2'))
,p_cHeight=>1
,p_colspan=>4
,p_grid_column=>1
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'NO'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(62097690921062122)
,p_name=>'P10_AVAILABLE_QUEUES'
,p_item_sequence=>15
,p_item_plug_id=>wwv_flow_api.id(62095286994062121)
,p_prompt=>'Saved Queue Criteria'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_lov=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT spqh.queue_name',
'      ,spqh.work_queue_header_id',
'FROM   spi_work_queue_header spqh',
'WHERE  spqh.queue_type = :P10_QUEUE_TYPE',
'AND    spqh.username = :APP_USER',
'AND    spqh.queue_name IS NOT NULL'))
,p_lov_display_null=>'YES'
,p_lov_null_text=>'- Select Queue -'
,p_lov_cascade_parent_items=>'P10_QUEUE_TYPE'
,p_ajax_items_to_submit=>'P10_QUEUE_TYPE'
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
 p_id=>wwv_flow_api.id(62098021860062123)
,p_name=>'P10_PUB_SRC'
,p_item_sequence=>45
,p_item_plug_id=>wwv_flow_api.id(62095286994062121)
,p_prompt=>'Publisher / Source'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_lov=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT ps.source_code||'' - ''||ps.description "Description"',
'      ,ps.source_id "Source ID"',
'FROM   prop_sources ps',
'WHERE  ps.source_id IN (SELECT DISTINCT source_id',
'                        FROM   spi_transaction_status sts',
'                        WHERE  sts.transaction_status IN (20,30,40)',
'                        AND    :P10_QUEUE_TYPE = 2',
'                        UNION',
'                        SELECT DISTINCT source_id',
'                        FROM   spi_transaction_status sts',
'                        WHERE  sts.transaction_status IN (40,50)',
'                        AND    :P10_QUEUE_TYPE = 1',
'                        UNION',
'                        SELECT DISTINCT source_id',
'                        FROM   spi_transaction_status sts',
'                        WHERE  sts.transaction_status = 60',
'                        AND    :P10_QUEUE_TYPE = 3',
'                        UNION',
'                        SELECT DISTINCT source_id',
'                        FROM   spi_transaction_status sts',
'                        WHERE  sts.transaction_status = 62',
'                        AND    :P10_QUEUE_TYPE = 4',
'                        UNION',
'                        SELECT DISTINCT source_id',
'                        FROM   spi_transaction_status sts',
'                        WHERE  sts.transaction_status = 65',
'                        AND    :P10_QUEUE_TYPE = 6',
'                       )',
'ORDER BY ps.source_code'))
,p_lov_display_null=>'YES'
,p_lov_null_text=>'-select-'
,p_lov_cascade_parent_items=>'P10_QUEUE_TYPE'
,p_ajax_items_to_submit=>'P10_QUEUE_TYPE'
,p_ajax_optimize_refresh=>'Y'
,p_cHeight=>1
,p_colspan=>4
,p_grid_column=>1
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51870006101240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(62099229452062124)
,p_name=>'P10_TO_DO_COUNT'
,p_item_sequence=>65
,p_item_plug_id=>wwv_flow_api.id(62095286994062121)
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
 p_id=>wwv_flow_api.id(62099699180062124)
,p_name=>'P10_FILE_NAME'
,p_item_sequence=>75
,p_item_plug_id=>wwv_flow_api.id(62095286994062121)
,p_prompt=>'File Name'
,p_display_as=>'NATIVE_POPUP_LOV'
,p_lov=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT DISTINCT FILE_NAME D,',
'       file_name R',
'       --DISTINCT batch_id||''-''||FILE_NAME D,',
'      -- batch_id R',
'FROM   spi_transaction_status sts',
'WHERE  sts.transaction_status IN (40,50)',
'AND    :P10_QUEUE_TYPE = 1',
'AND    NVL(sts.source_id, ''-1'') = NVL(:P10_PUB_SRC, NVL(sts.source_id, ''-1''))',
'UNION',
'SELECT DISTINCT FILE_NAME D, ',
'       file_name R ',
'       --DISTINCT batch_id||''-''||FILE_NAME D,',
'       --batch_id R',
'FROM   spi_transaction_status sts',
'WHERE  sts.transaction_status IN (20,30,40)',
'AND    :P10_QUEUE_TYPE = 2',
'AND    NVL(sts.source_id, ''-1'') = NVL(:P10_PUB_SRC, NVL(sts.source_id, ''-1''))',
'UNION',
'SELECT DISTINCT FILE_NAME D,',
'       file_name R ',
'        --DISTINCT batch_id||''-''||FILE_NAME D,',
'        --batch_id R',
'FROM   spi_transaction_status sts',
'WHERE  sts.transaction_status IN (60)',
'AND    :P10_QUEUE_TYPE = 3',
'AND    NVL(sts.source_id, ''-1'') = NVL(:P10_PUB_SRC, NVL(sts.source_id, ''-1''))',
'UNION',
'SELECT DISTINCT FILE_NAME D,',
'       file_name R ',
'       --DISTINCT batch_id||''-''||FILE_NAME D,',
'       --batch_id R',
'FROM   spi_transaction_status sts',
'WHERE  sts.transaction_status = 62',
'AND    :P10_QUEUE_TYPE = 4',
'AND    NVL(sts.source_id, ''-1'') = NVL(:P10_PUB_SRC, NVL(sts.source_id, ''-1''))',
'UNION',
'SELECT  DISTINCT FILE_NAME D,',
'       file_name R ',
'       --DISTINCT batch_id||''-''||FILE_NAME D,',
'       --batch_id R',
'FROM   spi_transaction_status sts',
'WHERE  sts.transaction_status = 65',
'AND    :P10_QUEUE_TYPE = 5',
'AND    NVL(sts.source_id, ''-1'') = NVL(:P10_PUB_SRC, NVL(sts.source_id, ''-1''))'))
,p_lov_display_null=>'YES'
,p_lov_null_text=>'-select-'
,p_lov_cascade_parent_items=>'P10_QUEUE_TYPE,P10_PUB_SRC'
,p_ajax_items_to_submit=>'P10_QUEUE_TYPE,P10_PUB_SRC'
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
 p_id=>wwv_flow_api.id(62100058962062124)
,p_name=>'P10_SUBMISSION_DATE_FROM'
,p_item_sequence=>95
,p_item_plug_id=>wwv_flow_api.id(62095286994062121)
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
 p_id=>wwv_flow_api.id(62100403808062124)
,p_name=>'P10_SUBMISSION_DATE_TO'
,p_item_sequence=>105
,p_item_plug_id=>wwv_flow_api.id(62095286994062121)
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
 p_id=>wwv_flow_api.id(62100825907062125)
,p_name=>'P10_DATE_SORT_ORDER'
,p_item_sequence=>115
,p_item_plug_id=>wwv_flow_api.id(62095286994062121)
,p_prompt=>'Sort Order'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_lov=>'STATIC2:Oldest;O,Newest;N'
,p_lov_display_null=>'YES'
,p_lov_null_text=>'- Select Sort Order -'
,p_cHeight=>1
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_column=>8
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(62101274466062125)
,p_name=>'P10_PARTY_NAME'
,p_item_sequence=>125
,p_item_plug_id=>wwv_flow_api.id(62095286994062121)
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
 p_id=>wwv_flow_api.id(62101690552062125)
,p_name=>'P10_PARTY_IPI_NUMBER'
,p_item_sequence=>135
,p_item_plug_id=>wwv_flow_api.id(62095286994062121)
,p_prompt=>'Party IPI#'
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
 p_id=>wwv_flow_api.id(62102088556062126)
,p_name=>'P10_PARTY_AFFILIATION'
,p_item_sequence=>145
,p_item_plug_id=>wwv_flow_api.id(62095286994062121)
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
 p_id=>wwv_flow_api.id(62102487557062126)
,p_name=>'P10_SORT_ORDER8'
,p_item_sequence=>155
,p_item_plug_id=>wwv_flow_api.id(62095286994062121)
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
 p_id=>wwv_flow_api.id(62102860225062126)
,p_name=>'P10_PROPERTY_PARTY'
,p_item_sequence=>165
,p_item_plug_id=>wwv_flow_api.id(62095286994062121)
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
 p_id=>wwv_flow_api.id(62103208814062127)
,p_name=>'P10_SORT_ORDER2'
,p_item_sequence=>175
,p_item_plug_id=>wwv_flow_api.id(62095286994062121)
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
 p_id=>wwv_flow_api.id(62103668243062127)
,p_name=>'P10_WRI_PUB_INDICATOR'
,p_item_sequence=>185
,p_item_plug_id=>wwv_flow_api.id(62095286994062121)
,p_prompt=>'Writer/Pub Indicator'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_lov=>'STATIC2:Writers;W,Publishers;P'
,p_lov_display_null=>'YES'
,p_lov_null_text=>'- Select Writers/Publishers -'
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
 p_id=>wwv_flow_api.id(62104030309062127)
,p_name=>'P10_SORT_ORDER3'
,p_item_sequence=>195
,p_item_plug_id=>wwv_flow_api.id(62095286994062121)
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
 p_id=>wwv_flow_api.id(62104454163062127)
,p_name=>'P10_SORT_ORDER4'
,p_item_sequence=>205
,p_item_plug_id=>wwv_flow_api.id(62095286994062121)
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
 p_id=>wwv_flow_api.id(62104805260062128)
,p_name=>'P10_SONG_REQUEST_ID'
,p_item_sequence=>215
,p_item_plug_id=>wwv_flow_api.id(62095286994062121)
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
 p_id=>wwv_flow_api.id(62105270509062128)
,p_name=>'P10_SORT_ORDER5'
,p_item_sequence=>225
,p_item_plug_id=>wwv_flow_api.id(62095286994062121)
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
 p_id=>wwv_flow_api.id(62105676159062128)
,p_name=>'P10_SUBMITTER_WORK_NO'
,p_item_sequence=>235
,p_item_plug_id=>wwv_flow_api.id(62095286994062121)
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
,p_inline_help_text=>'User can enter upto 50 values separated by commas.'
,p_attribute_01=>'Y'
,p_attribute_02=>'N'
,p_attribute_03=>'N'
,p_attribute_04=>'BOTH'
,p_item_comment=>'<b>User can enter upto 50 values separated by commas.</b>'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(62106048338062128)
,p_name=>'P10_PROP_ID_HFA_CODE'
,p_item_sequence=>245
,p_item_plug_id=>wwv_flow_api.id(62095286994062121)
,p_prompt=>'SESAC Property Number'
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
 p_id=>wwv_flow_api.id(62106437019062129)
,p_name=>'P10_SONG_TITLE'
,p_item_sequence=>255
,p_item_plug_id=>wwv_flow_api.id(62095286994062121)
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
 p_id=>wwv_flow_api.id(62106804238062129)
,p_name=>'P10_ISRC_REC_INFO'
,p_item_sequence=>265
,p_item_plug_id=>wwv_flow_api.id(62095286994062121)
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
 p_id=>wwv_flow_api.id(62107225906062129)
,p_name=>'P10_SORT_ORDER6'
,p_item_sequence=>275
,p_item_plug_id=>wwv_flow_api.id(62095286994062121)
,p_prompt=>'Sort Order'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_lov=>'STATIC2:ISRCs FIRST;Y,NO ISRCs FIRST;N'
,p_lov_display_null=>'YES'
,p_lov_null_text=>'- Select Sort Order -'
,p_cHeight=>1
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_column=>5
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(62107645421062129)
,p_name=>'P10_JINGLE_INDICATOR'
,p_item_sequence=>285
,p_item_plug_id=>wwv_flow_api.id(62095286994062121)
,p_prompt=>'Jingle Indicator'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_lov=>'STATIC2:Yes;Y,No;N'
,p_lov_display_null=>'YES'
,p_lov_null_text=>'- Select Jingle Indicator -'
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
 p_id=>wwv_flow_api.id(62108076277062130)
,p_name=>'P10_SORT_ORDER7'
,p_item_sequence=>295
,p_item_plug_id=>wwv_flow_api.id(62095286994062121)
,p_prompt=>'Sort Order'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_lov=>'STATIC2:JINGLE''S FIRST;Y,NO JINGLE''S FIRST;N'
,p_lov_display_null=>'YES'
,p_lov_null_text=>'- Select Sort Order -'
,p_cHeight=>1
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_column=>5
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(62108457890062130)
,p_name=>'P10_MATCH_SCORE'
,p_item_sequence=>305
,p_item_plug_id=>wwv_flow_api.id(62095286994062121)
,p_prompt=>'Match Score'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_lov=>'STATIC2:Ascending;A,Descending;D'
,p_lov_display_null=>'YES'
,p_lov_null_text=>'- Select Sort Order -'
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
 p_id=>wwv_flow_api.id(62108865965062130)
,p_name=>'P10_QUEUE_NAME'
,p_item_sequence=>25
,p_item_plug_id=>wwv_flow_api.id(62095286994062121)
,p_prompt=>'Saved Queue Criteria'
,p_display_as=>'NATIVE_TEXT_FIELD'
,p_cSize=>30
,p_cMaxlength=>4000
,p_begin_on_new_line=>'N'
,p_colspan=>4
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
 p_id=>wwv_flow_api.id(80888190097218410)
,p_name=>'P10_TRANS_TYPE'
,p_item_sequence=>85
,p_item_plug_id=>wwv_flow_api.id(62095286994062121)
,p_prompt=>'Transaction  Type'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_lov=>'STATIC:All;A,NWR;NWR,REV;REV'
,p_cHeight=>1
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_column=>5
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'NO'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_validation(
 p_id=>wwv_flow_api.id(62127668004062147)
,p_validation_name=>'Duplicate Queue Name'
,p_validation_sequence=>10
,p_validation=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'  l_count_queue   NUMBER;',
'',
'BEGIN',
'  SELECT COUNT(*)',
'  INTO   l_count_queue',
'  FROM   spi_work_queue_header',
'  WHERE  upper(trim(queue_name)) = upper(trim(:P10_QUEUE_NAME));',
'',
'  IF l_count_queue = 0',
'  THEN',
'    RETURN TRUE;',
'  ELSE',
'    RETURN FALSE;',
'  END IF; ',
'END;'))
,p_validation_type=>'FUNC_BODY_RETURNING_BOOLEAN'
,p_error_message=>'Queue Name in use.'
,p_always_execute=>'Y'
,p_validation_condition=>'P10_QUEUE_NAME'
,p_validation_condition_type=>'ITEM_IS_NOT_NULL'
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
);
wwv_flow_api.create_page_validation(
 p_id=>wwv_flow_api.id(34803833818762514)
,p_validation_name=>'Check Minimum Selection Criteria '
,p_validation_sequence=>20
,p_validation=>wwv_flow_string.join(wwv_flow_t_varchar2(
'BEGIN',
'    IF(:P10_AVAILABLE_QUEUES IS NULL AND :P10_QUEUE_NAME IS NULL AND :P10_PUB_SRC IS NULL AND :P10_FILE_NAME IS NULL AND :P10_SUBMISSION_DATE_FROM IS NULL AND :P10_SUBMISSION_DATE_TO IS NULL AND :P10_PARTY_NAME IS NULL AND :P10_PARTY_IPI_NUMBER IS NU'
||'LL AND :P10_SUBMITTER_WORK_NO IS NULL AND :P10_PROP_ID_HFA_CODE IS NULL AND :P10_SONG_TITLE IS NULL AND :P10_ISRC_REC_INFO IS NULL AND :P10_JINGLE_INDICATOR IS NULL) THEN',
'        :P10_QUEUE_TYPE := 0;',
'        RETURN FALSE;',
'    END IF;',
'END;'))
,p_validation_type=>'FUNC_BODY_RETURNING_BOOLEAN'
,p_error_message=>'Please enter some selection criteria.'
,p_validation_condition_type=>'NEVER'
,p_when_button_pressed=>wwv_flow_api.id(62096801533062122)
,p_error_display_location=>'INLINE_WITH_FIELD_AND_NOTIFICATION'
);
wwv_flow_api.create_page_validation(
 p_id=>wwv_flow_api.id(21346127763187448)
,p_validation_name=>'P10_PUB_SRC'
,p_validation_sequence=>30
,p_validation=>wwv_flow_string.join(wwv_flow_t_varchar2(
'BEGIN',
'    IF(:P10_PUB_SRC IS NULL) THEN',
'     --   :P10_QUEUE_TYPE := 0;',
'        RETURN FALSE;    ',
'    END IF;',
'END;'))
,p_validation_type=>'FUNC_BODY_RETURNING_BOOLEAN'
,p_error_message=>'Please select a Publisher/Source.'
,p_validation_condition=>'P10_QUEUE_TYPE'
,p_validation_condition2=>'6'
,p_validation_condition_type=>'VAL_OF_ITEM_IN_COND_NOT_EQ_COND2'
,p_when_button_pressed=>wwv_flow_api.id(62096801533062122)
,p_associated_item=>wwv_flow_api.id(62098021860062123)
,p_error_display_location=>'INLINE_WITH_FIELD_AND_NOTIFICATION'
);
wwv_flow_api.create_page_validation(
 p_id=>wwv_flow_api.id(36666529459113414)
,p_validation_name=>'never Queue Type 6'
,p_validation_sequence=>40
,p_validation=>wwv_flow_string.join(wwv_flow_t_varchar2(
'IF :P10_TYPE IS NULL THEN ',
'    RETURN FALSE;',
'END IF;'))
,p_validation_type=>'FUNC_BODY_RETURNING_BOOLEAN'
,p_error_message=>'Please select Type'
,p_validation_condition_type=>'NEVER'
,p_when_button_pressed=>wwv_flow_api.id(62096801533062122)
,p_associated_item=>wwv_flow_api.id(36666494755113413)
,p_error_display_location=>'INLINE_WITH_FIELD_AND_NOTIFICATION'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(33331310251548137)
,p_name=>'Work Queue Progress - Set value - Queue Type'
,p_event_sequence=>1
,p_triggering_element_type=>'ITEM'
,p_triggering_element=>'P10_QUEUE_TYPE'
,p_bind_type=>'bind'
,p_bind_event_type=>'change'
,p_display_when_type=>'EXISTS'
,p_display_when_cond=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT 1',
'FROM spi_work_queue_header',
'WHERE NVL(active_flag,''X'') = ''Y''',
'AND username = V(''APP_USER'')',
'AND status  = 10'))
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(33331412130548138)
,p_event_id=>wwv_flow_api.id(33331310251548137)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
'BEGIN',
'    IF :P10_QUEUE_TYPE <> 0 THEN',
'        :P10_QUEUE_TYPE := 0;',
'    END IF;',
'END;'))
,p_attribute_02=>'P10_QUEUE_TYPE'
,p_attribute_03=>'P10_QUEUE_TYPE'
,p_attribute_04=>'Y'
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(62131978005062153)
,p_name=>'on CheckAll on Report'
,p_event_sequence=>4
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'#selectunselectall'
,p_bind_type=>'bind'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(62132478659062153)
,p_event_id=>wwv_flow_api.id(62131978005062153)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
'$(''input:checkbox[name="f01"]'').each(function(){',
'if($(''#selectunselectall'').is('':checked''))',
'           this.checked = true;',
'        else this.checked = false; ',
'      });'))
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(62137986171062157)
,p_name=>'Clear Filters On Change Of Queue Selection'
,p_event_sequence=>10
,p_triggering_element_type=>'ITEM'
,p_triggering_element=>'P10_QUEUE_TYPE'
,p_bind_type=>'bind'
,p_bind_event_type=>'change'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(62138493731062157)
,p_event_id=>wwv_flow_api.id(62137986171062157)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_CLEAR'
,p_affected_elements_type=>'ITEM'
,p_affected_elements=>'P10_AVAILABLE_QUEUES,P10_PUB_SRC,P10_TO_DO_COUNT,P10_FILE_NAME,P10_SUBMISSION_DATE_FROM,P10_SUBMISSION_DATE_TO,P10_DATE_SORT_ORDER,P10_PARTY_NAME,P10_PARTY_IPI_NUMBER,P10_PARTY_AFFILIATION,P10_SORT_ORDER8,P10_PROPERTY_PARTY,P10_SORT_ORDER2,P10_WRI_PU'
||'B_INDICATOR,P10_SORT_ORDER3,P10_SORT_ORDER4,P10_SONG_REQUEST_ID,P10_SORT_ORDER5,P10_SUBMITTER_WORK_NO,P10_PROP_ID_HFA_CODE,P10_SONG_TITLE,P10_ISRC_REC_INFO,P10_SORT_ORDER6,P10_JINGLE_INDICATOR,P10_SORT_ORDER7,P10_MATCH_SCORE,P10_QUEUE_NAME'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(62137046753062156)
,p_name=>'Hide & Show Filters : Current Queue'
,p_event_sequence=>20
,p_triggering_element_type=>'ITEM'
,p_triggering_element=>'P10_QUEUE_TYPE'
,p_condition_element=>'P10_QUEUE_TYPE'
,p_triggering_condition_type=>'EQUALS'
,p_triggering_expression=>'0'
,p_bind_type=>'bind'
,p_bind_event_type=>'change'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(7578228207132038)
,p_event_id=>wwv_flow_api.id(62137046753062156)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_SHOW'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(62125154455062144)
,p_attribute_01=>'N'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(62137523297062156)
,p_event_id=>wwv_flow_api.id(62137046753062156)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_HIDE'
,p_affected_elements_type=>'ITEM'
,p_affected_elements=>'P10_AVAILABLE_QUEUES,P10_PUB_SRC,P10_TO_DO_COUNT,P10_FILE_NAME,P10_SUBMISSION_DATE_FROM,P10_SUBMISSION_DATE_TO,P10_DATE_SORT_ORDER,P10_PARTY_NAME,P10_PARTY_IPI_NUMBER,P10_PARTY_AFFILIATION,P10_SORT_ORDER8,P10_PROPERTY_PARTY,P10_SORT_ORDER2,P10_WRI_PU'
||'B_INDICATOR,P10_SORT_ORDER3,P10_MATCH_SCORE,P10_SORT_ORDER4,P10_SONG_REQUEST_ID,P10_SORT_ORDER5,P10_SUBMITTER_WORK_NO,P10_PROP_ID_HFA_CODE,P10_SONG_TITLE,P10_ISRC_REC_INFO,P10_SORT_ORDER6,P10_JINGLE_INDICATOR,P10_SORT_ORDER7,P10_QUEUE_NAME'
,p_attribute_01=>'N'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(7576908155132025)
,p_event_id=>wwv_flow_api.id(62137046753062156)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_HIDE'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(62095286994062121)
,p_attribute_01=>'N'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(62132804414062153)
,p_name=>'Hide & Show Filters : Party Match'
,p_event_sequence=>30
,p_triggering_element_type=>'ITEM'
,p_triggering_element=>'P10_QUEUE_TYPE'
,p_condition_element=>'P10_QUEUE_TYPE'
,p_triggering_condition_type=>'IN_LIST'
,p_triggering_expression=>'2,4,6'
,p_bind_type=>'bind'
,p_bind_event_type=>'change'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(7577147999132027)
,p_event_id=>wwv_flow_api.id(62132804414062153)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_SHOW'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(62095286994062121)
,p_attribute_01=>'N'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(62133370193062154)
,p_event_id=>wwv_flow_api.id(62132804414062153)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_SHOW'
,p_affected_elements_type=>'ITEM'
,p_affected_elements=>'P10_AVAILABLE_QUEUES,P10_QUEUE_NAME,P10_PUB_SRC,P10_FILE_NAME,P10_SUBMISSION_DATE_FROM,P10_SUBMISSION_DATE_TO,P10_DATE_SORT_ORDER,P10_PARTY_NAME,P10_PARTY_IPI_NUMBER,P10_PARTY_AFFILIATION,P10_SORT_ORDER8,P10_PROPERTY_PARTY,P10_SORT_ORDER2,P10_WRI_PUB'
||'_INDICATOR,P10_SORT_ORDER3,P10_MATCH_SCORE,P10_SORT_ORDER4,P10_SUBMITTER_WORK_NO'
,p_attribute_01=>'N'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(62133805190062154)
,p_event_id=>wwv_flow_api.id(62132804414062153)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_HIDE'
,p_affected_elements_type=>'ITEM'
,p_affected_elements=>'P10_SONG_REQUEST_ID,P10_SORT_ORDER5,P10_PROP_ID_HFA_CODE,P10_SONG_TITLE,P10_ISRC_REC_INFO,P10_SORT_ORDER6,P10_JINGLE_INDICATOR,P10_SORT_ORDER7,P10_HOLD_VALUE'
,p_attribute_01=>'N'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(7578496949132040)
,p_event_id=>wwv_flow_api.id(62132804414062153)
,p_event_result=>'TRUE'
,p_action_sequence=>40
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_HIDE'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(62125154455062144)
,p_attribute_01=>'N'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(62134229520062154)
,p_name=>'Hide & Show Filters : Property Match'
,p_event_sequence=>40
,p_triggering_element_type=>'ITEM'
,p_triggering_element=>'P10_QUEUE_TYPE'
,p_condition_element=>'P10_QUEUE_TYPE'
,p_triggering_condition_type=>'EQUALS'
,p_triggering_expression=>'1'
,p_bind_type=>'bind'
,p_bind_event_type=>'change'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(7577395415132029)
,p_event_id=>wwv_flow_api.id(62134229520062154)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_SHOW'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(62095286994062121)
,p_attribute_01=>'N'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(62135238905062155)
,p_event_id=>wwv_flow_api.id(62134229520062154)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_SHOW'
,p_affected_elements_type=>'ITEM'
,p_affected_elements=>'P10_AVAILABLE_QUEUES,P10_QUEUE_NAME,P10_PUB_SRC,P10_FILE_NAME,P10_SUBMISSION_DATE_FROM,P10_SUBMISSION_DATE_TO,P10_DATE_SORT_ORDER,P10_PARTY_NAME,P10_PARTY_IPI_NUMBER,P10_MATCH_SCORE,P10_SORT_ORDER4,P10_SONG_REQUEST_ID,P10_SORT_ORDER5,P10_SUBMITTER_WO'
||'RK_NO,P10_PROP_ID_HFA_CODE,P10_SONG_TITLE,P10_ISRC_REC_INFO,P10_SORT_ORDER6,P10_JINGLE_INDICATOR,P10_SORT_ORDER7'
,p_attribute_01=>'N'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(62134724251062155)
,p_event_id=>wwv_flow_api.id(62134229520062154)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_HIDE'
,p_affected_elements_type=>'ITEM'
,p_affected_elements=>'P10_PARTY_AFFILIATION,P10_SORT_ORDER8,P10_PROPERTY_PARTY,P10_SORT_ORDER2,P10_WRI_PUB_INDICATOR,P10_SORT_ORDER,P10_HOLD_VALUE'
,p_attribute_01=>'N'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(7578585618132041)
,p_event_id=>wwv_flow_api.id(62134229520062154)
,p_event_result=>'TRUE'
,p_action_sequence=>40
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_HIDE'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(62125154455062144)
,p_attribute_01=>'N'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(62135665010062155)
,p_name=>'Hide & Show Filters : Property Resolution'
,p_event_sequence=>50
,p_triggering_element_type=>'ITEM'
,p_triggering_element=>'P10_QUEUE_TYPE'
,p_condition_element=>'P10_QUEUE_TYPE'
,p_triggering_condition_type=>'IN_LIST'
,p_triggering_expression=>'3,5'
,p_bind_type=>'bind'
,p_bind_event_type=>'change'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(7577597182132031)
,p_event_id=>wwv_flow_api.id(62135665010062155)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_SHOW'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(62095286994062121)
,p_attribute_01=>'N'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(62136615754062156)
,p_event_id=>wwv_flow_api.id(62135665010062155)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_SHOW'
,p_affected_elements_type=>'ITEM'
,p_affected_elements=>'P10_AVAILABLE_QUEUES,P10_QUEUE_NAME,P10_PUB_SRC,P10_FILE_NAME,P10_SUBMISSION_DATE_FROM,P10_SUBMISSION_DATE_TO,P10_DATE_SORT_ORDER,P10_PARTY_NAME,P10_PARTY_IPI_NUMBER,P10_MATCH_SCORE,P10_SORT_ORDER4,P10_SONG_REQUEST_ID,P10_SORT_ORDER5,P10_SUBMITTER_WO'
||'RK_NO,P10_PROP_ID_HFA_CODE,P10_SONG_TITLE,P10_ISRC_REC_INFO,P10_SORT_ORDER6,P10_JINGLE_INDICATOR,P10_SORT_ORDER7,P10_HOLD_VALUE'
,p_attribute_01=>'N'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(62136153461062156)
,p_event_id=>wwv_flow_api.id(62135665010062155)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_HIDE'
,p_affected_elements_type=>'ITEM'
,p_affected_elements=>'P10_PARTY_AFFILIATION,P10_SORT_ORDER8,P10_PROPERTY_PARTY,P10_SORT_ORDER2,P10_WRI_PUB_INDICATOR,P10_SORT_ORDER3'
,p_attribute_01=>'N'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(7578671373132042)
,p_event_id=>wwv_flow_api.id(62135665010062155)
,p_event_result=>'TRUE'
,p_action_sequence=>40
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_HIDE'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(62125154455062144)
,p_attribute_01=>'N'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(33529551612128232)
,p_event_id=>wwv_flow_api.id(62135665010062155)
,p_event_result=>'TRUE'
,p_action_sequence=>50
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_HIDE'
,p_affected_elements_type=>'ITEM'
,p_affected_elements=>'P10_MATCH_SCORE'
,p_attribute_01=>'N'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(62129189234062150)
,p_name=>'Hide Sort Order'
,p_event_sequence=>60
,p_triggering_element_type=>'ITEM'
,p_triggering_element=>'P10_ISRC_REC_INFO'
,p_condition_element=>'P10_ISRC_REC_INFO'
,p_triggering_condition_type=>'NOT_NULL'
,p_bind_type=>'bind'
,p_bind_event_type=>'change'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(62129698889062151)
,p_event_id=>wwv_flow_api.id(62129189234062150)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_HIDE'
,p_affected_elements_type=>'ITEM'
,p_affected_elements=>'P10_SORT_ORDER6'
,p_attribute_01=>'N'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(62130115919062152)
,p_event_id=>wwv_flow_api.id(62129189234062150)
,p_event_result=>'FALSE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_SHOW'
,p_affected_elements_type=>'ITEM'
,p_affected_elements=>'P10_SORT_ORDER6'
,p_attribute_01=>'N'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(62130541603062152)
,p_name=>'Hide Sort Order'
,p_event_sequence=>70
,p_triggering_element_type=>'ITEM'
,p_triggering_element=>'P10_JINGLE_INDICATOR'
,p_condition_element=>'P10_JINGLE_INDICATOR'
,p_triggering_condition_type=>'NOT_NULL'
,p_bind_type=>'bind'
,p_bind_event_type=>'change'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(62131058773062152)
,p_event_id=>wwv_flow_api.id(62130541603062152)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_HIDE'
,p_affected_elements_type=>'ITEM'
,p_affected_elements=>'P10_SORT_ORDER7'
,p_attribute_01=>'N'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(62131550312062152)
,p_event_id=>wwv_flow_api.id(62130541603062152)
,p_event_result=>'FALSE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_SHOW'
,p_affected_elements_type=>'ITEM'
,p_affected_elements=>'P10_SORT_ORDER7'
,p_attribute_01=>'N'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(62138888748062157)
,p_name=>'Set Parameters On Selection Of Existing Queue'
,p_event_sequence=>80
,p_triggering_element_type=>'ITEM'
,p_triggering_element=>'P10_AVAILABLE_QUEUES'
,p_condition_element=>'P10_AVAILABLE_QUEUES'
,p_triggering_condition_type=>'NOT_NULL'
,p_bind_type=>'bind'
,p_bind_event_type=>'change'
,p_display_when_type=>'EXISTS'
,p_display_when_cond=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT 1',
'FROM   spi_work_queue_header',
'WHERE  work_queue_header_id = :P10_AVAILABLE_QUEUES',
'AND    queue_type <> 2'))
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(62139307332062158)
,p_event_id=>wwv_flow_api.id(62138888748062157)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
'BEGIN',
'',
'  BEGIN',
'    SELECT swqp.parameter_value',
'    INTO   :P10_PUB_SRC',
'    FROM   spi_work_queue_params swqp',
'    WHERE  swqp.work_queue_header_id = :P10_AVAILABLE_QUEUES',
'    AND    swqp.parameter_name = ''Publisher / Source'';',
'',
'  EXCEPTION',
'    WHEN NO_DATA_FOUND',
'    THEN',
'      :P10_PUB_SRC := NULL;',
'  END;',
'',
'  BEGIN',
'    SELECT swqp.parameter_value',
'    INTO   :P10_FILE_NAME',
'    FROM   spi_work_queue_params swqp',
'    WHERE  swqp.work_queue_header_id = :P10_AVAILABLE_QUEUES',
'    AND    swqp.parameter_name = ''File Name'';',
'',
'  EXCEPTION',
'    WHEN NO_DATA_FOUND',
'    THEN',
'      :P10_FILE_NAME := NULL;',
'  END;',
'',
'  BEGIN',
'    SELECT swqp.parameter_value',
'    INTO   :P10_PARTY_NAME',
'    FROM   spi_work_queue_params swqp',
'    WHERE  swqp.work_queue_header_id = :P10_AVAILABLE_QUEUES',
'    AND    swqp.parameter_name = ''Party Name'';',
'',
'  EXCEPTION',
'    WHEN NO_DATA_FOUND',
'    THEN',
'      :P10_PARTY_NAME := NULL;',
'  END;',
'',
'  BEGIN',
'    SELECT swqp.parameter_value',
'          ,swqp.parameter_sort',
'    INTO   :P10_SUBMISSION_DATE_FROM',
'          ,:P10_DATE_SORT_ORDER',
'    FROM   spi_work_queue_params swqp',
'    WHERE  swqp.work_queue_header_id = :P10_AVAILABLE_QUEUES',
'    AND    swqp.parameter_name = ''Submission Date From'';',
'',
'  EXCEPTION',
'    WHEN NO_DATA_FOUND',
'    THEN',
'      :P10_SUBMISSION_DATE_FROM := NULL;',
'      :P10_DATE_SORT_ORDER := NULL;',
'  END;',
'',
'  BEGIN',
'    SELECT swqp.parameter_value',
'          ,swqp.parameter_sort',
'    INTO   :P10_SUBMISSION_DATE_TO',
'          ,:P10_DATE_SORT_ORDER',
'    FROM   spi_work_queue_params swqp',
'    WHERE  swqp.work_queue_header_id = :P10_AVAILABLE_QUEUES',
'    AND    swqp.parameter_name = ''Submission Date To'';',
'',
'  EXCEPTION',
'    WHEN NO_DATA_FOUND',
'    THEN',
'      :P10_SUBMISSION_DATE_TO := NULL;',
'      :P10_DATE_SORT_ORDER := NULL;',
'  END;',
'',
'  BEGIN',
'    SELECT swqp.parameter_value',
'    INTO   :P10_PARTY_IPI_NUMBER',
'    FROM   spi_work_queue_params swqp',
'    WHERE  swqp.work_queue_header_id = :P10_AVAILABLE_QUEUES',
'    AND    swqp.parameter_name = ''Party IP / IPI#'';',
'',
'  EXCEPTION',
'    WHEN NO_DATA_FOUND',
'    THEN',
'      :P10_PARTY_IPI_NUMBER := NULL;',
'  END;',
'',
'  BEGIN',
'    SELECT swqp.parameter_value',
'    INTO   :P10_PARTY_AFFILIATION',
'    FROM   spi_work_queue_params swqp',
'    WHERE  swqp.work_queue_header_id = :P10_AVAILABLE_QUEUES',
'    AND    swqp.parameter_name = ''Party Affiliation'';',
'',
'  EXCEPTION',
'    WHEN NO_DATA_FOUND',
'    THEN',
'      :P10_PARTY_AFFILIATION := NULL;',
'  END;',
'',
'  BEGIN',
'    SELECT swqp.parameter_value',
'    INTO   :P10_PROPERTY_PARTY',
'    FROM   spi_work_queue_params swqp',
'    WHERE  swqp.work_queue_header_id = :P10_AVAILABLE_QUEUES',
'    AND    swqp.parameter_name = ''Property Count'';',
'',
'  EXCEPTION',
'    WHEN NO_DATA_FOUND',
'    THEN',
'      :P10_PROPERTY_PARTY := NULL;',
'  END;',
'',
'  BEGIN',
'    SELECT swqp.parameter_value',
'    INTO   :P10_WRI_PUB_INDICATOR',
'    FROM   spi_work_queue_params swqp',
'    WHERE  swqp.work_queue_header_id = :P10_AVAILABLE_QUEUES',
'    AND    swqp.parameter_name = ''Writer/Pub Indicator'';',
'',
'  EXCEPTION',
'    WHEN NO_DATA_FOUND',
'    THEN',
'      :P10_WRI_PUB_INDICATOR := NULL;',
'  END;',
'',
'  BEGIN',
'    SELECT swqp.parameter_value',
'    INTO   :P10_MATCH_SCORE',
'    FROM   spi_work_queue_params swqp',
'    WHERE  swqp.work_queue_header_id = :P10_AVAILABLE_QUEUES',
'    AND    swqp.parameter_name = ''Match Score'';',
'',
'  EXCEPTION',
'    WHEN NO_DATA_FOUND',
'    THEN',
'      :P10_MATCH_SCORE := NULL;',
'  END;',
'END;'))
,p_attribute_02=>'P10_AVAILABLE_QUEUES'
,p_attribute_03=>'P10_PUB_SRC,P10_FILE_NAME,P10_PARTY_NAME,P10_SUBMISSION_DATE_FROM,P10_DATE_SORT_ORDER,P10_SUBMISSION_DATE_TO,P10_PARTY_IPI_NUMBER,P10_PARTY_AFFILIATION,P10_PROPERTY_PARTY,P10_WRI_PUB_INDICATOR,P10_MATCH_SCORE'
,p_attribute_04=>'N'
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(62139833562062158)
,p_event_id=>wwv_flow_api.id(62138888748062157)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
'BEGIN',
'',
'  BEGIN',
'    SELECT swqp.parameter_value',
'    INTO   :P10_SUBMITTER_WORK_NO',
'    FROM   spi_work_queue_params swqp',
'    WHERE  swqp.work_queue_header_id = :P10_AVAILABLE_QUEUES',
'    AND    swqp.parameter_name = ''Submitter Work Number'';',
'',
'  EXCEPTION',
'    WHEN NO_DATA_FOUND',
'    THEN',
'      :P10_SUBMITTER_WORK_NO := NULL;',
'  END;',
'',
'  BEGIN',
'    SELECT swqp.parameter_value',
'    INTO   :P10_PROP_ID_HFA_CODE',
'    FROM   spi_work_queue_params swqp',
'    WHERE  swqp.work_queue_header_id = :P10_AVAILABLE_QUEUES',
'    AND    swqp.parameter_name = ''SESAC Property Number/HFA Song Code'';',
'',
'  EXCEPTION',
'    WHEN NO_DATA_FOUND',
'    THEN',
'      :P10_PROP_ID_HFA_CODE := NULL;',
'  END;',
'',
'  BEGIN',
'    SELECT swqp.parameter_value',
'    INTO   :P10_SONG_TITLE',
'    FROM   spi_work_queue_params swqp',
'    WHERE  swqp.work_queue_header_id = :P10_AVAILABLE_QUEUES',
'    AND    swqp.parameter_name = ''Song Title'';',
'',
'  EXCEPTION',
'    WHEN NO_DATA_FOUND',
'    THEN',
'      :P10_SONG_TITLE := NULL;',
'  END;',
'',
'  BEGIN',
'    SELECT swqp.parameter_value',
'          ,swqp.parameter_sort',
'    INTO   :P10_ISRC_REC_INFO',
'          ,:P10_SORT_ORDER6',
'    FROM   spi_work_queue_params swqp',
'    WHERE  swqp.work_queue_header_id = :P10_AVAILABLE_QUEUES',
'    AND    swqp.parameter_name = ''ISRC/Recording Info'';',
'',
'  EXCEPTION',
'    WHEN NO_DATA_FOUND',
'    THEN',
'      :P10_ISRC_REC_INFO := NULL;',
'     :P10_SORT_ORDER6   := NULL;',
'  END;',
'',
'  BEGIN',
'    SELECT swqp.parameter_value',
'          ,swqp.parameter_sort',
'    INTO   :P10_JINGLE_INDICATOR',
'          ,:P10_SORT_ORDER7',
'    FROM   spi_work_queue_params swqp',
'    WHERE  swqp.work_queue_header_id = :P10_AVAILABLE_QUEUES',
'    AND    swqp.parameter_name = ''Jingle Indicator'';',
'',
'  EXCEPTION',
'    WHEN NO_DATA_FOUND',
'    THEN',
'      :P10_JINGLE_INDICATOR := NULL;',
'      :P10_SORT_ORDER7      := NULL;',
'  END;',
'',
'END;'))
,p_attribute_02=>'P10_AVAILABLE_QUEUES'
,p_attribute_03=>'P10_SUBMITTER_WORK_NO,P10_PROP_ID_HFA_CODE,P10_SONG_TITLE,P10_ISRC_REC_INFO,P10_SORT_ORDER6,P10_JINGLE_INDICATOR,P10_SORT_ORDER7'
,p_attribute_04=>'N'
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
end;
/
begin
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(62140219806062159)
,p_name=>'Set Parameters On Selection Of Existing Queue (party)'
,p_event_sequence=>90
,p_triggering_element_type=>'ITEM'
,p_triggering_element=>'P10_AVAILABLE_QUEUES'
,p_condition_element=>'P10_AVAILABLE_QUEUES'
,p_triggering_condition_type=>'NOT_NULL'
,p_bind_type=>'bind'
,p_bind_event_type=>'change'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(62140766767062159)
,p_event_id=>wwv_flow_api.id(62140219806062159)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
'BEGIN',
'',
'  BEGIN',
'    SELECT swqp.parameter_value',
'    INTO   :P10_PUB_SRC',
'    FROM   spi_work_queue_params swqp',
'    WHERE  swqp.work_queue_header_id = :P10_AVAILABLE_QUEUES',
'    AND    swqp.parameter_name = ''Publisher / Source'';',
'',
'  EXCEPTION',
'    WHEN NO_DATA_FOUND',
'    THEN',
'      :P10_PUB_SRC := NULL;',
'  END;',
'',
'  BEGIN',
'    SELECT swqp.parameter_value',
'    INTO   :P10_FILE_NAME',
'    FROM   spi_work_queue_params swqp',
'    WHERE  swqp.work_queue_header_id = :P10_AVAILABLE_QUEUES',
'    AND    swqp.parameter_name = ''File Name'';',
'',
'  EXCEPTION',
'    WHEN NO_DATA_FOUND',
'    THEN',
'      :P10_FILE_NAME := NULL;',
'  END;',
'',
'  BEGIN',
'    SELECT swqp.parameter_value',
'    INTO   :P10_PARTY_NAME',
'    FROM   spi_work_queue_params swqp',
'    WHERE  swqp.work_queue_header_id = :P10_AVAILABLE_QUEUES',
'    AND    swqp.parameter_name = ''Party Name'';',
'',
'  EXCEPTION',
'    WHEN NO_DATA_FOUND',
'    THEN',
'      :P10_PARTY_NAME := NULL;',
'  END;',
'',
'  BEGIN',
'    SELECT swqp.parameter_value',
'          ,swqp.parameter_sort',
'    INTO   :P10_SUBMISSION_DATE_FROM',
'          ,:P10_DATE_SORT_ORDER',
'    FROM   spi_work_queue_params swqp',
'    WHERE  swqp.work_queue_header_id = :P10_AVAILABLE_QUEUES',
'    AND    swqp.parameter_name = ''Submission Date From'';',
'',
'  EXCEPTION',
'    WHEN NO_DATA_FOUND',
'    THEN',
'      :P10_SUBMISSION_DATE_FROM := NULL;',
'      :P10_DATE_SORT_ORDER := NULL;',
'  END;',
'',
'  BEGIN',
'    SELECT swqp.parameter_value',
'          ,swqp.parameter_sort',
'    INTO   :P10_SUBMISSION_DATE_TO',
'          ,:P10_DATE_SORT_ORDER',
'    FROM   spi_work_queue_params swqp',
'    WHERE  swqp.work_queue_header_id = :P10_AVAILABLE_QUEUES',
'    AND    swqp.parameter_name = ''Submission Date To'';',
'',
'  EXCEPTION',
'    WHEN NO_DATA_FOUND',
'    THEN',
'      :P10_SUBMISSION_DATE_TO := NULL;',
'      :P10_DATE_SORT_ORDER := NULL;',
'  END;',
'',
'  BEGIN',
'    SELECT swqp.parameter_value',
'    INTO   :P10_PARTY_IPI_NUMBER',
'    FROM   spi_work_queue_params swqp',
'    WHERE  swqp.work_queue_header_id = :P10_AVAILABLE_QUEUES',
'    AND    swqp.parameter_name = ''Party IP / IPI#'';',
'',
'  EXCEPTION',
'    WHEN NO_DATA_FOUND',
'    THEN',
'      :P10_PARTY_IPI_NUMBER := NULL;',
'  END;',
'',
'  BEGIN',
'    SELECT swqp.parameter_value',
'    INTO   :P10_PARTY_AFFILIATION',
'    FROM   spi_work_queue_params swqp',
'    WHERE  swqp.work_queue_header_id = :P10_AVAILABLE_QUEUES',
'    AND    swqp.parameter_name = ''Party Affiliation'';',
'',
'  EXCEPTION',
'    WHEN NO_DATA_FOUND',
'    THEN',
'      :P10_PARTY_AFFILIATION := NULL;',
'  END;',
'',
'  BEGIN',
'    SELECT swqp.parameter_value',
'    INTO   :P10_PROPERTY_PARTY',
'    FROM   spi_work_queue_params swqp',
'    WHERE  swqp.work_queue_header_id = :P10_AVAILABLE_QUEUES',
'    AND    swqp.parameter_name = ''Property Count'';',
'',
'  EXCEPTION',
'    WHEN NO_DATA_FOUND',
'    THEN',
'      :P10_PROPERTY_PARTY := NULL;',
'  END;',
'',
'  BEGIN',
'    SELECT swqp.parameter_value',
'    INTO   :P10_WRI_PUB_INDICATOR',
'    FROM   spi_work_queue_params swqp',
'    WHERE  swqp.work_queue_header_id = :P10_AVAILABLE_QUEUES',
'    AND    swqp.parameter_name = ''Writer/Pub Indicator'';',
'',
'  EXCEPTION',
'    WHEN NO_DATA_FOUND',
'    THEN',
'      :P10_WRI_PUB_INDICATOR := NULL;',
'  END;',
'',
'  BEGIN',
'    SELECT swqp.parameter_value',
'    INTO   :P10_MATCH_SCORE',
'    FROM   spi_work_queue_params swqp',
'    WHERE  swqp.work_queue_header_id = :P10_AVAILABLE_QUEUES',
'    AND    swqp.parameter_name = ''Match Score'';',
'',
'  EXCEPTION',
'    WHEN NO_DATA_FOUND',
'    THEN',
'      :P10_MATCH_SCORE := NULL;',
'  END;',
'END;'))
,p_attribute_02=>'P10_AVAILABLE_QUEUES'
,p_attribute_03=>'P10_PUB_SRC,P10_FILE_NAME,P10_PARTY_NAME,P10_SUBMISSION_DATE_FROM,P10_DATE_SORT_ORDER,P10_SUBMISSION_DATE_TO,P10_PARTY_IPI_NUMBER,P10_PARTY_AFFILIATION,P10_PROPERTY_PARTY,P10_WRI_PUB_INDICATOR,P10_MATCH_SCORE'
,p_attribute_04=>'N'
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(62141123012062159)
,p_name=>'Hide/Show Create WQ Button'
,p_event_sequence=>100
,p_triggering_element_type=>'ITEM'
,p_triggering_element=>'P10_QUEUE_TYPE'
,p_condition_element=>'P10_QUEUE_TYPE'
,p_triggering_condition_type=>'NULL'
,p_bind_type=>'bind'
,p_bind_event_type=>'change'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(62141690501062162)
,p_event_id=>wwv_flow_api.id(62141123012062159)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_HIDE'
,p_affected_elements_type=>'BUTTON'
,p_affected_button_id=>wwv_flow_api.id(62096801533062122)
,p_attribute_01=>'N'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(62142132033062162)
,p_event_id=>wwv_flow_api.id(62141123012062159)
,p_event_result=>'FALSE'
,p_action_sequence=>20
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_SHOW'
,p_affected_elements_type=>'BUTTON'
,p_affected_button_id=>wwv_flow_api.id(62096801533062122)
,p_attribute_01=>'N'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(62142543699062162)
,p_name=>'Queue Name Empty'
,p_event_sequence=>110
,p_triggering_element_type=>'BUTTON'
,p_triggering_button_id=>wwv_flow_api.id(62096801533062122)
,p_bind_type=>'bind'
,p_bind_event_type=>'click'
,p_display_when_type=>'NEVER'
,p_da_event_comment=>'Disabled after Daryl''s Email'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(62143087406062163)
,p_event_id=>wwv_flow_api.id(62142543699062162)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
'var queue_name = $v(''P10_QUEUE_NAME'');',
'',
'if (!queue_name) {',
'    var res = confirm(''Do you want to proceed with empty queue name'');',
'    if (res == true)',
'        apex.submit(''INSERT'');',
'} else',
'    apex.submit(''INSERT'');'))
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(7577753766132033)
,p_name=>'Init Screen'
,p_event_sequence=>120
,p_bind_type=>'bind'
,p_bind_event_type=>'ready'
,p_display_when_type=>'NEVER'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(7577880077132034)
,p_event_id=>wwv_flow_api.id(7577753766132033)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_SHOW'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(62125154455062144)
,p_attribute_01=>'N'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(7578093918132036)
,p_event_id=>wwv_flow_api.id(7577753766132033)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_HIDE'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(62095286994062121)
,p_attribute_01=>'N'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(34376685302973328)
,p_name=>'never Hide & Show Filters : CA REP Aff'
,p_event_sequence=>130
,p_triggering_element_type=>'ITEM'
,p_triggering_element=>'P10_QUEUE_TYPE'
,p_condition_element=>'P10_QUEUE_TYPE'
,p_triggering_condition_type=>'EQUALS'
,p_triggering_expression=>'6'
,p_bind_type=>'bind'
,p_bind_event_type=>'change'
,p_display_when_type=>'NEVER'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(34376784886973329)
,p_event_id=>wwv_flow_api.id(34376685302973328)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_SHOW'
,p_affected_elements_type=>'ITEM'
,p_affected_elements=>'P10_AFFILIATE_REP,P10_TYPE'
,p_attribute_01=>'N'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(34376880925973330)
,p_event_id=>wwv_flow_api.id(34376685302973328)
,p_event_result=>'FALSE'
,p_action_sequence=>10
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_HIDE'
,p_affected_elements_type=>'ITEM'
,p_affected_elements=>'P10_AFFILIATE_REP,P10_TYPE'
,p_attribute_01=>'N'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(25109272668958230)
,p_name=>'SHOW_SPINNER'
,p_event_sequence=>140
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'.test'
,p_bind_type=>'live'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(25109343749958231)
,p_event_id=>wwv_flow_api.id(25109272668958230)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>'apex.widget.waitPopup();'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(11647801136834318)
,p_name=>'Set Priority flag on change'
,p_event_sequence=>150
,p_triggering_element_type=>'ITEM'
,p_triggering_element=>'P10_PRIORITY_FLAG'
,p_bind_type=>'bind'
,p_bind_event_type=>'change'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(11647977639834319)
,p_event_id=>wwv_flow_api.id(11647801136834318)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>'null;'
,p_attribute_02=>'P10_PRIORITY_FLAG'
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(62127930436062148)
,p_process_sequence=>10
,p_process_point=>'AFTER_SUBMIT'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'onClick Create Work Queue - Never'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'',
'  l_request_id      CLOB := null;',
'  l_work_header_id  NUMBER;',
'',
'',
'',
'BEGIN',
'  IF :P10_HEADER_ID IS NOT NULL',
'  THEN',
'    spi_work_queue_procs.spi_process_wq_details (:P10_HEADER_ID,''A'');',
'  END IF;',
'  INSERT INTO spi_work_queue_header',
'                                    (',
'                                      queue_type',
'                                     -- ,publisher_source',
'                                     -- ,file_id',
'                                      ,status',
'                                      ,username',
'                                      ,active_flag',
'                                    )',
'                                    VALUES',
'                                    (',
'                                      :p10_queue_type',
'                                     -- ,:p10_pub_src',
'                                     -- ,:p10_batch_id',
'                                      ,10',
'                                      ,v(''APP_USER'')',
'                                      ,''Y''',
'                                    ) RETURNING work_queue_header_id INTO l_work_header_id;',
'',
'    FOR i IN 1..apex_application.g_f01.count',
'    LOOP',
'      ',
'      l_request_id := l_request_id||'',''||apex_application.g_f01(i);',
'    ',
'    END LOOP;',
'    l_request_id := TRIM(LEADING '','' FROM l_request_id);',
'    COMMIT ;',
'',
'    DBMS_SCHEDULER.CREATE_JOB ( job_name               => ''SPI_WQ_DETAILS_''||l_work_header_id',
'                                 ,job_type             => ''PLSQL_BLOCK''',
'                                 ,job_action           => ''BEGIN',
'                                                             spi_work_queue_procs.spi_cre_work_queue_details ( ''''''',
'                                                                                                                        ||l_request_id',
'                                                                                                                        ||'''''',''''''',
'                                                                                                                        ||l_work_header_id',
'                                                                                                                        ||''''''',
'                                                                                                                       );',
'                                                           END;''',
'                                 ,start_date           => sysdate+1/24*1/60*1/12',
'                                 ,end_date             => SYSDATE+1/24',
'                                 ,enabled              => TRUE',
'                                 ,comments             => ''Inserts Work Queue Details''',
'                                );',
'',
'    --spi_cre_work_queue_details(l_request_id,l_work_header_id);',
'END;'))
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
,p_process_when_button_id=>wwv_flow_api.id(62096801533062122)
,p_process_when_type=>'NEVER'
,p_process_success_message=>'Work Queue has been created Successfully!'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(62128357934062149)
,p_process_sequence=>20
,p_process_point=>'AFTER_SUBMIT'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'onClick Create New Work Queue'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'  v_work_queue_status_code  NUMBER;',
'  l_header_id               NUMBER;',
'  l_app_user                VARCHAR2(100);',
'  l_loc                     NUMBER;',
'  v_other_parameters        job_fileload_schedule_pkg.other_parameters_rec;',
'  l_err_code                VARCHAR2(500);',
' BEGIN',
'   BEGIN',
'   ',
'',
'SELECT TO_NUMBER(lookup_code)',
'            ,v(''app_user'')',
'      INTO   v_work_queue_status_code',
'            ,l_app_user',
'      FROM   prop_lookups ps',
'      WHERE  ps.lookup_type = ''SPI_WORK_QUEUE_STATUS''',
'      AND    ps.meaning     = ''SCHEDULED''',
'      AND rownum =1 ;',
'',
'    EXCEPTION',
'      WHEN NO_DATA_FOUND THEN',
'        v_work_queue_status_code := 0;',
'    END;',
'',
'    l_loc := 10;',
'',
'    --spi_work_queue_procs.check_existing_queue(l_app_user);',
'    BEGIN',
'',
'      SELECT work_queue_header_id',
'      INTO   l_header_id',
'      FROM   spi_work_queue_header',
'      WHERE  username = v(''app_user'')',
'      AND    active_flag = ''Y'';',
'',
'      spi_work_queue_procs.spi_process_wq_details( l_header_id,',
'                                                   ''A''',
'                                                 );    ',
'      ------------',
'      -- Update the details in Job Central as user is forcefully stoppingjob',
'      ------------',
'--      job_fileload_schedule_pkg.stop_job(p_job_name =>''SPI_WQ_DETAILS_''||l_header_id);',
'                                                 ',
'    EXCEPTION',
'      WHEN OTHERS ',
'      THEN',
'        NULL;',
'    END;',
'	',
'    l_loc := 20;',
'BEGIN',
'    INSERT INTO spi_work_queue_header',
'    ( ',
'        queue_type',
'        ,status',
'        ,username',
'        ,active_flag',
'    )',
'    VALUES',
'    ( ',
'        :P10_QUEUE_TYPE',
'        ,v_work_queue_status_code',
'        ,v(''APP_USER'')',
'        ,''Y''',
'    ) RETURNING work_queue_header_id INTO l_header_id;',
'    EXCEPTION',
'    WHEN OTHERS THEN',
'    l_err_code := SQLERRM;',
'    END;',
'                                    ',
'    IF :P10_QUEUE_TYPE = ''2'' THEN',
'      :CURRENT_QUEUE_MENU := 20;',
'      :P20_HEADER_ID      := l_header_id;',
'    END IF;',
'    ',
'    IF :P10_QUEUE_TYPE = ''1'' THEN',
'      :CURRENT_QUEUE_MENU := 30;',
'      :P30_HEADER_ID      := l_header_id;',
'    END IF;',
'    ',
'    IF :P10_QUEUE_TYPE = ''3'' THEN',
'      :CURRENT_QUEUE_MENU := 90;',
'      :P90_HEADER_ID      := l_header_id;',
'    END IF;',
'    ',
'    IF :P10_QUEUE_TYPE = ''4'' THEN',
'      :CURRENT_QUEUE_MENU := 100;',
'      :P90_HEADER_ID      := l_header_id;',
'    END IF;    ',
'    ',
'    IF :P10_QUEUE_TYPE = ''5'' THEN',
'      :CURRENT_QUEUE_MENU := 110;',
'      :P90_HEADER_ID      := l_header_id;',
'    END IF;',
'    ',
'    IF :P10_QUEUE_TYPE = ''6'' THEN',
'      :CURRENT_QUEUE_MENU := 510;',
'      :P510_HEADER_ID      := l_header_id;',
'    END IF;  ',
'    ',
'    l_loc := 40;',
'',
'',
'    v_other_parameters.param1 := :P10_QUEUE_TYPE;',
'    v_other_parameters.param16 :=l_header_id;',
'    v_other_parameters.param2 := ''BEGIN UPDATE spi_work_queue_header SET status=10 WHERE work_queue_header_id = ''||l_header_id||'';COMMIT; spi_work_queue_procs.generate_work_queue ( ''''''',
'                                                               ||:P10_QUEUE_TYPE',
'                                                               ||'''''',''''''',
'                                                               ||:P10_PUB_SRC',
'                                                               ||'''''',''''''',
'                                                               ||:P10_SPI_TRANSACTION_ID',
'                                                               ||'''''',''''''',
'                                                               ||:P10_FILE_NAME',
'                                                               ||'''''',''''''',
'                                                               ||:P10_TRANS_TYPE',
'                                                               ||'''''',''''''',
'                                                               ||TO_DATE(:P10_SUBMISSION_DATE_FROM,''YYYY-MM-DD'')',
'                                                               ||'''''',''''''',
'                                                               ||TO_DATE(:P10_SUBMISSION_DATE_TO,''YYYY-MM-DD'')',
'                                                               ||'''''',''''''',
'                                                               ||:P10_DATE_SORT_ORDER',
'                                                               ||'''''',''''''',
'                                                               ||REPLACE(TRIM(:P10_PARTY_NAME),'''''''',''~@#$#!'')',
'                                                               ||'''''',''''''',
'                                                               ||:P10_PARTY_IPI_NUMBER',
'                                                               ||'''''',''''''',
'                                                               ||:P10_PARTY_AFFILIATION',
'                                                               ||'''''',''''''',
'                                                               ||:P10_PROPERTY_PARTY',
'                                                               ||'''''',''''''',
'                                                               ||:P10_WRI_PUB_INDICATOR',
'                                                               ||'''''',''''''',
'                                                               ||:P10_MATCH_SCORE',
'                                                               ||'''''',''''''',
'                                                               ||:P10_SONG_REQUEST_ID',
'                                                               ||'''''',''''''',
'                                                               ||:P10_SORT_ORDER5',
'                                                               ||'''''',''''''',
'                                                               ||:P10_SUBMITTER_WORK_NO',
'                                                               ||'''''',''''''',
'                                                               ||:P10_PROP_ID_HFA_CODE',
'                                                               ||'''''',''''''',
'                                                               ||REPLACE(TRIM(:P10_SONG_TITLE),'''''''',''~@#$#!'')',
'                                                               ||'''''',''''''',
'                                                               ||:P10_ISRC_REC_INFO',
'                                                               ||'''''',''''''',
'                                                               ||:P10_SORT_ORDER6',
'                                                               ||'''''',''''''',
'                                                               ||:P10_JINGLE_INDICATOR',
'                                                               ||'''''',''''''',
'                                                               ||:P10_SORT_ORDER7',
'                                                               ||'''''',''''''',
'                                                               ||:P10_QUEUE_NAME',
'                                                               ||'''''',''''''',
'                                                               ||l_app_user',
'                                                               ||'''''',''''''',
'                                                               ||:P10_AVAILABLE_QUEUES',
'                                                               ||'''''',''''''',
'                                                               ||l_header_id',
'                                                               ||'''''',''''''',
'                                                               ||null',
'                                                               ||'''''',''''''',
'                                                               ||null',
'                                                               ||'''''',''''''',
'                                                               ||:P10_PRIORITY_FLAG',
'                                                               ||'''''',''''''',
'                                                               ||:P10_HOLD_VALUE',
'                                                               ||''''''',
'                                                              );',
'                   END;'';',
'debug_write(v_other_parameters.param2, ''SPI_WORK_QUEUE_Generation: page 10'');',
'    ------------------------------',
'    -- Add this in Job Central',
'    ------------------------------',
'    job_fileload_schedule_pkg.load_schedule_details',
'    (',
'        p_job_type          => ''SPI_WORK_QUEUE'',',
'        p_schedule_date     => SYSDATE,',
'        p_action_by         => V(''APP_USER''),',
'        p_process_type      =>''L'',',
'        p_other_parameters  => v_other_parameters',
'    ); ',
'    ',
'END;'))
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
,p_process_when_button_id=>wwv_flow_api.id(62096801533062122)
,p_process_when=>'P10_QUEUE_TYPE'
,p_process_when_type=>'VAL_OF_ITEM_IN_COND_NOT_EQ_COND2'
,p_process_when2=>'6'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(29619076023493514)
,p_process_sequence=>30
,p_process_point=>'AFTER_SUBMIT'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'Create Work Queue for CA'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'  v_work_queue_status_code  NUMBER;',
'  l_header_id               NUMBER;',
'  l_app_user                VARCHAR2(100);',
'  l_loc                     NUMBER;',
'  v_other_parameters        job_fileload_schedule_pkg.other_parameters_rec;',
'  l_err_code                VARCHAR2(500);',
' BEGIN',
'   BEGIN',
'      SELECT TO_NUMBER(lookup_code)',
'            ,v(''app_user'')',
'      INTO   v_work_queue_status_code',
'            ,l_app_user',
'      FROM   prop_lookups ps',
'      WHERE  ps.lookup_type = ''SPI_WORK_QUEUE_STATUS''',
'      AND    ps.meaning     = ''SCHEDULED''',
'      AND rownum =1 ;',
'',
'    EXCEPTION',
'      WHEN NO_DATA_FOUND THEN',
'        v_work_queue_status_code := 0;',
'    END;',
'',
'    l_loc := 10;',
'',
'    ',
'	BEGIN',
'		SELECT 	wq_header_id',
'		INTO 	l_header_id',
'		FROM 	spi_ot_work_queues_header',
'		WHERE 	wq_type = 6',
'		AND 	username = :P120_AFFILIATE',
'		AND 	active_flag = ''Y'';',
'	EXCEPTION WHEN NO_DATA_FOUND THEN',
'		INSERT INTO spi_ot_work_queues_header 	(	wq_type,',
'													username,',
'													status,',
'													active_flag',
'												)',
'										VALUES	(	6,',
'													:APP_USER,',
'													v_work_queue_status_code,',
'													''Y''',
'												)RETURNING wq_header_id INTO l_header_id;',
'	END;',
'',
'                                    ',
'	/*',
'    IF :P10_QUEUE_TYPE = ''6'' THEN',
'      :CURRENT_QUEUE_MENU := 510;',
'      :P510_HEADER_ID      := l_header_id;',
'    END IF;  ',
'    */',
'    l_loc := 40;',
'',
'    v_other_parameters.param1 := :P10_QUEUE_TYPE;',
'    v_other_parameters.param16 :=l_header_id;',
'    ',
'    v_other_parameters.param2 := ''BEGIN UPDATE spi_ot_work_queues_header SET status=10 WHERE wq_header_id = ''||l_header_id||'';COMMIT; spi_work_queue_procs.generate_work_queue ( ''''''',
'                                                               ||:P10_QUEUE_TYPE',
'                                                               ||'''''',''''''',
'                                                               ||:P10_PUB_SRC',
'                                                               ||'''''',''''''',
'                                                               ||:P10_SPI_TRANSACTION_ID',
'                                                               ||'''''',''''''',
'                                                               ||:P10_FILE_NAME',
'                                                               ||'''''',''''''',
'                                                               ||:P10_TRANS_TYPE',
'                                                               ||'''''',''''''',
'                                                               ||TO_DATE(:P10_SUBMISSION_DATE_FROM,''YYYY-MM-DD'')',
'                                                               ||'''''',''''''',
'                                                               ||TO_DATE(:P10_SUBMISSION_DATE_TO,''YYYY-MM-DD'')',
'                                                               ||'''''',''''''',
'                                                               ||:P10_DATE_SORT_ORDER',
'                                                               ||'''''',''''''',
'                                                               ||:P10_PARTY_NAME',
'                                                               ||'''''',''''''',
'                                                               ||:P10_PARTY_IPI_NUMBER',
'                                                               ||'''''',''''''',
'                                                               ||:P10_PARTY_AFFILIATION',
'                                                               ||'''''',''''''',
'                                                               ||:P10_PROPERTY_PARTY',
'                                                               ||'''''',''''''',
'                                                               ||:P10_WRI_PUB_INDICATOR',
'                                                               ||'''''',''''''',
'                                                               ||:P10_MATCH_SCORE',
'                                                               ||'''''',''''''',
'                                                               ||:P10_SONG_REQUEST_ID',
'                                                               ||'''''',''''''',
'                                                               ||:P10_SORT_ORDER5',
'                                                               ||'''''',''''''',
'                                                               ||:P10_SUBMITTER_WORK_NO',
'                                                               ||'''''',''''''',
'                                                               ||:P10_PROP_ID_HFA_CODE',
'                                                               ||'''''',''''''',
'                                                               ||:P10_SONG_TITLE',
'                                                               ||'''''',''''''',
'                                                               ||:P10_ISRC_REC_INFO',
'                                                               ||'''''',''''''',
'                                                               ||:P10_SORT_ORDER6',
'                                                               ||'''''',''''''',
'                                                               ||:P10_JINGLE_INDICATOR',
'                                                               ||'''''',''''''',
'                                                               ||:P10_SORT_ORDER7',
'                                                               ||'''''',''''''',
'                                                               ||:P10_QUEUE_NAME',
'                                                               ||'''''',''''''',
'                                                               ||l_app_user',
'                                                               ||'''''',''''''',
'                                                               ||:P10_AVAILABLE_QUEUES',
'                                                               ||'''''',''''''',
'                                                               ||l_header_id',
'                                                               ||'''''',''''''',
'                                                               ||:P10_AFFILIATE_REP',
'                                                               ||'''''',''''''',
'                                                               ||:P10_TYPE',
'                                                               ||'''''',''''''',
'                                                               ||:P10_PRIORITY_FLAG',
'                                                               ||'''''',''''''',
'                                                               ||:P10_HOLD_VALUE',
'                                                               ||''''''',
'                                                              );',
'                   END;'';',
'    ------------------------------',
'    -- Add this in Job Central',
'    ------------------------------',
'    job_fileload_schedule_pkg.load_schedule_details',
'    (',
'        p_job_type          => ''SPI_WORK_QUEUE'',',
'        p_schedule_date     => SYSDATE,',
'        p_action_by         => V(''APP_USER''),',
'        p_process_type      =>''L'',',
'        p_other_parameters  => v_other_parameters',
'    ); ',
'END;'))
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
,p_process_when_button_id=>wwv_flow_api.id(62096801533062122)
,p_process_when=>'P10_QUEUE_TYPE'
,p_process_when_type=>'VAL_OF_ITEM_IN_COND_EQ_COND2'
,p_process_when2=>'6'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(62128793074062150)
,p_process_sequence=>30
,p_process_point=>'BEFORE_HEADER'
,p_process_type=>'NATIVE_SESSION_STATE'
,p_process_name=>'Clear'
,p_attribute_01=>'CLEAR_CACHE_CURRENT_PAGE'
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
