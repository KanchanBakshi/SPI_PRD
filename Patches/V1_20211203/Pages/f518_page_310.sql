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
--   Date and Time:   07:27 Wednesday November 17, 2021
--   Exported By:     SCHOPRA
--   Flashback:       0
--   Export Type:     Page Export
--   Version:         18.2.0.00.12
--   Instance ID:     69431613733717
--

prompt --application/pages/delete_00310
begin
wwv_flow_api.remove_page (p_flow_id=>wwv_flow.g_flow_id, p_page_id=>310);
end;
/
prompt --application/pages/page_00310
begin
wwv_flow_api.create_page(
 p_id=>310
,p_user_interface_id=>wwv_flow_api.id(51875501499240850)
,p_name=>'Property Details'
,p_step_title=>'Property Details'
,p_reload_on_submit=>'A'
,p_warn_on_unsaved_changes=>'N'
,p_step_sub_title=>'Property Details'
,p_step_sub_title_type=>'TEXT_WITH_SUBSTITUTIONS'
,p_autocomplete_on_off=>'ON'
,p_group_id=>wwv_flow_api.id(68043383818229663)
,p_javascript_code=>wwv_flow_string.join(wwv_flow_t_varchar2(
'var htmldb_delete_message=''"DELETE_CONFIRM_MSG"'';',
'',
'function delrec(pThis,pRECID)',
'{',
'    var vAction;',
'',
'    if(pThis.checked)',
'    {',
'    vAction = ''A'';',
'    }',
'    else',
'    {',
'    vAction = ''R'';',
'    }',
'    var ajaxRequest = new htmldb_Get(null,&APP_ID.,''APPLICATION_PROCESS=DELRECORDINGS'',310);',
'    ajaxRequest.addParam(''x01'',pRECID);',
'    ajaxRequest.addParam(''x02'',vAction);    ',
'    ajaxResult = ajaxRequest.get();  ',
'}',
'',
''))
,p_javascript_code_onload=>'$(".t-Report-report > tbody > tr:odd > td").css( "background-color", "rgb(227, 227, 227)" )'
,p_inline_css=>wwv_flow_string.join(wwv_flow_t_varchar2(
'#P310_PROP_HOLD_REASON_DISPLAY{ ',
'    color: red;',
'}',
'',
'.customAlternatingRow .a-IRR-table tr:nth-child(odd) td {',
'    background-color: yellow;',
'}',
'',
'.customAlternatingRow .a-IRR-table tr:nth-child(even) td {',
'    background-color: yellowgreen;',
'}',
'',
'span.com-wrap {',
'float: left;',
'width: 100px;',
'}',
'',
'.divTable{',
'	display: table;',
'	width: 100%;',
'}',
'.divTableRow {',
'	display: table-row;',
'}',
'.divTableHeading {',
'	background-color: #EEE;',
'	display: table-header-group;  ',
'}',
'.divTableCell, .divTableHead {',
'	border: 1px solid #999999;',
'	display: table-cell;',
'	padding: 3px 10px;',
'}',
'.divTableHeading {',
'	background-color: #EEE;',
'	display: table-header-group;',
'	font-weight: bold;',
'}',
'.divTableFoot {',
'	background-color: #EEE;',
'	display: table-footer-group;',
'	font-weight: bold;',
'}',
'.divTableBody {',
'	display: table-row-group;',
'}'))
,p_page_template_options=>'#DEFAULT#'
,p_required_role=>wwv_flow_api.id(69303254223531622)
,p_help_text=>'No help is available for this page.'
,p_last_updated_by=>'PKUMAR'
,p_last_upd_yyyymmddhh24miss=>'20211028060709'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(11643894853815328)
,p_plug_name=>'Recordings'
,p_region_name=>'REC'
,p_region_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51848808455240825)
,p_plug_display_sequence=>175
,p_include_in_reg_disp_sel_yn=>'Y'
,p_plug_display_point=>'BODY_3'
,p_query_type=>'SQL'
,p_plug_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT DISTINCT  apex_item.checkbox2( 5,c001 ,'' onClick ="delrec(this,''||c001||'')"'',p_checked_values => c025)SELECT_REC,',
'c001 RECORDING_ID,',
'c002 source_property_id,',
'c003 property_id,',
'c004 source,',
'c005 rec_title,',
'c006 property_number,',
'c007 status,',
'c008 hold_reason_id,',
'c009 isrc,',
'c010 ean,',
'c011 artist,',
'c012 Album,',
'C013 Recording_Label,',
'C014 release_date,',
'c015 recording_format,',
'c016 Bds_Song_Id,',
'c017 link_date,',
'c018 Action,',
'c019 CREATED_BY,',
'c020 CREATION_DATE,',
'c021 LAST_UPDATED_BY,',
'c022 LAST_UPDATE_DATE',
'FROM APEX_COLLECTIONS ',
'WHERE COLLECTION_NAME = ''P310_RECORDINGS_COLL''',
'AND :P310_QUERY_REC = ''Y''',
'AND c024 = ''NOT_DELETED''',
'AND C002 = :P310_PROP_ID',
''))
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
,p_plug_header=>'<div style="overflow:auto;">'
,p_plug_footer=>'</div>'
);
wwv_flow_api.create_worksheet(
 p_id=>wwv_flow_api.id(26404154309254517)
,p_max_row_count=>'1000000'
,p_show_nulls_as=>'-'
,p_pagination_type=>'ROWS_X_TO_Y_OF_Z'
,p_pagination_display_pos=>'TOP_AND_BOTTOM_LEFT'
,p_report_list_mode=>'TABS'
,p_show_detail_link=>'N'
,p_show_notify=>'Y'
,p_download_formats=>'CSV:HTML:EMAIL:XLS:PDF:RTF'
,p_owner=>'SKUMARAN'
,p_internal_uid=>26404154309254517
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26404282423254518)
,p_db_column_name=>'SELECT_REC'
,p_display_order=>10
,p_column_identifier=>'A'
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
,p_security_scheme=>wwv_flow_api.id(17255026364731121)
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26404338178254519)
,p_db_column_name=>'RECORDING_ID'
,p_display_order=>20
,p_column_identifier=>'B'
,p_column_label=>'&nbsp;'
,p_column_link=>'f?p=&APP_ID.:50092:&SESSION.::&DEBUG.:RP,50092:P50092_RECORDING_ID,P50092_SONG_PROPERTY_ID,P50092_BDS_SONG_NO:#RECORDING_ID#,#SOURCE_PROPERTY_ID#,#BDS_SONG_ID#'
,p_column_linktext=>'<img src="#IMAGE_PREFIX#app_ui/img/icons/apex-edit-pencil.png" class="apex-edit-pencil" alt="">'
,p_column_type=>'STRING'
,p_display_text_as=>'WITHOUT_MODIFICATION'
,p_security_scheme=>wwv_flow_api.id(17255026364731121)
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26404431797254520)
,p_db_column_name=>'SOURCE_PROPERTY_ID'
,p_display_order=>30
,p_column_identifier=>'C'
,p_column_label=>'Source Property Id'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26404533813254521)
,p_db_column_name=>'PROPERTY_ID'
,p_display_order=>40
,p_column_identifier=>'D'
,p_column_label=>'Recording Property Id'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26404627489254522)
,p_db_column_name=>'SOURCE'
,p_display_order=>50
,p_column_identifier=>'E'
,p_column_label=>'Source'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26404704799254523)
,p_db_column_name=>'REC_TITLE'
,p_display_order=>60
,p_column_identifier=>'F'
,p_column_label=>'Recording Title'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26404813668254524)
,p_db_column_name=>'PROPERTY_NUMBER'
,p_display_order=>70
,p_column_identifier=>'G'
,p_column_label=>'Property Number'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26404996845254525)
,p_db_column_name=>'STATUS'
,p_display_order=>80
,p_column_identifier=>'H'
,p_column_label=>'Status'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26405098600254526)
,p_db_column_name=>'HOLD_REASON_ID'
,p_display_order=>90
,p_column_identifier=>'I'
,p_column_label=>'Hold Reason Id'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26405181853254527)
,p_db_column_name=>'ISRC'
,p_display_order=>100
,p_column_identifier=>'J'
,p_column_label=>'ISRC'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26405237767254528)
,p_db_column_name=>'EAN'
,p_display_order=>110
,p_column_identifier=>'K'
,p_column_label=>'EAN'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26405366900254529)
,p_db_column_name=>'ARTIST'
,p_display_order=>120
,p_column_identifier=>'L'
,p_column_label=>'Artist'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26405459608254530)
,p_db_column_name=>'ALBUM'
,p_display_order=>130
,p_column_identifier=>'M'
,p_column_label=>'Album'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26405531693254531)
,p_db_column_name=>'RECORDING_LABEL'
,p_display_order=>140
,p_column_identifier=>'N'
,p_column_label=>'Recording Label'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26405664248254532)
,p_db_column_name=>'RELEASE_DATE'
,p_display_order=>150
,p_column_identifier=>'O'
,p_column_label=>'Release Date'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26405775187254533)
,p_db_column_name=>'RECORDING_FORMAT'
,p_display_order=>160
,p_column_identifier=>'P'
,p_column_label=>'Recording Format'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26405807731254534)
,p_db_column_name=>'BDS_SONG_ID'
,p_display_order=>170
,p_column_identifier=>'Q'
,p_column_label=>'BDS Song Id'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26405999881254535)
,p_db_column_name=>'LINK_DATE'
,p_display_order=>180
,p_column_identifier=>'R'
,p_column_label=>'Link Date'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26406094406254536)
,p_db_column_name=>'ACTION'
,p_display_order=>190
,p_column_identifier=>'S'
,p_column_label=>'Action'
,p_column_type=>'STRING'
,p_display_text_as=>'WITHOUT_MODIFICATION'
,p_display_condition_type=>'VAL_OF_ITEM_IN_COND_EQ_COND2'
,p_display_condition=>'P310_STATUS'
,p_display_condition2=>'Active'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26406116089254537)
,p_db_column_name=>'CREATED_BY'
,p_display_order=>200
,p_column_identifier=>'T'
,p_column_label=>'Created By'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26406229208254538)
,p_db_column_name=>'CREATION_DATE'
,p_display_order=>210
,p_column_identifier=>'U'
,p_column_label=>'Creation Date'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26406391509254539)
,p_db_column_name=>'LAST_UPDATED_BY'
,p_display_order=>220
,p_column_identifier=>'V'
,p_column_label=>'Last Updated By'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26406409732254540)
,p_db_column_name=>'LAST_UPDATE_DATE'
,p_display_order=>230
,p_column_identifier=>'W'
,p_column_label=>'Last Update Date'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(26522539538519806)
,p_application_user=>'APXWS_DEFAULT'
,p_report_seq=>10
,p_report_alias=>'265226'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'SELECT_REC:RECORDING_ID:PROPERTY_NUMBER:REC_TITLE:STATUS:ISRC:EAN:ARTIST:ALBUM:RECORDING_LABEL:RELEASE_DATE:BDS_SONG_ID:LINK_DATE:PROPERTY_ID:ACTION:'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(16737876040038813)
,p_name=>'Versions'
,p_region_name=>'VERS'
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>85
,p_include_in_reg_disp_sel_yn=>'Y'
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#:t-Report--altRowsDefault:t-Report--rowHighlight'
,p_display_point=>'BODY_3'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT  PROP_VER_ID,        ',
' Master_property_id,   ',
' Version_Property_id,    ',
' Language_id,       ',
'  CASE Version_Type  WHEN ''INS''  THEN ''Instrumental''',
'                 WHEN  ''TRS''  THEN ''Translation''',
'                 WHEN  ''VOC''  THEN ''Vocal''',
'       END  Version_type,',
' Created_by,         ',
' Creation_date,      ',
' Last_updated_by,    ',
' Last_update_date,   ',
'(select property_number from prop_properties where property_id = Version_Property_id',
'                                                and source_id = 7',
'                                                and property_type_id = 1 ) Version_property_number,',
'                                                ',
' (select property_number from prop_properties where property_id = Master_property_id',
'                                                and source_id = 7',
'                                                and property_type_id = 1)master_property_number,',
'-- :P310_PROP_NUMBER master_property_number,',
' (select title from prop_properties where property_id = Version_Property_id',
'                                                and source_id = 7',
'                                                and property_type_id = 1) Version_title',
'                                                FROM PROP_PROPERTIES_VERSIONS ',
'                                                WHERE -- Master_property_id = :P310_PROP_ID',
'                                                :P310_PROP_ID IN (Master_property_id,Version_Property_id)',
'                                                -- WHERE :P310_QUERY_VESRIONS= ''Y'''))
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>'-'
,p_query_num_rows_type=>'NEXT_PREVIOUS_LINKS'
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_csv_output=>'N'
,p_prn_output=>'N'
,p_sort_null=>'L'
,p_plug_query_strip_html=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(16737968537038814)
,p_query_column_id=>1
,p_column_alias=>'PROP_VER_ID'
,p_column_display_sequence=>1
,p_use_as_row_header=>'N'
,p_column_link=>'f?p=&APP_ID.:50098:&SESSION.::&DEBUG.:RP:P50098_MASTER_PROPERTY_NUMBER,P50098_VERSION_ID:&P310_PROP_NUMBER.,#PROP_VER_ID#'
,p_column_linktext=>'<img src="#IMAGE_PREFIX#app_ui/img/icons/apex-edit-pencil.png" class="apex-edit-pencil" alt="">'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(16738091088038815)
,p_query_column_id=>2
,p_column_alias=>'MASTER_PROPERTY_ID'
,p_column_display_sequence=>5
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(16738170985038816)
,p_query_column_id=>3
,p_column_alias=>'VERSION_PROPERTY_ID'
,p_column_display_sequence=>6
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(16893766216341906)
,p_query_column_id=>4
,p_column_alias=>'LANGUAGE_ID'
,p_column_display_sequence=>8
,p_column_heading=>'Language'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_display_as=>'TEXT_FROM_LOV'
,p_named_lov=>wwv_flow_api.id(68674396253277503)
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(16738256385038817)
,p_query_column_id=>5
,p_column_alias=>'VERSION_TYPE'
,p_column_display_sequence=>7
,p_column_heading=>'Version Type'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(16738328708038818)
,p_query_column_id=>6
,p_column_alias=>'CREATED_BY'
,p_column_display_sequence=>9
,p_column_heading=>'Created By'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(16738466735038819)
,p_query_column_id=>7
,p_column_alias=>'CREATION_DATE'
,p_column_display_sequence=>10
,p_column_heading=>'Creation Date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(16738599399038820)
,p_query_column_id=>8
,p_column_alias=>'LAST_UPDATED_BY'
,p_column_display_sequence=>11
,p_column_heading=>'Last Updated By'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(16738619576038821)
,p_query_column_id=>9
,p_column_alias=>'LAST_UPDATE_DATE'
,p_column_display_sequence=>12
,p_column_heading=>'Last Update Date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(16893898608341907)
,p_query_column_id=>10
,p_column_alias=>'VERSION_PROPERTY_NUMBER'
,p_column_display_sequence=>3
,p_column_heading=>'Version Property Number'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(16893930008341908)
,p_query_column_id=>11
,p_column_alias=>'MASTER_PROPERTY_NUMBER'
,p_column_display_sequence=>2
,p_column_heading=>'Master Property Number'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(16896438793341933)
,p_query_column_id=>12
,p_column_alias=>'VERSION_TITLE'
,p_column_display_sequence=>4
,p_column_heading=>'Version Title'
,p_use_as_row_header=>'N'
,p_column_link=>'f?p=&APP_ID.:310:&SESSION.:APEX_CLONE_SESSION:&DEBUG.:RP:P310_PROP_ID,P310_CALLING_PAGE_ID:#VERSION_PROPERTY_ID#,310'
,p_column_linktext=>'#VERSION_TITLE#'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(34043400295522827)
,p_plug_name=>'Consolidated Holds'
,p_region_name=>'COHO'
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>235
,p_include_in_reg_disp_sel_yn=>'Y'
,p_plug_display_point=>'BODY_3'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_attribute_01=>'N'
,p_attribute_02=>'HTML'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(34044603056522839)
,p_plug_name=>'Share Level Holds'
,p_parent_plug_id=>wwv_flow_api.id(34043400295522827)
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>10
,p_plug_display_point=>'BODY'
,p_query_type=>'SQL'
,p_plug_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select  AIP.IP_ID,psp.COLLECTOR_ID ,pp.property_number,AIP.IP_NUMBER,psp.SHARE_PERCENTAGE,AFT.RIGHT_DESCRIPTION,AR.ROLE_DESC,PHT.DESCRIPTION',
'from 	prop_properties pp , ',
'       prop_share_collectors psp , ',
'       pay_hold_types pht,',
'       AFF_RIGHT_TYPES AFT,',
'       AFF_ROLES  AR,',
'	   AFF_INTERESTED_PARTIES AIP',
'where  pp.PROPERTY_ID = psp.PROPERTY_ID ',
'and    psp.HOLD_TYPE_ID =  pht.HOLD_TYPE_ID',
'AND		AIP.IP_ID = PSP.COLLECTOR_ID',
'and    psp.PROPERTY_ID = :P310_PROP_ID  and		pp.source_id = 7 and pp.property_type_id =1',
'and    AFT.RIGHT_TYPE_ID= psp.RIGHT_TYPE_ID',
'AND    AR.ROLE_ID = PSP.ROLE_ID',
'AND     PSP.END_DATE_ACTIVE is NULL',
'AND :P310_QUERY_CONSOLIADTED_HOLDS = ''Y''',
'group by psp.COLLECTOR_ID,AIP.IP_NUMBER,psp.SHARE_PERCENTAGE,AFT.RIGHT_DESCRIPTION,pp.property_number,AR.ROLE_DESC,PHT.DESCRIPTION,AIP.IP_ID;'))
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
);
wwv_flow_api.create_worksheet(
 p_id=>wwv_flow_api.id(34044746279522840)
,p_max_row_count=>'1000000'
,p_show_nulls_as=>'-'
,p_pagination_type=>'ROWS_X_TO_Y'
,p_pagination_display_pos=>'BOTTOM_RIGHT'
,p_report_list_mode=>'TABS'
,p_show_detail_link=>'N'
,p_show_notify=>'Y'
,p_download_formats=>'CSV:HTML:EMAIL:XLS:PDF:RTF'
,p_owner=>'RGARG'
,p_internal_uid=>34044746279522840
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34044847471522841)
,p_db_column_name=>'COLLECTOR_ID'
,p_display_order=>10
,p_column_identifier=>'A'
,p_column_label=>'Collector id'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34044935917522842)
,p_db_column_name=>'IP_NUMBER'
,p_display_order=>20
,p_column_identifier=>'B'
,p_column_label=>'Ip number'
,p_column_link=>'f?p=&APP_ID.:260:&SESSION.:APEX_CLONE_SESSION:&DEBUG.:RP:P260_IP_ID:#IP_ID#'
,p_column_linktext=>'#IP_NUMBER#'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34045069089522843)
,p_db_column_name=>'SHARE_PERCENTAGE'
,p_display_order=>30
,p_column_identifier=>'C'
,p_column_label=>'Share percentage'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34045131574522844)
,p_db_column_name=>'RIGHT_DESCRIPTION'
,p_display_order=>40
,p_column_identifier=>'D'
,p_column_label=>'Right description'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34045226232522845)
,p_db_column_name=>'ROLE_DESC'
,p_display_order=>50
,p_column_identifier=>'E'
,p_column_label=>'Role desc'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34045352999522846)
,p_db_column_name=>'DESCRIPTION'
,p_display_order=>60
,p_column_identifier=>'F'
,p_column_label=>'Hold Description'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34295138644208913)
,p_db_column_name=>'PROPERTY_NUMBER'
,p_display_order=>70
,p_column_identifier=>'G'
,p_column_label=>'Property number'
,p_column_type=>'STRING'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34500455377150701)
,p_db_column_name=>'IP_ID'
,p_display_order=>80
,p_column_identifier=>'H'
,p_column_label=>'Ip id'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(34292956011170481)
,p_application_user=>'APXWS_DEFAULT'
,p_report_seq=>10
,p_report_alias=>'342930'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'COLLECTOR_ID:IP_NUMBER:SHARE_PERCENTAGE:RIGHT_DESCRIPTION:ROLE_DESC:DESCRIPTION:PROPERTY_NUMBER:IP_ID'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(34045421809522847)
,p_plug_name=>'Memo Adjustment'
,p_parent_plug_id=>wwv_flow_api.id(34043400295522827)
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>20
,p_plug_display_point=>'BODY'
,p_query_type=>'SQL'
,p_plug_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT MEMO_NUM,FROM_PUBLISHER_NUM,SONG_CODE,PUBLISHER_SEQ_NUM,PUBLISHER_NUM ,PUBLISHER_SPLIT',
'FROM   MC_DIST_MEMO_ADJ MDM',
'WHERE  MDM.MEMO_TYPE_CODE=''SS'' ',
'AND		SONG_PROPERTY_ID =:P310_PROP_ID',
'AND :P310_QUERY_CONSOLIADTED_HOLDS = ''Y'';'))
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
);
wwv_flow_api.create_worksheet(
 p_id=>wwv_flow_api.id(34045549606522848)
,p_max_row_count=>'1000000'
,p_show_nulls_as=>'-'
,p_pagination_type=>'ROWS_X_TO_Y'
,p_pagination_display_pos=>'BOTTOM_RIGHT'
,p_report_list_mode=>'TABS'
,p_show_detail_link=>'N'
,p_show_notify=>'Y'
,p_download_formats=>'CSV:HTML:EMAIL:XLS:PDF:RTF'
,p_owner=>'RGARG'
,p_internal_uid=>34045549606522848
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34045648543522849)
,p_db_column_name=>'MEMO_NUM'
,p_display_order=>10
,p_column_identifier=>'A'
,p_column_label=>'Memo num'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34045757972522850)
,p_db_column_name=>'FROM_PUBLISHER_NUM'
,p_display_order=>20
,p_column_identifier=>'B'
,p_column_label=>'From publisher num'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34293943301208901)
,p_db_column_name=>'SONG_CODE'
,p_display_order=>30
,p_column_identifier=>'C'
,p_column_label=>'Song code'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34294082539208902)
,p_db_column_name=>'PUBLISHER_SEQ_NUM'
,p_display_order=>40
,p_column_identifier=>'D'
,p_column_label=>'Publisher seq num'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34294177203208903)
,p_db_column_name=>'PUBLISHER_NUM'
,p_display_order=>50
,p_column_identifier=>'E'
,p_column_label=>'Publisher num'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34294292640208904)
,p_db_column_name=>'PUBLISHER_SPLIT'
,p_display_order=>60
,p_column_identifier=>'F'
,p_column_label=>'Publisher split'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(34301257882210890)
,p_application_user=>'APXWS_DEFAULT'
,p_report_seq=>10
,p_report_alias=>'343013'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'MEMO_NUM:FROM_PUBLISHER_NUM:SONG_CODE:PUBLISHER_SEQ_NUM:PUBLISHER_NUM:PUBLISHER_SPLIT'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(37909488243150725)
,p_plug_name=>'Registration History'
,p_region_name=>'RIGHIST'
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>155
,p_include_in_reg_disp_sel_yn=>'Y'
,p_plug_display_point=>'BODY_3'
,p_query_type=>'SQL'
,p_plug_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT *',
'FROM ',
'    (',
'    SELECT ',
'        source.description Source,',
'        tst.submitter_work_id,',
'        tst.file_name,',
'        logs.column_name,',
'        to_char(logs.date_changed,''RRRR-MM-DD HH24:MI:SS'') date_changed,',
'        logs.changed_by,',
'        CASE ',
'            WHEN logs.column_name = ''REJECTION_CODE'' THEN',
'                NVL(',
'                    (',
'                        SELECT REPLACE(REPLACE(error_message,''#''),'''''''')',
'                        FROM cwr_messages ',
'                        WHERE message_type = ''REJ''',
'                        AND cwr_message_id = logs.prev_value',
'                    ),logs.prev_value',
'                   )',
'            WHEN logs.column_name = ''TRANSACTION_STATUS'' THEN',
'                NVL(',
'                    (',
'                        SELECT  lookup_code || '' - ''|| description ',
'                        FROM prop_lookups ',
'                        WHERE lookup_type = ''SPI_STATUS''',
'                        AND lookup_code = logs.prev_value',
'                    ),logs.prev_value',
'                   )',
'            WHEN logs.column_name = ''SELECTED_SESAC_PROPERTY_ID'' THEN',
'                NVL(',
'                    (',
'                        SELECT  to_char(SELECTED_SESAC_PROPERTY_ID)',
'                        FROM SPI_TRANSACTION_STATUS ',
'                        WHERE transaction_status_id = logs.key_value1',
'                        AND SELECTED_SESAC_PROPERTY_ID = logs.prev_value',
'                    ),logs.prev_value',
'                   )',
'        END prev_value,',
'        CASE ',
'            WHEN logs.column_name = ''REJECTION_CODE''THEN',
'                NVL(',
'                    (',
'                        SELECT REPLACE(REPLACE(error_message,''#''),'''''''')',
'                        FROM cwr_messages ',
'                        WHERE message_type = ''REJ''',
'                        AND cwr_message_id = logs.new_value',
'                    ),logs.new_value',
'                   )',
'            WHEN logs.column_name = ''TRANSACTION_STATUS'' THEN',
'                NVL(',
'                    (',
'                        SELECT lookup_code || '' - ''|| description ',
'                        FROM prop_lookups ',
'                        WHERE lookup_type = ''SPI_STATUS''',
'                        AND lookup_code = logs.new_value',
'                    ),logs.new_value',
'                   )',
'            WHEN logs.column_name = ''SELECTED_SESAC_PROPERTY_ID'' THEN',
'                NVL(',
'                    (',
'                        SELECT  to_char(SELECTED_SESAC_PROPERTY_ID)',
'                        FROM SPI_TRANSACTION_STATUS ',
'                        WHERE transaction_status_id = logs.key_value1',
'                        AND SELECTED_SESAC_PROPERTY_ID = logs.new_value',
'                    ),logs.new_value',
'                   )',
'               END new_value',
'    FROM spi_transaction_status tst, hist_change_logs logs, prop_sources source, spi_property_xref spx',
'    WHERE spx.submitter_source_id  = tst.source_id	',
'    AND spx.submitter_work_id = tst.submitter_work_id',
'    AND (spx.sesac_property_id = :P310_PROP_ID OR tst.property_id = :P310_PROP_ID)',
'    AND logs.table_name = ''SPI_TRANSACTION_STATUS''',
'    AND logs.key_value1 = tst.transaction_status_id',
'    AND logs.sql_code = ''PROPERTY'' ',
'    AND source.source_id = tst.source_id	',
')',
'WHERE :P310_QUERY_REG_HISTORY = ''Y'''))
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
,p_plug_header=>'Please enter search criteria and press Go'
);
end;
/
begin
wwv_flow_api.create_worksheet(
 p_id=>wwv_flow_api.id(37909595853150726)
,p_max_row_count=>'1000000'
,p_allow_save_rpt_public=>'Y'
,p_save_rpt_public_auth_scheme=>wwv_flow_api.id(6185302949729668)
,p_show_nulls_as=>'-'
,p_pagination_type=>'ROWS_X_TO_Y_OF_Z'
,p_pagination_display_pos=>'TOP_AND_BOTTOM_LEFT'
,p_report_list_mode=>'TABS'
,p_show_detail_link=>'N'
,p_show_notify=>'Y'
,p_download_formats=>'CSV:HTML:EMAIL:XLS:PDF:RTF'
,p_owner=>'VYADAV'
,p_internal_uid=>37909595853150726
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37909684663150727)
,p_db_column_name=>'COLUMN_NAME'
,p_display_order=>10
,p_column_identifier=>'A'
,p_column_label=>'Column name'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37909837537150729)
,p_db_column_name=>'CHANGED_BY'
,p_display_order=>30
,p_column_identifier=>'C'
,p_column_label=>'Changed by'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37909916747150730)
,p_db_column_name=>'PREV_VALUE'
,p_display_order=>40
,p_column_identifier=>'D'
,p_column_label=>'Prev value'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37910029522150731)
,p_db_column_name=>'NEW_VALUE'
,p_display_order=>50
,p_column_identifier=>'E'
,p_column_label=>'New value'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(43295825524079943)
,p_db_column_name=>'DATE_CHANGED'
,p_display_order=>60
,p_column_identifier=>'F'
,p_column_label=>'Date changed'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(43296009960079945)
,p_db_column_name=>'SOURCE'
,p_display_order=>70
,p_column_identifier=>'H'
,p_column_label=>'Source'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(43296160161079946)
,p_db_column_name=>'SUBMITTER_WORK_ID'
,p_display_order=>80
,p_column_identifier=>'I'
,p_column_label=>'Submitter work id'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(43296208889079947)
,p_db_column_name=>'FILE_NAME'
,p_display_order=>90
,p_column_identifier=>'J'
,p_column_label=>'File name'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(38162487446410247)
,p_application_user=>'APXWS_DEFAULT'
,p_report_seq=>10
,p_report_alias=>'381625'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'SOURCE:COLUMN_NAME:CHANGED_BY:PREV_VALUE:NEW_VALUE:DATE_CHANGED::SUBMITTER_WORK_ID:FILE_NAME'
,p_sort_column_1=>'DATE_CHANGED'
,p_sort_direction_1=>'DESC'
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
 p_id=>wwv_flow_api.id(39568973197436819)
,p_plug_name=>'Writer/Pub Relations'
,p_region_name=>'PUBREL'
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>96
,p_include_in_reg_disp_sel_yn=>'Y'
,p_plug_display_point=>'BODY_3'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_attribute_01=>'N'
,p_attribute_02=>'HTML'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(26554449124540420)
,p_name=>'Writer/Pub Relations'
,p_parent_plug_id=>wwv_flow_api.id(39568973197436819)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>95
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--removeHeader:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#:t-Report--altRowsDefault:t-Report--rowHighlight'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'Select Relation_Id',
'      ,Property_Id',
'      ,Writer_Ip_Id',
'--      ,APEX_ITEM.CHECKBOX2(1,Relation_Id) DEL_RELATION',
'      ,(Select Ip_Number',
'          From Aff_Interested_Parties ',
'         Where Writer_Ip_Id = Ip_Id)    Writer_Affiliate_Number',
'      ,(Select Full_Name ',
'          From Aff_Ip_Names ',
'         Where Writer_Ip_Id = Ip_Id ',
'           And Name_Type_Id = 6)    Writer_Name',
'      ,Publisher_Ip_Id',
'      ,(Select Ip_Number',
'          From Aff_Interested_Parties ',
'         Where Publisher_Ip_Id = Ip_Id)    Publisher_Affiliate_Number',
'      ,(Select Full_Name ',
'          From Aff_Ip_Names ',
'         Where Publisher_Ip_Id = Ip_Id ',
'           And Name_Type_Id    = 6) Publisher_Name',
'      ,Creation_Date',
'      ,Created_By',
'      ,Last_Update_Date',
'      ,Last_Updated_By',
'  From Prop_Ip_Relations ',
' Where Property_Id = :P310_PROP_ID AND :P310_QUERY_PUBREL=''Y'''))
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>'-'
,p_query_no_data_found=>'No Data Found'
,p_query_num_rows_type=>'ROW_RANGES_IN_SELECT_LIST'
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_csv_output=>'N'
,p_prn_output=>'N'
,p_sort_null=>'L'
,p_plug_query_strip_html=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(26554548624540421)
,p_query_column_id=>1
,p_column_alias=>'RELATION_ID'
,p_column_display_sequence=>1
,p_column_link=>'f?p=&APP_ID.:2142:&SESSION.::&DEBUG.:RP:P2142_RELATION_ID,P2142_CALLING_PAGE_ID,P2142_PROP_NAME:#RELATION_ID#,&APP_PAGE_ID.,&P310_PROP_NAME.'
,p_column_linktext=>'<img src="#IMAGE_PREFIX#app_ui/img/icons/apex-edit-pencil.png" class="apex-edit-pencil" alt="">'
,p_disable_sort_column=>'N'
,p_report_column_required_role=>wwv_flow_api.id(71705400949038146)
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(26554705979540423)
,p_query_column_id=>2
,p_column_alias=>'PROPERTY_ID'
,p_column_display_sequence=>2
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(26554893130540424)
,p_query_column_id=>3
,p_column_alias=>'WRITER_IP_ID'
,p_column_display_sequence=>3
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(26554923950540425)
,p_query_column_id=>4
,p_column_alias=>'WRITER_AFFILIATE_NUMBER'
,p_column_display_sequence=>4
,p_column_heading=>'Writer Number'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(26555020366540426)
,p_query_column_id=>5
,p_column_alias=>'WRITER_NAME'
,p_column_display_sequence=>5
,p_column_heading=>'Writer Name'
,p_column_link=>'f?p=&APP_ID.:260:&SESSION.:APEX_CLONE_SESSION:&DEBUG.:RP,260:P260_IP_ID,P260_CALLING_PAGE_ID:#WRITER_IP_ID#,310'
,p_column_linktext=>'#WRITER_NAME#'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(26555153710540427)
,p_query_column_id=>6
,p_column_alias=>'PUBLISHER_IP_ID'
,p_column_display_sequence=>6
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(26555283594540428)
,p_query_column_id=>7
,p_column_alias=>'PUBLISHER_AFFILIATE_NUMBER'
,p_column_display_sequence=>7
,p_column_heading=>'Publisher Number'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(26555301671540429)
,p_query_column_id=>8
,p_column_alias=>'PUBLISHER_NAME'
,p_column_display_sequence=>8
,p_column_heading=>'Publisher Name'
,p_column_link=>'f?p=&APP_ID.:260:&SESSION.:APEX_CLONE_SESSION:&DEBUG.:RP,260:P260_IP_ID,P260_CALLING_PAGE_ID:#PUBLISHER_IP_ID#,310'
,p_column_linktext=>'#PUBLISHER_NAME#'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(26555425340540430)
,p_query_column_id=>9
,p_column_alias=>'CREATION_DATE'
,p_column_display_sequence=>9
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(26555537481540431)
,p_query_column_id=>10
,p_column_alias=>'CREATED_BY'
,p_column_display_sequence=>10
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(26555602990540432)
,p_query_column_id=>11
,p_column_alias=>'LAST_UPDATE_DATE'
,p_column_display_sequence=>11
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(26555786523540433)
,p_query_column_id=>12
,p_column_alias=>'LAST_UPDATED_BY'
,p_column_display_sequence=>12
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(79182903153398056)
,p_name=>'WPL - Ownership(Performance Right Type) '
,p_region_name=>'wplreport'
,p_parent_plug_id=>wwv_flow_api.id(39568973197436819)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>980
,p_region_css_classes=>'customAlternatingRow'
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#:t-Report--altRowsDefault:t-Report--rowHighlight'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>'SELECT * FROM spi_song_regist_report_stg WHERE session_id = V(''APP_SESSION'')'
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>1000
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>'-'
,p_query_num_rows_type=>'ROWS_X_TO_Y'
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_csv_output=>'N'
,p_prn_output=>'N'
,p_sort_null=>'L'
,p_plug_query_strip_html=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(39615003591961248)
,p_query_column_id=>1
,p_column_alias=>'WRITER_IP_IDS'
,p_column_display_sequence=>1
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(39615434719961250)
,p_query_column_id=>2
,p_column_alias=>'PUBLISHER_IP_IDS'
,p_column_display_sequence=>2
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(39615803340961250)
,p_query_column_id=>3
,p_column_alias=>'WRITER_DATA'
,p_column_display_sequence=>3
,p_column_heading=>'Writer Data'
,p_use_as_row_header=>'N'
,p_display_as=>'WITHOUT_MODIFICATION'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(39616204928961251)
,p_query_column_id=>4
,p_column_alias=>'PUBLISHER_DATA'
,p_column_display_sequence=>4
,p_column_heading=>'Publisher Data'
,p_use_as_row_header=>'N'
,p_display_as=>'WITHOUT_MODIFICATION'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(39616640577961251)
,p_query_column_id=>5
,p_column_alias=>'SESSION_ID'
,p_column_display_sequence=>5
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(39569090110436820)
,p_query_column_id=>6
,p_column_alias=>'STG_ID'
,p_column_display_sequence=>6
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(64335187838550696)
,p_name=>'Title'
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>105
,p_include_in_reg_disp_sel_yn=>'Y'
,p_region_template_options=>'#DEFAULT#:t-Region--hiddenOverflow'
,p_component_template_options=>'#DEFAULT#'
,p_display_column=>1
,p_display_point=>'BODY_3'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select pat.aka_type_code, pa.aka_name, src.source_code, src.description src_desc, src.source_group,',
'pp.property_id, pa.creation_date entry_date, ppt.code prop_type',
'from prop_properties pp, prop_property_types ppt, prop_sources src, prop_akas pa, prop_aka_types pat',
'where ppt.property_type_id = pp.property_type_id',
'and src.source_id = pa.source_id ',
'-- Source of the AKA not source of the song',
'and PAT.AKA_TYPE_ID = PA.AKA_TYPE_ID',
'and PA.PROPERTY_ID = pp.property_id',
'and pp.property_id = :P310_PROP_ID',
'--and pat.aka_type_code <> ''Society AKA'''))
,p_display_condition_type=>'NEVER'
,p_read_only_when_type=>'NEVER'
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>' - '
,p_query_break_cols=>'0'
,p_query_no_data_found=>'No AKAs found'
,p_query_num_rows_type=>'ROW_RANGES_IN_SELECT_LIST'
,p_query_row_count_max=>500
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_break_type_flag=>'DEFAULT_BREAK_FORMATTING'
,p_csv_output=>'Y'
,p_csv_output_link_text=>'Excel'
,p_query_asc_image=>'apex/builder/dup.gif'
,p_query_asc_image_attr=>'width="16" height="16" alt="" '
,p_query_desc_image=>'apex/builder/ddown.gif'
,p_query_desc_image_attr=>'width="16" height="16" alt="" '
,p_plug_query_strip_html=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64335543561550696)
,p_query_column_id=>1
,p_column_alias=>'AKA_TYPE_CODE'
,p_column_display_sequence=>1
,p_column_heading=>'AKA Type'
,p_default_sort_column_sequence=>1
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64335925081550696)
,p_query_column_id=>2
,p_column_alias=>'AKA_NAME'
,p_column_display_sequence=>2
,p_column_heading=>'AKA Name'
,p_default_sort_column_sequence=>2
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64336343837550697)
,p_query_column_id=>3
,p_column_alias=>'SOURCE_CODE'
,p_column_display_sequence=>3
,p_column_heading=>'Source'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64336741528550697)
,p_query_column_id=>4
,p_column_alias=>'SRC_DESC'
,p_column_display_sequence=>4
,p_column_heading=>'Source Description'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64337191337550698)
,p_query_column_id=>5
,p_column_alias=>'SOURCE_GROUP'
,p_column_display_sequence=>5
,p_column_heading=>'Source Group'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64337529739550698)
,p_query_column_id=>6
,p_column_alias=>'PROPERTY_ID'
,p_column_display_sequence=>6
,p_column_heading=>'Property ID'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64337917254550698)
,p_query_column_id=>7
,p_column_alias=>'ENTRY_DATE'
,p_column_display_sequence=>7
,p_column_heading=>'Entry Date'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64338318949550699)
,p_query_column_id=>8
,p_column_alias=>'PROP_TYPE'
,p_column_display_sequence=>8
,p_column_heading=>'Prop Type'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(64338733337550700)
,p_plug_name=>'Work'
,p_region_name=>'WORK'
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--hiddenOverflow'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>115
,p_include_in_reg_disp_sel_yn=>'Y'
,p_plug_new_grid_row=>false
,p_plug_display_column=>1
,p_plug_display_point=>'BODY_3'
,p_plug_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_plug_query_headings_type=>'QUERY_COLUMNS'
,p_plug_query_num_rows=>15
,p_plug_query_num_rows_type=>'NEXT_PREVIOUS_LINKS'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_plug_query_show_nulls_as=>' - '
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_attribute_01=>'N'
,p_attribute_02=>'HTML'
,p_attribute_03=>'N'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(11506530495788902)
,p_name=>'SESAC & Non-SESAC Properties'
,p_parent_plug_id=>wwv_flow_api.id(64338733337550700)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>10
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#:t-Report--altRowsDefault:t-Report--rowHighlight'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT',
'    pp.property_id,',
'    pp.title title,',
'    pp.property_number,',
'    ps.description Source_name,',
'    ps.source_code Source_from,',
'    pp.creation_date first_date_submit,',
'    pp.last_update_date last_date_submit,',
'    pp.batch_id,',
'    pp.source_id,',
'    (select file_name from cwr_batches where batch_id = pp.batch_id  and  transaction_type = ''HDR'' ) file_name ',
'FROM prop_properties pp, prop_sources ps',
'WHERE pp.source_id = ps.source_id',
'AND pp.property_type_id  = 1',
'AND pp.property_number in ( select property_number from prop_properties where property_id = :P310_PROP_ID ) and :P310_QUERY_WORK=''Y''',
'ORDER BY ps.description',
'',
'',
'',
''))
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
 p_id=>wwv_flow_api.id(11507592311788912)
,p_query_column_id=>1
,p_column_alias=>'PROPERTY_ID'
,p_column_display_sequence=>1
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(12018757360870516)
,p_query_column_id=>2
,p_column_alias=>'TITLE'
,p_column_display_sequence=>3
,p_column_heading=>'Title'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(11507420764788911)
,p_query_column_id=>3
,p_column_alias=>'PROPERTY_NUMBER'
,p_column_display_sequence=>2
,p_column_heading=>'Property Number'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(11508124148788918)
,p_query_column_id=>4
,p_column_alias=>'SOURCE_NAME'
,p_column_display_sequence=>5
,p_column_heading=>'Source Name'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(11508050698788917)
,p_query_column_id=>5
,p_column_alias=>'SOURCE_FROM'
,p_column_display_sequence=>4
,p_column_heading=>'Source From'
,p_column_link=>'f?p=&APP_ID.:310:&SESSION.:APEX_CLONE_SESSION:&DEBUG.:RP,310:P310_PROP_ID:#PROPERTY_ID#'
,p_column_linktext=>'#SOURCE_FROM#'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(11507776780788914)
,p_query_column_id=>6
,p_column_alias=>'FIRST_DATE_SUBMIT'
,p_column_display_sequence=>7
,p_column_heading=>'First Date Submit'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(11507805643788915)
,p_query_column_id=>7
,p_column_alias=>'LAST_DATE_SUBMIT'
,p_column_display_sequence=>8
,p_column_heading=>'Last Date Submit'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(11507056188788907)
,p_query_column_id=>8
,p_column_alias=>'BATCH_ID'
,p_column_display_sequence=>6
,p_column_heading=>'Batch ID'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(11508573018788922)
,p_query_column_id=>9
,p_column_alias=>'SOURCE_ID'
,p_column_display_sequence=>10
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(11508237729788919)
,p_query_column_id=>10
,p_column_alias=>'FILE_NAME'
,p_column_display_sequence=>9
,p_column_heading=>'File name'
,p_column_link=>'f?p=&APP_ID.:19:&SESSION.::&DEBUG.:RP,19:P19_FILE_NAME:#FILE_NAME#'
,p_column_linktext=>'#FILE_NAME#'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(64340749383550702)
,p_name=>'Work Notes'
,p_parent_plug_id=>wwv_flow_api.id(64338733337550700)
,p_template=>wwv_flow_api.id(51848808455240825)
,p_display_sequence=>650
,p_region_template_options=>'#DEFAULT#'
,p_component_template_options=>'#DEFAULT#'
,p_new_grid_row=>false
,p_new_grid_column=>false
,p_display_column=>1
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select * from prop_notes',
'where property_id = :P310_PROP_ID',
'and display_flag = ''Y'''))
,p_display_condition_type=>'NEVER'
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>30
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>' - '
,p_query_break_cols=>'0'
,p_query_no_data_found=>'no notes found'
,p_query_num_rows_type=>'NEXT_PREVIOUS_LINKS'
,p_query_row_count_max=>500
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_break_type_flag=>'DEFAULT_BREAK_FORMATTING'
,p_csv_output=>'Y'
,p_csv_output_link_text=>'Excel'
,p_query_asc_image=>'apex/builder/dup.gif'
,p_query_asc_image_attr=>'width="16" height="16" alt="" '
,p_query_desc_image=>'apex/builder/ddown.gif'
,p_query_desc_image_attr=>'width="16" height="16" alt="" '
,p_plug_query_strip_html=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64341121951550702)
,p_query_column_id=>1
,p_column_alias=>'PROP_NOTE_ID'
,p_column_display_sequence=>1
,p_column_heading=>'PROP_NOTE_ID'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64341561050550702)
,p_query_column_id=>2
,p_column_alias=>'PROPERTY_ID'
,p_column_display_sequence=>2
,p_column_heading=>'PROPERTY_ID'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64341961849550703)
,p_query_column_id=>3
,p_column_alias=>'NOTE_SEQ'
,p_column_display_sequence=>3
,p_column_heading=>'SEQ'
,p_default_sort_column_sequence=>1
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64342371152550703)
,p_query_column_id=>4
,p_column_alias=>'NOTES'
,p_column_display_sequence=>4
,p_column_heading=>'NOTES'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64342761993550703)
,p_query_column_id=>5
,p_column_alias=>'NOTE_PURPOSE'
,p_column_display_sequence=>5
,p_column_heading=>'PURPOSE'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64343138040550704)
,p_query_column_id=>6
,p_column_alias=>'NOTE_PRIORITY'
,p_column_display_sequence=>6
,p_column_heading=>'PRIORITY'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64343545111550704)
,p_query_column_id=>7
,p_column_alias=>'DISPLAY_FLAG'
,p_column_display_sequence=>7
,p_column_heading=>'DISPLAY FLAG'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64343955015550704)
,p_query_column_id=>8
,p_column_alias=>'CREATED_BY'
,p_column_display_sequence=>8
,p_column_heading=>'CREATED BY'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64344301854550705)
,p_query_column_id=>9
,p_column_alias=>'CREATION_DATE'
,p_column_display_sequence=>9
,p_column_heading=>'CREATION DATE'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64344710881550705)
,p_query_column_id=>10
,p_column_alias=>'LAST_UPDATED_BY'
,p_column_display_sequence=>10
,p_column_heading=>'LAST UPDATED BY'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64345109143550705)
,p_query_column_id=>11
,p_column_alias=>'LAST_UPDATE_DATE'
,p_column_display_sequence=>11
,p_column_heading=>'LAST UPDATE DATE'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(64345533618550706)
,p_plug_name=>'Ownership'
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--hiddenOverflow'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>65
,p_include_in_reg_disp_sel_yn=>'Y'
,p_plug_new_grid_row=>false
,p_plug_display_column=>1
,p_plug_display_point=>'BODY_3'
,p_plug_query_headings_type=>'QUERY_COLUMNS'
,p_plug_query_num_rows_type=>'NEXT_PREVIOUS_LINKS'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_plug_query_show_nulls_as=>' - '
,p_plug_display_condition_type=>'NEVER'
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_attribute_01=>'N'
,p_attribute_02=>'HTML'
,p_attribute_03=>'N'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(64345908039550706)
,p_name=>'Owner Collections'
,p_parent_plug_id=>wwv_flow_api.id(64345533618550706)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>660
,p_region_template_options=>'#DEFAULT#:t-Region--hiddenOverflow'
,p_component_template_options=>'#DEFAULT#'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select t.* ,',
'aff_ip.get_current_IPI_society(t.collector_id) coll_soc',
'from PROP_SHARE_COLLECTORS_tmp t',
'where session_id = v (''SESSION'')'))
,p_display_condition_type=>'NEVER'
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>20
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>' - '
,p_query_break_cols=>'0'
,p_query_no_data_found=>'No Collection data found'
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
 p_id=>wwv_flow_api.id(64346395926550706)
,p_query_column_id=>1
,p_column_alias=>'SESSION_ID'
,p_column_display_sequence=>26
,p_column_heading=>'Session Id'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64346753965550707)
,p_query_column_id=>2
,p_column_alias=>'SESSION_DATE'
,p_column_display_sequence=>27
,p_column_heading=>'Session Date'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64347177915550707)
,p_query_column_id=>3
,p_column_alias=>'PROPERTY_ID'
,p_column_display_sequence=>28
,p_column_heading=>'Property Id'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64347584149550707)
,p_query_column_id=>4
,p_column_alias=>'CALC_SHARE_PCT'
,p_column_display_sequence=>6
,p_column_heading=>'Collector Share '
,p_use_as_row_header=>'N'
,p_column_format=>'999G999G999G999G990D0000'
,p_column_alignment=>'RIGHT'
,p_heading_alignment=>'RIGHT'
,p_disable_sort_column=>'N'
,p_sum_column=>'Y'
,p_lov_show_nulls=>'NO'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64347996999550707)
,p_query_column_id=>5
,p_column_alias=>'THE_LEVEL'
,p_column_display_sequence=>4
,p_column_heading=>'Level'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_lov_show_nulls=>'NO'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64348344752550708)
,p_query_column_id=>6
,p_column_alias=>'COLLECTOR_AGREEMENT_ID'
,p_column_display_sequence=>12
,p_column_heading=>'Collector Agreement Id'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64348742812550708)
,p_query_column_id=>7
,p_column_alias=>'COLLECTOR_ID'
,p_column_display_sequence=>29
,p_column_heading=>'Collector Id'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64349103792550708)
,p_query_column_id=>8
,p_column_alias=>'COLLECTOR'
,p_column_display_sequence=>1
,p_column_heading=>'Collector'
,p_column_link=>'f?p=&APP_ID.:260:&SESSION.:APEX_CLONE_SESSION:&DEBUG.::P260_IP_ID:#COLLECTOR_ID#'
,p_column_linktext=>'#COLLECTOR#'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64349555127550709)
,p_query_column_id=>9
,p_column_alias=>'COLLECTS_ON_AGREEMENT_ID'
,p_column_display_sequence=>11
,p_column_heading=>'Collects On Agreement Id'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64349974657550709)
,p_query_column_id=>10
,p_column_alias=>'SHARE_PERCENT'
,p_column_display_sequence=>5
,p_column_heading=>'Share Percent'
,p_use_as_row_header=>'N'
,p_column_format=>'999G999G999G999G990D0000'
,p_column_alignment=>'RIGHT'
,p_heading_alignment=>'RIGHT'
,p_disable_sort_column=>'N'
,p_hidden_column=>'Y'
,p_lov_show_nulls=>'NO'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64350372177550709)
,p_query_column_id=>11
,p_column_alias=>'ORIG_SHARE_PCT'
,p_column_display_sequence=>3
,p_column_heading=>'Owner Share'
,p_use_as_row_header=>'N'
,p_column_format=>'999G999G999G999G990D0000'
,p_column_alignment=>'RIGHT'
,p_heading_alignment=>'RIGHT'
,p_disable_sort_column=>'N'
,p_lov_show_nulls=>'NO'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64350751321550709)
,p_query_column_id=>12
,p_column_alias=>'HEIR_PATH'
,p_column_display_sequence=>25
,p_column_heading=>'Heir Path'
,p_default_sort_column_sequence=>1
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64351111850550710)
,p_query_column_id=>13
,p_column_alias=>'COLL_AGREEMENT_TYPE_ID'
,p_column_display_sequence=>13
,p_column_heading=>'Coll Agreement Type Id'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64351558711550710)
,p_query_column_id=>14
,p_column_alias=>'CREATION_CLASS_ID'
,p_column_display_sequence=>14
,p_column_heading=>'Creation Class Id'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64351961838550710)
,p_query_column_id=>15
,p_column_alias=>'RIGHT_TYPE_ID'
,p_column_display_sequence=>15
,p_column_heading=>'Right Type Id'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64352356291550711)
,p_query_column_id=>16
,p_column_alias=>'ROLE_ID'
,p_column_display_sequence=>16
,p_column_heading=>'Role Id'
,p_disable_sort_column=>'N'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64352704149550711)
,p_query_column_id=>17
,p_column_alias=>'START_DATE'
,p_column_display_sequence=>7
,p_column_heading=>'Start Date'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_lov_show_nulls=>'NO'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64353186334550711)
,p_query_column_id=>18
,p_column_alias=>'END_DATE'
,p_column_display_sequence=>8
,p_column_heading=>'End Date'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64353543078550711)
,p_query_column_id=>19
,p_column_alias=>'EFFECTIVE_DATE'
,p_column_display_sequence=>9
,p_column_heading=>'Effective Date'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64353914070550712)
,p_query_column_id=>20
,p_column_alias=>'SIGNATURE_DATE'
,p_column_display_sequence=>17
,p_column_heading=>'Signature Date'
,p_disable_sort_column=>'N'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64354377322550712)
,p_query_column_id=>21
,p_column_alias=>'AMMENDMENT_DATE'
,p_column_display_sequence=>18
,p_column_heading=>'Ammendment Date'
,p_disable_sort_column=>'N'
,p_hidden_column=>'Y'
);
end;
/
begin
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64354745493550712)
,p_query_column_id=>22
,p_column_alias=>'RENEW_ALLOWED_FLAG'
,p_column_display_sequence=>19
,p_column_heading=>'Renew Allowed Flag'
,p_disable_sort_column=>'N'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64355106261550713)
,p_query_column_id=>23
,p_column_alias=>'RENEWAL_TERM_MONTHS'
,p_column_display_sequence=>20
,p_column_heading=>'Renewal Term Months'
,p_disable_sort_column=>'N'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64355598081550713)
,p_query_column_id=>24
,p_column_alias=>'STATUS'
,p_column_display_sequence=>10
,p_column_heading=>'Status'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64355930425550713)
,p_query_column_id=>25
,p_column_alias=>'ALERT_DATE'
,p_column_display_sequence=>21
,p_column_heading=>'Alert Date'
,p_disable_sort_column=>'N'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64356306924550714)
,p_query_column_id=>26
,p_column_alias=>'DOMESTIC_PAY_THRU_DATE'
,p_column_display_sequence=>22
,p_column_heading=>'Domestic Pay Thru Date'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64356756133550714)
,p_query_column_id=>27
,p_column_alias=>'FOREIGN_PAY_THRU_DATE'
,p_column_display_sequence=>23
,p_column_heading=>'Foreign Pay Thru Date'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64357106427550714)
,p_query_column_id=>28
,p_column_alias=>'IGNORE_EARN_DATE_FLAG'
,p_column_display_sequence=>24
,p_column_heading=>'Ignore Earn Date Flag'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64357511951550715)
,p_query_column_id=>29
,p_column_alias=>'COLL_SOC'
,p_column_display_sequence=>2
,p_column_heading=>'Collector Society'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(64357996627550715)
,p_plug_name=>'Ownership'
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--scrollBody'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>45
,p_include_in_reg_disp_sel_yn=>'Y'
,p_plug_new_grid_row=>false
,p_plug_display_column=>1
,p_plug_display_point=>'BODY_3'
,p_query_type=>'SQL'
,p_plug_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT afs.society_name  source,',
'  ainv.ip_id,',
'  ainv.full_name,',
'  ainv.ip_number,',
'  ainv.ipi_base_num,',
'  ainv.ipi_name_number ,',
'  psc.role_id,',
'  psc.right_type_id,',
'  psc.collector_id,',
'  psc.share_collector_id,',
'  psc.share_percentage,',
'  psc.affiliated_society_id,',
'  NVL2(rep.aff_category_hfa, rep.aff_category||''<HR>''||rep.aff_category_hfa, rep.aff_category)  derived_status, ',
' -- aff_society_code current_society_code,',
'-- (SELECT SOCIETY_NAME',
'--      FROM aff_societies',
'--      WHERE SOCIETY_CODE =rep.AFF_SOCIETY_CODE) current_society_code,',
'      spi_screen_utility.membership_society(psc.share_collector_id) current_society_code,',
'      psc.hold_type_id AS Hold_Type,',
' --(select description from PAY_HOLD_TYPES',
'--where hold_type_id =  NVL(pp.hold_type_id,psc.hold_type_id)) Hold_Type, Sachin',
'--where hold_type_id =  pp.hold_reason_id) Hold_Type,',
'DECODE((SELECT SUMMARY_ROLE_CODE FROM aff_roles  WHERE  aff_roles.ROLE_ID=psc.ROLE_ID),''P'',''Publisher'',''W'',''Writer'',psc.ROLE_ID) Role_Type,',
'(SELECT ROLE_CODE FROM aff_roles  WHERE  aff_roles.ROLE_ID=psc.ROLE_ID) ROLE_CODE,',
'(SELECT ROLE_DESC FROM aff_roles  WHERE  aff_roles.ROLE_ID=psc.ROLE_ID) ROLE_DESC,',
'(select RIGHT_TYPE from aff_right_types where  aff_right_types.RIGHT_TYPE_ID = psc.RIGHT_TYPE_ID) RIGHT_TYPE,',
'(select RIGHT_DESCRIPTION from aff_right_types where  aff_right_types.RIGHT_TYPE_ID = psc.RIGHT_TYPE_ID) RIGHT_TYPE_Desc,',
'aff_ip.Get_Current_IPI_Society(psc.COLLECTOR_ID) ipi_society_code,',
'spi_screen_utility.get_coll_admin_ip_numbers(psc.share_collector_id) coll_admin_ip_numbers,',
'spi_screen_utility.get_coll_admin_territories(psc.share_collector_id) coll_admin_territories,',
'spi_screen_utility.get_coll_admin_role_ids(psc.share_collector_id) coll_admin_role_ids,',
'spi_screen_utility.get_coll_territories(psc.share_collector_id) coll_territories,',
'spi_screen_utility.get_share_owner_admin(psc.share_collector_id) coll_admins,',
'-- by Animesh (CASE psc.right_type_id WHEN 2 THEN spi_screen_utility.get_share_owner_admin(psc.property_id, psc.owner_id) ELSE NULL END) admin',
'--(select PARENT_PARTY_ID from AFF_HIERARCHY where CHILD_PARTY_ID=psc.OWNER_ID AND ROWNUM=1) Admin_IP_ID,',
'--(select FULL_NAME from aff_ip_names   where ip_id=(select PARENT_PARTY_ID from AFF_HIERARCHY where CHILD_PARTY_ID=psc.OWNER_ID AND ROWNUM=1)) ADMIN,',
'PSC.authoritative_flag ',
'FROM prop_properties pp,',
'  prop_share_collectors psc,',
'  (SELECT ip.IP_ID,',
'    full_name,',
'    ip.IP_NUMBER,',
'    ip.IPI_BASE_NUMBER ipi_base_num,',
'    ain.name_number ipi_name_number,',
'    ip.group_number',
'  FROM aff_interested_parties ip,',
'    aff_ip_names ain,',
'    aff_name_types nt',
'  WHERE ain.ip_id               = ip.ip_id',
'  AND nt.name_Type_id           = ain.name_type_id',
'  AND ain.name_type_id          = 6',
'  ) ainv,',
'  aff_societies afs,',
'  apcts.rep_affiliates rep',
'WHERE psc.property_id                            = pp.property_id',
'AND psc.affiliated_society_id = afs.society_id(+)',
'AND pp.property_id                               = :P310_PROP_ID',
'AND ainv.ip_id                                   = psc.COLLECTOR_ID',
'AND ainv.ip_id                                   = rep.ip_id(+)',
'AND TRUNC(nvl(START_DATE_ACTIVE, sysdate))<=TRUNC(SYSDATE)',
'AND NVL(psc.end_date_active,TRUNC(sysdate + 1)) >= TRUNC(sysdate +1)',
'--AND     psc.share_percentage != 0',
'AND     psc.share_percentage >= 0',
'--AND     (SELECT SUMMARY_ROLE_CODE FROM aff_roles  WHERE  aff_roles.ROLE_ID=psc.ROLE_ID) =''P''            ',
'ORDER BY ainv.full_name'))
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
);
wwv_flow_api.create_worksheet(
 p_id=>wwv_flow_api.id(69230279693028239)
,p_max_row_count=>'1000000'
,p_allow_save_rpt_public=>'Y'
,p_save_rpt_public_auth_scheme=>wwv_flow_api.id(6185302949729668)
,p_show_nulls_as=>'-'
,p_pagination_type=>'ROWS_X_TO_Y'
,p_pagination_display_pos=>'BOTTOM_RIGHT'
,p_report_list_mode=>'TABS'
,p_show_detail_link=>'N'
,p_show_notify=>'Y'
,p_download_formats=>'CSV:HTML:EMAIL:XLS:PDF:RTF'
,p_owner=>'PKUMAR'
,p_internal_uid=>69230279693028239
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(69230316889028240)
,p_db_column_name=>'SOURCE'
,p_display_order=>10
,p_column_identifier=>'A'
,p_column_label=>'Source'
,p_column_type=>'STRING'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(69230497297028241)
,p_db_column_name=>'FULL_NAME'
,p_display_order=>20
,p_column_identifier=>'B'
,p_column_label=>'Owner Name'
,p_column_link=>'f?p=&APP_ID.:260:&SESSION.:APEX_CLONE_SESSION:&DEBUG.:RP:P260_AFFILIATE_NO,P260_CALLING_PAGE_ID,P260_IP_ID:#IP_NUMBER#,310,#IP_ID#'
,p_column_linktext=>'#FULL_NAME#'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(69230549099028242)
,p_db_column_name=>'IP_NUMBER'
,p_display_order=>50
,p_column_identifier=>'C'
,p_column_label=>'Owner IP Number'
,p_column_type=>'STRING'
,p_static_id=>'ownr_ip'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(69230635744028243)
,p_db_column_name=>'IPI_BASE_NUM'
,p_display_order=>60
,p_column_identifier=>'D'
,p_column_label=>'IPI Base Num'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(69230701609028244)
,p_db_column_name=>'IPI_NAME_NUMBER'
,p_display_order=>70
,p_column_identifier=>'E'
,p_column_label=>'IPI Name Number'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(69230874410028245)
,p_db_column_name=>'ROLE_ID'
,p_display_order=>80
,p_column_identifier=>'F'
,p_column_label=>'Role ID'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(69230945275028246)
,p_db_column_name=>'RIGHT_TYPE_ID'
,p_display_order=>90
,p_column_identifier=>'G'
,p_column_label=>'Right Type ID'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(69231327707028250)
,p_db_column_name=>'AFFILIATED_SOCIETY_ID'
,p_display_order=>130
,p_column_identifier=>'K'
,p_column_label=>'Affiliated Society ID'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(5818221990476701)
,p_db_column_name=>'DERIVED_STATUS'
,p_display_order=>140
,p_column_identifier=>'L'
,p_column_label=>'Derived Status'
,p_column_type=>'STRING'
,p_display_text_as=>'WITHOUT_MODIFICATION'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(5818300112476702)
,p_db_column_name=>'CURRENT_SOCIETY_CODE'
,p_display_order=>150
,p_column_identifier=>'M'
,p_column_label=>'Membership Society Code'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(20488918622530002)
,p_db_column_name=>'IPI_SOCIETY_CODE'
,p_display_order=>160
,p_column_identifier=>'U'
,p_column_label=>'IPI Society Code'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(5820159799476720)
,p_db_column_name=>'ROLE_TYPE'
,p_display_order=>180
,p_column_identifier=>'O'
,p_column_label=>'Role Type'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(5820744733476726)
,p_db_column_name=>'ROLE_CODE'
,p_display_order=>190
,p_column_identifier=>'R'
,p_column_label=>'Owner Role Code'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(5820887795476727)
,p_db_column_name=>'ROLE_DESC'
,p_display_order=>200
,p_column_identifier=>'S'
,p_column_label=>'Role Description'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(5820552121476724)
,p_db_column_name=>'RIGHT_TYPE'
,p_display_order=>210
,p_column_identifier=>'P'
,p_column_label=>'Right Type'
,p_column_type=>'STRING'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(5820646729476725)
,p_db_column_name=>'RIGHT_TYPE_DESC'
,p_display_order=>220
,p_column_identifier=>'Q'
,p_column_label=>'Right Type Description'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(32444510811173403)
,p_db_column_name=>'IP_ID'
,p_display_order=>260
,p_column_identifier=>'X'
,p_column_label=>'Ip id'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(18432578510528007)
,p_db_column_name=>'COLLECTOR_ID'
,p_display_order=>270
,p_column_identifier=>'Y'
,p_column_label=>'Collector id'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(18432668536528008)
,p_db_column_name=>'SHARE_COLLECTOR_ID'
,p_display_order=>280
,p_column_identifier=>'Z'
,p_column_label=>'Share collector id'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(18432774081528009)
,p_db_column_name=>'SHARE_PERCENTAGE'
,p_display_order=>290
,p_column_identifier=>'AA'
,p_column_label=>'Owner Share Percentage'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(18433839455528020)
,p_db_column_name=>'COLL_TERRITORIES'
,p_display_order=>300
,p_column_identifier=>'AE'
,p_column_label=>'Owner Territories'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(18433635327528018)
,p_db_column_name=>'COLL_ADMIN_TERRITORIES'
,p_display_order=>310
,p_column_identifier=>'AC'
,p_column_label=>'Admin Territories'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(18433711828528019)
,p_db_column_name=>'COLL_ADMIN_ROLE_IDS'
,p_display_order=>320
,p_column_identifier=>'AD'
,p_column_label=>'Admin Role Id'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(18433910742528021)
,p_db_column_name=>'COLL_ADMIN_IP_NUMBERS'
,p_display_order=>330
,p_column_identifier=>'AF'
,p_column_label=>'Admin IP Numbers'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(18434076776528022)
,p_db_column_name=>'COLL_ADMINS'
,p_display_order=>340
,p_column_identifier=>'AG'
,p_column_label=>'Admin Names'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(21883310708684612)
,p_db_column_name=>'HOLD_TYPE'
,p_display_order=>350
,p_column_identifier=>'AH'
,p_column_label=>'Hold type'
,p_column_type=>'NUMBER'
,p_display_text_as=>'LOV_ESCAPE_SC'
,p_column_alignment=>'RIGHT'
,p_rpt_named_lov=>wwv_flow_api.id(62720310045606139)
,p_rpt_show_filter_lov=>'1'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(36261251878352917)
,p_db_column_name=>'AUTHORITATIVE_FLAG'
,p_display_order=>360
,p_column_identifier=>'AI'
,p_column_label=>'Authoritative flag'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(5842225024478062)
,p_application_user=>'APXWS_DEFAULT'
,p_report_seq=>10
,p_report_alias=>'58423'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'IP_NUMBER:FULL_NAME:ROLE_TYPE:IPI_BASE_NUM:IPI_NAME_NUMBER:DERIVED_STATUS:CURRENT_SOCIETY_CODE:RIGHT_TYPE_DESC:ROLE_CODE:IPI_SOCIETY_CODE:SHARE_PERCENTAGE:COLL_ADMIN_TERRITORIES:COLL_ADMIN_ROLE_IDS:COLL_TERRITORIES:COLL_ADMIN_IP_NUMBERS:COLL_ADMINS:H'
||'OLD_TYPE:AUTHORITATIVE_FLAG:'
,p_break_on=>'RIGHT_TYPE_DESC:0:0:0:0'
,p_break_enabled_on=>'RIGHT_TYPE_DESC:0:0:0:0'
,p_sum_columns_on_break=>'SHARE_PERCENTAGE'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(6090946245586129)
,p_application_user=>'APXWS_ALTERNATIVE'
,p_name=>'By Role Type'
,p_report_seq=>10
,p_report_alias=>'60910'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'IP_NUMBER:FULL_NAME:ROLE_DESC:RIGHT_TYPE:SOURCE:ROLE_TYPE:IPI_BASE_NUM:IPI_NAME_NUMBER:DERIVED_STATUS:CURRENT_SOCIETY_CODE:IPI_SOCIETY_CODE:ROLE_CODE::IP_ID:COLLECTOR_ID:SHARE_COLLECTOR_ID:SHARE_PERCENTAGE:COLL_ADMIN_TERRITORIES:COLL_ADMIN_ROLE_IDS:C'
||'OLL_TERRITORIES:COLL_ADMIN_IP_NUMBERS:COLL_ADMINS:AUTHORITATIVE_FLAG'
,p_break_on=>'ROLE_TYPE:0:0:0:0:0'
,p_break_enabled_on=>'ROLE_TYPE:0:0:0:0:0'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(6179227002369047)
,p_application_user=>'APXWS_ALTERNATIVE'
,p_name=>'By Right Type'
,p_report_seq=>10
,p_report_alias=>'61793'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'IP_NUMBER:SOURCE:FULL_NAME:ROLE_TYPE:IPI_BASE_NUM:IPI_NAME_NUMBER:DERIVED_STATUS:CURRENT_SOCIETY_CODE:RIGHT_TYPE_DESC:ROLE_CODE:ROLE_DESC_IP_ID_TERRITORY:IPI_SOCIETY_CODE:IP_ID:COLLECTOR_ID:SHARE_COLLECTOR_ID:SHARE_PERCENTAGE:COLL_ADMIN_TERRITORIES:C'
||'OLL_ADMIN_ROLE_IDS:COLL_TERRITORIES:COLL_ADMIN_IP_NUMBERS:COLL_ADMINS:AUTHORITATIVE_FLAG'
,p_break_on=>'RIGHT_TYPE_DESC:0:0:0:0:0'
,p_break_enabled_on=>'RIGHT_TYPE_DESC:0:0:0:0:0'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(22935774206627721)
,p_plug_name=>'Share Owners from Non SESAC Sources'
,p_parent_plug_id=>wwv_flow_api.id(64357996627550715)
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--scrollBody:t-Form--slimPadding'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>10
,p_plug_display_point=>'BODY'
,p_query_type=>'SQL'
,p_plug_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT get_party_name(psc.collector_id) ip_name,',
'    share_collector_id,',
'    psc.property_id property_id,',
'    (SELECT title FROM prop_properties WHERE property_id = psc.property_id',
'    ) property_title,',
'    (SELECT role_desc FROM aff_roles WHERE role_id = psc.role_id',
'    ) role,',
'    role_id,',
'    (SELECT right_description',
'    FROM aff_right_types',
'    WHERE right_type_id = psc.right_type_id',
'    ) right_type,',
'    right_type_id,',
'    collector_id,',
'    share_percentage,',
'    (SELECT society_name',
'      ||''-''',
'      ||society_country',
'    FROM aff_societies',
'    WHERE society_id = psc.affiliated_society_id',
'    ) Society,',
'    (SELECT a.description',
'    FROM prop_sources a,',
'      prop_properties pp',
'    WHERE a.source_id  = pp.source_id',
'    AND pp.property_id = psc.property_id',
'    ) Source_Name,',
'    Start_Date_Active,',
'    End_Date_Active,',
'    (SELECT  cwr.file_name',
'      FROM    cwr_batches cwr,',
'              prop_properties pp',
'      WHERE   cwr.batch_id = pp.batch_id',
'      AND     pp.property_id = psc.property_id',
'      AND     ROWNUM = 1) CWR_FILE,',
'      spi_screen_utility.get_coll_territories(psc.share_collector_id) coll_territories',
'  FROM PROP_SHARE_COLLECTORS psc',
'  WHERE psc.property_id IN',
'    (SELECT property_id',
'    FROM prop_properties',
'    WHERE property_number = :P310_PROP_NUMBER',
'    AND source_id        <> 7',
'    )',
'    AND psc.end_date_active IS NULL'))
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
);
wwv_flow_api.create_worksheet(
 p_id=>wwv_flow_api.id(22935893485627722)
,p_max_row_count=>'1000000'
,p_show_nulls_as=>'-'
,p_pagination_type=>'ROWS_X_TO_Y'
,p_pagination_display_pos=>'BOTTOM_RIGHT'
,p_report_list_mode=>'TABS'
,p_show_detail_link=>'N'
,p_show_notify=>'Y'
,p_download_formats=>'CSV:HTML:EMAIL:XLS:PDF:RTF'
,p_owner=>'DCOLLINS'
,p_internal_uid=>22935893485627722
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(22935905067627723)
,p_db_column_name=>'IP_NAME'
,p_display_order=>10
,p_column_identifier=>'A'
,p_column_label=>'Owner Name'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(22936107282627725)
,p_db_column_name=>'PROPERTY_ID'
,p_display_order=>30
,p_column_identifier=>'C'
,p_column_label=>'Property id'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(22936297343627726)
,p_db_column_name=>'PROPERTY_TITLE'
,p_display_order=>40
,p_column_identifier=>'D'
,p_column_label=>'Title'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(22936384936627727)
,p_db_column_name=>'ROLE'
,p_display_order=>50
,p_column_identifier=>'E'
,p_column_label=>'Role'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(22936439995627728)
,p_db_column_name=>'ROLE_ID'
,p_display_order=>60
,p_column_identifier=>'F'
,p_column_label=>'Role id'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(22936504058627729)
,p_db_column_name=>'RIGHT_TYPE'
,p_display_order=>70
,p_column_identifier=>'G'
,p_column_label=>'Right Type'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(22936651866627730)
,p_db_column_name=>'RIGHT_TYPE_ID'
,p_display_order=>80
,p_column_identifier=>'H'
,p_column_label=>'Right type id'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(22937096117627734)
,p_db_column_name=>'SOCIETY'
,p_display_order=>120
,p_column_identifier=>'L'
,p_column_label=>'Society'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(22937163471627735)
,p_db_column_name=>'SOURCE_NAME'
,p_display_order=>130
,p_column_identifier=>'M'
,p_column_label=>'Source name'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(22937218506627736)
,p_db_column_name=>'START_DATE_ACTIVE'
,p_display_order=>140
,p_column_identifier=>'N'
,p_column_label=>'Start Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(22937357126627737)
,p_db_column_name=>'END_DATE_ACTIVE'
,p_display_order=>150
,p_column_identifier=>'O'
,p_column_label=>'End Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(22937471443627738)
,p_db_column_name=>'CWR_FILE'
,p_display_order=>160
,p_column_identifier=>'P'
,p_column_label=>'CWR File'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(18432864687528010)
,p_db_column_name=>'SHARE_COLLECTOR_ID'
,p_display_order=>170
,p_column_identifier=>'Q'
,p_column_label=>'Share collector id'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(18432945260528011)
,p_db_column_name=>'COLLECTOR_ID'
,p_display_order=>180
,p_column_identifier=>'R'
,p_column_label=>'Collector id'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(18433081135528012)
,p_db_column_name=>'SHARE_PERCENTAGE'
,p_display_order=>190
,p_column_identifier=>'S'
,p_column_label=>'Share percentage'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(22192133385234801)
,p_db_column_name=>'COLL_TERRITORIES'
,p_display_order=>200
,p_column_identifier=>'T'
,p_column_label=>'Territories'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(23339091455731437)
,p_application_user=>'APXWS_DEFAULT'
,p_report_seq=>10
,p_report_alias=>'233391'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'IP_NAME:PROPERTY_TITLE:ROLE:RIGHT_TYPE:SOCIETY:SOURCE_NAME:CWR_FILE:SHARE_PERCENTAGE:COLL_TERRITORIES'
,p_sort_column_1=>'IP_NAME'
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
,p_break_on=>'RIGHT_TYPE:0:0:0:0:0'
,p_break_enabled_on=>'RIGHT_TYPE:0:0:0:0:0'
,p_sum_columns_on_break=>'SHARE_PERCENTAGE'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(64363927274550720)
,p_name=>'Cue Sheets'
,p_region_name=>'CUESH'
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>195
,p_include_in_reg_disp_sel_yn=>'Y'
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#'
,p_display_point=>'BODY_3'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT cs.cue_sheet_id,',
'  cs.program_title,',
'  cs.episode_number,',
'  cs.episode_title,',
'  to_char(cs.orig_air_date,''MM/DD/YYYY'')air_date ,',
'  cs.tms_database_key,',
'  csl.sesac_song_number,',
'  csl.title,',
'  csl.duration duration,',
'  csl.usage_type_code,',
'  -- cs.cue_sheet_type_id,',
'  --cs.performance_source_id ,',
'  --cs.entered_date,',
'  cs.cue_sheet_number Legacy_Cue_Sheet#,',
'  --cs.total_duration ,',
'  -- cs.split_work_flag,',
'  -- cs.split_work_bmi_flag,',
'  -- cs.split_work_ascap_flag,',
'  -- cs.split_work_other_flag,',
'  --cs.split_flags_updated ,',
'  csl.link_property_id',
'FROM prop_cue_sheets cs,',
'  PROP_CUE_SHEET_LINES csl',
'WHERE cs.cue_sheet_id = csl.cue_sheet_id',
'AND link_property_id  = :P310_PROP_ID and :P310_QUERY_CUESHEET=''Y'''))
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>30
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>' - '
,p_query_no_data_found=>'no data found'
,p_query_num_rows_type=>'ROW_RANGES_IN_SELECT_LIST'
,p_query_row_count_max=>500000
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
 p_id=>wwv_flow_api.id(64366341939550722)
,p_query_column_id=>1
,p_column_alias=>'CUE_SHEET_ID'
,p_column_display_sequence=>1
,p_column_heading=>'Cue Sheet ID'
,p_column_link=>'f?p=&APP_ID.:5120:&SESSION.:APEX_CLONE_SESSION:&DEBUG.::P5120_CUE_SHEET_ID,P5120_CALLING_PAGE:#CUE_SHEET_ID#,310'
,p_column_linktext=>'#CUE_SHEET_ID#'
,p_default_sort_column_sequence=>1
,p_disable_sort_column=>'N'
,p_display_as=>'WITHOUT_MODIFICATION'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64366736389550722)
,p_query_column_id=>2
,p_column_alias=>'PROGRAM_TITLE'
,p_column_display_sequence=>2
,p_column_heading=>'Program Title'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64367144548550722)
,p_query_column_id=>3
,p_column_alias=>'EPISODE_NUMBER'
,p_column_display_sequence=>4
,p_column_heading=>'Episode Number'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64367550385550722)
,p_query_column_id=>4
,p_column_alias=>'EPISODE_TITLE'
,p_column_display_sequence=>3
,p_column_heading=>'Episode Title'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64367966356550723)
,p_query_column_id=>5
,p_column_alias=>'AIR_DATE'
,p_column_display_sequence=>5
,p_column_heading=>'Air Date'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64368748438550723)
,p_query_column_id=>6
,p_column_alias=>'TMS_DATABASE_KEY'
,p_column_display_sequence=>6
,p_column_heading=>'TMS Database Key'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64368399039550723)
,p_query_column_id=>7
,p_column_alias=>'SESAC_SONG_NUMBER'
,p_column_display_sequence=>7
,p_column_heading=>'Sesac Song Number'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64364363492550720)
,p_query_column_id=>8
,p_column_alias=>'TITLE'
,p_column_display_sequence=>8
,p_column_heading=>'Sesac Song Title'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64364768294550721)
,p_query_column_id=>9
,p_column_alias=>'DURATION'
,p_column_display_sequence=>10
,p_column_heading=>'Duration'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64365132078550721)
,p_query_column_id=>10
,p_column_alias=>'USAGE_TYPE_CODE'
,p_column_display_sequence=>9
,p_column_heading=>'Usage Type Code'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64365568243550721)
,p_query_column_id=>11
,p_column_alias=>'LEGACY_CUE_SHEET#'
,p_column_display_sequence=>11
,p_column_heading=>'Legacy Cue Sheet#'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64365936618550721)
,p_query_column_id=>12
,p_column_alias=>'LINK_PROPERTY_ID'
,p_column_display_sequence=>12
,p_column_heading=>'Link Property Id'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(64369162137550724)
,p_name=>'Commercials'
,p_region_name=>'COMM'
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>185
,p_include_in_reg_disp_sel_yn=>'Y'
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--hiddenOverflow'
,p_component_template_options=>'#DEFAULT#'
,p_display_point=>'BODY_3'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select c.commercial_id nav,c.commercial_id, c.ad_title, c.brand_name, c.product_name, c.ad_description',
', c.from_air_date, c.to_air_date, C.DURATION , c.cable_flag, c.network_flag',
', c.syndicated_flag, c.local_flag, c.international_flag, c.radio_flag, c.internet_flag',
', ca.agency_name,',
'--, chk_comm_id(c.commercial_id,:P310_PROP_ID ) ',
'''chk_comm_id'' comm_exists',
'from prop_commercials c, prop_comm_agency ca',
'where ca.agency_id(+) = c.agency_id',
'and c.commercial_id in ( select cl.commercial_id from prop_commercial_lines cl',
'where cl.song_property_id = :P310_PROP_ID ) and :P310_QUERY_COMM=''Y'''))
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>30
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>' - '
,p_query_break_cols=>'0'
,p_query_no_data_found=>'No Commercials found'
,p_query_num_rows_type=>'NEXT_PREVIOUS_LINKS'
,p_query_row_count_max=>500
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_break_type_flag=>'DEFAULT_BREAK_FORMATTING'
,p_csv_output=>'Y'
,p_csv_output_link_text=>'Excel'
,p_prn_output=>'N'
,p_query_asc_image=>'apex/builder/dup.gif'
,p_query_asc_image_attr=>'width="16" height="16" alt="" '
,p_query_desc_image=>'apex/builder/ddown.gif'
,p_query_desc_image_attr=>'width="16" height="16" alt="" '
,p_plug_query_strip_html=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(32821605737945105)
,p_query_column_id=>1
,p_column_alias=>'NAV'
,p_column_display_sequence=>1
,p_column_heading=>'Commercial ID'
,p_column_link=>'f?p=&APP_ID.:7110:&SESSION.::&DEBUG.:RP:P7110_COMMERCIAL_ID,P7110_PROP_ID:#COMMERCIAL_ID#,&P310_PROP_ID.'
,p_column_linktext=>'#NAV#'
,p_column_alignment=>'CENTER'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64369513571550724)
,p_query_column_id=>2
,p_column_alias=>'COMMERCIAL_ID'
,p_column_display_sequence=>2
,p_column_heading=>'Commercial ID'
,p_column_link=>'f?p=&APP_ID.:7100:&SESSION.::&DEBUG.:RP:P7100_AD_TITLE,P7100_BRAND_NAME,P7100_PRODUCT_NAME,P7100_FROM_AIR_DATE:#AD_TITLE#,#BRAND_NAME#,#PRODUCT_NAME#,#FROM_AIR_DATE#'
,p_column_linktext=>'#COMMERCIAL_ID#'
,p_disable_sort_column=>'N'
,p_display_when_cond_type=>'NEVER'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
end;
/
begin
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64369940464550724)
,p_query_column_id=>3
,p_column_alias=>'AD_TITLE'
,p_column_display_sequence=>3
,p_column_heading=>'Ad Title'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64370361844550725)
,p_query_column_id=>4
,p_column_alias=>'BRAND_NAME'
,p_column_display_sequence=>4
,p_column_heading=>'Brand Name'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64370764132550725)
,p_query_column_id=>5
,p_column_alias=>'PRODUCT_NAME'
,p_column_display_sequence=>5
,p_column_heading=>'Product Name'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64371130324550726)
,p_query_column_id=>6
,p_column_alias=>'AD_DESCRIPTION'
,p_column_display_sequence=>6
,p_column_heading=>'Ad Description'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64371539292550726)
,p_query_column_id=>7
,p_column_alias=>'FROM_AIR_DATE'
,p_column_display_sequence=>7
,p_column_heading=>'From Air Date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64371970657550726)
,p_query_column_id=>8
,p_column_alias=>'TO_AIR_DATE'
,p_column_display_sequence=>8
,p_column_heading=>'To Air Date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64372344375550726)
,p_query_column_id=>9
,p_column_alias=>'DURATION'
,p_column_display_sequence=>9
,p_column_heading=>'Duration'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64372766071550727)
,p_query_column_id=>10
,p_column_alias=>'CABLE_FLAG'
,p_column_display_sequence=>10
,p_column_heading=>'Cable Flag'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64373166544550727)
,p_query_column_id=>11
,p_column_alias=>'NETWORK_FLAG'
,p_column_display_sequence=>11
,p_column_heading=>'Network Flag'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64373573842550727)
,p_query_column_id=>12
,p_column_alias=>'SYNDICATED_FLAG'
,p_column_display_sequence=>12
,p_column_heading=>'Syndicated Flag'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64373927451550728)
,p_query_column_id=>13
,p_column_alias=>'LOCAL_FLAG'
,p_column_display_sequence=>13
,p_column_heading=>'Local Flag'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64374374630550728)
,p_query_column_id=>14
,p_column_alias=>'INTERNATIONAL_FLAG'
,p_column_display_sequence=>14
,p_column_heading=>'International Flag'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64374721864550728)
,p_query_column_id=>15
,p_column_alias=>'RADIO_FLAG'
,p_column_display_sequence=>15
,p_column_heading=>'Radio Flag'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64375110032550728)
,p_query_column_id=>16
,p_column_alias=>'INTERNET_FLAG'
,p_column_display_sequence=>16
,p_column_heading=>'Internet Flag'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64375578923550729)
,p_query_column_id=>17
,p_column_alias=>'AGENCY_NAME'
,p_column_display_sequence=>17
,p_column_heading=>'Agency Name'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64375909465550729)
,p_query_column_id=>18
,p_column_alias=>'COMM_EXISTS'
,p_column_display_sequence=>18
,p_column_heading=>'Commercial Codes Exist?'
,p_column_link=>'f?p=&APP_ID.:7100:&SESSION.::&DEBUG.:RP:P7100_AD_TITLE,P7100_BRAND_NAME,P7100_PRODUCT_NAME,P7100_FROM_AIR_DATE,P7100_TO_AIR_DATE:#AD_TITLE#,#BRAND_NAME#,#PRODUCT_NAME#,#FROM_AIR_DATE#,#TO_AIR_DATE#'
,p_column_linktext=>'#COMM_EXISTS#'
,p_disable_sort_column=>'N'
,p_display_when_cond_type=>'NEVER'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(64376707028550730)
,p_name=>'Notes'
,p_region_name=>'NOTE'
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>215
,p_include_in_reg_disp_sel_yn=>'Y'
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--hiddenOverflow'
,p_component_template_options=>'#DEFAULT#:t-Report--altRowsDefault:t-Report--rowHighlight'
,p_display_point=>'BODY_3'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'/*select ',
'"PROP_NOTE_ID",',
'"PROPERTY_ID",',
'--NVL(NOTE_SEQ,0) AS "NOTE_SEQ",',
'"NOTES",',
'NVL(NOTE_PRIORITY, ''N'') AS "NOTE_PRIORITY",',
'"DISPLAY_FLAG",',
'"CREATED_BY",',
'"CREATION_DATE",',
'"LAST_UPDATED_BY",',
'"LAST_UPDATE_DATE"',
'from PROP_NOTES',
'WHERE property_id = :P310_PROP_ID and :P310_QUERY_NOTE=''Y''',
'--AND NVL(NOTE_PRIORITY, ''N'')',
'ORDER BY NVL(NOTE_PRIORITY, ''N'') DESC, CREATION_DATE DESC */',
'',
'',
'select "PROP_NOTE_ID",',
'"PROPERTY_ID",',
'--NVL(NOTE_SEQ,0) AS "NOTE_SEQ",',
'"NOTES",',
'NVL(NOTE_PRIORITY, ''N'') AS "NOTE_PRIORITY",',
'"DISPLAY_FLAG",',
'"CREATED_BY",',
'--"CREATION_DATE",',
'"LAST_UPDATED_BY",',
'"LAST_UPDATE_DATE",',
'CASE ',
'    WHEN last_updated_by = ''HFA_DATA_LOAD_SONG_NOTE'' ',
'    THEN TO_DATE(SUBSTR(notes, INSTR(notes, '': '')+2, 17), ''YYYY-MM-DD.HH24.MI.SS'')',
'     ELSE last_update_date',
'     end CREATION_DATE1 ',
'from prop_notes',
'where property_id = :P310_PROP_ID',
'and NVL(display_flag, ''Y'') = ''Y''',
'and :P310_QUERY_NOTE=''Y''',
'order by  NVL(NOTE_PRIORITY, ''N'') DESC,CREATION_DATE1 DESC',
''))
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>' - '
,p_query_no_data_found=>'no data found'
,p_query_num_rows_type=>'ROW_RANGES_IN_SELECT_LIST'
,p_query_row_count_max=>5000
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
 p_id=>wwv_flow_api.id(64378798302550731)
,p_query_column_id=>1
,p_column_alias=>'PROP_NOTE_ID'
,p_column_display_sequence=>1
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64379186867550731)
,p_query_column_id=>2
,p_column_alias=>'PROPERTY_ID'
,p_column_display_sequence=>2
,p_column_heading=>'Property Id'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64377531606550730)
,p_query_column_id=>3
,p_column_alias=>'NOTES'
,p_column_display_sequence=>3
,p_column_heading=>'Notes'
,p_use_as_row_header=>'N'
,p_column_html_expression=>wwv_flow_string.join(wwv_flow_t_varchar2(
'<div >',
'<pre style="white-space:pre-wrap">#NOTES#</pre>',
'</div>'))
,p_display_as=>'WITHOUT_MODIFICATION'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
,p_column_comment=>'Added by Saurav on request of Gary to display fixed format based on the spl character in the notes text'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64378375577550731)
,p_query_column_id=>4
,p_column_alias=>'NOTE_PRIORITY'
,p_column_display_sequence=>4
,p_column_heading=>'Priority'
,p_use_as_row_header=>'N'
,p_display_as=>'TEXT_FROM_LOV'
,p_named_lov=>wwv_flow_api.id(62410853427715388)
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64379559219550732)
,p_query_column_id=>5
,p_column_alias=>'DISPLAY_FLAG'
,p_column_display_sequence=>5
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64379977830550732)
,p_query_column_id=>6
,p_column_alias=>'CREATED_BY'
,p_column_display_sequence=>6
,p_column_heading=>'Created By'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64380721123550732)
,p_query_column_id=>7
,p_column_alias=>'LAST_UPDATED_BY'
,p_column_display_sequence=>7
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64381112211550733)
,p_query_column_id=>8
,p_column_alias=>'LAST_UPDATE_DATE'
,p_column_display_sequence=>8
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(47643813388622828)
,p_query_column_id=>9
,p_column_alias=>'CREATION_DATE1'
,p_column_display_sequence=>9
,p_column_heading=>'Creation Date'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(64409597038550754)
,p_plug_name=>'Parameters'
,p_region_template_options=>'#DEFAULT#'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51848808455240825)
,p_plug_display_sequence=>10
,p_plug_new_grid_row=>false
,p_plug_new_grid_column=>false
,p_plug_display_column=>1
,p_plug_display_point=>'BODY_3'
,p_plug_query_headings_type=>'QUERY_COLUMNS'
,p_plug_query_num_rows_type=>'NEXT_PREVIOUS_LINKS'
,p_plug_query_show_nulls_as=>' - '
,p_plug_display_condition_type=>'NEVER'
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_attribute_01=>'N'
,p_attribute_02=>'HTML'
,p_attribute_03=>'N'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(64411169385550755)
,p_plug_name=>'Property Header Display'
,p_region_template_options=>'#DEFAULT#:is-expanded:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51846539847240824)
,p_plug_display_sequence=>10
,p_plug_grid_column_span=>12
,p_plug_display_point=>'BODY_3'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_attribute_01=>'N'
,p_attribute_02=>'HTML'
,p_attribute_03=>'N'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(64419529963550760)
,p_plug_name=>'&nbsp;'
,p_region_template_options=>'#DEFAULT#:t-TabsRegion-mod--simple'
,p_plug_template=>wwv_flow_api.id(51851408341240826)
,p_plug_display_sequence=>30
,p_include_in_reg_disp_sel_yn=>'Y'
,p_plug_new_grid_column=>false
,p_plug_display_point=>'BODY_3'
,p_plug_source_type=>'NATIVE_DISPLAY_SELECTOR'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_attribute_01=>'STANDARD'
,p_attribute_02=>'N'
,p_attribute_03=>'Y'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(64428776498550768)
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
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(64438765154550775)
,p_name=>'old_Recordings'
,p_region_name=>'REC'
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>165
,p_include_in_reg_disp_sel_yn=>'Y'
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--hiddenOverflow'
,p_component_template_options=>'#DEFAULT#:t-Report--staticRowColors:t-Report--rowHighlightOff'
,p_display_point=>'BODY_3'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT DISTINCT pr.RECORDING_ID, pp.source_property_id,',
'pp.property_id,',
'src.description source,',
'  pp.title rec_title,',
'  pp.property_number,',
'  pp.status,',
'  pp.hold_reason_id,',
'  pr.isrc,',
'  pr.ean,',
'   trim(artist_last_name',
'  ||'' ''',
'  ||artist_first_name) artist,',
'  pc.collection_title Album,',
'  pc.label_name Recording_Label,',
'  pc.release_date release_date,',
'  pr.recording_format,',
'  pa.aka_name Bds_Song_Id,',
'  pa.creation_date link_date,',
'',
'         DECODE (',
'            DECODE (',
'               NVL (',
'                  (SELECT aka_name',
'                     FROM prop_akas a, prop_aka_types b',
'                    WHERE     a.aka_type_id = b.aka_type_id',
'                          AND UPPER (b.aka_type_code) = ''BDS_ENCORE''',
'                          AND a.property_id = pp.property_id',
'                          AND a.aka_name IS NOT NULL',
'                          AND ROWNUM = 1),',
'                  ''''),',
'               '''', ''Link'',',
'               ''Unlink''),',
'            ''Link'',    ''<a href="f?p=518:341:''',
'                    || v (''APP_SESSION'')',
'                    || ''::::P341_REC_PROPERTY_ID,P310_BDS_SONG_ID_UNLINK,P341_BDS_SONG_ID,P341_ARTIST,P341_ARTIST_DISP,P341_BDS_SONG_TITLE,P341_BDS_SONG_TITLE_DISP,P341_CAME_FROM,P341_SOURCE_PROPERTY_ID:''',
'                    || pp.property_id',
'                    || '',,,''',
'                    || TRIM (artist_last_name || '' '' || artist_first_name)',
'                    || '',''',
'                    || TRIM (artist_last_name || '' '' || artist_first_name)',
'                    || '',''',
'                    || trim(pp.title)',
'                    || '',''',
'                    || trim(pp.title)',
'                    || '',''',
'                    || 310',
'                    || '',''',
'                    || :P310_PROP_ID',
'                    || ''">''',
'                    || ''Link''',
'                    || ''</a>'',',
'               ''<a href="f?p=518:310:''',
'            || v (''APP_SESSION'')',
'            || ''::::P310_PAGE_ID,P310_REC_PROPERTY_ID,P310_BDS_SONG_ID_UNLINK:''',
'            || ''310''',
'            || '',''',
'            || pp.property_id',
'            || '',''',
'            || (SELECT aka_name',
'                  FROM (  SELECT aka_name',
'                            FROM prop_akas a, prop_aka_types b',
'                           WHERE     a.aka_type_id = b.aka_type_id',
'                                 AND UPPER (b.aka_type_code) = ''BDS_ENCORE''',
'                                 AND a.property_id = pp.property_id',
'                                 AND aka_name IS NOT NULL',
'                        ORDER BY ROWNUM DESC)',
'                 WHERE ROWNUM = 1)',
'            || ''">''',
'            || ''UnLink''',
'            || ''</a>'')',
'            Action,',
'            pp.CREATED_BY CREATED_BY,',
'            pp.CREATION_DATE CREATION_DATE,',
'            pp.LAST_UPDATED_BY LAST_UPDATED_BY,',
'            pp.LAST_UPDATE_DATE LAST_UPDATE_DATE',
'FROM prop_performers ppr,',
'  prop_recordings pr,',
'  prop_properties pp,',
'  prop_sources src,',
'  prop_akas pa,',
'  prop_collection_records pcr,',
'  prop_collections pc',
'WHERE pr.property_id      = pp.property_id',
'AND pp.source_property_id	 = :P310_PROP_ID',
'AND pp.property_type_id   = 2',
'AND pp.source_id = :P310_PROP_SOURCE_ID',
'AND pp.status = ''A''',
'AND ppr.property_id (+)      = pp.property_id',
'AND src.source_id(+)         = pp.source_id',
'--AND (pa.aka_type_id  is null or pa.aka_type_id = (select pat.aka_type_id from prop_aka_types pat where upper(pat.aka_type_code)=''BDS_ENCORE''))',
'AND pa.aka_type_id(+)=3',
'AND pa.property_id(+)        = pr.property_id',
'AND pr.recording_id = pcr.recording_id(+)',
'AND pcr.collection_id = pc.collection_id(+) and :P310_QUERY_REC=''Y'''))
,p_display_condition_type=>'NEVER'
,p_header=>'<div style="overflow:auto;">'
,p_footer=>'</div>'
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>' - '
,p_query_no_data_found=>'no data found'
,p_query_num_rows_type=>'ROW_RANGES_IN_SELECT_LIST'
,p_query_row_count_max=>50000
,p_pagination_display_position=>'TOP_AND_BOTTOM_LEFT'
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
 p_id=>wwv_flow_api.id(15176927463593248)
,p_query_column_id=>1
,p_column_alias=>'RECORDING_ID'
,p_column_display_sequence=>1
,p_column_link=>'f?p=&APP_ID.:50092:&SESSION.::&DEBUG.:RP,50092:P50092_RECORDING_ID,P50092_SONG_PROPERTY_ID,P50092_BDS_SONG_NO:#RECORDING_ID#,#SOURCE_PROPERTY_ID#,#BDS_SONG_ID#'
,p_column_linktext=>'<img src="#IMAGE_PREFIX#app_ui/img/icons/apex-edit-pencil.png" class="apex-edit-pencil" alt="">'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_report_column_required_role=>wwv_flow_api.id(17255026364731121)
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64439165454550776)
,p_query_column_id=>2
,p_column_alias=>'SOURCE_PROPERTY_ID'
,p_column_display_sequence=>2
,p_hidden_column=>'Y'
,p_display_as=>'HIDDEN'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64439545833550776)
,p_query_column_id=>3
,p_column_alias=>'PROPERTY_ID'
,p_column_display_sequence=>17
,p_column_heading=>'Recording Property ID'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64439987438550777)
,p_query_column_id=>4
,p_column_alias=>'SOURCE'
,p_column_display_sequence=>5
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64440386706550777)
,p_query_column_id=>5
,p_column_alias=>'REC_TITLE'
,p_column_display_sequence=>4
,p_column_heading=>'Recording Title'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64440718803550778)
,p_query_column_id=>6
,p_column_alias=>'PROPERTY_NUMBER'
,p_column_display_sequence=>3
,p_column_heading=>'Property Number'
,p_use_as_row_header=>'N'
,p_default_sort_column_sequence=>1
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64441114080550778)
,p_query_column_id=>7
,p_column_alias=>'STATUS'
,p_column_display_sequence=>6
,p_column_heading=>'Status'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64441582214550778)
,p_query_column_id=>8
,p_column_alias=>'HOLD_REASON_ID'
,p_column_display_sequence=>8
,p_hidden_column=>'Y'
,p_display_as=>'WITHOUT_MODIFICATION'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64441910658550779)
,p_query_column_id=>9
,p_column_alias=>'ISRC'
,p_column_display_sequence=>7
,p_column_heading=>'ISRC'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64442360465550779)
,p_query_column_id=>10
,p_column_alias=>'EAN'
,p_column_display_sequence=>9
,p_column_heading=>'EAN'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64442792097550780)
,p_query_column_id=>11
,p_column_alias=>'ARTIST'
,p_column_display_sequence=>10
,p_column_heading=>'Artist'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64443154662550780)
,p_query_column_id=>12
,p_column_alias=>'ALBUM'
,p_column_display_sequence=>11
,p_column_heading=>'Album'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64443577837550780)
,p_query_column_id=>13
,p_column_alias=>'RECORDING_LABEL'
,p_column_display_sequence=>12
,p_column_heading=>'Recording Label'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64443947407550781)
,p_query_column_id=>14
,p_column_alias=>'RELEASE_DATE'
,p_column_display_sequence=>13
,p_column_heading=>'Release Date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64444391779550781)
,p_query_column_id=>15
,p_column_alias=>'RECORDING_FORMAT'
,p_column_display_sequence=>14
,p_column_heading=>'Recording Format'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64444767045550781)
,p_query_column_id=>16
,p_column_alias=>'BDS_SONG_ID'
,p_column_display_sequence=>15
,p_column_heading=>'BDS Song ID'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64445176692550782)
,p_query_column_id=>17
,p_column_alias=>'LINK_DATE'
,p_column_display_sequence=>16
,p_column_heading=>'Link Date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64445501747550782)
,p_query_column_id=>18
,p_column_alias=>'ACTION'
,p_column_display_sequence=>18
,p_column_heading=>'Action'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_display_when_cond_type=>'VAL_OF_ITEM_IN_COND_EQ_COND2'
,p_display_when_condition=>'P310_STATUS'
,p_display_when_condition2=>'Active'
,p_display_as=>'WITHOUT_MODIFICATION'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(13585379259450114)
,p_query_column_id=>19
,p_column_alias=>'CREATED_BY'
,p_column_display_sequence=>19
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(13585405436450115)
,p_query_column_id=>20
,p_column_alias=>'CREATION_DATE'
,p_column_display_sequence=>20
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(13585504509450116)
,p_query_column_id=>21
,p_column_alias=>'LAST_UPDATED_BY'
,p_column_display_sequence=>21
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(13585659162450117)
,p_query_column_id=>22
,p_column_alias=>'LAST_UPDATE_DATE'
,p_column_display_sequence=>22
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(64446788704550783)
,p_plug_name=>'Change History'
,p_region_name=>'HIST'
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--scrollBody'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>225
,p_include_in_reg_disp_sel_yn=>'Y'
,p_plug_display_point=>'BODY_3'
,p_query_type=>'SQL'
,p_plug_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT *',
'FROM ',
'    (',
'        SELECT ',
'            column_name,',
'            date_changed,',
'            changed_by,',
'            DECODE(column_name,''HOLD_REASON_ID'',(',
'                                                    SELECT description',
'                                                    FROM pay_hold_types',
'                                                    WHERE hold_type_id = prev_value',
'                                                ),',
'                   prev_value) prev_value,',
'            DECODE(column_name,''HOLD_REASON_ID'',(',
'                                                    SELECT description',
'                                                    FROM pay_hold_types',
'                                                    WHERE hold_type_id = new_value',
'                                                ),',
'                   new_value)new_value',
'        FROM apcts.hist_change_logs',
'        WHERE sql_code = ''PROPERTY''',
'        AND table_name = ''PROP_PROPERTIES''',
'        AND key_value1 = :P310_PROP_ID',
'        UNION ALL',
'        SELECT ',
'            column_name,',
'            date_changed,',
'            changed_by,',
'            prev_value,',
'            new_value',
'        FROM apcts.hist_change_logs',
'        WHERE sql_code = ''PROPERTY''',
'        AND table_name = ''PROP_EPISODES''',
'        AND key_value1 = :P310_PROP_ID',
'        UNION ALL',
'        SELECT ',
'            column_name,',
'            date_changed,',
'            changed_by,',
'            prev_value,',
'            new_value',
'        FROM apcts.hist_change_logs',
'        WHERE sql_code = ''PROPERTY''',
'        AND table_name = ''PROP_SHARE_COLLECTORS''',
'        AND key_value1 = :P310_PROP_ID',
'        UNION ALL',
'        SELECT ',
'            column_name,',
'            date_changed,',
'            changed_by,',
'            prev_value,',
'            new_value',
'        FROM apcts.hist_change_logs',
'        WHERE sql_code = ''PROPERTY''',
'        AND table_name = ''PROP_CUE_SHEETS''',
'        AND key_value1 IN',
'                       (',
'                            SELECT pcs.cue_sheet_id',
'                            FROM prop_cue_sheets pcs, prop_episodes pe',
'                            WHERE pcs.episode_id = pe.episode_id',
'                            AND pe.property_id = :P310_PROP_ID',
'                       )',
'        UNION ALL',
'        SELECT ',
'            column_name,',
'            date_changed,',
'            changed_by,',
'            prev_value,',
'            new_value',
'        FROM apcts.hist_change_logs',
'        WHERE sql_code = ''PROPERTY''',
'        AND table_name = ''PROP_CUE_SHEET_LINES''',
'        AND key_value1 IN',
'                       (',
'                            SELECT DISTINCT pcs.cue_sheet_id',
'                            FROM prop_cue_sheets pcs,prop_episodes pe,prop_cue_sheet_lines psl',
'                            WHERE pcs.episode_id = pe.episode_id',
'                            AND pcs.cue_sheet_id = psl.cue_sheet_id',
'                            AND pe.property_id = :P310_PROP_ID',
'                       )',
'        UNION ALL',
'        SELECT ',
'            column_name,',
'            date_changed,',
'            changed_by,',
'            prev_value,',
'            new_value',
'        FROM apcts.hist_change_logs',
'        WHERE sql_code = ''PROPERTY''',
'        AND table_name = ''PROP_COLLECTOR_AGREEMENTS''',
'        AND key_value2 IN',
'                        (',
'                            SELECT g.collector_Agreement_id',
'                            FROM prop_group_props gp, prop_groups g',
'                            WHERE gp.property_id = :P310_PROP_ID',
'                            AND gp.group_id = g.group_id',
'                            AND group_name = ''PAY''',
'                        )',
'        UNION ALL',
'        SELECT ',
'            column_name,',
'            date_changed,',
'            changed_by,',
'            prev_value,',
'            new_value',
'        FROM apcts.hist_change_logs',
'        WHERE sql_code = ''PROPERTY''',
'        AND table_name = ''PROP_NOTES''',
'        AND key_value1 = :P310_PROP_ID ',
'        UNION ALL',
'        SELECT ',
'            column_name,',
'            date_changed,',
'            changed_by,',
'            prev_value,',
'            new_value',
'        FROM apcts.hist_change_logs',
'        WHERE sql_code = ''PROPERTY''',
'        AND table_name = ''PROP_AKAS''',
'        AND key_value1 = :P310_PROP_ID ',
'        UNION ALL',
'        SELECT ',
'            column_name,',
'            date_changed,',
'            changed_by,',
'            prev_value,',
'            new_value',
'        FROM apcts.hist_change_logs',
'        WHERE sql_code = ''PROPERTY''',
'        AND table_name = ''PROP_IP_RELATIONS''',
'        AND key_value1 = :P310_PROP_ID ',
'        UNION ALL',
'        SELECT ',
'            column_name,',
'            date_changed,',
'            changed_by,',
'            prev_value,',
'            new_value',
'        FROM apcts.hist_change_logs',
'        WHERE sql_code = ''PROPERTY''',
'        AND table_name = ''PROP_RECORDINGS''',
'        AND key_value1 = :P310_PROP_ID ',
'        UNION ALL',
'        SELECT ',
'            column_name,',
'            date_changed,',
'            changed_by,',
'            prev_value,',
'            new_value',
'        FROM apcts.hist_change_logs',
'        WHERE sql_code = ''PROPERTY''',
'        AND table_name = ''SPI_TRANSACTION_STATUS''',
'        AND key_value1 = :P310_PROP_NUMBER',
'    )',
'WHERE :P310_QUERY_HISTORY = ''Y'''))
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
,p_plug_header=>'Please enter search criteria and press Go'
);
wwv_flow_api.create_worksheet(
 p_id=>wwv_flow_api.id(4539657448564131)
,p_max_row_count=>'1000000'
,p_allow_save_rpt_public=>'Y'
,p_save_rpt_public_auth_scheme=>wwv_flow_api.id(6185302949729668)
,p_show_nulls_as=>'-'
,p_pagination_type=>'ROWS_X_TO_Y'
,p_pagination_display_pos=>'BOTTOM_RIGHT'
,p_report_list_mode=>'TABS'
,p_show_detail_link=>'N'
,p_show_notify=>'Y'
,p_download_formats=>'CSV:HTML:EMAIL:XLS:PDF:RTF'
,p_owner=>'DCOLLINS'
,p_internal_uid=>4539657448564131
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(4539736402564132)
,p_db_column_name=>'COLUMN_NAME'
,p_display_order=>10
,p_column_identifier=>'A'
,p_column_label=>'Column name'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(4539850978564133)
,p_db_column_name=>'DATE_CHANGED'
,p_display_order=>20
,p_column_identifier=>'B'
,p_column_label=>'Date changed'
,p_column_type=>'DATE'
,p_heading_alignment=>'LEFT'
,p_tz_dependent=>'N'
);
end;
/
begin
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(4539946926564134)
,p_db_column_name=>'CHANGED_BY'
,p_display_order=>30
,p_column_identifier=>'C'
,p_column_label=>'Changed by'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(4540053890564135)
,p_db_column_name=>'PREV_VALUE'
,p_display_order=>40
,p_column_identifier=>'D'
,p_column_label=>'Prev value'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(4540157091564136)
,p_db_column_name=>'NEW_VALUE'
,p_display_order=>50
,p_column_identifier=>'E'
,p_column_label=>'New value'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(4605992826928668)
,p_application_user=>'APXWS_DEFAULT'
,p_report_seq=>10
,p_report_alias=>'46060'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'COLUMN_NAME:DATE_CHANGED:CHANGED_BY:PREV_VALUE:NEW_VALUE'
,p_sort_column_1=>'DATE_CHANGED'
,p_sort_direction_1=>'DESC'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(5815913130315124)
,p_plug_name=>'Share Owner History'
,p_parent_plug_id=>wwv_flow_api.id(64446788704550783)
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>20
,p_plug_display_point=>'BODY'
,p_query_type=>'SQL'
,p_plug_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'  SELECT pp.property_number,',
'         pp.title,',
'         ra.aff_num,',
'         ra.aff_name,',
'         r.role_code,',
'         DECODE(r.SUMMARY_ROLE_CODE,''P'',''Publisher'',''W'',''Writer'',pso.ROLE_ID) owner_Role_Type,',
'         DECODE(r.SUMMARY_ROLE_CODE,''P'',''Publisher'',''W'',''Writer'',psa.ROLE_ID) admin_Role_Type,',
'         r.ROLE_DESC,',
'          (SELECT role_desc FROM aff_roles WHERE role_id = pso.role_id',
'            ) role,',
'         s.society_code || ''-'' || s.society_name prop_share_society,        ',
'         pso.share_collector_id, ',
'         psa.share_admin_id, ',
'         pso.right_type_id, ',
'          (SELECT right_description',
'                FROM aff_right_types',
'                WHERE right_type_id = pso.right_type_id',
'        ) right_type,',
'         pso.start_date_active, ',
'         pso.end_date_active,',
'         pso.hold_type_id,',
'         pso.authoritative_flag,',
'         pso.last_updated_by,',
'         pso.last_update_date,',
'    spi_screen_utility.get_party_name(pso.collector_id) owner,',
'    spi_screen_utility.get_ip_number(pso.collector_id)  owner_ip_number,',
'    spi_screen_utility.get_hfa_number(pso.collector_id) owner_hfa_number,',
'    pso.share_percentage, ',
'    spi_screen_utility.get_party_name(psa.admin_id) admin,',
'    spi_screen_utility.get_ip_number(psa.admin_id)  admin_ip_number,',
'    spi_screen_utility.get_hfa_number(psa.admin_id) admin_hfa_number,',
'    psa.admin_share_percent,',
'    psa.start_date_active Admin_Start_date, ',
'    psa.end_date_active Admin_end_date,',
'--         Prop_share_Owner.get_territory_main(:P310_PROP_ID,pso.owner_id,TRUNC(pso.end_date_active),pso.right_type_id)Territory',
'        Prop_share_Owner.get_territory_main(:P310_PROP_ID,pso.collector_id,TRUNC(pso.end_date_active),pso.right_type_id)Territory,',
'        Prop_share_Owner.get_territory_main(:P310_PROP_ID,psa.admin_id,TRUNC(psa.end_date_active),pso.right_type_id)Admin_Territory',
'',
'    FROM prop_share_collectors pso,',
'         prop_properties  pp,',
'         rep_affiliates   ra,',
'         aff_societies    s,',
'         aff_roles        r,',
'         prop_share_Admins psa',
'   WHERE -- (pso.end_date_active IS NOT NULL or psa.end_date_active is not null) AND',
'           pso.share_collector_id = psa.share_collector_id(+)',
'         AND pso.collector_id = ra.ip_id(+)',
'         AND pso.role_id = r.role_id',
'         AND pso.affiliated_society_id = s.society_id(+)',
'         AND pso.property_id = pp.property_id',
'         AND pso.property_id = :P310_PROP_ID',
'         AND right_type_id in (1,2)',
'ORDER BY pso.end_date_active '))
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
);
wwv_flow_api.create_worksheet(
 p_id=>wwv_flow_api.id(7683845673449708)
,p_max_row_count=>'1000000'
,p_show_nulls_as=>'-'
,p_pagination_type=>'ROWS_X_TO_Y'
,p_pagination_display_pos=>'BOTTOM_RIGHT'
,p_report_list_mode=>'TABS'
,p_show_detail_link=>'N'
,p_show_notify=>'Y'
,p_download_formats=>'CSV:HTML:EMAIL:XLS:PDF:RTF'
,p_owner=>'KBAKSHI'
,p_internal_uid=>7683845673449708
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7683971498449709)
,p_db_column_name=>'PROPERTY_NUMBER'
,p_display_order=>10
,p_column_identifier=>'A'
,p_column_label=>'Property Number'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7684052873449710)
,p_db_column_name=>'TITLE'
,p_display_order=>20
,p_column_identifier=>'B'
,p_column_label=>'Title'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7684110803449711)
,p_db_column_name=>'AFF_NUM'
,p_display_order=>30
,p_column_identifier=>'C'
,p_column_label=>'Aff Num'
,p_column_type=>'STRING'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7684292429449712)
,p_db_column_name=>'AFF_NAME'
,p_display_order=>40
,p_column_identifier=>'D'
,p_column_label=>'Aff Name'
,p_column_type=>'STRING'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7684325563449713)
,p_db_column_name=>'ROLE_CODE'
,p_display_order=>50
,p_column_identifier=>'E'
,p_column_label=>'Role Code'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7684457103449714)
,p_db_column_name=>'OWNER_ROLE_TYPE'
,p_display_order=>60
,p_column_identifier=>'F'
,p_column_label=>'Owner Role Type'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7684599211449715)
,p_db_column_name=>'ADMIN_ROLE_TYPE'
,p_display_order=>70
,p_column_identifier=>'G'
,p_column_label=>'Admin Role Type'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7684607863449716)
,p_db_column_name=>'ROLE_DESC'
,p_display_order=>80
,p_column_identifier=>'H'
,p_column_label=>'Role Desc'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7684706373449717)
,p_db_column_name=>'ROLE'
,p_display_order=>90
,p_column_identifier=>'I'
,p_column_label=>'Role'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7684815066449718)
,p_db_column_name=>'PROP_SHARE_SOCIETY'
,p_display_order=>100
,p_column_identifier=>'J'
,p_column_label=>'Prop Share Society'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7684909331449719)
,p_db_column_name=>'SHARE_COLLECTOR_ID'
,p_display_order=>110
,p_column_identifier=>'K'
,p_column_label=>'Share Collector Id'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7685063203449720)
,p_db_column_name=>'SHARE_ADMIN_ID'
,p_display_order=>120
,p_column_identifier=>'L'
,p_column_label=>'Share Admin Id'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7685114553449721)
,p_db_column_name=>'RIGHT_TYPE_ID'
,p_display_order=>130
,p_column_identifier=>'M'
,p_column_label=>'Right Type Id'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7685237575449722)
,p_db_column_name=>'RIGHT_TYPE'
,p_display_order=>140
,p_column_identifier=>'N'
,p_column_label=>'Right Type'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7685341577449723)
,p_db_column_name=>'START_DATE_ACTIVE'
,p_display_order=>150
,p_column_identifier=>'O'
,p_column_label=>'Owner Start Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7685408135449724)
,p_db_column_name=>'END_DATE_ACTIVE'
,p_display_order=>160
,p_column_identifier=>'P'
,p_column_label=>'Owner End Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7685591100449725)
,p_db_column_name=>'HOLD_TYPE_ID'
,p_display_order=>170
,p_column_identifier=>'Q'
,p_column_label=>'Hold Type Id'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7685602402449726)
,p_db_column_name=>'AUTHORITATIVE_FLAG'
,p_display_order=>180
,p_column_identifier=>'R'
,p_column_label=>'Authoritative Flag'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7685716670449727)
,p_db_column_name=>'LAST_UPDATED_BY'
,p_display_order=>190
,p_column_identifier=>'S'
,p_column_label=>'Last Updated By'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7685898370449728)
,p_db_column_name=>'LAST_UPDATE_DATE'
,p_display_order=>200
,p_column_identifier=>'T'
,p_column_label=>'Last Update Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7685950537449729)
,p_db_column_name=>'OWNER'
,p_display_order=>210
,p_column_identifier=>'U'
,p_column_label=>'Owner'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7686009330449730)
,p_db_column_name=>'OWNER_IP_NUMBER'
,p_display_order=>220
,p_column_identifier=>'V'
,p_column_label=>'Owner IP Number'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7686173810449731)
,p_db_column_name=>'OWNER_HFA_NUMBER'
,p_display_order=>230
,p_column_identifier=>'W'
,p_column_label=>'Owner HFA Number'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7686206746449732)
,p_db_column_name=>'SHARE_PERCENTAGE'
,p_display_order=>240
,p_column_identifier=>'X'
,p_column_label=>'Owner Share Percentage'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7686339610449733)
,p_db_column_name=>'ADMIN'
,p_display_order=>250
,p_column_identifier=>'Y'
,p_column_label=>'Admin'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7686424177449734)
,p_db_column_name=>'ADMIN_IP_NUMBER'
,p_display_order=>260
,p_column_identifier=>'Z'
,p_column_label=>'Admin IP Number'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7686538935449735)
,p_db_column_name=>'ADMIN_HFA_NUMBER'
,p_display_order=>270
,p_column_identifier=>'AA'
,p_column_label=>'Admin HFA Number'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7686638140449736)
,p_db_column_name=>'ADMIN_SHARE_PERCENT'
,p_display_order=>280
,p_column_identifier=>'AB'
,p_column_label=>'Admin Share Percent'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7686758547449737)
,p_db_column_name=>'ADMIN_START_DATE'
,p_display_order=>290
,p_column_identifier=>'AC'
,p_column_label=>'Admin Start Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7686892665449738)
,p_db_column_name=>'ADMIN_END_DATE'
,p_display_order=>300
,p_column_identifier=>'AD'
,p_column_label=>'Admin End Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7686986069449739)
,p_db_column_name=>'TERRITORY'
,p_display_order=>310
,p_column_identifier=>'AE'
,p_column_label=>'Owner Territory'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7687063340449740)
,p_db_column_name=>'ADMIN_TERRITORY'
,p_display_order=>320
,p_column_identifier=>'AF'
,p_column_label=>'Admin Territory'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(7699231352462926)
,p_application_user=>'APXWS_DEFAULT'
,p_report_seq=>10
,p_report_alias=>'76993'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'RIGHT_TYPE:PROPERTY_NUMBER:OWNER_IP_NUMBER:OWNER:OWNER_HFA_NUMBER:SHARE_PERCENTAGE:ROLE_CODE:OWNER_ROLE_TYPE:ROLE:TERRITORY:START_DATE_ACTIVE:END_DATE_ACTIVE:ADMIN_IP_NUMBER:ADMIN:ADMIN_HFA_NUMBER:ADMIN_SHARE_PERCENT:ADMIN_ROLE_TYPE:ADMIN_TERRITORY:A'
||'DMIN_START_DATE:ADMIN_END_DATE:AUTHORITATIVE_FLAG:LAST_UPDATED_BY:LAST_UPDATE_DATE:'
,p_sort_column_1=>'LAST_UPDATE_DATE'
,p_sort_direction_1=>'DESC'
,p_break_on=>'RIGHT_TYPE:0:0:0:0:0'
,p_break_enabled_on=>'RIGHT_TYPE:0:0:0:0:0'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(64449163463550785)
,p_name=>'Share Owner History'
,p_parent_plug_id=>wwv_flow_api.id(64446788704550783)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>10
,p_region_template_options=>'#DEFAULT#:t-Region--hiddenOverflow'
,p_component_template_options=>'#DEFAULT#'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'  SELECT pp.property_number,',
'         pp.title,',
'         ra.aff_num,',
'         ra.aff_name,',
'         r.role_code,',
'         pso.share_percentage default_share_percent, --pso.default_share_percent,',
'         s.society_code || ''-'' || s.society_name prop_share_society,',
'         pso.start_date_active,',
'         pso.end_date_active,',
'         pso.hold_type_id,',
'         pso.last_updated_by,',
'--         Prop_share_Owner.get_territory_main(:P310_PROP_ID,pso.owner_id,TRUNC(pso.end_date_active),pso.right_type_id)Territory',
'        Prop_share_Owner.get_territory_main(:P310_PROP_ID,pso.collector_id,TRUNC(pso.end_date_active),pso.right_type_id)Territory',
'    FROM prop_share_collectors pso,',
'         prop_properties  pp,',
'         rep_affiliates   ra,',
'         aff_societies    s,',
'         aff_roles        r',
'   WHERE pso.end_date_active IS NOT NULL',
'         AND pso.collector_id = ra.ip_id(+)',
'         AND pso.role_id = r.role_id',
'         AND pso.affiliated_society_id = s.society_id(+)',
'         AND pso.property_id = pp.property_id',
'         AND pso.property_id = :P310_PROP_ID',
'ORDER BY 1, 3'))
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
 p_id=>wwv_flow_api.id(64449589413550786)
,p_query_column_id=>1
,p_column_alias=>'PROPERTY_NUMBER'
,p_column_display_sequence=>1
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64449929597550786)
,p_query_column_id=>2
,p_column_alias=>'TITLE'
,p_column_display_sequence=>2
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64450324426550786)
,p_query_column_id=>3
,p_column_alias=>'AFF_NUM'
,p_column_display_sequence=>3
,p_column_heading=>'Share Owner Number'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64450701112550787)
,p_query_column_id=>4
,p_column_alias=>'AFF_NAME'
,p_column_display_sequence=>4
,p_column_heading=>'Share Owner Name'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64451121237550787)
,p_query_column_id=>5
,p_column_alias=>'ROLE_CODE'
,p_column_display_sequence=>5
,p_column_heading=>'Role code'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64451582545550787)
,p_query_column_id=>6
,p_column_alias=>'DEFAULT_SHARE_PERCENT'
,p_column_display_sequence=>6
,p_column_heading=>'Default share percent'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64451931984550787)
,p_query_column_id=>7
,p_column_alias=>'PROP_SHARE_SOCIETY'
,p_column_display_sequence=>7
,p_column_heading=>'Prop Share Society'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64452367329550788)
,p_query_column_id=>8
,p_column_alias=>'START_DATE_ACTIVE'
,p_column_display_sequence=>8
,p_column_heading=>'Start Date Active'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64452743669550788)
,p_query_column_id=>9
,p_column_alias=>'END_DATE_ACTIVE'
,p_column_display_sequence=>9
,p_column_heading=>'End Date Active'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64453170025550788)
,p_query_column_id=>10
,p_column_alias=>'HOLD_TYPE_ID'
,p_column_display_sequence=>11
,p_column_heading=>'Hold type id'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64453511677550789)
,p_query_column_id=>11
,p_column_alias=>'LAST_UPDATED_BY'
,p_column_display_sequence=>12
,p_column_heading=>'Last updated by'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(11419053989058737)
,p_query_column_id=>12
,p_column_alias=>'TERRITORY'
,p_column_display_sequence=>10
,p_column_heading=>'Territory'
,p_use_as_row_header=>'N'
,p_display_as=>'WITHOUT_MODIFICATION'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(64486395977550813)
,p_name=>'Registration History - old'
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>145
,p_include_in_reg_disp_sel_yn=>'Y'
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--hiddenOverflow'
,p_component_template_options=>'#DEFAULT#'
,p_display_point=>'BODY_3'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT ',
'    logs.column_name,',
'    logs.date_changed,',
'    logs.changed_by,',
'    logs.prev_value, ',
'    logs.new_value',
'FROM spi_transaction_status tst, hist_change_logs logs',
'WHERE tst.selected_sesac_property_id IS NOT NULL ',
'AND tst.selected_sesac_property_id = :P310_PROP_ID',
'AND logs.table_name = ''SPI_TRANSACTION_STATUS''',
'AND logs.key_value1 = tst.transaction_status_id',
'AND logs.sql_code = ''PROPERTY'' '))
,p_display_condition_type=>'NEVER'
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>' - '
,p_query_no_data_found=>'No history exists.'
,p_query_num_rows_type=>'NEXT_PREVIOUS_LINKS'
,p_query_row_count_max=>500
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_csv_output=>'N'
,p_prn_output=>'N'
,p_sort_null=>'L'
,p_query_asc_image=>'apex/builder/dup.gif'
,p_query_asc_image_attr=>'width="16" height="16" alt="" '
,p_query_desc_image=>'apex/builder/ddown.gif'
,p_query_desc_image_attr=>'width="16" height="16" alt="" '
,p_plug_query_strip_html=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(37908954676150720)
,p_query_column_id=>1
,p_column_alias=>'COLUMN_NAME'
,p_column_display_sequence=>1
,p_column_heading=>'Column name'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(37909014962150721)
,p_query_column_id=>2
,p_column_alias=>'DATE_CHANGED'
,p_column_display_sequence=>2
,p_column_heading=>'Date changed'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(37909187805150722)
,p_query_column_id=>3
,p_column_alias=>'CHANGED_BY'
,p_column_display_sequence=>3
,p_column_heading=>'Changed by'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(37909272249150723)
,p_query_column_id=>4
,p_column_alias=>'PREV_VALUE'
,p_column_display_sequence=>4
,p_column_heading=>'Prev value'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(37909307894150724)
,p_query_column_id=>5
,p_column_alias=>'NEW_VALUE'
,p_column_display_sequence=>5
,p_column_heading=>'New value'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(67104601493925876)
,p_plug_name=>'Agreements'
,p_region_name=>'AGREEMENTS'
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--removeHeader:t-Region--scrollBody'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>125
,p_include_in_reg_disp_sel_yn=>'Y'
,p_plug_display_point=>'BODY_3'
,p_plug_query_num_rows=>15
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_attribute_01=>'N'
,p_attribute_02=>'HTML'
,p_attribute_03=>'Y'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(64328306678550690)
,p_name=>'Affiliation Agreements'
,p_region_name=>'AREEMENTS1'
,p_parent_plug_id=>wwv_flow_api.id(67104601493925876)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>430
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--hiddenOverflow'
,p_component_template_options=>'#DEFAULT#'
,p_new_grid_row=>false
,p_new_grid_column=>false
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select * from ',
'(SELECT trim(soc.society_name)',
'  ||'' ''',
'  ||soc.society_country society ,',
'  rc.role_code',
'  ||'' ''',
'  ||rc.role_desc role_type ,',
'  ama.signature_date,',
'  ama.membership_share ,',
'  valid_from_date,',
'  valid_to_date,',
'  ammendment_date,',
'  effective_date ,',
'  ain.primary_name aff_name,',
'  ama.renewal_term_months,',
'  ama.status,',
'  ama.alert_date,',
'  ama.domestic_pay_thru_date ,',
'  MIN(ama.membership_agreement_id) membership_agreement_id,',
'  pgp.property_id,',
'count(1) row_count',
'FROM AFF_MEMBERSHIP_AGREEMENTS ama,',
'  AFF_SOCIETIES soc,',
'  AFF_ROLES rc,',
'  AFF_RIGHT_TYPES rt ,',
'  PROP_GROUPS pg,',
'  PROP_GROUP_PROPS pgp,',
'  AFF_IP_NAME_V ain',
'WHERE ama.group_id   = pg.group_id',
'AND PGP.GROUP_ID     = pg.group_id',
'AND pgp.property_id  = :P310_PROP_ID',
'AND SOC.SOCIETY_ID   = AMA.SOCIETY_ID',
'AND ain.ip_id        = ama.ip_id',
'AND RC.ROLE_ID       = AMA.ROLE_ID',
'AND RT.RIGHT_TYPE_ID = AMA.RIGHT_TYPE_ID',
'and nvl (ama.valid_to_date, sysdate + 1) > sysdate',
'and exists (select 1 from aff_interested_parties aip where aip.ip_id=ama.ip_id and aip.status_code_id=120)',
' GROUP BY trim(soc.society_name)',
'  ||'' ''',
'  ||soc.society_country  ,',
'  rc.role_code',
'  ||'' ''',
'  ||rc.role_desc  ,',
'  ama.signature_date,',
'  ama.membership_share ,',
'  valid_from_date,',
'  valid_to_date,',
'  ammendment_date,',
'  effective_date ,',
'  ain.primary_name ,',
'  ama.renewal_term_months,',
'  ama.status,',
'  ama.alert_date,',
'  ama.domestic_pay_thru_date ,',
'  aff_ip.Get_Current_IPI_Society(ama.ip_id),',
'  pgp.property_id',
' )',
' where :P310_QUERY_AGREEMENTS = ''Y'''))
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>20
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>' - '
,p_query_break_cols=>'0'
,p_query_no_data_found=>'no data found'
,p_query_num_rows_type=>'NEXT_PREVIOUS_LINKS'
,p_query_row_count_max=>500
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_break_type_flag=>'DEFAULT_BREAK_FORMATTING'
,p_csv_output=>'Y'
,p_csv_output_link_text=>'Excel'
,p_query_asc_image=>'apex/builder/dup.gif'
,p_query_asc_image_attr=>'width="16" height="16" alt="" '
,p_query_desc_image=>'apex/builder/ddown.gif'
,p_query_desc_image_attr=>'width="16" height="16" alt="" '
,p_plug_query_strip_html=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64329992986550692)
,p_query_column_id=>1
,p_column_alias=>'SOCIETY'
,p_column_display_sequence=>3
,p_column_heading=>'Society'
,p_default_sort_column_sequence=>1
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64330342504550692)
,p_query_column_id=>2
,p_column_alias=>'ROLE_TYPE'
,p_column_display_sequence=>4
,p_column_heading=>'Role Type'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64330728782550692)
,p_query_column_id=>3
,p_column_alias=>'SIGNATURE_DATE'
,p_column_display_sequence=>7
,p_column_heading=>'Signature Date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64331165927550693)
,p_query_column_id=>4
,p_column_alias=>'MEMBERSHIP_SHARE'
,p_column_display_sequence=>2
,p_column_heading=>'Membership Share'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64331596602550693)
,p_query_column_id=>5
,p_column_alias=>'VALID_FROM_DATE'
,p_column_display_sequence=>5
,p_column_heading=>'Valid From Date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64331943195550693)
,p_query_column_id=>6
,p_column_alias=>'VALID_TO_DATE'
,p_column_display_sequence=>6
,p_column_heading=>'Valid To Date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64332368382550694)
,p_query_column_id=>7
,p_column_alias=>'AMMENDMENT_DATE'
,p_column_display_sequence=>8
,p_column_heading=>'Ammendment Date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64332706484550694)
,p_query_column_id=>8
,p_column_alias=>'EFFECTIVE_DATE'
,p_column_display_sequence=>9
,p_column_heading=>'Effective Date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64333193749550694)
,p_query_column_id=>9
,p_column_alias=>'AFF_NAME'
,p_column_display_sequence=>1
,p_column_heading=>'Party Name'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_lov_show_nulls=>'YES'
,p_lov_display_extra=>'YES'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64333571200550694)
,p_query_column_id=>10
,p_column_alias=>'RENEWAL_TERM_MONTHS'
,p_column_display_sequence=>10
,p_column_heading=>'Renewal Term Months'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64333972315550695)
,p_query_column_id=>11
,p_column_alias=>'STATUS'
,p_column_display_sequence=>11
,p_column_heading=>'Status'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64334341258550695)
,p_query_column_id=>12
,p_column_alias=>'ALERT_DATE'
,p_column_display_sequence=>12
,p_column_heading=>'Alert Date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64334780513550695)
,p_query_column_id=>13
,p_column_alias=>'DOMESTIC_PAY_THRU_DATE'
,p_column_display_sequence=>13
,p_column_heading=>'Domestic Pay Thru Date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64328748168550691)
,p_query_column_id=>14
,p_column_alias=>'MEMBERSHIP_AGREEMENT_ID'
,p_column_display_sequence=>14
,p_column_heading=>'Membership Agreement Id'
,p_column_link=>'f?p=&APP_ID.:7240:&SESSION.::&DEBUG.:RP,7240:P7240_MEMBERSHIP_AGR_ID,P7240_CALLING_PAGE_ID,P7240_REQUEST:#MEMBERSHIP_AGREEMENT_ID#,&APP_PAGE_ID.,AGR'
,p_column_linktext=>'#MEMBERSHIP_AGREEMENT_ID#'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_display_as=>'WITHOUT_MODIFICATION'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64329158396550691)
,p_query_column_id=>15
,p_column_alias=>'PROPERTY_ID'
,p_column_display_sequence=>15
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64329528933550692)
,p_query_column_id=>16
,p_column_alias=>'ROW_COUNT'
,p_column_display_sequence=>16
,p_column_heading=>'Row Count'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(64381996642550734)
,p_name=>'Collection Agreements'
,p_parent_plug_id=>wwv_flow_api.id(67104601493925876)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>460
,p_region_template_options=>'#DEFAULT#:t-Region--hiddenOverflow'
,p_component_template_options=>'#DEFAULT#:t-Report--stretch:t-Report--altRowsDefault:t-Report--rowHighlight'
,p_new_grid_row=>false
,p_new_grid_column=>false
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
' select * from ',
' (    ',
'    SELECT LEVEL the_level,',
'        collector_agreement_id,',
'        collector_id,',
'        (select primary_name from aff_ip_name_v where ip_id = collector_id ) collector,',
'       (select ip_number from aff_ip_name_v where ip_id = collector_id ) collectornum,',
'    (select primary_name from aff_ip_name_v where ip_id = ( select collector_id  from PROP_COLLECTOR_AGREEMENTS pca2 where pca2.collector_agreement_id = pca1.collects_on_agreement_id and pca2.right_type_id = pca1.right_type_id) ) parent,',
'    (select ip_number from aff_ip_name_v where ip_id = ( select collector_id  from PROP_COLLECTOR_AGREEMENTS pca2 where pca2.collector_agreement_id = pca1.collects_on_agreement_id and pca2.right_type_id = pca1.right_type_id) ) parentnum,',
'    (select collector_id  from PROP_COLLECTOR_AGREEMENTS pca2 where pca2.collector_agreement_id = pca1.collects_on_agreement_id and pca2.right_type_id = pca1.right_type_id)  parentid,',
'    collects_on_agreement_id,',
'    share_percent,',
'    heir_path,',
'    right_type_id,',
'    COLL_AGREEMENT_TYPE_ID,',
'    CLASSIFICATION',
'    FROM PROP_COLLECTOR_AGREEMENTS pca1',
'    WHERE end_date IS NULL',
'--    AND right_type_id = 1',
'    AND UPPER(NVL(status, ''A'')) = ''A''',
'    CONNECT BY collects_on_agreement_id = PRIOR collector_agreement_id',
'    START WITH collector_agreement_id IN ',
'    (',
'        SELECT g.collector_agreement_id',
'        FROM prop_group_props gp, prop_groups g, prop_collector_agreements pc2',
'        WHERE gp.property_id = :P310_prop_id',
'        AND gp.GROUP_ID = g.GROUP_ID',
'        AND group_type_id = 0',
'        AND g.collector_agreement_id = pc2.collector_agreement_id',
'        AND pc2.end_date IS NULL',
'        AND pc2.coll_agreement_type_id = 1',
'        AND pc2.collects_on_agreement_id IS NULL ',
'    )',
'    order siblings by collector_id',
') stg',
'where :P310_QUERY_AGREEMENTS = ''Y''',
'and EXISTS',
'(',
'    SELECT NULL',
'    FROM prop_group_props gp, prop_groups g',
'    WHERE gp.property_id = :P310_prop_id',
'    AND gp.GROUP_ID = g.GROUP_ID',
'    AND group_type_id = 0',
'    AND g.collector_agreement_id = stg.collector_agreement_id',
')',
''))
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>20
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>' - '
,p_query_no_data_found=>'Shareholders keep all the money!'
,p_query_num_rows_type=>'ROW_RANGES_WITH_LINKS'
,p_query_row_count_max=>500
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
end;
/
begin
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64382308410550734)
,p_query_column_id=>1
,p_column_alias=>'THE_LEVEL'
,p_column_display_sequence=>1
,p_column_heading=>'The Level'
,p_use_as_row_header=>'N'
,p_column_alignment=>'RIGHT'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64382702523550734)
,p_query_column_id=>2
,p_column_alias=>'COLLECTOR_AGREEMENT_ID'
,p_column_display_sequence=>9
,p_hidden_column=>'Y'
,p_display_as=>'WITHOUT_MODIFICATION'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64383183622550734)
,p_query_column_id=>3
,p_column_alias=>'COLLECTOR_ID'
,p_column_display_sequence=>10
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64383590710550735)
,p_query_column_id=>4
,p_column_alias=>'COLLECTOR'
,p_column_display_sequence=>4
,p_column_heading=>'Collects on behalf of'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(36668908714113438)
,p_query_column_id=>5
,p_column_alias=>'COLLECTORNUM'
,p_column_display_sequence=>12
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(36669044559113439)
,p_query_column_id=>6
,p_column_alias=>'PARENT'
,p_column_display_sequence=>5
,p_column_heading=>'Payee'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(36669147578113440)
,p_query_column_id=>7
,p_column_alias=>'PARENTNUM'
,p_column_display_sequence=>13
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(36669224699113441)
,p_query_column_id=>8
,p_column_alias=>'PARENTID'
,p_column_display_sequence=>14
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64383933906550735)
,p_query_column_id=>9
,p_column_alias=>'COLLECTS_ON_AGREEMENT_ID'
,p_column_display_sequence=>11
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(36669368308113442)
,p_query_column_id=>10
,p_column_alias=>'SHARE_PERCENT'
,p_column_display_sequence=>2
,p_column_heading=>'Share Percent'
,p_use_as_row_header=>'N'
,p_column_format=>'999G999G999G999G990D00'
,p_column_alignment=>'RIGHT'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64384782239550736)
,p_query_column_id=>11
,p_column_alias=>'HEIR_PATH'
,p_column_display_sequence=>3
,p_column_heading=>'Heir Path'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64385949584550736)
,p_query_column_id=>12
,p_column_alias=>'RIGHT_TYPE_ID'
,p_column_display_sequence=>6
,p_column_heading=>'Right Type'
,p_use_as_row_header=>'N'
,p_display_as=>'TEXT_FROM_LOV'
,p_inline_lov=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT right_description d',
'      ,right_type_id r',
'FROM   aff_right_types'))
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(19390259251587523)
,p_query_column_id=>13
,p_column_alias=>'COLL_AGREEMENT_TYPE_ID'
,p_column_display_sequence=>7
,p_column_heading=>'Type'
,p_use_as_row_header=>'N'
,p_display_as=>'TEXT_FROM_LOV'
,p_named_lov=>wwv_flow_api.id(64834507396304316)
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(36669434228113443)
,p_query_column_id=>14
,p_column_alias=>'CLASSIFICATION'
,p_column_display_sequence=>8
,p_column_heading=>'Classification'
,p_use_as_row_header=>'N'
,p_display_as=>'TEXT_FROM_LOV'
,p_named_lov=>wwv_flow_api.id(31448311584450840)
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(67132619301952725)
,p_plug_name=>'Permissions'
,p_region_name=>'PERMISSIONS'
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--scrollBody'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>135
,p_include_in_reg_disp_sel_yn=>'Y'
,p_plug_display_point=>'BODY_3'
,p_plug_query_num_rows=>15
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_attribute_01=>'N'
,p_attribute_02=>'HTML'
,p_attribute_03=>'Y'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(64420359819550761)
,p_name=>'Collection Agreement Permissions'
,p_parent_plug_id=>wwv_flow_api.id(67132619301952725)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>600
,p_region_template_options=>'t-Region--hiddenOverflow'
,p_component_template_options=>'#DEFAULT#'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select * from ',
'( SELECT agr_permission_id             ,',
'collector_agreement_id        ,',
'collector_id                  ,',
'(SELECT full_name from aff_ip_names ',
'where ip_id = pcp.collector_id',
'AND name_type_id = 6) Collector,',
'earner_id                     ,',
'(SELECT full_name from aff_ip_names ',
'where ip_id = NVL(pcp.earner_id,pcp.collector_id)',
'AND name_type_id = 6) Owner,',
'(SELECT primary_name FROM aff_ip_name_V ',
'where ip_number = pcp.admin_acct',
') admin_acct ,',
'(SELECT primary_name FROM aff_ip_name_v ',
'where ip_number = pcp.acct_admined',
') acct_admined ,',
'agreement_type                ,',
'can_view_statemts             ,',
'can_view_catalog              ,',
'can_add_works                 ,',
'can_add_recording             ,',
'can_add_commercial            ,',
'can_add_direct_deposit        ,',
'can_view_add_live_perf        ,',
'can_approve_myspace           ,',
'can_upload_digital_recordgs   ,',
'can_update_acct_info          ,',
'can_opt_out_of_paperless  ',
' FROM PROP_COLL_AGR_PERMISSIONS pcp',
'where  collector_agreement_id IN(SELECT    collector_agreement_id',
'            FROM PROP_COLLECTOR_AGREEMENTS pca',
'where EXISTS',
'              (SELECT 1',
'                 FROM prop_group_props gp, prop_groups g',
'                WHERE     gp.property_id = :P310_prop_id',
'                      AND gp.GROUP_ID = g.GROUP_ID',
'                      AND g.collector_Agreement_id = pca.collector_Agreement_id',
'                      and group_name = ''PAY'')',
'CONNECT BY PRIOR collector_Agreement_id = collects_on_Agreement_id',
'START with collects_on_Agreement_id IS NULL)',
' )',
' where :P310_QUERY_PERMISSIONS = ''Y''',
''))
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_headings_type=>'QUERY_COLUMNS_INITCAP'
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>' - '
,p_query_no_data_found=>'No record exists.'
,p_query_num_rows_type=>'NEXT_PREVIOUS_LINKS'
,p_query_row_count_max=>500
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_csv_output=>'N'
,p_prn_output=>'N'
,p_sort_null=>'L'
,p_query_asc_image=>'apex/builder/dup.gif'
,p_query_asc_image_attr=>'width="16" height="16" alt="" '
,p_query_desc_image=>'apex/builder/ddown.gif'
,p_query_desc_image_attr=>'width="16" height="16" alt="" '
,p_plug_query_strip_html=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64421122549550762)
,p_query_column_id=>1
,p_column_alias=>'AGR_PERMISSION_ID'
,p_column_display_sequence=>1
,p_column_heading=>'AGR_PERMISSION_ID'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_hidden_column=>'Y'
,p_lov_show_nulls=>'NO'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64421590328550762)
,p_query_column_id=>2
,p_column_alias=>'COLLECTOR_AGREEMENT_ID'
,p_column_display_sequence=>2
,p_column_heading=>'COLLECTOR_AGREEMENT_ID'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_hidden_column=>'Y'
,p_lov_show_nulls=>'NO'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64421947177550763)
,p_query_column_id=>3
,p_column_alias=>'COLLECTOR_ID'
,p_column_display_sequence=>3
,p_column_heading=>'COLLECTOR_ID'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_hidden_column=>'Y'
,p_lov_show_nulls=>'NO'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64422374945550763)
,p_query_column_id=>4
,p_column_alias=>'COLLECTOR'
,p_column_display_sequence=>6
,p_column_heading=>'COLLECTOR'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_lov_show_nulls=>'NO'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64420725903550762)
,p_query_column_id=>5
,p_column_alias=>'EARNER_ID'
,p_column_display_sequence=>4
,p_column_heading=>'EARNER_ID'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_hidden_column=>'Y'
,p_lov_show_nulls=>'NO'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64422766820550763)
,p_query_column_id=>6
,p_column_alias=>'OWNER'
,p_column_display_sequence=>5
,p_column_heading=>'OWNER'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64423101497550764)
,p_query_column_id=>7
,p_column_alias=>'ADMIN_ACCT'
,p_column_display_sequence=>8
,p_column_heading=>'ADMIN_ACCT'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64423586009550764)
,p_query_column_id=>8
,p_column_alias=>'ACCT_ADMINED'
,p_column_display_sequence=>7
,p_column_heading=>'ACCT_ADMINED'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64423922196550764)
,p_query_column_id=>9
,p_column_alias=>'AGREEMENT_TYPE'
,p_column_display_sequence=>9
,p_column_heading=>'AGREEMENT_TYPE'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64424334070550764)
,p_query_column_id=>10
,p_column_alias=>'CAN_VIEW_STATEMTS'
,p_column_display_sequence=>10
,p_column_heading=>'CAN_VIEW_STATEMTS'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64424739998550765)
,p_query_column_id=>11
,p_column_alias=>'CAN_VIEW_CATALOG'
,p_column_display_sequence=>11
,p_column_heading=>'CAN_VIEW_CATALOG'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64425132058550765)
,p_query_column_id=>12
,p_column_alias=>'CAN_ADD_WORKS'
,p_column_display_sequence=>12
,p_column_heading=>'CAN_ADD_WORKS'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64425568645550765)
,p_query_column_id=>13
,p_column_alias=>'CAN_ADD_RECORDING'
,p_column_display_sequence=>13
,p_column_heading=>'CAN_ADD_RECORDING'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64425927985550766)
,p_query_column_id=>14
,p_column_alias=>'CAN_ADD_COMMERCIAL'
,p_column_display_sequence=>14
,p_column_heading=>'CAN_ADD_COMMERCIAL'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64426347552550766)
,p_query_column_id=>15
,p_column_alias=>'CAN_ADD_DIRECT_DEPOSIT'
,p_column_display_sequence=>15
,p_column_heading=>'CAN_ADD_DIRECT_DEPOSIT'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64426794124550766)
,p_query_column_id=>16
,p_column_alias=>'CAN_VIEW_ADD_LIVE_PERF'
,p_column_display_sequence=>16
,p_column_heading=>'CAN_VIEW_ADD_LIVE_PERF'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64427110514550766)
,p_query_column_id=>17
,p_column_alias=>'CAN_APPROVE_MYSPACE'
,p_column_display_sequence=>17
,p_column_heading=>'CAN_APPROVE_MYSPACE'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64427524871550767)
,p_query_column_id=>18
,p_column_alias=>'CAN_UPLOAD_DIGITAL_RECORDGS'
,p_column_display_sequence=>18
,p_column_heading=>'CAN_UPLOAD_DIGITAL_RECORDGS'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64427963247550767)
,p_query_column_id=>19
,p_column_alias=>'CAN_UPDATE_ACCT_INFO'
,p_column_display_sequence=>19
,p_column_heading=>'CAN_UPDATE_ACCT_INFO'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64428329864550767)
,p_query_column_id=>20
,p_column_alias=>'CAN_OPT_OUT_OF_PAPERLESS'
,p_column_display_sequence=>20
,p_column_heading=>'CAN_OPT_OUT_OF_PAPERLESS'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(64487960422550815)
,p_name=>'Non Collection Agreement Permissions'
,p_parent_plug_id=>wwv_flow_api.id(67132619301952725)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>610
,p_region_template_options=>'#DEFAULT#:t-Region--hiddenOverflow'
,p_component_template_options=>'#DEFAULT#'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT *',
'FROM(',
'        SELECT ',
'            agr_permission_id,',
'            collector_agreement_id,',
'            collector_id,',
'            (',
'                SELECT full_name',
'                FROM aff_ip_names',
'                WHERE ip_id = pcp.collector_id ',
'                AND name_type_id = 6',
'            )collector,',
'            earner_id,',
'            (',
'                SELECT full_name',
'                FROM aff_ip_names',
'                WHERE ip_id = pcp.earner_id ',
'                AND name_type_id = 6',
'            ) owner,',
'            (',
'                SELECT full_name',
'                FROM aff_ip_names',
'                WHERE ip_id = pcp.admin_acct ',
'                AND name_type_id = 6',
'            ) admin_acct,',
'            (',
'                SELECT full_name',
'                FROM aff_ip_names',
'                WHERE ip_id = pcp.acct_admined ',
'                AND name_type_id = 6',
'            )acct_admined,',
'            agreement_type,',
'            can_view_statemts,',
'            can_view_catalog,',
'            can_add_works,',
'            can_add_recording,',
'            can_add_commercial,',
'            can_add_direct_deposit,',
'            can_view_add_live_perf,',
'            can_approve_myspace,',
'            can_upload_digital_recordgs,',
'            can_update_acct_info,',
'            can_opt_out_of_paperless',
'        FROM prop_coll_agr_permissions pcp',
'        WHERE collector_agreement_id = -1',
'        AND EXISTS(',
'                    SELECT 1',
'                    FROM prop_share_collectors',
'                    WHERE collector_id = pcp.earner_id',
'                    AND property_id = :P310_PROP_ID',
'                  )',
'    )',
' WHERE :P310_QUERY_PERMISSIONS = ''Y'''))
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_headings_type=>'QUERY_COLUMNS_INITCAP'
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>' - '
,p_query_no_data_found=>'No record exists.'
,p_query_num_rows_type=>'NEXT_PREVIOUS_LINKS'
,p_query_row_count_max=>500
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_csv_output=>'N'
,p_prn_output=>'N'
,p_sort_null=>'L'
,p_query_asc_image=>'apex/builder/dup.gif'
,p_query_asc_image_attr=>'width="16" height="16" alt="" '
,p_query_desc_image=>'apex/builder/ddown.gif'
,p_query_desc_image_attr=>'width="16" height="16" alt="" '
,p_plug_query_strip_html=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64488341480550815)
,p_query_column_id=>1
,p_column_alias=>'AGR_PERMISSION_ID'
,p_column_display_sequence=>1
,p_column_heading=>'AGR_PERMISSION_ID'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64488713541550815)
,p_query_column_id=>2
,p_column_alias=>'COLLECTOR_AGREEMENT_ID'
,p_column_display_sequence=>2
,p_column_heading=>'COLLECTOR_AGREEMENT_ID'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64489160140550816)
,p_query_column_id=>3
,p_column_alias=>'COLLECTOR_ID'
,p_column_display_sequence=>3
,p_column_heading=>'COLLECTOR_ID'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64489596572550816)
,p_query_column_id=>4
,p_column_alias=>'COLLECTOR'
,p_column_display_sequence=>5
,p_column_heading=>'COLLECTOR'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64489941654550816)
,p_query_column_id=>5
,p_column_alias=>'EARNER_ID'
,p_column_display_sequence=>6
,p_column_heading=>'EARNER_ID'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64490374805550816)
,p_query_column_id=>6
,p_column_alias=>'OWNER'
,p_column_display_sequence=>4
,p_column_heading=>'OWNER'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64490712407550817)
,p_query_column_id=>7
,p_column_alias=>'ADMIN_ACCT'
,p_column_display_sequence=>8
,p_column_heading=>'ADMIN_ACCT'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64491199529550817)
,p_query_column_id=>8
,p_column_alias=>'ACCT_ADMINED'
,p_column_display_sequence=>7
,p_column_heading=>'ACCT_ADMINED'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64491572119550817)
,p_query_column_id=>9
,p_column_alias=>'AGREEMENT_TYPE'
,p_column_display_sequence=>9
,p_column_heading=>'AGREEMENT_TYPE'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64491991045550818)
,p_query_column_id=>10
,p_column_alias=>'CAN_VIEW_STATEMTS'
,p_column_display_sequence=>10
,p_column_heading=>'CAN_VIEW_STATEMTS'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64492372340550818)
,p_query_column_id=>11
,p_column_alias=>'CAN_VIEW_CATALOG'
,p_column_display_sequence=>11
,p_column_heading=>'CAN_VIEW_CATALOG'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64492744206550818)
,p_query_column_id=>12
,p_column_alias=>'CAN_ADD_WORKS'
,p_column_display_sequence=>12
,p_column_heading=>'CAN_ADD_WORKS'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64493107069550818)
,p_query_column_id=>13
,p_column_alias=>'CAN_ADD_RECORDING'
,p_column_display_sequence=>13
,p_column_heading=>'CAN_ADD_RECORDING'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64493576700550819)
,p_query_column_id=>14
,p_column_alias=>'CAN_ADD_COMMERCIAL'
,p_column_display_sequence=>14
,p_column_heading=>'CAN_ADD_COMMERCIAL'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64493993374550819)
,p_query_column_id=>15
,p_column_alias=>'CAN_ADD_DIRECT_DEPOSIT'
,p_column_display_sequence=>15
,p_column_heading=>'CAN_ADD_DIRECT_DEPOSIT'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64494340294550820)
,p_query_column_id=>16
,p_column_alias=>'CAN_VIEW_ADD_LIVE_PERF'
,p_column_display_sequence=>16
,p_column_heading=>'CAN_VIEW_ADD_LIVE_PERF'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64494724302550820)
,p_query_column_id=>17
,p_column_alias=>'CAN_APPROVE_MYSPACE'
,p_column_display_sequence=>17
,p_column_heading=>'CAN_APPROVE_MYSPACE'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64495137188550820)
,p_query_column_id=>18
,p_column_alias=>'CAN_UPLOAD_DIGITAL_RECORDGS'
,p_column_display_sequence=>18
,p_column_heading=>'CAN_UPLOAD_DIGITAL_RECORDGS'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64495549067550820)
,p_query_column_id=>19
,p_column_alias=>'CAN_UPDATE_ACCT_INFO'
,p_column_display_sequence=>19
,p_column_heading=>'CAN_UPDATE_ACCT_INFO'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64495973603550821)
,p_query_column_id=>20
,p_column_alias=>'CAN_OPT_OUT_OF_PAPERLESS'
,p_column_display_sequence=>20
,p_column_heading=>'CAN_OPT_OUT_OF_PAPERLESS'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(67166923599110709)
,p_plug_name=>'Summary'
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_escape_on_http_output=>'Y'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>35
,p_include_in_reg_disp_sel_yn=>'Y'
,p_plug_display_point=>'BODY_3'
,p_plug_query_num_rows=>15
,p_plug_display_condition_type=>'NEVER'
,p_attribute_01=>'N'
,p_attribute_02=>'HTML'
,p_attribute_03=>'Y'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(64391566935550741)
,p_name=>'Summary Coll Agr Permissions'
,p_parent_plug_id=>wwv_flow_api.id(67166923599110709)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>960
,p_region_template_options=>'#DEFAULT#:t-Region--hiddenOverflow'
,p_component_template_options=>'#DEFAULT#'
,p_new_grid_column=>false
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT agr_permission_id             ,',
'collector_agreement_id        ,',
'collector_id                  ,',
'(SELECT full_name from aff_ip_names ',
'where ip_id = pcp.collector_id',
'AND name_type_id = 6) Collector,',
'earner_id                     ,',
'(SELECT full_name from aff_ip_names ',
'where ip_id = NVL(pcp.earner_id,pcp.collector_id)',
'AND name_type_id = 6) Owner,',
'(SELECT full_name from aff_ip_names ',
'where ip_id = pcp.admin_acct',
'AND name_type_id = 6) admin_acct ,',
'(SELECT full_name from aff_ip_names ',
'where ip_id = pcp.acct_admined',
'AND name_type_id = 6) acct_admined ,',
'agreement_type                ,',
'can_view_statemts             ,',
'can_view_catalog              ,',
'can_add_works                 ,',
'can_add_recording             ,',
'can_add_commercial            ,',
'can_add_direct_deposit        ,',
'can_view_add_live_perf        ,',
'can_approve_myspace           ,',
'can_upload_digital_recordgs   ,',
'can_update_acct_info          ,',
'can_opt_out_of_paperless  ',
' FROM PROP_COLL_AGR_PERMISSIONS pcp',
'where collector_agreement_id IN(SELECT    collector_agreement_id',
'            FROM PROP_COLLECTOR_AGREEMENTS pca',
'where EXISTS',
'              (SELECT 1',
'                 FROM prop_group_props gp, prop_groups g',
'                WHERE     gp.property_id = :P310_prop_id',
'                      AND gp.GROUP_ID = g.GROUP_ID',
'                      AND g.collector_Agreement_id = pca.collector_Agreement_id',
'                      and group_name = ''PAY'')',
'CONNECT BY PRIOR collector_Agreement_id = collects_on_Agreement_id',
'START with collects_on_Agreement_id IS NULL)'))
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_headings_type=>'QUERY_COLUMNS_INITCAP'
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>' - '
,p_query_break_cols=>'0'
,p_query_no_data_found=>'no data found'
,p_query_num_rows_type=>'NEXT_PREVIOUS_LINKS'
,p_query_row_count_max=>500
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_break_type_flag=>'DEFAULT_BREAK_FORMATTING'
,p_csv_output=>'N'
,p_prn_output=>'N'
,p_query_asc_image=>'apex/builder/dup.gif'
,p_query_asc_image_attr=>'width="16" height="16" alt="" '
,p_query_desc_image=>'apex/builder/ddown.gif'
,p_query_desc_image_attr=>'width="16" height="16" alt="" '
,p_plug_query_strip_html=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64391969965550741)
,p_query_column_id=>1
,p_column_alias=>'AGR_PERMISSION_ID'
,p_column_display_sequence=>1
,p_column_heading=>'AGR_PERMISSION_ID'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_hidden_column=>'Y'
,p_lov_show_nulls=>'NO'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64392378924550742)
,p_query_column_id=>2
,p_column_alias=>'COLLECTOR_AGREEMENT_ID'
,p_column_display_sequence=>2
,p_column_heading=>'COLLECTOR_AGREEMENT_ID'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_hidden_column=>'Y'
,p_lov_show_nulls=>'NO'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64392796186550742)
,p_query_column_id=>3
,p_column_alias=>'COLLECTOR_ID'
,p_column_display_sequence=>3
,p_column_heading=>'COLLECTOR_ID'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_hidden_column=>'Y'
,p_lov_show_nulls=>'NO'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64393185706550742)
,p_query_column_id=>4
,p_column_alias=>'COLLECTOR'
,p_column_display_sequence=>6
,p_column_heading=>'COLLECTOR'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_lov_show_nulls=>'NO'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64393599098550742)
,p_query_column_id=>5
,p_column_alias=>'EARNER_ID'
,p_column_display_sequence=>4
,p_column_heading=>'EARNER_ID'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_hidden_column=>'Y'
,p_lov_show_nulls=>'NO'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64393985236550743)
,p_query_column_id=>6
,p_column_alias=>'OWNER'
,p_column_display_sequence=>5
,p_column_heading=>'OWNER'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64394364643550743)
,p_query_column_id=>7
,p_column_alias=>'ADMIN_ACCT'
,p_column_display_sequence=>8
,p_column_heading=>'ADMIN_ACCT'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64394780114550743)
,p_query_column_id=>8
,p_column_alias=>'ACCT_ADMINED'
,p_column_display_sequence=>7
,p_column_heading=>'ACCT_ADMINED'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64395148157550743)
,p_query_column_id=>9
,p_column_alias=>'AGREEMENT_TYPE'
,p_column_display_sequence=>9
,p_column_heading=>'AGREEMENT_TYPE'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64395585005550744)
,p_query_column_id=>10
,p_column_alias=>'CAN_VIEW_STATEMTS'
,p_column_display_sequence=>10
,p_column_heading=>'CAN_VIEW_STATEMTS'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64395912029550744)
,p_query_column_id=>11
,p_column_alias=>'CAN_VIEW_CATALOG'
,p_column_display_sequence=>11
,p_column_heading=>'CAN_VIEW_CATALOG'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64396303991550744)
,p_query_column_id=>12
,p_column_alias=>'CAN_ADD_WORKS'
,p_column_display_sequence=>12
,p_column_heading=>'CAN_ADD_WORKS'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64396702796550745)
,p_query_column_id=>13
,p_column_alias=>'CAN_ADD_RECORDING'
,p_column_display_sequence=>13
,p_column_heading=>'CAN_ADD_RECORDING'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64397177565550745)
,p_query_column_id=>14
,p_column_alias=>'CAN_ADD_COMMERCIAL'
,p_column_display_sequence=>14
,p_column_heading=>'CAN_ADD_COMMERCIAL'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64397592628550745)
,p_query_column_id=>15
,p_column_alias=>'CAN_ADD_DIRECT_DEPOSIT'
,p_column_display_sequence=>15
,p_column_heading=>'CAN_ADD_DIRECT_DEPOSIT'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64397957301550745)
,p_query_column_id=>16
,p_column_alias=>'CAN_VIEW_ADD_LIVE_PERF'
,p_column_display_sequence=>16
,p_column_heading=>'CAN_VIEW_ADD_LIVE_PERF'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64398391754550746)
,p_query_column_id=>17
,p_column_alias=>'CAN_APPROVE_MYSPACE'
,p_column_display_sequence=>17
,p_column_heading=>'CAN_APPROVE_MYSPACE'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64398716102550746)
,p_query_column_id=>18
,p_column_alias=>'CAN_UPLOAD_DIGITAL_RECORDGS'
,p_column_display_sequence=>18
,p_column_heading=>'CAN_UPLOAD_DIGITAL_RECORDGS'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64399164247550746)
,p_query_column_id=>19
,p_column_alias=>'CAN_UPDATE_ACCT_INFO'
,p_column_display_sequence=>19
,p_column_heading=>'CAN_UPDATE_ACCT_INFO'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64399512182550746)
,p_query_column_id=>20
,p_column_alias=>'CAN_OPT_OUT_OF_PAPERLESS'
,p_column_display_sequence=>20
,p_column_heading=>'CAN_OPT_OUT_OF_PAPERLESS'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(64399968430550747)
,p_name=>'Summary Non Coll Agr Permissions'
,p_parent_plug_id=>wwv_flow_api.id(67166923599110709)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>970
,p_region_template_options=>'t-Region--hiddenOverflow'
,p_component_template_options=>'#DEFAULT#'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT ',
'    agr_permission_id,',
'    collector_agreement_id,',
'    collector_id,',
'    (',
'        SELECT full_name',
'        FROM aff_ip_names',
'        WHERE ip_id = pcp.collector_id ',
'        AND name_type_id = 6',
'    )collector,',
'    earner_id,',
'    (',
'        SELECT full_name',
'        FROM aff_ip_names',
'        WHERE ip_id = pcp.earner_id ',
'        AND name_type_id = 6',
'    )owner,',
'    (',
'        SELECT full_name',
'        FROM aff_ip_names',
'        WHERE ip_id = pcp.admin_acct ',
'        AND name_type_id = 6',
'    ) admin_acct,',
'    (',
'        SELECT full_name',
'        FROM aff_ip_names',
'        WHERE ip_id = pcp.acct_admined ',
'        AND name_type_id = 6',
'    )acct_admined,',
'    agreement_type,',
'    can_view_statemts,',
'    can_view_catalog,',
'    can_add_works,',
'    can_add_recording,',
'    can_add_commercial,',
'    can_add_direct_deposit,',
'    can_view_add_live_perf,',
'    can_approve_myspace,',
'    can_upload_digital_recordgs,',
'    can_update_acct_info,',
'    can_opt_out_of_paperless',
'FROM prop_coll_agr_permissions pcp',
'WHERE collector_agreement_id = -1',
'AND EXISTS(',
'            SELECT 1',
'            FROM prop_share_collectors',
'            WHERE collector_id = pcp.earner_id',
'            AND property_id = :P310_PROP_ID',
'          )'))
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_headings_type=>'QUERY_COLUMNS_INITCAP'
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
 p_id=>wwv_flow_api.id(64400350285550747)
,p_query_column_id=>1
,p_column_alias=>'AGR_PERMISSION_ID'
,p_column_display_sequence=>1
,p_column_heading=>'AGR_PERMISSION_ID'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64400776105550748)
,p_query_column_id=>2
,p_column_alias=>'COLLECTOR_AGREEMENT_ID'
,p_column_display_sequence=>2
,p_column_heading=>'COLLECTOR_AGREEMENT_ID'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64401139961550748)
,p_query_column_id=>3
,p_column_alias=>'COLLECTOR_ID'
,p_column_display_sequence=>3
,p_column_heading=>'COLLECTOR_ID'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64401510079550748)
,p_query_column_id=>4
,p_column_alias=>'COLLECTOR'
,p_column_display_sequence=>5
,p_column_heading=>'COLLECTOR'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64401979407550748)
,p_query_column_id=>5
,p_column_alias=>'EARNER_ID'
,p_column_display_sequence=>6
,p_column_heading=>'EARNER_ID'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64402377663550749)
,p_query_column_id=>6
,p_column_alias=>'OWNER'
,p_column_display_sequence=>4
,p_column_heading=>'OWNER'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64402789146550749)
,p_query_column_id=>7
,p_column_alias=>'ADMIN_ACCT'
,p_column_display_sequence=>8
,p_column_heading=>'ADMIN_ACCT'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64407148636550752)
,p_query_column_id=>8
,p_column_alias=>'ACCT_ADMINED'
,p_column_display_sequence=>7
,p_column_heading=>'ACCT_ADMINED'
,p_disable_sort_column=>'N'
);
end;
/
begin
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64407570760550752)
,p_query_column_id=>9
,p_column_alias=>'AGREEMENT_TYPE'
,p_column_display_sequence=>9
,p_column_heading=>'AGREEMENT_TYPE'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64407930534550753)
,p_query_column_id=>10
,p_column_alias=>'CAN_VIEW_STATEMTS'
,p_column_display_sequence=>10
,p_column_heading=>'CAN_VIEW_STATEMTS'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64403133837550749)
,p_query_column_id=>11
,p_column_alias=>'CAN_VIEW_CATALOG'
,p_column_display_sequence=>11
,p_column_heading=>'CAN_VIEW_CATALOG'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64403514961550749)
,p_query_column_id=>12
,p_column_alias=>'CAN_ADD_WORKS'
,p_column_display_sequence=>12
,p_column_heading=>'CAN_ADD_WORKS'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64403985705550750)
,p_query_column_id=>13
,p_column_alias=>'CAN_ADD_RECORDING'
,p_column_display_sequence=>13
,p_column_heading=>'CAN_ADD_RECORDING'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64404343021550750)
,p_query_column_id=>14
,p_column_alias=>'CAN_ADD_COMMERCIAL'
,p_column_display_sequence=>14
,p_column_heading=>'CAN_ADD_COMMERCIAL'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64404757947550750)
,p_query_column_id=>15
,p_column_alias=>'CAN_ADD_DIRECT_DEPOSIT'
,p_column_display_sequence=>15
,p_column_heading=>'CAN_ADD_DIRECT_DEPOSIT'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64405107457550751)
,p_query_column_id=>16
,p_column_alias=>'CAN_VIEW_ADD_LIVE_PERF'
,p_column_display_sequence=>16
,p_column_heading=>'CAN_VIEW_ADD_LIVE_PERF'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64405561689550751)
,p_query_column_id=>17
,p_column_alias=>'CAN_APPROVE_MYSPACE'
,p_column_display_sequence=>17
,p_column_heading=>'CAN_APPROVE_MYSPACE'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64405915720550751)
,p_query_column_id=>18
,p_column_alias=>'CAN_UPLOAD_DIGITAL_RECORDGS'
,p_column_display_sequence=>18
,p_column_heading=>'CAN_UPLOAD_DIGITAL_RECORDGS'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64406341807550751)
,p_query_column_id=>19
,p_column_alias=>'CAN_UPDATE_ACCT_INFO'
,p_column_display_sequence=>19
,p_column_heading=>'CAN_UPDATE_ACCT_INFO'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64406713405550752)
,p_query_column_id=>20
,p_column_alias=>'CAN_OPT_OUT_OF_PAPERLESS'
,p_column_display_sequence=>20
,p_column_heading=>'CAN_OPT_OUT_OF_PAPERLESS'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(64429126877550768)
,p_name=>'Summary Coll Agreements'
,p_parent_plug_id=>wwv_flow_api.id(67166923599110709)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>950
,p_region_template_options=>'#DEFAULT#:t-Region--hiddenOverflow'
,p_component_template_options=>'#DEFAULT#'
,p_new_grid_row=>false
,p_new_grid_column=>false
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'    SELECT LEVEL the_level,',
'           collector_agreement_id,',
'           collector_id,',
'           (select ip_number||'' - ''||primary_name from aff_ip_name_v where ip_id = collector_id ) collector,',
'           collects_on_agreement_id,',
'           share_percent,',
'           heir_path,',
'           pca.coll_agreement_type_id',
'           , creation_class_id, right_type_id, role_id, start_date, end_date, effective_date,',
'           signature_date, ammendment_date, renew_allowed_flag, renewal_term_months, status, alert_date, ',
'           domestic_pay_thru_date, foreign_pay_thru_date, ignore_earn_date_flag',
'      FROM PROP_COLLECTOR_AGREEMENTS pca',
'where EXISTS',
'              (SELECT 1',
'                 FROM prop_group_props gp, prop_groups g',
'                WHERE     gp.property_id = :P310_prop_id',
'                      AND gp.GROUP_ID = g.GROUP_ID',
'                      AND g.collector_Agreement_id = pca.collector_Agreement_id',
'                      and group_name = ''PAY'')',
'CONNECT BY PRIOR collector_Agreement_id = collects_on_Agreement_id',
'START with collects_on_Agreement_id IS NULL',
''))
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>20
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>' - '
,p_query_break_cols=>'0'
,p_query_no_data_found=>'Shareholders keep all the money!'
,p_query_num_rows_type=>'NEXT_PREVIOUS_LINKS'
,p_query_row_count_max=>500
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_break_type_flag=>'DEFAULT_BREAK_FORMATTING'
,p_csv_output=>'Y'
,p_csv_output_link_text=>'Excel'
,p_query_asc_image=>'apex/builder/dup.gif'
,p_query_asc_image_attr=>'width="16" height="16" alt="" '
,p_query_desc_image=>'apex/builder/ddown.gif'
,p_query_desc_image_attr=>'width="16" height="16" alt="" '
,p_plug_query_strip_html=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64430766752550769)
,p_query_column_id=>1
,p_column_alias=>'THE_LEVEL'
,p_column_display_sequence=>1
,p_column_heading=>'The Level'
,p_column_alignment=>'RIGHT'
,p_heading_alignment=>'RIGHT'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64431163278550770)
,p_query_column_id=>2
,p_column_alias=>'COLLECTOR_AGREEMENT_ID'
,p_column_display_sequence=>7
,p_column_heading=>'Collector Agreement Id'
,p_column_link=>'f?p=&APP_ID.:315:&SESSION.:APEX_CLONE_SESSION:&DEBUG.:RP,315:P315_COLLECTOR_AGREEMENT_ID:#COLLECTOR_AGREEMENT_ID#'
,p_column_linktext=>'#COLLECTOR_AGREEMENT_ID#'
,p_heading_alignment=>'LEFT'
,p_display_as=>'WITHOUT_MODIFICATION'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64431587361550770)
,p_query_column_id=>3
,p_column_alias=>'COLLECTOR_ID'
,p_column_display_sequence=>2
,p_column_heading=>'Collector Id'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64431953918550770)
,p_query_column_id=>4
,p_column_alias=>'COLLECTOR'
,p_column_display_sequence=>3
,p_column_heading=>'Collector'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64432393642550770)
,p_query_column_id=>5
,p_column_alias=>'COLLECTS_ON_AGREEMENT_ID'
,p_column_display_sequence=>4
,p_column_heading=>'Collects On Agreement Id'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_hidden_column=>'Y'
,p_lov_show_nulls=>'NO'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64432786291550771)
,p_query_column_id=>6
,p_column_alias=>'SHARE_PERCENT'
,p_column_display_sequence=>5
,p_column_heading=>'Share Percent'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_lov_show_nulls=>'NO'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64433174657550771)
,p_query_column_id=>7
,p_column_alias=>'HEIR_PATH'
,p_column_display_sequence=>6
,p_column_heading=>'Heir Path'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_default_sort_column_sequence=>1
,p_disable_sort_column=>'N'
,p_lov_show_nulls=>'NO'
,p_column_width=>200
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64433590830550771)
,p_query_column_id=>8
,p_column_alias=>'COLL_AGREEMENT_TYPE_ID'
,p_column_display_sequence=>8
,p_column_heading=>'Coll Agreement Type Id'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64433944845550772)
,p_query_column_id=>9
,p_column_alias=>'CREATION_CLASS_ID'
,p_column_display_sequence=>9
,p_column_heading=>'Creation Class'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64434380495550772)
,p_query_column_id=>10
,p_column_alias=>'RIGHT_TYPE_ID'
,p_column_display_sequence=>10
,p_column_heading=>'Right Type'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64434729027550772)
,p_query_column_id=>11
,p_column_alias=>'ROLE_ID'
,p_column_display_sequence=>11
,p_column_heading=>'Role'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64435116719550773)
,p_query_column_id=>12
,p_column_alias=>'START_DATE'
,p_column_display_sequence=>12
,p_column_heading=>'Start Date'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64435503142550773)
,p_query_column_id=>13
,p_column_alias=>'END_DATE'
,p_column_display_sequence=>13
,p_column_heading=>'End Date'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64435973205550773)
,p_query_column_id=>14
,p_column_alias=>'EFFECTIVE_DATE'
,p_column_display_sequence=>14
,p_column_heading=>'Effective Date'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64436391713550773)
,p_query_column_id=>15
,p_column_alias=>'SIGNATURE_DATE'
,p_column_display_sequence=>15
,p_column_heading=>'Signature Date'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64436745269550774)
,p_query_column_id=>16
,p_column_alias=>'AMMENDMENT_DATE'
,p_column_display_sequence=>16
,p_column_heading=>'Ammendment Date'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64437184096550774)
,p_query_column_id=>17
,p_column_alias=>'RENEW_ALLOWED_FLAG'
,p_column_display_sequence=>17
,p_column_heading=>'Renew Allowed Flag'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64437510663550774)
,p_query_column_id=>18
,p_column_alias=>'RENEWAL_TERM_MONTHS'
,p_column_display_sequence=>18
,p_column_heading=>'Renewal Term Months'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64437993036550774)
,p_query_column_id=>19
,p_column_alias=>'STATUS'
,p_column_display_sequence=>19
,p_column_heading=>'Status'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64438316122550775)
,p_query_column_id=>20
,p_column_alias=>'ALERT_DATE'
,p_column_display_sequence=>20
,p_column_heading=>'Alert Date'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64429517012550769)
,p_query_column_id=>21
,p_column_alias=>'DOMESTIC_PAY_THRU_DATE'
,p_column_display_sequence=>21
,p_column_heading=>'Domestic Pay Thru Date'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64429916763550769)
,p_query_column_id=>22
,p_column_alias=>'FOREIGN_PAY_THRU_DATE'
,p_column_display_sequence=>22
,p_column_heading=>'Foreign Pay Thru Date'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64430335960550769)
,p_query_column_id=>23
,p_column_alias=>'IGNORE_EARN_DATE_FLAG'
,p_column_display_sequence=>23
,p_column_heading=>'Ignore Earn Date Flag'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(64457190484550791)
,p_name=>'Summary Title'
,p_parent_plug_id=>wwv_flow_api.id(67166923599110709)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>50
,p_region_template_options=>'t-Region--hiddenOverflow'
,p_component_template_options=>'#DEFAULT#'
,p_new_grid_row=>false
,p_new_grid_column=>false
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select pat.aka_type_code, pa.aka_name, src.source_code, src.description src_desc, src.source_group,',
'pp.property_id, pp.entry_date, ppt.code prop_type',
'from prop_properties pp, prop_property_types ppt, prop_sources src, prop_akas pa, prop_aka_types pat',
'where ppt.property_type_id = pp.property_type_id',
'and src.source_id = pa.source_id ',
'and PA.AKA_TYPE_ID = 1',
'and PAT.AKA_TYPE_ID = PA.AKA_TYPE_ID',
'and PA.PROPERTY_ID = pp.property_id',
'and pp.property_id = :P310_PROP_ID',
''))
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>' - '
,p_query_break_cols=>'0'
,p_query_no_data_found=>'No AKAs found'
,p_query_num_rows_type=>'NEXT_PREVIOUS_LINKS'
,p_query_row_count_max=>500
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_break_type_flag=>'DEFAULT_BREAK_FORMATTING'
,p_csv_output=>'Y'
,p_csv_output_link_text=>'Excel'
,p_query_asc_image=>'apex/builder/dup.gif'
,p_query_asc_image_attr=>'width="16" height="16" alt="" '
,p_query_desc_image=>'apex/builder/ddown.gif'
,p_query_desc_image_attr=>'width="16" height="16" alt="" '
,p_plug_query_strip_html=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64459572356550793)
,p_query_column_id=>1
,p_column_alias=>'AKA_TYPE_CODE'
,p_column_display_sequence=>1
,p_column_heading=>'AKA Type'
,p_default_sort_column_sequence=>1
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64459940522550794)
,p_query_column_id=>2
,p_column_alias=>'AKA_NAME'
,p_column_display_sequence=>2
,p_column_heading=>'AKA Name'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64460395659550794)
,p_query_column_id=>3
,p_column_alias=>'SOURCE_CODE'
,p_column_display_sequence=>3
,p_column_heading=>'Source'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64457505854550792)
,p_query_column_id=>4
,p_column_alias=>'SRC_DESC'
,p_column_display_sequence=>4
,p_column_heading=>'Source Description'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64457913433550792)
,p_query_column_id=>5
,p_column_alias=>'SOURCE_GROUP'
,p_column_display_sequence=>5
,p_column_heading=>'Source Group'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64458341218550792)
,p_query_column_id=>6
,p_column_alias=>'PROPERTY_ID'
,p_column_display_sequence=>6
,p_column_heading=>'Property ID'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64458735994550793)
,p_query_column_id=>7
,p_column_alias=>'ENTRY_DATE'
,p_column_display_sequence=>7
,p_column_heading=>'Entry Date'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64459182788550793)
,p_query_column_id=>8
,p_column_alias=>'PROP_TYPE'
,p_column_display_sequence=>8
,p_column_heading=>'Prop Type'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(64460731890550794)
,p_plug_name=>'Summary Work'
,p_parent_plug_id=>wwv_flow_api.id(67166923599110709)
,p_region_template_options=>'#DEFAULT#:t-Region--hiddenOverflow'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>80
,p_plug_new_grid_column=>false
,p_plug_display_point=>'BODY'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_attribute_01=>'N'
,p_attribute_02=>'HTML'
,p_attribute_03=>'N'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(64461931012550795)
,p_plug_name=>'Summary Ownership'
,p_parent_plug_id=>wwv_flow_api.id(67166923599110709)
,p_region_template_options=>'#DEFAULT#'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51848808455240825)
,p_plug_display_sequence=>110
,p_plug_new_grid_column=>false
,p_plug_display_point=>'BODY'
,p_plug_query_headings_type=>'QUERY_COLUMNS'
,p_plug_query_num_rows_type=>'NEXT_PREVIOUS_LINKS'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_plug_query_show_nulls_as=>' - '
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_attribute_01=>'N'
,p_attribute_02=>'HTML'
,p_attribute_03=>'N'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(64462300429550796)
,p_name=>'&nbsp;'
,p_parent_plug_id=>wwv_flow_api.id(64461931012550795)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>640
,p_region_template_options=>'#DEFAULT#:t-Region--hiddenOverflow'
,p_component_template_options=>'#DEFAULT#:t-Report--staticRowColors:t-Report--rowHighlightOff'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT ',
'    afs.society_name  source,',
'    ainv.full_name,',
'    ainv.ip_number,',
'    ainv.ipi_base_num,',
'    ainv.ipi_name_number ,',
'    psc.role_id,',
'    psc.right_type_id,',
'    psc.collector_id,',
'    psc.share_collector_id ,',
'    psc.share_percentage,',
'    psc.affiliated_society_id,',
'    (',
'        SELECT description ',
'        FROM pay_hold_types',
'        WHERE hold_type_id =  NVL(pp.hold_type_id,psc.hold_type_id)',
'    ) hold_type',
'FROM prop_properties pp,',
'    prop_share_collectors psc,',
'  (',
'    SELECT ip.ip_id,',
'        full_name,',
'        ip.IP_NUMBER,',
'        ip.IPI_BASE_NUMBER ipi_base_num,',
'        ain.name_number ipi_name_number,',
'        ip.group_number',
'    FROM aff_interested_parties ip,',
'        aff_ip_names ain,',
'        aff_name_types nt',
'    WHERE ain.ip_id               = ip.ip_id',
'    AND nt.name_Type_id           = ain.name_type_id',
'    AND ain.name_type_id          = 6',
'    --AND NVL(ip.prospect_flag,''N'') = ''N''',
'  ) ainv,',
'  aff_societies afs',
'WHERE psc.property_id                            = pp.property_id',
'AND psc.affiliated_society_id = afs.society_id(+)',
'AND pp.property_id                               = :P310_PROP_ID',
'AND ainv.ip_id                                   = psc.collector_id',
'AND TRUNC(psc.start_date_active)<=TRUNC(SYSDATE)',
'AND NVL(psc.end_date_active,TRUNC(sysdate + 1)) >= TRUNC(sysdate +1)',
'AND (',
'        SELECT summary_role_code ',
'        FROM aff_roles  ',
'        WHERE  aff_roles.role_id=psc.role_id',
'    ) =''P''  ',
'ORDER BY ainv.group_number,',
'  psc.affiliated_society_id,',
'  psc.role_id DESC'))
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>' - '
,p_query_break_cols=>'0'
,p_query_no_data_found=>'no data found'
,p_query_num_rows_type=>'NEXT_PREVIOUS_LINKS'
,p_query_row_count_max=>500
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_break_type_flag=>'DEFAULT_BREAK_FORMATTING'
,p_csv_output=>'Y'
,p_csv_output_link_text=>'Excel'
,p_query_asc_image=>'apex/builder/dup.gif'
,p_query_asc_image_attr=>'width="16" height="16" alt="" '
,p_query_desc_image=>'apex/builder/ddown.gif'
,p_query_desc_image_attr=>'width="16" height="16" alt="" '
,p_plug_query_strip_html=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64462755850550796)
,p_query_column_id=>1
,p_column_alias=>'SOURCE'
,p_column_display_sequence=>1
,p_column_heading=>'Source'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64463147210550796)
,p_query_column_id=>2
,p_column_alias=>'FULL_NAME'
,p_column_display_sequence=>3
,p_column_heading=>'Full Name'
,p_column_link=>'f?p=&APP_ID.:260:&SESSION.:APEX_CLONE_SESSION:&DEBUG.:RP,260:P260_IP_ID:#OWNER_ID#'
,p_column_linktext=>'#FULL_NAME#'
,p_heading_alignment=>'LEFT'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64463563079550797)
,p_query_column_id=>3
,p_column_alias=>'IP_NUMBER'
,p_column_display_sequence=>2
,p_column_heading=>'IP Number'
,p_column_link=>'f?p=&APP_ID.:260:&SESSION.:APEX_CLONE_SESSION:&DEBUG.::P260_IP_ID,P260_CALLING_PAGE_ID,P260_REQUEST:#OWNER_ID#,&APP_PAGE_ID.,OWNER'
,p_column_linktext=>'#IP_NUMBER#'
,p_heading_alignment=>'LEFT'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64463984091550797)
,p_query_column_id=>4
,p_column_alias=>'IPI_BASE_NUM'
,p_column_display_sequence=>7
,p_column_heading=>'Ipi Base Num'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64464307191550797)
,p_query_column_id=>5
,p_column_alias=>'IPI_NAME_NUMBER'
,p_column_display_sequence=>8
,p_column_heading=>'Ipi Name Number'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64464729217550797)
,p_query_column_id=>6
,p_column_alias=>'ROLE_ID'
,p_column_display_sequence=>4
,p_column_heading=>'Role'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_display_as=>'TEXT_FROM_LOV'
,p_named_lov=>wwv_flow_api.id(62720921871606139)
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64465125174550798)
,p_query_column_id=>7
,p_column_alias=>'RIGHT_TYPE_ID'
,p_column_display_sequence=>5
,p_hidden_column=>'Y'
,p_display_when_cond_type=>'NEVER'
,p_display_as=>'WITHOUT_MODIFICATION'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(20067378854554623)
,p_query_column_id=>8
,p_column_alias=>'COLLECTOR_ID'
,p_column_display_sequence=>10
,p_column_heading=>'Collector id'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(20067400001554624)
,p_query_column_id=>9
,p_column_alias=>'SHARE_COLLECTOR_ID'
,p_column_display_sequence=>11
,p_column_heading=>'Share collector id'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(20067509671554625)
,p_query_column_id=>10
,p_column_alias=>'SHARE_PERCENTAGE'
,p_column_display_sequence=>12
,p_column_heading=>'Share percentage'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64466744162550799)
,p_query_column_id=>11
,p_column_alias=>'AFFILIATED_SOCIETY_ID'
,p_column_display_sequence=>6
,p_column_heading=>'Party Society'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_display_as=>'TEXT_FROM_LOV'
,p_named_lov=>wwv_flow_api.id(62954498999980409)
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64467147695550799)
,p_query_column_id=>12
,p_column_alias=>'HOLD_TYPE'
,p_column_display_sequence=>9
,p_column_heading=>'Hold Type'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(64467540999550799)
,p_name=>'Owner Collections'
,p_parent_plug_id=>wwv_flow_api.id(64461931012550795)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>930
,p_region_template_options=>'#DEFAULT#:t-Region--hiddenOverflow'
,p_component_template_options=>'#DEFAULT#'
,p_new_grid_column=>false
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select t.* ,',
'aff_ip.get_current_IPI_society(t.collector_id) coll_soc',
'from PROP_SHARE_COLLECTORS_tmp t',
'where session_id = v (''SESSION'')'))
,p_display_when_condition=>'P310_PROP_ID'
,p_display_condition_type=>'ITEM_IS_NOT_NULL'
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>20
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>' - '
,p_query_break_cols=>'0'
,p_query_no_data_found=>'No Collection data found'
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
 p_id=>wwv_flow_api.id(64467988222550800)
,p_query_column_id=>1
,p_column_alias=>'SESSION_ID'
,p_column_display_sequence=>26
,p_column_heading=>'Session Id'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64468389698550800)
,p_query_column_id=>2
,p_column_alias=>'SESSION_DATE'
,p_column_display_sequence=>27
,p_column_heading=>'Session Date'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64468775709550800)
,p_query_column_id=>3
,p_column_alias=>'PROPERTY_ID'
,p_column_display_sequence=>28
,p_column_heading=>'Property Id'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64469182462550801)
,p_query_column_id=>4
,p_column_alias=>'CALC_SHARE_PCT'
,p_column_display_sequence=>6
,p_column_heading=>'Collector Share '
,p_use_as_row_header=>'N'
,p_column_format=>'999G999G999G999G990D0000'
,p_column_alignment=>'RIGHT'
,p_heading_alignment=>'RIGHT'
,p_disable_sort_column=>'N'
,p_sum_column=>'Y'
,p_lov_show_nulls=>'NO'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64469501070550801)
,p_query_column_id=>5
,p_column_alias=>'THE_LEVEL'
,p_column_display_sequence=>4
,p_column_heading=>'Level'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_lov_show_nulls=>'NO'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64469928644550801)
,p_query_column_id=>6
,p_column_alias=>'COLLECTOR_AGREEMENT_ID'
,p_column_display_sequence=>12
,p_column_heading=>'Collector Agreement Id'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64470397137550802)
,p_query_column_id=>7
,p_column_alias=>'COLLECTOR_ID'
,p_column_display_sequence=>29
,p_column_heading=>'Collector Id'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64470785701550802)
,p_query_column_id=>8
,p_column_alias=>'COLLECTOR'
,p_column_display_sequence=>1
,p_column_heading=>'Collector'
,p_column_link=>'f?p=&APP_ID.:260:&SESSION.:APEX_CLONE_SESSION:&DEBUG.::P260_IP_ID:#COLLECTOR_ID#'
,p_column_linktext=>'#COLLECTOR#'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64471108580550802)
,p_query_column_id=>9
,p_column_alias=>'COLLECTS_ON_AGREEMENT_ID'
,p_column_display_sequence=>11
,p_column_heading=>'Collects On Agreement Id'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64471559912550802)
,p_query_column_id=>10
,p_column_alias=>'SHARE_PERCENT'
,p_column_display_sequence=>5
,p_column_heading=>'Share Percent'
,p_use_as_row_header=>'N'
,p_column_format=>'999G999G999G999G990D0000'
,p_column_alignment=>'RIGHT'
,p_heading_alignment=>'RIGHT'
,p_disable_sort_column=>'N'
,p_hidden_column=>'Y'
,p_lov_show_nulls=>'NO'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64471965760550803)
,p_query_column_id=>11
,p_column_alias=>'ORIG_SHARE_PCT'
,p_column_display_sequence=>3
,p_column_heading=>'Owner Share'
,p_use_as_row_header=>'N'
,p_column_format=>'999G999G999G999G990D0000'
,p_column_alignment=>'RIGHT'
,p_heading_alignment=>'RIGHT'
,p_disable_sort_column=>'N'
,p_lov_show_nulls=>'NO'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64472342672550803)
,p_query_column_id=>12
,p_column_alias=>'HEIR_PATH'
,p_column_display_sequence=>25
,p_column_heading=>'Heir Path'
,p_default_sort_column_sequence=>1
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64472712190550803)
,p_query_column_id=>13
,p_column_alias=>'COLL_AGREEMENT_TYPE_ID'
,p_column_display_sequence=>13
,p_column_heading=>'Coll Agreement Type Id'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64473161564550803)
,p_query_column_id=>14
,p_column_alias=>'CREATION_CLASS_ID'
,p_column_display_sequence=>14
,p_column_heading=>'Creation Class Id'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64473537280550804)
,p_query_column_id=>15
,p_column_alias=>'RIGHT_TYPE_ID'
,p_column_display_sequence=>15
,p_column_heading=>'Right Type Id'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64473908532550804)
,p_query_column_id=>16
,p_column_alias=>'ROLE_ID'
,p_column_display_sequence=>16
,p_column_heading=>'Role Id'
,p_disable_sort_column=>'N'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64474319842550804)
,p_query_column_id=>17
,p_column_alias=>'START_DATE'
,p_column_display_sequence=>7
,p_column_heading=>'Start Date'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_lov_show_nulls=>'NO'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64474742498550805)
,p_query_column_id=>18
,p_column_alias=>'END_DATE'
,p_column_display_sequence=>8
,p_column_heading=>'End Date'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64475189008550805)
,p_query_column_id=>19
,p_column_alias=>'EFFECTIVE_DATE'
,p_column_display_sequence=>9
,p_column_heading=>'Effective Date'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64475572359550805)
,p_query_column_id=>20
,p_column_alias=>'SIGNATURE_DATE'
,p_column_display_sequence=>17
,p_column_heading=>'Signature Date'
,p_disable_sort_column=>'N'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64475924287550806)
,p_query_column_id=>21
,p_column_alias=>'AMMENDMENT_DATE'
,p_column_display_sequence=>18
,p_column_heading=>'Ammendment Date'
,p_disable_sort_column=>'N'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64476398326550806)
,p_query_column_id=>22
,p_column_alias=>'RENEW_ALLOWED_FLAG'
,p_column_display_sequence=>19
,p_column_heading=>'Renew Allowed Flag'
,p_disable_sort_column=>'N'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64476795704550806)
,p_query_column_id=>23
,p_column_alias=>'RENEWAL_TERM_MONTHS'
,p_column_display_sequence=>20
,p_column_heading=>'Renewal Term Months'
,p_disable_sort_column=>'N'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64477156687550806)
,p_query_column_id=>24
,p_column_alias=>'STATUS'
,p_column_display_sequence=>10
,p_column_heading=>'Status'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64477556136550807)
,p_query_column_id=>25
,p_column_alias=>'ALERT_DATE'
,p_column_display_sequence=>21
,p_column_heading=>'Alert Date'
,p_disable_sort_column=>'N'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64477926284550807)
,p_query_column_id=>26
,p_column_alias=>'DOMESTIC_PAY_THRU_DATE'
,p_column_display_sequence=>22
,p_column_heading=>'Domestic Pay Thru Date'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64478345400550807)
,p_query_column_id=>27
,p_column_alias=>'FOREIGN_PAY_THRU_DATE'
,p_column_display_sequence=>23
,p_column_heading=>'Foreign Pay Thru Date'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64478799099550808)
,p_query_column_id=>28
,p_column_alias=>'IGNORE_EARN_DATE_FLAG'
,p_column_display_sequence=>24
,p_column_heading=>'Ignore Earn Date Flag'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64479182864550808)
,p_query_column_id=>29
,p_column_alias=>'COLL_SOC'
,p_column_display_sequence=>2
,p_column_heading=>'Collector Society'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(64479520863550808)
,p_name=>'Summary Agreements'
,p_parent_plug_id=>wwv_flow_api.id(67166923599110709)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>440
,p_region_template_options=>'#DEFAULT#:t-Region--hiddenOverflow'
,p_component_template_options=>'#DEFAULT#'
,p_new_grid_column=>false
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT trim(soc.society_name)',
'  ||'' ''',
'  ||soc.society_country society ,',
'  rc.role_code',
'  ||'' ''',
'  ||rc.role_desc role_type ,',
'  ama.signature_date,',
'  ama.membership_share ,',
'  valid_from_date,',
'  valid_to_date,',
'  ammendment_date,',
'  effective_date ,',
'  ain.primary_name aff_name,',
'  ama.renewal_term_months,',
'  ama.status,',
'  ama.alert_date,',
'  ama.domestic_pay_thru_date ,',
'  MIN(ama.membership_agreement_id) membership_agreement_id,',
'  pgp.property_id,',
'count(1) row_count',
'FROM AFF_MEMBERSHIP_AGREEMENTS ama,',
'  AFF_SOCIETIES soc,',
'  AFF_ROLES rc,',
'  AFF_RIGHT_TYPES rt ,',
'  PROP_GROUPS pg,',
'  PROP_GROUP_PROPS pgp,',
'  AFF_IP_NAME_V ain',
'WHERE ama.group_id   = pg.group_id',
'AND PGP.GROUP_ID     = pg.group_id',
'AND pgp.property_id  = :P310_PROP_ID',
'AND SOC.SOCIETY_ID   = AMA.SOCIETY_ID',
'AND ain.ip_id        = ama.ip_id',
'AND RC.ROLE_ID       = AMA.ROLE_ID',
'AND RT.RIGHT_TYPE_ID = AMA.RIGHT_TYPE_ID',
'GROUP BY trim(soc.society_name)',
'  ||'' ''',
'  ||soc.society_country  ,',
'  rc.role_code',
'  ||'' ''',
'  ||rc.role_desc  ,',
'  ama.signature_date,',
'  ama.membership_share ,',
'  valid_from_date,',
'  valid_to_date,',
'  ammendment_date,',
'  effective_date ,',
'  ain.primary_name ,',
'  ama.renewal_term_months,',
'  ama.status,',
'  ama.alert_date,',
'  ama.domestic_pay_thru_date ,',
'  pgp.property_id'))
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>20
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>' - '
,p_query_break_cols=>'0'
,p_query_no_data_found=>'no data found'
,p_query_num_rows_type=>'NEXT_PREVIOUS_LINKS'
,p_query_row_count_max=>500
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_break_type_flag=>'DEFAULT_BREAK_FORMATTING'
,p_csv_output=>'Y'
,p_csv_output_link_text=>'Excel'
,p_query_asc_image=>'apex/builder/dup.gif'
,p_query_asc_image_attr=>'width="16" height="16" alt="" '
,p_query_desc_image=>'apex/builder/ddown.gif'
,p_query_desc_image_attr=>'width="16" height="16" alt="" '
,p_plug_query_strip_html=>'Y'
);
end;
/
begin
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64479979712550809)
,p_query_column_id=>1
,p_column_alias=>'SOCIETY'
,p_column_display_sequence=>3
,p_column_heading=>'Society'
,p_default_sort_column_sequence=>1
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64480354666550809)
,p_query_column_id=>2
,p_column_alias=>'ROLE_TYPE'
,p_column_display_sequence=>4
,p_column_heading=>'Role Type'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64480723969550809)
,p_query_column_id=>3
,p_column_alias=>'SIGNATURE_DATE'
,p_column_display_sequence=>7
,p_column_heading=>'Signature Date'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64481195667550810)
,p_query_column_id=>4
,p_column_alias=>'MEMBERSHIP_SHARE'
,p_column_display_sequence=>2
,p_column_heading=>'Membership Share'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64481534034550810)
,p_query_column_id=>5
,p_column_alias=>'VALID_FROM_DATE'
,p_column_display_sequence=>5
,p_column_heading=>'Valid From Date'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64481980726550810)
,p_query_column_id=>6
,p_column_alias=>'VALID_TO_DATE'
,p_column_display_sequence=>6
,p_column_heading=>'Valid To Date'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64482317847550810)
,p_query_column_id=>7
,p_column_alias=>'AMMENDMENT_DATE'
,p_column_display_sequence=>8
,p_column_heading=>'Ammendment Date'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64482721995550811)
,p_query_column_id=>8
,p_column_alias=>'EFFECTIVE_DATE'
,p_column_display_sequence=>9
,p_column_heading=>'Effective Date'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64483135603550811)
,p_query_column_id=>9
,p_column_alias=>'AFF_NAME'
,p_column_display_sequence=>1
,p_column_heading=>'Interested Party'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64483569026550811)
,p_query_column_id=>10
,p_column_alias=>'RENEWAL_TERM_MONTHS'
,p_column_display_sequence=>10
,p_column_heading=>'Renewal Term Months'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64483998297550812)
,p_query_column_id=>11
,p_column_alias=>'STATUS'
,p_column_display_sequence=>12
,p_column_heading=>'Status'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64484330455550812)
,p_query_column_id=>12
,p_column_alias=>'ALERT_DATE'
,p_column_display_sequence=>13
,p_column_heading=>'Alert Date'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64484742772550812)
,p_query_column_id=>13
,p_column_alias=>'DOMESTIC_PAY_THRU_DATE'
,p_column_display_sequence=>14
,p_column_heading=>'Domestic Pay Thru Date'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64485132657550812)
,p_query_column_id=>14
,p_column_alias=>'MEMBERSHIP_AGREEMENT_ID'
,p_column_display_sequence=>11
,p_column_heading=>'Membership Agreement Id'
,p_column_link=>'f?p=&APP_ID.:315:&SESSION.:APEX_CLONE_SESSION:&DEBUG.:RP,315:P315_MEMBERSHIP_AGREEMENT_ID:#MEMBERSHIP_AGREEMENT_ID#'
,p_column_linktext=>'#MEMBERSHIP_AGREEMENT_ID#'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64485539743550813)
,p_query_column_id=>15
,p_column_alias=>'PROPERTY_ID'
,p_column_display_sequence=>15
,p_column_heading=>'Property Id'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64485939163550813)
,p_query_column_id=>16
,p_column_alias=>'ROW_COUNT'
,p_column_display_sequence=>16
,p_column_heading=>'Row Count'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(67227392185257096)
,p_plug_name=>'Usages'
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--scrollBody'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>205
,p_include_in_reg_disp_sel_yn=>'Y'
,p_plug_display_point=>'BODY_3'
,p_plug_query_num_rows=>15
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_plug_display_condition_type=>'NEVER'
,p_attribute_01=>'N'
,p_attribute_02=>'HTML'
,p_attribute_03=>'Y'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(64408367978550753)
,p_plug_name=>'Usage Links'
,p_parent_plug_id=>wwv_flow_api.id(67227392185257096)
,p_region_template_options=>'#DEFAULT#:t-Region--hiddenOverflow'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>750
,p_plug_display_point=>'BODY'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_attribute_01=>'N'
,p_attribute_02=>'HTML'
,p_attribute_03=>'N'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(64453946275550789)
,p_name=>'Usage Summary'
,p_parent_plug_id=>wwv_flow_api.id(67227392185257096)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>830
,p_region_template_options=>'#DEFAULT#:t-Region--hiddenOverflow'
,p_component_template_options=>'#DEFAULT#'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select right_type_id, source_property_number, perf_category_id, territory_id, source_id,Qty  ',
'from prop_usg_details_mv',
'where property_id = :P310_PROP_ID'))
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>25
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>' - '
,p_query_break_cols=>'0'
,p_query_no_data_found=>'no data found'
,p_query_num_rows_type=>'NEXT_PREVIOUS_LINKS'
,p_query_row_count_max=>500
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_break_type_flag=>'DEFAULT_BREAK_FORMATTING'
,p_csv_output=>'Y'
,p_csv_output_link_text=>'Excel'
,p_query_asc_image=>'apex/builder/dup.gif'
,p_query_asc_image_attr=>'width="16" height="16" alt="" '
,p_query_desc_image=>'apex/builder/ddown.gif'
,p_query_desc_image_attr=>'width="16" height="16" alt="" '
,p_plug_query_strip_html=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64454370685550789)
,p_query_column_id=>1
,p_column_alias=>'RIGHT_TYPE_ID'
,p_column_display_sequence=>1
,p_column_heading=>'RIGHT TYPE'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_display_as=>'TEXT_FROM_LOV'
,p_named_lov=>wwv_flow_api.id(62952354574980407)
,p_lov_show_nulls=>'NO'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64454706387550790)
,p_query_column_id=>2
,p_column_alias=>'SOURCE_PROPERTY_NUMBER'
,p_column_display_sequence=>2
,p_column_heading=>'SOURCE PROPERTY NUMBER'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_lov_show_nulls=>'NO'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64455102087550790)
,p_query_column_id=>3
,p_column_alias=>'PERF_CATEGORY_ID'
,p_column_display_sequence=>4
,p_column_heading=>'PERF CATEGORY'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_display_as=>'TEXT_FROM_LOV'
,p_named_lov=>wwv_flow_api.id(62956290779980411)
,p_lov_show_nulls=>'NO'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64455595590550790)
,p_query_column_id=>4
,p_column_alias=>'TERRITORY_ID'
,p_column_display_sequence=>3
,p_column_heading=>'TERRITORY'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_display_as=>'TEXT_FROM_LOV'
,p_named_lov=>wwv_flow_api.id(62955684789980410)
,p_lov_show_nulls=>'NO'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64455906131550790)
,p_query_column_id=>5
,p_column_alias=>'SOURCE_ID'
,p_column_display_sequence=>5
,p_column_heading=>'SOURCE'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_display_as=>'TEXT_FROM_LOV'
,p_named_lov=>wwv_flow_api.id(62955084925980410)
,p_lov_show_nulls=>'NO'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(64456396749550791)
,p_query_column_id=>6
,p_column_alias=>'QTY'
,p_column_display_sequence=>6
,p_column_heading=>'QTY'
,p_column_alignment=>'RIGHT'
,p_heading_alignment=>'RIGHT'
,p_default_sort_column_sequence=>1
,p_default_sort_dir=>'desc'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(67434368603330120)
,p_name=>'Titles(AKAs)'
,p_region_name=>'TITLE'
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>75
,p_include_in_reg_disp_sel_yn=>'Y'
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#'
,p_display_point=>'BODY_3'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select pa.aka_id,pat.aka_type_code, pa.aka_name, src.source_code, src.description src_desc, src.source_group,',
'pp.property_id, pa.creation_date, ppt.code prop_type, pa.language_id,',
'CASE WHEN pa.aka_type_id = 51 THEN NULL ELSE',
'''<a href="''||APEX_UTIL.PREPARE_URL(''f?p=&APP_ID.:50120:&APP_SESSION.::NO:50120:P50120_AKA_ID,P50120_CALLING_PAGE_ID,P50120_PROPERTY_ID_1:''||pa.aka_id||'',''||50120||'',''||pp.property_id)||''" ><img src="#IMAGE_PREFIX#menu/pencil16x16.gif" alt=""></a>'' ',
'END redirect',
'from prop_properties pp, prop_property_types ppt, prop_sources src, prop_akas pa, prop_aka_types pat',
'where ppt.property_type_id = pp.property_type_id',
'and src.source_id = NVL(pa.source_id, 7) ',
'-- Source of the AKA not source of the song',
'and PAT.AKA_TYPE_ID = PA.AKA_TYPE_ID',
'and PA.PROPERTY_ID = pp.property_id',
'and pp.property_id = :P310_PROP_ID AND :P310_QUERY_TITLE=''Y''',
'--and pat.aka_type_code <> ''Society AKA'''))
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_no_data_found=>'no data found'
,p_query_num_rows_type=>'ROW_RANGES_IN_SELECT_LIST'
,p_query_row_count_max=>500
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_csv_output=>'N'
,p_prn_output=>'N'
,p_sort_null=>'L'
,p_query_asc_image=>'apex/builder/dup.gif'
,p_query_asc_image_attr=>'width="16" height="16" alt="" '
,p_query_desc_image=>'apex/builder/ddown.gif'
,p_query_desc_image_attr=>'width="16" height="16" alt="" '
,p_plug_query_strip_html=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(67436670913330122)
,p_query_column_id=>1
,p_column_alias=>'AKA_ID'
,p_column_display_sequence=>1
,p_column_heading=>'Select'
,p_column_link=>'f?p=&APP_ID.:50120:&SESSION.::&DEBUG.::P50120_AKA_ID,P50120_CALLING_PAGE_ID,P50120_PROPERTY_ID_1:#AKA_ID#,&APP_PAGE_ID.,#PROPERTY_ID#'
,p_column_linktext=>'<img src="#IMAGE_PREFIX#menu/pencil16x16.gif" alt="">'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_display_when_cond_type=>'NEVER'
,p_report_column_required_role=>wwv_flow_api.id(71705400949038146)
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(67437043711330122)
,p_query_column_id=>2
,p_column_alias=>'AKA_TYPE_CODE'
,p_column_display_sequence=>3
,p_column_heading=>'AKA Type Code'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(67437460306330123)
,p_query_column_id=>3
,p_column_alias=>'AKA_NAME'
,p_column_display_sequence=>4
,p_column_heading=>'AKA Name'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(67437894278330123)
,p_query_column_id=>4
,p_column_alias=>'SOURCE_CODE'
,p_column_display_sequence=>5
,p_column_heading=>'Source Code'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(67434612250330121)
,p_query_column_id=>5
,p_column_alias=>'SRC_DESC'
,p_column_display_sequence=>6
,p_column_heading=>'Src Desc'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(67435035808330121)
,p_query_column_id=>6
,p_column_alias=>'SOURCE_GROUP'
,p_column_display_sequence=>7
,p_column_heading=>'Source Group'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(67435440925330121)
,p_query_column_id=>7
,p_column_alias=>'PROPERTY_ID'
,p_column_display_sequence=>9
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(13725406811802135)
,p_query_column_id=>8
,p_column_alias=>'CREATION_DATE'
,p_column_display_sequence=>10
,p_column_heading=>'Entry Date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(67436250480330122)
,p_query_column_id=>9
,p_column_alias=>'PROP_TYPE'
,p_column_display_sequence=>11
,p_column_heading=>'Prop Type'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(29508319699115507)
,p_query_column_id=>10
,p_column_alias=>'LANGUAGE_ID'
,p_column_display_sequence=>8
,p_column_heading=>'Language Code'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_display_as=>'TEXT_FROM_LOV'
,p_named_lov=>wwv_flow_api.id(68674396253277503)
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(22913750184170404)
,p_query_column_id=>11
,p_column_alias=>'REDIRECT'
,p_column_display_sequence=>2
,p_column_heading=>'Select'
,p_use_as_row_header=>'N'
,p_column_alignment=>'CENTER'
,p_heading_alignment=>'LEFT'
,p_report_column_required_role=>wwv_flow_api.id(71705400949038146)
,p_display_as=>'WITHOUT_MODIFICATION'
,p_derived_column=>'N'
,p_include_in_export=>'N'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(35019053144952218)
,p_name=>'Submitter Work(s)'
,p_parent_plug_id=>wwv_flow_api.id(67434368603330120)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>76
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#:t-Report--altRowsDefault:t-Report--rowHighlight'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT ps.description as Source, spx.property_xref_id, spx.submitter_source_id, spx.sesac_property_id, spx.submitter_work_id, ps.ip_id, ',
'        (SELECT ip_number FROM aff_interested_parties WHERE ip_id = ps.ip_id) ip_number, ',
'        (SELECT full_name FROM aff_ip_names WHERE ip_id = ps.ip_id AND name_type_id = 6) full_name',
'        FROM SPI_PROPERTY_XREF spx, PROP_SOURCES ps',
'        WHERE spx.submitter_source_id = ps.source_id',
'        AND spx.sesac_property_id = :P310_PROP_ID AND :P310_QUERY_TITLE=''Y'';'))
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>'-'
,p_query_no_data_found=>'No data found'
,p_query_num_rows_type=>'ROW_RANGES_IN_SELECT_LIST'
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_csv_output=>'N'
,p_prn_output=>'N'
,p_sort_null=>'L'
,p_plug_query_strip_html=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(28297661620779501)
,p_query_column_id=>1
,p_column_alias=>'SOURCE'
,p_column_display_sequence=>5
,p_column_heading=>'Submitter Source'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(35019185459952219)
,p_query_column_id=>2
,p_column_alias=>'PROPERTY_XREF_ID'
,p_column_display_sequence=>1
,p_column_link=>'f?p=&APP_ID.:31:&SESSION.::&DEBUG.:RP,31:P31_PROPERTY_XREF_ID:#PROPERTY_XREF_ID#'
,p_column_linktext=>'<img src="#IMAGE_PREFIX#menu/pencil16x16.gif" alt="">'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(35019237695952220)
,p_query_column_id=>3
,p_column_alias=>'SUBMITTER_SOURCE_ID'
,p_column_display_sequence=>2
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(35019360787952221)
,p_query_column_id=>4
,p_column_alias=>'SESAC_PROPERTY_ID'
,p_column_display_sequence=>3
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(35019418282952222)
,p_query_column_id=>5
,p_column_alias=>'SUBMITTER_WORK_ID'
,p_column_display_sequence=>4
,p_column_heading=>'Submitter Work Id'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(35020409728952232)
,p_query_column_id=>6
,p_column_alias=>'IP_ID'
,p_column_display_sequence=>8
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(35020369423952231)
,p_query_column_id=>7
,p_column_alias=>'IP_NUMBER'
,p_column_display_sequence=>6
,p_column_heading=>'IP Number'
,p_column_link=>'f?p=&APP_ID.:260:&SESSION.:APEX_CLONE_SESSION:&DEBUG.:RP,260:P260_IP_ID,P260_AFFILIATE_NO,P260_CALLING_PAGE_ID:#IP_ID#,#IP_NUMBER#,310'
,p_column_linktext=>'#IP_NUMBER#'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(35019862282952226)
,p_query_column_id=>8
,p_column_alias=>'FULL_NAME'
,p_column_display_sequence=>7
,p_column_heading=>'Full Name'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(11418380484058730)
,p_button_sequence=>10
,p_button_plug_id=>wwv_flow_api.id(64357996627550715)
,p_button_name=>'EDITPERSHAREOWNER'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Edit PER Share Owners'
,p_button_position=>'ABOVE_BOX'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:50198:&SESSION.::&DEBUG.:RP,50198:P50198_PROPERTY_ID,P50198_SONG_NUMBER,P50198_CALLING_PAGE_ID,P50198_RIGHT_TYPE,P50198_CREATED_FLAG,P50198_PROP_SHARE_OWERS:&P310_PROP_ID.,&P310_PROP_NUMBER.,310,1,N,'
,p_icon_css_classes=>'fa-pencil'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(17261641272120017)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(15175662830593235)
,p_button_sequence=>10
,p_button_plug_id=>wwv_flow_api.id(64381996642550734)
,p_button_name=>'P310_COLLECTION_AGREEMENTS'
,p_button_static_id=>'P50100_MEMBER_AGREEMENTS'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Collection Agreements'
,p_button_position=>'ABOVE_BOX'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:50140:&SESSION.::&DEBUG.:50140:P50140_PROPERTY_ID:&P310_PROP_ID.'
,p_button_condition_type=>'NEVER'
,p_icon_css_classes=>'fa-star'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(17200066639506016)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(15177088801593249)
,p_button_sequence=>10
,p_button_plug_id=>wwv_flow_api.id(64438765154550775)
,p_button_name=>'Add'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Add Recording'
,p_button_position=>'ABOVE_BOX'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:50092:&SESSION.::&DEBUG.:RP,50092:P50092_SONG_PROPERTY_ID,P50092_SONG_TITLE:&P310_PROP_ID.,&P310_PROP_NAME.'
,p_icon_css_classes=>'fa-plus'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(17255026364731121)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(22778732466532695)
,p_button_sequence=>10
,p_button_plug_id=>wwv_flow_api.id(64328306678550690)
,p_button_name=>'P310_MEMBER_AGREEMENTS'
,p_button_static_id=>'P50100_MEMBER_AGREEMENTS'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Member Agreements'
,p_button_position=>'ABOVE_BOX'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:50130:&SESSION.::&DEBUG.:50130:P50130_PROPERTY_ID:&P50100_PROPERTY_ID.'
,p_button_condition_type=>'NEVER'
,p_icon_css_classes=>'fa-star'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(17200066639506016)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(13585953430450120)
,p_button_sequence=>20
,p_button_plug_id=>wwv_flow_api.id(64357996627550715)
,p_button_name=>'EDITMECSHAREOWNER'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Edit MEC Share Owners'
,p_button_position=>'ABOVE_BOX'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:50298:&SESSION.::&DEBUG.:RP,50298:P50298_PROPERTY_ID,P50298_SONG_NUMBER,P50298_CALLING_PAGE_ID,P50298_RIGHT_TYPE,P50298_CREATED_FLAG,P50298_PROP_SHARE_OWERS:&P310_PROP_ID.,&P310_PROP_NUMBER.,310,2,N,'
,p_icon_css_classes=>'fa-pencil'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(17261641272120017)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(17495469039570908)
,p_button_sequence=>30
,p_button_plug_id=>wwv_flow_api.id(64357996627550715)
,p_button_name=>'EDITOTHERSHAREOWNER'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Edit Sync Share Owners'
,p_button_position=>'ABOVE_BOX'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:50398:&SESSION.::&DEBUG.:RP,50398:P50398_PROPERTY_ID,P50398_SONG_NUMBER,P50398_CALLING_PAGE_ID,P50398_RIGHT_TYPE,P50398_CREATED_FLAG,P50398_PROP_SHARE_OWERS:&P310_PROP_ID.,&P310_PROP_NUMBER.,310,3,N,'
,p_icon_css_classes=>'fa-pencil'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(17261641272120017)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(80887367047218402)
,p_button_sequence=>40
,p_button_plug_id=>wwv_flow_api.id(64357996627550715)
,p_button_name=>'EDITOTHERSHAREOWNER_1'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Edit Print Share Owners'
,p_button_position=>'ABOVE_BOX'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:50398:&SESSION.::&DEBUG.:RP,50398:P50398_PROPERTY_ID,P50398_SONG_NUMBER,P50398_CALLING_PAGE_ID,P50398_RIGHT_TYPE,P50398_CREATED_FLAG,P50398_PROP_SHARE_OWERS:&P310_PROP_ID.,&P310_PROP_NUMBER.,310,4,N,'
,p_icon_css_classes=>'fa-pencil'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(17261641272120017)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(34185884085070701)
,p_button_sequence=>50
,p_button_plug_id=>wwv_flow_api.id(64357996627550715)
,p_button_name=>'EDIT_SHARE_HOLD'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Edit Share Holds'
,p_button_position=>'ABOVE_BOX'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:3:&SESSION.::&DEBUG.:RP,3:P3_PROP_PROPERTY:&P310_PROP_ID.'
,p_icon_css_classes=>'fa-pencil'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(17261641272120017)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(11644185722815331)
,p_button_sequence=>10
,p_button_plug_id=>wwv_flow_api.id(11643894853815328)
,p_button_name=>'Add_1'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Add Recording'
,p_button_position=>'BELOW_BOX'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:50092:&SESSION.::&DEBUG.:RP,50092:P50092_SONG_PROPERTY_ID,P50092_SONG_TITLE:&P310_PROP_ID.,&P310_PROP_NAME.'
,p_icon_css_classes=>'fa-plus'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(17255026364731121)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(11646946572834309)
,p_button_sequence=>20
,p_button_plug_id=>wwv_flow_api.id(11643894853815328)
,p_button_name=>'Delete_Recordings'
,p_button_action=>'SUBMIT'
,p_button_template_options=>'#DEFAULT#:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Delete Recordings'
,p_button_position=>'BELOW_BOX'
,p_button_alignment=>'LEFT'
,p_icon_css_classes=>'fa-eraser'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(17255026364731121)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(67271856309755424)
,p_button_sequence=>270
,p_button_plug_id=>wwv_flow_api.id(67434368603330120)
,p_button_name=>'P310_AKAS'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#'
,p_button_template_id=>wwv_flow_api.id(51870385911240841)
,p_button_image_alt=>'Add New AKA'
,p_button_position=>'BODY'
,p_button_redirect_url=>'f?p=&APP_ID.:50120:&SESSION.::&DEBUG.:50120:P50120_CALLING_PAGE_ID,P50120_PROPERTY_ID,P50120_PROPERTY_ID_1:&APP_PAGE_ID.,&P310_PROP_ID.,&P310_PROP_ID.'
,p_button_condition_type=>'NEVER'
,p_grid_new_grid=>false
,p_grid_new_row=>'Y'
,p_grid_new_column=>'N'
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(64409161265550753)
,p_button_sequence=>370
,p_button_plug_id=>wwv_flow_api.id(64408367978550753)
,p_button_name=>'P310_ENCORE_USAGE'
,p_button_static_id=>'P310_ENCORE_USAGE'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#'
,p_button_template_id=>wwv_flow_api.id(51870385911240841)
,p_button_image_alt=>'Encore Usage'
,p_button_position=>'BODY'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:6200:&SESSION.::&DEBUG.:RP,6200:P6200_PROPERTY_ID:&P310_PROP_ID.'
,p_button_condition_type=>'NEVER'
,p_grid_new_grid=>false
,p_grid_new_row=>'N'
,p_grid_new_column=>'Y'
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(64408764437550753)
,p_button_sequence=>380
,p_button_plug_id=>wwv_flow_api.id(64408367978550753)
,p_button_name=>'P310_BACKGROUND_USAGE'
,p_button_static_id=>'P310_BACKGROUND_USAGE'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#'
,p_button_template_id=>wwv_flow_api.id(51870385911240841)
,p_button_image_alt=>'Background Usage'
,p_button_position=>'BODY'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:6210:&SESSION.::&DEBUG.::P6210_PROPERTY_ID:&P310_PROP_ID.'
,p_grid_new_grid=>false
,p_grid_new_row=>'N'
,p_grid_new_column=>'Y'
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(64411925260550756)
,p_button_sequence=>10
,p_button_plug_id=>wwv_flow_api.id(64411169385550755)
,p_button_name=>'CANCEL'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Cancel'
,p_button_position=>'BOTTOM'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:&P310_CALLING_PAGE_ID.:&SESSION.::&DEBUG.:::'
,p_icon_css_classes=>'fa-rotate-left '
,p_grid_new_grid=>false
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(33362707080353083)
,p_button_sequence=>20
,p_button_plug_id=>wwv_flow_api.id(64411169385550755)
,p_button_name=>'SCANNED_DOCS'
,p_button_action=>'DEFINED_BY_DA'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Scanned Documents'
,p_button_position=>'BOTTOM'
,p_button_alignment=>'LEFT'
,p_warn_on_unsaved_changes=>null
,p_icon_css_classes=>'fa-picture-o'
,p_grid_new_grid=>false
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(64413165374550756)
,p_button_sequence=>30
,p_button_plug_id=>wwv_flow_api.id(64411169385550755)
,p_button_name=>'EDIT_PROPERTY'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Edit Property'
,p_button_position=>'BOTTOM'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:50100:&SESSION.::&DEBUG.::P50100_PROPERTY_ID,P50100_CALLING_PAGE_ID:&P310_PROP_ID.,&APP_PAGE_ID.'
,p_button_condition=>'NVL(:P310_PROP_SOURCE_ID,7)=7'
,p_button_condition_type=>'PLSQL_EXPRESSION'
,p_icon_css_classes=>'fa-pencil'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(71705400949038146)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(38353446346969145)
,p_button_sequence=>40
,p_button_plug_id=>wwv_flow_api.id(64411169385550755)
,p_button_name=>'CLONE_PROP'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Clone Property'
,p_button_position=>'BOTTOM'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:315:&SESSION.::&DEBUG.:RP:P315_PROP_ID:&P310_PROP_ID.'
,p_button_condition=>'NVL(:P310_PROP_SOURCE_ID,7)=7'
,p_button_condition_type=>'PLSQL_EXPRESSION'
,p_icon_css_classes=>'fa-files-o'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(71705400949038146)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(80887443918218403)
,p_button_sequence=>50
,p_button_plug_id=>wwv_flow_api.id(64411169385550755)
,p_button_name=>'BULK_CLONE'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Bulk Clone'
,p_button_position=>'BOTTOM'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:320:&SESSION.:TRUNC:&DEBUG.:RP:P320_PROP_ID:&P310_PROP_ID.'
,p_button_condition=>'NVL(:P310_PROP_SOURCE_ID,7)=7'
,p_button_condition_type=>'PLSQL_EXPRESSION'
,p_icon_css_classes=>'fa-arrows-alt'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(71705400949038146)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(16738788377038822)
,p_button_sequence=>10
,p_button_plug_id=>wwv_flow_api.id(16737876040038813)
,p_button_name=>'Add_Song_Version'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Add Song Version'
,p_button_position=>'TOP'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:50098:&SESSION.::&DEBUG.:50098:P50098_CALLING_PAGE_ID,P50098_MASTER_PROPERTY_ID,P50098_PROP_NAME,P50098_MASTER_PROPERTY_NUMBER:310,&P310_PROP_ID.,&P310_PROP_NAME.,&P310_PROP_NUMBER.'
,p_icon_css_classes=>'fa-plus'
,p_grid_new_grid=>false
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(26555888841540434)
,p_button_sequence=>10
,p_button_plug_id=>wwv_flow_api.id(26554449124540420)
,p_button_name=>'P310_ADD_NEW_RELATION'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Add New Relation'
,p_button_position=>'TOP'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:2142:&SESSION.::&DEBUG.:2142:P2142_CALLING_PAGE_ID,P2142_PROPERTY_ID,P2142_PROP_NAME:&APP_PAGE_ID.,&P310_PROP_ID.,&P310_PROP_NAME.'
,p_icon_css_classes=>'fa-plus'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(71705400949038146)
);
end;
/
begin
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(64376301763550729)
,p_button_sequence=>10
,p_button_plug_id=>wwv_flow_api.id(64369162137550724)
,p_button_name=>'ADD_COMM_JINGLE'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Add Commercial Jingles'
,p_button_position=>'TOP'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:7110:&SESSION.::&DEBUG.:RP,7110:P7110_CALLING_PAGE_ID,P7110_PROP_ID:310,&P310_PROP_ID.'
,p_button_condition=>'NVL(:P310_PROP_SOURCE_ID,7)=7'
,p_button_condition_type=>'PLSQL_EXPRESSION'
,p_icon_css_classes=>'fa-plus'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(71705400949038146)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(15175717257593236)
,p_button_sequence=>20
,p_button_plug_id=>wwv_flow_api.id(64420359819550761)
,p_button_name=>'P310_NON_COLLECTION_AGR_PERMISSIONS'
,p_button_static_id=>'P50100_MEMBER_AGREEMENTS'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Non Collection Agr Permissions'
,p_button_position=>'TOP'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:50140:&SESSION.::&DEBUG.:50140:P50140_PROPERTY_ID:&P310_PROP_ID.'
,p_button_condition_type=>'NEVER'
,p_icon_css_classes=>'fa-star'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(17200066639506016)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(64456774027550791)
,p_button_sequence=>30
,p_button_plug_id=>wwv_flow_api.id(64453946275550789)
,p_button_name=>'EARNINGS_SUMMARY'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#'
,p_button_template_id=>wwv_flow_api.id(51870385911240841)
,p_button_image_alt=>'Earnings Summary'
,p_button_position=>'TOP'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:360:&SESSION.::&DEBUG.:RP:P360_PROPERTY_ID,P360_CALLING_PAGE_ID,P360_REQUEST:&P310_PROP_ID.,&APP_PAGE_ID.,USAGE'
,p_grid_new_grid=>false
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(64381571322550733)
,p_button_sequence=>50
,p_button_plug_id=>wwv_flow_api.id(64376707028550730)
,p_button_name=>'ADD_NOTE'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Add Note'
,p_button_position=>'TOP'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:50110:&SESSION.::&DEBUG.:RP,50110:P50110_PROPERTY_ID:&P310_PROP_ID.'
,p_button_condition=>'NVL(:P310_PROP_SOURCE_ID,7)=7'
,p_button_condition_type=>'PLSQL_EXPRESSION'
,p_icon_css_classes=>'fa-plus'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(71705400949038146)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(67438228321330123)
,p_button_sequence=>270
,p_button_plug_id=>wwv_flow_api.id(67434368603330120)
,p_button_name=>'P310_AKAS'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Add New AKA'
,p_button_position=>'TOP'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:50120:&SESSION.::&DEBUG.:50120:P50120_CALLING_PAGE_ID,P50120_PROPERTY_ID,P50120_PROPERTY_ID_1:&APP_PAGE_ID.,&P310_PROP_ID.,&P310_PROP_ID.'
,p_button_condition=>'NVL(:P310_PROP_SOURCE_ID,7)=7'
,p_button_condition_type=>'PLSQL_EXPRESSION'
,p_icon_css_classes=>'fa-plus'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(71705400949038146)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(35019926256952227)
,p_button_sequence=>280
,p_button_plug_id=>wwv_flow_api.id(35019053144952218)
,p_button_name=>'P310_WORKS'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Add New Submitter Work'
,p_button_position=>'TOP'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:31:&SESSION.::&DEBUG.:31:P31_SESAC_PROPERTY_ID:&P310_PROP_ID.'
,p_button_condition=>'NVL(:P310_PROP_SOURCE_ID,7)=7'
,p_button_condition_type=>'PLSQL_EXPRESSION'
,p_icon_css_classes=>'fa-plus'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(71705400949038146)
);
wwv_flow_api.create_page_branch(
 p_id=>wwv_flow_api.id(64498303038550823)
,p_branch_name=>'Go To Page 250'
,p_branch_action=>'f?p=&APP_ID.:250:&SESSION.&success_msg=#SUCCESS_MSG#'
,p_branch_point=>'AFTER_PROCESSING'
,p_branch_type=>'REDIRECT_URL'
,p_branch_sequence=>20
,p_branch_condition_type=>'REQUEST_NOT_EQUAL_CONDITION'
,p_branch_condition=>'Delete_Recordings'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(4538731606564122)
,p_name=>'P310_QUERY_HISTORY'
,p_item_sequence=>50
,p_item_plug_id=>wwv_flow_api.id(64409597038550754)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(4538922700564124)
,p_name=>'P310_QUERY_AGREEMENTS'
,p_item_sequence=>130
,p_item_plug_id=>wwv_flow_api.id(64409597038550754)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(4756699777734544)
,p_name=>'P310_QUERY_PERMISSIONS'
,p_item_sequence=>140
,p_item_plug_id=>wwv_flow_api.id(64409597038550754)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(6939387718121005)
,p_name=>'P310_CREATION_DATE'
,p_item_sequence=>330
,p_item_plug_id=>wwv_flow_api.id(64338733337550700)
,p_prompt=>'Creation Date'
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
 p_id=>wwv_flow_api.id(9771678807790015)
,p_name=>'P310_STATUTORY_TERMINATION'
,p_item_sequence=>270
,p_item_plug_id=>wwv_flow_api.id(64411169385550755)
,p_prompt=>'Statutory Termination'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_colspan=>3
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(9771762007790016)
,p_name=>'P310_NEW_DERIVATIVE_PROP_ID'
,p_item_sequence=>280
,p_item_plug_id=>wwv_flow_api.id(64411169385550755)
,p_prompt=>'New Derivative'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_escape_on_http_output=>'N'
,p_attribute_01=>'N'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(9771841032790017)
,p_name=>'P310_TERMINATION_EFF_DATE'
,p_item_sequence=>290
,p_item_plug_id=>wwv_flow_api.id(64411169385550755)
,p_prompt=>'Termination Effective Date'
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
 p_id=>wwv_flow_api.id(11508317802788920)
,p_name=>'P310_PROP_SOURCE_ID'
,p_item_sequence=>400
,p_item_plug_id=>wwv_flow_api.id(64338733337550700)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(11508415063788921)
,p_name=>'P310_PROP_SOURCE'
,p_item_sequence=>410
,p_item_plug_id=>wwv_flow_api.id(64338733337550700)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(11643902468815329)
,p_name=>'P310_REC_PROPERTY_ID_1'
,p_item_sequence=>10
,p_item_plug_id=>wwv_flow_api.id(11643894853815328)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(11644005289815330)
,p_name=>'P310_BDS_SONG_ID_1'
,p_item_sequence=>20
,p_item_plug_id=>wwv_flow_api.id(11643894853815328)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(16738809022038823)
,p_name=>'P310_QUERY_VESRIONS'
,p_item_sequence=>160
,p_item_plug_id=>wwv_flow_api.id(64409597038550754)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(21345609141187443)
,p_name=>'P310_COPYRIGHT_DATE'
,p_item_sequence=>90
,p_item_plug_id=>wwv_flow_api.id(64411169385550755)
,p_prompt=>'Copyright Date'
,p_display_as=>'NATIVE_DATE_PICKER'
,p_cSize=>30
,p_colspan=>3
,p_grid_label_column_span=>1
,p_read_only_when_type=>'ALWAYS'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_04=>'button'
,p_attribute_05=>'N'
,p_attribute_07=>'NONE'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(21345789774187444)
,p_name=>'P310_LANGUAGE_CODE'
,p_item_sequence=>40
,p_item_plug_id=>wwv_flow_api.id(64411169385550755)
,p_prompt=>'Language Code'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_named_lov=>'AFF_LANGUAGE_CODES'
,p_lov=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select language d, language_id r',
'from aff_language_codes',
'order by 1'))
,p_lov_display_null=>'YES'
,p_cHeight=>1
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_label_column_span=>1
,p_read_only_when_type=>'ALWAYS'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(33671284432694127)
,p_name=>'P310_WRITER_NAMES'
,p_item_sequence=>230
,p_item_plug_id=>wwv_flow_api.id(64411169385550755)
,p_prompt=>'Writer Names'
,p_source=>'RETURN spi_screen_utility.get_writers(:P310_PROP_ID);'
,p_source_type=>'FUNCTION_BODY'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_colspan=>3
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'Y'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(33673151168694146)
,p_name=>'P310_PROP_HOLD_REASON'
,p_item_sequence=>235
,p_item_plug_id=>wwv_flow_api.id(64411169385550755)
,p_prompt=>'Property Hold Reason'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT listagg(hold, '''') within group (order by hold) hold',
'FROM',
'(',
'    select ht.description ||''<BR>'' hold',
'    from prop_properties pp, pay_hold_types ht',
'    where  property_id = :P310_PROP_ID',
'    and pp.hold_type_id = ht.hold_type_id',
'    union all',
'    select DECODE(listagg(description, '';'') within group (order by description),NULL,null,''Prop. Share Holds: ''|| listagg(description, '';'') within group (order by description)) HOLD',
'    from',
'    (',
'        select ht.description',
'        from prop_share_collectors psc, pay_hold_types ht',
'        where property_id = :P310_PROP_ID',
'        and psc.hold_type_id = ht.hold_type_id',
'        and ht.active_flag = ''Y''',
'        and end_date_active is null',
'        and ht.hold_type_id is not null',
'        group by ht.description',
'    )',
');'))
,p_source_type=>'QUERY'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_escape_on_http_output=>'N'
,p_attribute_01=>'Y'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(34044104332522834)
,p_name=>'P310_PROPERTY_HOLDS'
,p_item_sequence=>10
,p_item_plug_id=>wwv_flow_api.id(34043400295522827)
,p_prompt=>'Property Holds'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'Y'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(37910574101150736)
,p_name=>'P310_QUERY_REG_HISTORY'
,p_item_sequence=>10
,p_item_plug_id=>wwv_flow_api.id(37909488243150725)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(42343200198324340)
,p_name=>'P310_QUERY_TITLE'
,p_item_sequence=>60
,p_item_plug_id=>wwv_flow_api.id(64409597038550754)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(42499534535432801)
,p_name=>'P310_QUERY_PUBREL'
,p_item_sequence=>70
,p_item_plug_id=>wwv_flow_api.id(64409597038550754)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(42499652630432802)
,p_name=>'P310_QUERY_WORK'
,p_item_sequence=>80
,p_item_plug_id=>wwv_flow_api.id(64409597038550754)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(42500186957432807)
,p_name=>'P310_QUERY_REC'
,p_item_sequence=>90
,p_item_plug_id=>wwv_flow_api.id(64409597038550754)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(42500601029432812)
,p_name=>'P310_QUERY_COMM'
,p_item_sequence=>100
,p_item_plug_id=>wwv_flow_api.id(64409597038550754)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(42501181312432817)
,p_name=>'P310_QUERY_CUESHEET'
,p_item_sequence=>110
,p_item_plug_id=>wwv_flow_api.id(64409597038550754)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(42501690107432822)
,p_name=>'P310_QUERY_NOTE'
,p_item_sequence=>120
,p_item_plug_id=>wwv_flow_api.id(64409597038550754)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(47643641069622826)
,p_name=>'P310_QUERY_CONSOLIADTED_HOLDS'
,p_item_sequence=>150
,p_item_plug_id=>wwv_flow_api.id(64409597038550754)
,p_prompt=>'Query consoliadted holds'
,p_display_as=>'NATIVE_TEXT_FIELD'
,p_cSize=>30
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'N'
,p_attribute_04=>'TEXT'
,p_attribute_05=>'BOTH'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(64339117685550700)
,p_name=>'P310_DURATION'
,p_item_sequence=>280
,p_item_plug_id=>wwv_flow_api.id(64338733337550700)
,p_prompt=>'Duration (HHMMSS)'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(64339535655550701)
,p_name=>'P310_HOLD_REASON_ID'
,p_item_sequence=>290
,p_item_plug_id=>wwv_flow_api.id(64338733337550700)
,p_prompt=>'Hold Reason'
,p_display_as=>'NATIVE_HIDDEN'
,p_cSize=>30
,p_cMaxlength=>4000
,p_cHeight=>1
,p_cAttributes=>'nowrap="nowrap"'
,p_begin_on_new_line=>'N'
,p_colspan=>1
,p_rowspan=>1
,p_label_alignment=>'RIGHT'
,p_field_alignment=>'LEFT-CENTER'
,p_display_when_type=>'NEVER'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(64339922671550701)
,p_name=>'P310_PUBLIC_DOMAIN_FLAG'
,p_item_sequence=>310
,p_item_plug_id=>wwv_flow_api.id(64338733337550700)
,p_prompt=>'Public Domain'
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
 p_id=>wwv_flow_api.id(64340307920550701)
,p_name=>'P310_ENTRY_DATE'
,p_item_sequence=>320
,p_item_plug_id=>wwv_flow_api.id(64338733337550700)
,p_prompt=>'Entry Date'
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
 p_id=>wwv_flow_api.id(64409989079550754)
,p_name=>'P310_PROP_ID'
,p_item_sequence=>10
,p_item_plug_id=>wwv_flow_api.id(64409597038550754)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(64410363638550754)
,p_name=>'P310_PAGE_ID'
,p_item_sequence=>30
,p_item_plug_id=>wwv_flow_api.id(64409597038550754)
,p_source=>':APP_PAGE_ID'
,p_source_type=>'STATIC'
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(64410731599550755)
,p_name=>'P310_BDS_SONG_ID_UNLINK'
,p_item_sequence=>40
,p_item_plug_id=>wwv_flow_api.id(64409597038550754)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(64413537237550757)
,p_name=>'P310_PROP_NUMBER'
,p_item_sequence=>10
,p_item_plug_id=>wwv_flow_api.id(64411169385550755)
,p_prompt=>'Property Number'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_colspan=>3
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(64413915662550757)
,p_name=>'P310_PROP_NAME'
,p_item_sequence=>30
,p_item_plug_id=>wwv_flow_api.id(64411169385550755)
,p_prompt=>'Property Name'
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
 p_id=>wwv_flow_api.id(64414398494550757)
,p_name=>'P310_STATUS'
,p_item_sequence=>250
,p_item_plug_id=>wwv_flow_api.id(64411169385550755)
,p_prompt=>'Status'
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
 p_id=>wwv_flow_api.id(64414714354550757)
,p_name=>'P310_SESAC_IP_PCT'
,p_item_sequence=>50
,p_item_plug_id=>wwv_flow_api.id(64411169385550755)
,p_item_default=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'    v_num   NUMBER := 0;',
'BEGIN',
'    BEGIN',
'        SELECT SUM (pso.share_percentage)',
'        INTO v_num',
'        FROM prop_share_collectors pso',
'        WHERE pso.property_id = :P310_PROP_ID',
'        AND pso.affiliated_society_id = 72',
'        AND NVL (pso.end_date_active, TRUNC (SYSDATE) + 1) > SYSDATE',
'        AND NVL (pso.share_percentage, 0) <> 0;',
'    EXCEPTION',
'        WHEN OTHERS THEN',
'            v_num := 0;',
'    END;',
'',
'    RETURN NVL (v_num, 0);',
'END;'))
,p_item_default_type=>'PLSQL_FUNCTION_BODY'
,p_prompt=>'SESAC IP %'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_colspan=>3
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(64415134326550758)
,p_name=>'P310_PUBLIC_DOMAIN_FLAG2'
,p_item_sequence=>60
,p_item_plug_id=>wwv_flow_api.id(64411169385550755)
,p_prompt=>'Public Domain Flag'
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
 p_id=>wwv_flow_api.id(64415516073550758)
,p_name=>'P310_ISWC_NUM'
,p_item_sequence=>70
,p_item_plug_id=>wwv_flow_api.id(64411169385550755)
,p_prompt=>'ISWC #'
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
 p_id=>wwv_flow_api.id(64415916368550758)
,p_name=>'P310_COPYRIGHT_NUM'
,p_item_sequence=>110
,p_item_plug_id=>wwv_flow_api.id(64411169385550755)
,p_prompt=>'Copyright #'
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
 p_id=>wwv_flow_api.id(64416329365550758)
,p_name=>'P310_PERF_RIGHT_FLAG'
,p_item_sequence=>80
,p_item_plug_id=>wwv_flow_api.id(64411169385550755)
,p_prompt=>'Performance Rights'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_named_lov=>'YES_NO'
,p_lov=>'.'||wwv_flow_api.id(62410853427715388)||'.'
,p_begin_on_new_line=>'N'
,p_colspan=>2
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'Y'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(64416737048550759)
,p_name=>'P310_SYNC_RIGHT_FLAG'
,p_item_sequence=>220
,p_item_plug_id=>wwv_flow_api.id(64411169385550755)
,p_prompt=>'Sync Rights'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_begin_on_new_line=>'N'
,p_colspan=>2
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(64417195668550759)
,p_name=>'P310_MECH_RIGHT_FLAG'
,p_item_sequence=>140
,p_item_plug_id=>wwv_flow_api.id(64411169385550755)
,p_prompt=>'Mechanical Rights'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_begin_on_new_line=>'N'
,p_colspan=>2
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(64417557297550759)
,p_name=>'P310_GRAND_RIGHT_FLAG'
,p_item_sequence=>260
,p_item_plug_id=>wwv_flow_api.id(64411169385550755)
,p_prompt=>'Grand Rights'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_begin_on_new_line=>'N'
,p_colspan=>2
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(64417945822550759)
,p_name=>'P310_CALLING_PAGE_ID'
,p_item_sequence=>150
,p_item_plug_id=>wwv_flow_api.id(64411169385550755)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(64418342349550760)
,p_name=>'P310_REQUEST'
,p_item_sequence=>160
,p_item_plug_id=>wwv_flow_api.id(64411169385550755)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(64418751071550760)
,p_name=>'P310_PROP_ID_2'
,p_item_sequence=>170
,p_item_plug_id=>wwv_flow_api.id(64411169385550755)
,p_source=>'P310_PROP_ID'
,p_source_type=>'ITEM'
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(64419137322550760)
,p_name=>'P310_DERIVED460_STATUS'
,p_item_sequence=>180
,p_item_plug_id=>wwv_flow_api.id(64411169385550755)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(64445953314550783)
,p_name=>'P310_REC_PROPERTY_ID'
,p_item_sequence=>350
,p_item_plug_id=>wwv_flow_api.id(64438765154550775)
,p_display_as=>'NATIVE_HIDDEN'
,p_cMaxlength=>4000
,p_label_alignment=>'RIGHT'
,p_field_alignment=>'LEFT-CENTER'
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(64446370295550783)
,p_name=>'P310_BDS_SONG_ID'
,p_item_sequence=>360
,p_item_plug_id=>wwv_flow_api.id(64438765154550775)
,p_display_as=>'NATIVE_HIDDEN'
,p_cMaxlength=>4000
,p_label_alignment=>'RIGHT'
,p_field_alignment=>'LEFT-CENTER'
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(64461137361550795)
,p_name=>'P310_PUBLIC_DOMAIN_FLAG1'
,p_item_sequence=>330
,p_item_plug_id=>wwv_flow_api.id(64460731890550794)
,p_prompt=>'Public Domain'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_cSize=>3
,p_cMaxlength=>4000
,p_cHeight=>1
,p_cAttributes=>'nowrap="nowrap"'
,p_rowspan=>1
,p_label_alignment=>'RIGHT'
,p_field_alignment=>'LEFT-CENTER'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'N'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(64461598701550795)
,p_name=>'P310_ENTRY_DATE1'
,p_item_sequence=>340
,p_item_plug_id=>wwv_flow_api.id(64460731890550794)
,p_prompt=>'Entry Date'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_cSize=>15
,p_cMaxlength=>4000
,p_cHeight=>1
,p_cAttributes=>'nowrap="nowrap"'
,p_begin_on_new_line=>'N'
,p_rowspan=>1
,p_label_alignment=>'RIGHT'
,p_field_alignment=>'LEFT-CENTER'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'N'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(69153386558056445)
,p_name=>'P310_MEC_LICENSE_COUNT'
,p_item_sequence=>130
,p_item_plug_id=>wwv_flow_api.id(64411169385550755)
,p_prompt=>'MEC License Count'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'Y'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(69228054007028217)
,p_name=>'P310_FIRST_USE_REFUSAL'
,p_item_sequence=>190
,p_item_plug_id=>wwv_flow_api.id(64411169385550755)
,p_prompt=>'First Use Refusal'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_colspan=>3
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'Y'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(69228180233028218)
,p_name=>'P310_WARNING_CODE'
,p_item_sequence=>200
,p_item_plug_id=>wwv_flow_api.id(64411169385550755)
,p_prompt=>'Warning Code'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'Y'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(69228221453028219)
,p_name=>'P310_WARNING_DATE'
,p_item_sequence=>210
,p_item_plug_id=>wwv_flow_api.id(64411169385550755)
,p_prompt=>'Warning Date'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'Y'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(69228382197028220)
,p_name=>'P310_HFA_SONG_CODE'
,p_item_sequence=>20
,p_item_plug_id=>wwv_flow_api.id(64411169385550755)
,p_prompt=>'HFA Song Code'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'Y'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_validation(
 p_id=>wwv_flow_api.id(11646763395834307)
,p_validation_name=>'onClick_Delete_recordings'
,p_validation_sequence=>10
,p_validation=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'v_coll_count NUMBER;',
'BEGIN',
'select count(*) into v_coll_count from apex_collections',
'                           where collection_name = ''P310_RECORDINGS_COLL''',
'                               and c023 = ''Y''',
'                               and c002 = :P310_PROP_ID;',
'    IF v_coll_count = 0 THEN ',
'        RETURN(''Please select a record to delete.'');',
'     END IF;',
'  END;'))
,p_validation_type=>'FUNC_BODY_RETURNING_ERR_TEXT'
,p_error_display_location=>'INLINE_WITH_FIELD_AND_NOTIFICATION'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(67615479017836605)
,p_name=>'REFERSH_AKA'
,p_event_sequence=>10
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(67434368603330120)
,p_bind_type=>'bind'
,p_bind_event_type=>'apexafterclosedialog'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(67615852228836607)
,p_event_id=>wwv_flow_api.id(67615479017836605)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(67434368603330120)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(16894388715341912)
,p_name=>'REFERSH_VERSIONS'
,p_event_sequence=>20
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(16737876040038813)
,p_bind_type=>'bind'
,p_bind_event_type=>'apexafterclosedialog'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(16894452165341913)
,p_event_id=>wwv_flow_api.id(16894388715341912)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(16737876040038813)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(35020129761952229)
,p_name=>'REFERSH_SUBMITTER_WORKS'
,p_event_sequence=>30
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(35019053144952218)
,p_bind_type=>'bind'
,p_bind_event_type=>'apexafterclosedialog'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(35020291705952230)
,p_event_id=>wwv_flow_api.id(35020129761952229)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(35019053144952218)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(4587786646674641)
,p_name=>'P310_QUERY_HISTORY'
,p_event_sequence=>40
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'[href="#HIST"]'
,p_bind_type=>'one'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(4588150625674641)
,p_event_id=>wwv_flow_api.id(4587786646674641)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
':P310_QUERY_HISTORY := ''Y'';',
':P310_PROPERTY_TAB := 12;'))
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(4588600159674641)
,p_event_id=>wwv_flow_api.id(4587786646674641)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>'$s("P310_QUERY_HISTORY ","Y");'
,p_stop_execution_on_error=>'Y'
);
end;
/
begin
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(6939554881121007)
,p_event_id=>wwv_flow_api.id(4587786646674641)
,p_event_result=>'TRUE'
,p_action_sequence=>40
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(64446788704550783)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(37910119901150732)
,p_name=>'P310_QUERY_REG_HISTORY'
,p_event_sequence=>50
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'[href="#RIGHIST"]'
,p_bind_type=>'one'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(37910285268150733)
,p_event_id=>wwv_flow_api.id(37910119901150732)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
':P310_QUERY_REG_HISTORY := ''Y'';',
':P310_PROPERTY_TAB := 7;'))
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(37910305592150734)
,p_event_id=>wwv_flow_api.id(37910119901150732)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>'$s("P310_QUERY_REG_HISTORY ","Y");'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(37910457475150735)
,p_event_id=>wwv_flow_api.id(37910119901150732)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(37909488243150725)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(4539074510564125)
,p_name=>'P310_QUERY_AGREEMENTS'
,p_event_sequence=>60
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'[href="#AGREEMENTS"]'
,p_bind_type=>'one'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(4539191999564126)
,p_event_id=>wwv_flow_api.id(4539074510564125)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
':P310_QUERY_AGREEMENTS:= ''Y'';',
':P310_PROPERTY_TAB:= 5;'))
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(4539263356564127)
,p_event_id=>wwv_flow_api.id(4539074510564125)
,p_event_result=>'TRUE'
,p_action_sequence=>40
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
'//var $wP;',
'',
'// Start the page overlay',
'//setTimeout(function(){$wP = apex.widget.waitPopup();}, 30);',
'',
'//apex.event.trigger( "#AREEMENTS2", "apexrefresh" );',
'//apex.event.trigger( "#AREEMENTS1", "apexrefresh" );',
'// remove overlay and indicator',
'//$wP.remove();',
'',
'$s("P310_QUERY_AGREEMENTS","Y");'))
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(6939869573121010)
,p_event_id=>wwv_flow_api.id(4539074510564125)
,p_event_result=>'TRUE'
,p_action_sequence=>60
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(64328306678550690)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(6940061233121012)
,p_event_id=>wwv_flow_api.id(4539074510564125)
,p_event_result=>'TRUE'
,p_action_sequence=>80
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(64381996642550734)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(4756776705734545)
,p_name=>'P310_QUERY_PERMISSIONS'
,p_event_sequence=>70
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'[href="#PERMISSIONS"]'
,p_bind_type=>'one'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(4756982348734547)
,p_event_id=>wwv_flow_api.id(4756776705734545)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
':P310_QUERY_PERMISSIONS := ''Y'';',
':P310_PROPERTY_TAB := 6;'))
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(4756862478734546)
,p_event_id=>wwv_flow_api.id(4756776705734545)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>'$s("P310_QUERY_PERMISSIONS" ,"Y");'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(6939945971121011)
,p_event_id=>wwv_flow_api.id(4756776705734545)
,p_event_result=>'TRUE'
,p_action_sequence=>40
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(64420359819550761)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(6940149193121013)
,p_event_id=>wwv_flow_api.id(4756776705734545)
,p_event_result=>'TRUE'
,p_action_sequence=>50
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(64487960422550815)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(42343378848324341)
,p_name=>'P310_QUERY_TITLE'
,p_event_sequence=>80
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'[href="#TITLE"]'
,p_bind_type=>'one'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42343408322324342)
,p_event_id=>wwv_flow_api.id(42343378848324341)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
':P310_QUERY_TITLE:= ''Y'';',
':P310_PROPERTY_TAB:= 2;'))
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42343501123324343)
,p_event_id=>wwv_flow_api.id(42343378848324341)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>'$s("P310_QUERY_TITLE" ,"Y");'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42343610120324344)
,p_event_id=>wwv_flow_api.id(42343378848324341)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(67434368603330120)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42343735582324345)
,p_event_id=>wwv_flow_api.id(42343378848324341)
,p_event_result=>'TRUE'
,p_action_sequence=>40
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(35019053144952218)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(42343849569324346)
,p_name=>'P310_QUERY_PUBREL'
,p_event_sequence=>90
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'[href="#PUBREL"]'
,p_bind_type=>'one'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42343916205324347)
,p_event_id=>wwv_flow_api.id(42343849569324346)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
':P310_QUERY_PUBREL:= ''Y'';',
':P310_PROPERTY_TAB:= 3;'))
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42344095723324348)
,p_event_id=>wwv_flow_api.id(42343849569324346)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>'$s("P310_QUERY_PUBREL" ,"Y");'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42344151620324349)
,p_event_id=>wwv_flow_api.id(42343849569324346)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(26554449124540420)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(42499767609432803)
,p_name=>'P310_QUERY_WORK'
,p_event_sequence=>100
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'[href="#WORK"]'
,p_bind_type=>'one'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42499884846432804)
,p_event_id=>wwv_flow_api.id(42499767609432803)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
':P310_QUERY_WORK:= ''Y'';',
':P310_PROPERTY_TAB:=4;'))
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42499908284432805)
,p_event_id=>wwv_flow_api.id(42499767609432803)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>'$s("P310_QUERY_WORK" ,"Y");'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42500010951432806)
,p_event_id=>wwv_flow_api.id(42499767609432803)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(11506530495788902)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(42500268285432808)
,p_name=>'P310_QUERY_REC'
,p_event_sequence=>110
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'[href="#REC"]'
,p_bind_type=>'one'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(366280081926891801)
,p_event_id=>wwv_flow_api.id(42500268285432808)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>'waitPopup = apex.widget.waitPopup();'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42500330650432809)
,p_event_id=>wwv_flow_api.id(42500268285432808)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
':P310_QUERY_REC:= ''Y'';',
':P310_PROPERTY_TAB:= 8;'))
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42500413138432810)
,p_event_id=>wwv_flow_api.id(42500268285432808)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
'$s("P310_QUERY_REC" ,"Y");',
'$s("P310_PROPERTY_TAB",8);'))
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(11647571715834315)
,p_event_id=>wwv_flow_api.id(42500268285432808)
,p_event_result=>'TRUE'
,p_action_sequence=>40
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'v_rec_coll long;',
'BEGIN    ',
'   IF(APEX_COLLECTION.COLLECTION_EXISTS (p_collection_name => ''P310_RECORDINGS_COLL'') )THEN',
'                APEX_COLLECTION.DELETE_COLLECTION (p_collection_name=>''P310_RECORDINGS_COLL'');',
'    END IF;',
'',
'v_rec_coll := ''SELECT DISTINCT pr.RECORDING_ID, pp.source_property_id,pp.property_id,src.description source,pp.title rec_title,pp.property_number,pp.status,pp.hold_reason_id,pr.isrc,pr.ean,',
'trim(artist_last_name||'''' ''''||artist_first_name) artist,pc.collection_title Album,pc.label_name Recording_Label,pc.release_date release_date,',
'pr.recording_format,pa.aka_name Bds_Song_Id,pa.creation_date link_date,DECODE(DECODE(NVL((pa.aka_name),''''''''),'''''''', ''''Link'''',''''Unlink''''),''''Link'''', ''''<a href="f?p=518:341:''''|| v (''''APP_SESSION'''')|| ''''::::P341_REC_PROPERTY_ID,P310_BDS_SONG_ID_UNLINK,P34'
||'1_BDS_SONG_ID,P341_ARTIST,P341_ARTIST_DISP,P341_BDS_SONG_TITLE,P341_BDS_SONG_TITLE_DISP,P341_CAME_FROM,P341_SOURCE_PROPERTY_ID:''''|| pp.property_id',
'|| '''',,,''''||TRIM (artist_last_name||'''' '''' ||artist_first_name)||'''',''''||TRIM(artist_last_name||'''' ''''||artist_first_name)',
'                        || '''',''''',
'                        || trim(pp.title)',
'                        || '''',''''',
'                        || trim(pp.title)',
'                        || '''',''''',
'                        || 310',
'                        || '''','''' ',
'                        || '' ||:P310_PROP_ID || ''',
'                        || ''''">''''',
'                        || ''''Link''''',
'                        || ''''</a>'''',',
'                   ''''<a href="f?p=518:310:''''',
'                || v (''''APP_SESSION'''')',
'                || ''''::::P310_PAGE_ID,P310_REC_PROPERTY_ID,P310_BDS_SONG_ID_UNLINK:''''',
'                || ''''310''''',
'                || '''',''''',
'                || pp.property_id',
'                || '''',''''',
'                || pa.aka_name',
'                || ''''">''''',
'                || ''''UnLink''''',
'                || ''''</a>'''')',
'                Action,',
'                pp.CREATED_BY CREATED_BY,',
'                pp.CREATION_DATE CREATION_DATE,',
'                pp.LAST_UPDATED_BY LAST_UPDATED_BY,',
'                pp.LAST_UPDATE_DATE LAST_UPDATE_DATE,',
'                ''''N'''',',
'                ''''NOT_DELETED''''',
'    FROM prop_performers ppr,',
'      prop_recordings pr,',
'      prop_properties pp,',
'      prop_songs ps,',
'      prop_sources src,',
'      (SELECT * ',
'      FROM prop_akas pa ',
'      WHERE aka_type_id in (select pat.aka_type_id from prop_aka_types pat where upper(pat.aka_type_code)=''''BDS_ENCORE'''')',
'      ) pa,',
'      prop_collection_records pcr,',
'      prop_collections pc',
'    WHERE pr.property_id      = pp.property_id',
'    and ps.property_id        = '' || :P310_PROP_ID ||',
'   ''AND ps.song_id            = pr.song_id',
'    AND pr.property_id       = ppr.property_id(+)',
'    AND pp.source_property_id	 = '' || :P310_PROP_ID ||',
'    '' AND pp.property_type_id   =  2',
'     AND pp.source_id = '' || :P310_PROP_SOURCE_ID ||',
'   '' AND pp.status = ''''A''''',
'    AND ppr.property_id (+)      = pp.property_id',
'    AND src.source_id(+)         = pp.source_id',
'   /*AND (pa.aka_type_id  is null or pa.aka_type_id = (select pat.aka_type_id from prop_aka_types pat where upper(pat.aka_type_code)=''''BDS_ENCORE''''))',
'   Commented by Saurav on 21-APR-2020',
'   */',
'    AND pa.property_id(+)        = pr.property_id',
'    AND pr.recording_id = pcr.recording_id(+)',
'    AND pcr.collection_id = pc.collection_id(+)   and  '' || '''''''' || :P310_QUERY_REC || '''''''' || ''  =''''Y''''''; ',
'',
' ',
'    APEX_COLLECTION.CREATE_COLLECTION_FROM_QUERY( p_collection_name => ''P310_RECORDINGS_COLL'',',
'                                                  p_query => v_rec_coll );',
'       exception when others then',
'            null;',
'            end;'))
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42500518305432811)
,p_event_id=>wwv_flow_api.id(42500268285432808)
,p_event_result=>'TRUE'
,p_action_sequence=>50
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(11643894853815328)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(366280190386891802)
,p_event_id=>wwv_flow_api.id(42500268285432808)
,p_event_result=>'TRUE'
,p_action_sequence=>60
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>'waitPopup.remove();'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(42500766525432813)
,p_name=>'P310_QUERY_COMM'
,p_event_sequence=>120
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'[href="#COMM"]'
,p_bind_type=>'one'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42500863746432814)
,p_event_id=>wwv_flow_api.id(42500766525432813)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
':P310_QUERY_COMM:= ''Y'';',
':P310_PROPERTY_TAB:= 9;'))
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42500918819432815)
,p_event_id=>wwv_flow_api.id(42500766525432813)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>'$s("P310_QUERY_COMM" ,"Y");'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42501093778432816)
,p_event_id=>wwv_flow_api.id(42500766525432813)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(64369162137550724)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(42501257694432818)
,p_name=>'P310_QUERY_CUESHEET'
,p_event_sequence=>130
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'[href="#CUESH"]'
,p_bind_type=>'one'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42501385056432819)
,p_event_id=>wwv_flow_api.id(42501257694432818)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
':P310_QUERY_CUESHEET:= ''Y'';',
':P310_PROPERTY_TAB:= 10;'))
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42501443763432820)
,p_event_id=>wwv_flow_api.id(42501257694432818)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>'$s("P310_QUERY_CUESHEET" ,"Y");'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42501500105432821)
,p_event_id=>wwv_flow_api.id(42501257694432818)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(64363927274550720)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(42501783454432823)
,p_name=>'P310_QUERY_NOTE'
,p_event_sequence=>140
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'[href="#NOTE"]'
,p_bind_type=>'one'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42501856774432824)
,p_event_id=>wwv_flow_api.id(42501783454432823)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
':P310_QUERY_NOTE:= ''Y'';',
':P310_PROPERTY_TAB := 11;'))
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42501919405432825)
,p_event_id=>wwv_flow_api.id(42501783454432823)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>'$s("P310_QUERY_NOTE" ,"Y");'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42502016694432826)
,p_event_id=>wwv_flow_api.id(42501783454432823)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(64376707028550730)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(47643268698622822)
,p_name=>'P310_QUERY_CONSOLIDATED_HOLDS'
,p_event_sequence=>150
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'[href="#COHO"]'
,p_bind_type=>'one'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(47643330828622823)
,p_event_id=>wwv_flow_api.id(47643268698622822)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
':P310_QUERY_CONSOLIADTED_HOLDS:= ''Y'';',
':P310_PROPERTY_TAB:=13;'))
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(47643463663622824)
,p_event_id=>wwv_flow_api.id(47643268698622822)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>'$s("P310_QUERY_CONSOLIADTED_HOLDS" ,"Y");'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(47643575011622825)
,p_event_id=>wwv_flow_api.id(47643268698622822)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(34045421809522847)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(47643769610622827)
,p_event_id=>wwv_flow_api.id(47643268698622822)
,p_event_result=>'TRUE'
,p_action_sequence=>40
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(34044603056522839)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(16739171806038826)
,p_name=>'P310_QUERY_VESRIONS'
,p_event_sequence=>160
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'[href="#VERS"]'
,p_bind_type=>'one'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(16739239456038827)
,p_event_id=>wwv_flow_api.id(16739171806038826)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
':P310_QUERY_VESRIONS := ''Y'';',
':P310_PROPERTY_TAB := 14;'))
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(16739385973038828)
,p_event_id=>wwv_flow_api.id(16739171806038826)
,p_event_result=>'TRUE'
,p_action_sequence=>60
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>'$s("P310_QUERY_VESRIONS" ,"Y");'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(16739408903038829)
,p_event_id=>wwv_flow_api.id(16739171806038826)
,p_event_result=>'TRUE'
,p_action_sequence=>80
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(16737876040038813)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(22934689382627710)
,p_name=>'REFRESH_RECORDING'
,p_event_sequence=>170
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(11643894853815328)
,p_bind_type=>'bind'
,p_bind_event_type=>'apexafterclosedialog'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(11647685175834316)
,p_event_id=>wwv_flow_api.id(22934689382627710)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'v_rec_coll long;',
'BEGIN    ',
'   IF(APEX_COLLECTION.COLLECTION_EXISTS (p_collection_name => ''P310_RECORDINGS_COLL'') )THEN',
'                APEX_COLLECTION.DELETE_COLLECTION (p_collection_name=>''P310_RECORDINGS_COLL'');',
'    END IF;',
'',
'v_rec_coll := ''SELECT DISTINCT pr.RECORDING_ID, pp.source_property_id,pp.property_id,src.description source,pp.title rec_title,pp.property_number,pp.status,pp.hold_reason_id,pr.isrc,pr.ean,',
'trim(artist_last_name||'''' ''''||artist_first_name) artist,pc.collection_title Album,pc.label_name Recording_Label,pc.release_date release_date,',
'pr.recording_format,pa.aka_name Bds_Song_Id,pa.creation_date link_date,DECODE(DECODE(NVL((pa.aka_name),''''''''),'''''''', ''''Link'''',''''Unlink''''),''''Link'''', ''''<a href="f?p=518:341:''''|| v (''''APP_SESSION'''')|| ''''::::P341_REC_PROPERTY_ID,P310_BDS_SONG_ID_UNLINK,P34'
||'1_BDS_SONG_ID,P341_ARTIST,P341_ARTIST_DISP,P341_BDS_SONG_TITLE,P341_BDS_SONG_TITLE_DISP,P341_CAME_FROM,P341_SOURCE_PROPERTY_ID:''''|| pp.property_id',
'|| '''',,,''''||TRIM (artist_last_name||'''' '''' ||artist_first_name)||'''',''''||TRIM(artist_last_name||'''' ''''||artist_first_name)',
'                        || '''',''''',
'                        || trim(pp.title)',
'                        || '''',''''',
'                        || trim(pp.title)',
'                        || '''',''''',
'                        || 310',
'                        || '''','''' ',
'                        || '' ||:P310_PROP_ID || ''',
'                        || ''''">''''',
'                        || ''''Link''''',
'                        || ''''</a>'''',',
'                   ''''<a href="f?p=518:310:''''',
'                || v (''''APP_SESSION'''')',
'                || ''''::::P310_PAGE_ID,P310_REC_PROPERTY_ID,P310_BDS_SONG_ID_UNLINK:''''',
'                || ''''310''''',
'                || '''',''''',
'                || pp.property_id',
'                || '''',''''',
'                || pa.aka_name',
'                || ''''">''''',
'                || ''''UnLink''''',
'                || ''''</a>'''')',
'                Action,',
'                pp.CREATED_BY CREATED_BY,',
'                pp.CREATION_DATE CREATION_DATE,',
'                pp.LAST_UPDATED_BY LAST_UPDATED_BY,',
'                pp.LAST_UPDATE_DATE LAST_UPDATE_DATE,',
'                ''''N'''',',
'                ''''NOT_DELETED''''',
'    FROM prop_performers ppr,',
'      prop_recordings pr,',
'      prop_properties pp,',
'      prop_songs ps,',
'      prop_sources src,',
'      (SELECT * ',
'      FROM prop_akas pa ',
'      WHERE aka_type_id in (select pat.aka_type_id from prop_aka_types pat where upper(pat.aka_type_code)=''''BDS_ENCORE'''')',
'      ) pa,',
'      prop_collection_records pcr,',
'      prop_collections pc',
'    WHERE pr.property_id      = pp.property_id',
'    and ps.property_id        = '' || :P310_PROP_ID ||',
'   ''AND ps.song_id            = pr.song_id',
'    AND pr.property_id       = ppr.property_id(+)',
'    AND pp.source_property_id	 = '' || :P310_PROP_ID ||',
'    '' AND pp.property_type_id   =  2',
'     AND pp.source_id = '' || :P310_PROP_SOURCE_ID ||',
'   '' AND pp.status = ''''A''''',
'    AND ppr.property_id (+)      = pp.property_id',
'    AND src.source_id(+)         = pp.source_id',
'   /*AND (pa.aka_type_id  is null or pa.aka_type_id = (select pat.aka_type_id from prop_aka_types pat where upper(pat.aka_type_code)=''''BDS_ENCORE''''))',
'   Commented by Saurav on 21-APR-2020',
'   */',
'    AND pa.property_id(+)        = pr.property_id',
'    AND pr.recording_id = pcr.recording_id(+)',
'    AND pcr.collection_id = pc.collection_id(+)   and  '' || '''''''' || :P310_QUERY_REC || '''''''' || ''  =''''Y''''''; ',
'',
' ',
'    APEX_COLLECTION.CREATE_COLLECTION_FROM_QUERY( p_collection_name => ''P310_RECORDINGS_COLL'',',
'                                                  p_query => v_rec_coll );',
'       exception when others then',
'            null;',
'            end;'))
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(22934725767627711)
,p_event_id=>wwv_flow_api.id(22934689382627710)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(11643894853815328)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(23240848330825310)
,p_name=>'REFRESH_COMMERCIALS'
,p_event_sequence=>180
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(64369162137550724)
,p_bind_type=>'bind'
,p_bind_event_type=>'apexafterclosedialog'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(23240975155825311)
,p_event_id=>wwv_flow_api.id(23240848330825310)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(64369162137550724)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(22935084027627714)
,p_name=>'REFRESH_NOTES'
,p_event_sequence=>190
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(64376707028550730)
,p_bind_type=>'bind'
,p_bind_event_type=>'apexafterclosedialog'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(22935178178627715)
,p_event_id=>wwv_flow_api.id(22935084027627714)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(64376707028550730)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(26556262959540438)
,p_name=>'REFRESH_PUBLISHER_WRITER_RELATIONS'
,p_event_sequence=>200
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(26554449124540420)
,p_bind_type=>'bind'
,p_bind_event_type=>'apexafterclosedialog'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(26556370240540439)
,p_event_id=>wwv_flow_api.id(26556262959540438)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(26554449124540420)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(28317312491272609)
,p_name=>'REFRESH_PROP_HEADER'
,p_event_sequence=>210
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(64411169385550755)
,p_bind_type=>'bind'
,p_bind_event_type=>'apexafterclosedialog'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(28317420934272610)
,p_event_id=>wwv_flow_api.id(28317312491272609)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(64411169385550755)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(28318357175272619)
,p_name=>'VIEW_SCANNED_DOCUMNETS'
,p_event_sequence=>220
,p_triggering_element_type=>'BUTTON'
,p_triggering_button_id=>wwv_flow_api.id(33362707080353083)
,p_bind_type=>'bind'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(28318472619272620)
,p_event_id=>wwv_flow_api.id(28318357175272619)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
'//window.open(''http://aotcstapw.sesac.com/livelink/livelink.exe?func=search&where1="Attr_442013_2":"'' + $v("P310_PROP_NUMBER") + ''"'', ''_blank'');',
'',
'var appItemVal = ''&APP_OPENTXT_PROP_URL.'';',
'window.open(appItemVal + ''":"'' + $v("P310_PROP_NUMBER") + ''"'', ''_blank'');',
''))
,p_stop_execution_on_error=>'Y'
);
end;
/
begin
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(34188716617070730)
,p_name=>'REFRES_PSO'
,p_event_sequence=>230
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(64357996627550715)
,p_bind_type=>'bind'
,p_bind_event_type=>'apexafterclosedialog'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(34188869022070731)
,p_event_id=>wwv_flow_api.id(34188716617070730)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(64357996627550715)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(21091955365189708)
,p_name=>'REFRESH_CHANG_HIST'
,p_event_sequence=>240
,p_bind_type=>'live'
,p_bind_event_type=>'ready'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(21092028286189709)
,p_event_id=>wwv_flow_api.id(21091955365189708)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
'$(''#HIST_tab a'').click(function(){',
'  apex.region( "HIST" ).refresh();',
' });'))
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(23995091612728015)
,p_name=>'SET REPORT TOTAL TEXT'
,p_event_sequence=>250
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(64357996627550715)
,p_bind_type=>'bind'
,p_bind_event_type=>'apexafterrefresh'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(23995160518728016)
,p_event_id=>wwv_flow_api.id(23995091612728015)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
'$("td:empty[headers*=''ownr_ip'']").each(function(){',
'$(this).text(''Report Total:'');',
'});',
'',
'$("td:empty[headers=''ownr_ip'']").each(function(){',
'$(this).text(''Report Total:'');',
'});'))
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(11646557489834305)
,p_name=>'SelectunSelectCheckbox_recordings'
,p_event_sequence=>260
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'#selectunselectall'
,p_bind_type=>'live'
,p_bind_delegate_to_selector=>'#REC'
,p_bind_event_type=>'change'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(11646675085834306)
,p_event_id=>wwv_flow_api.id(11646557489834305)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
'if ($( ''#REC #selectunselectall'' ).is('':checked'')) {',
'      $(''#REC input[type=checkbox][name=f05]'').prop(''checked'',true);',
'        var ajaxRequest = new htmldb_Get(null,&APP_ID.,''APPLICATION_PROCESS=DEL_REC'',310);',
'        ajaxRequest.addParam(''x03'',1); // Insert all values',
'        ajaxRequest.addParam(''x04'',''2'');',
'        gReturn = ajaxRequest.get();',
'        ajaxRequest = null;',
'    } else {',
'      $(''#REC input[type=checkbox][name=f05]'').prop(''checked'',false);',
'        var ajaxRequest = new htmldb_Get(null,&APP_ID.,''APPLICATION_PROCESS=DEL_REC'',310);',
'        ajaxRequest.addParam(''x03'',1); // Insert all values',
'        ajaxRequest.addParam(''x04'',''3'');',
'        gReturn = ajaxRequest.get();',
'        ajaxRequest = null;',
'      ',
'    }',
'  '))
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(39621426871105071)
,p_process_sequence=>80
,p_process_point=>'AFTER_FOOTER'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'Delete report data'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DELETE FROM spi_song_regist_report_stg WHERE session_id = V(''APP_SESSION'');',
'DELETE FROM spi_song_regist_details_stg WHERE session_id = V(''APP_SESSION'');'))
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(39617016385963276)
,p_process_sequence=>50
,p_process_point=>'AFTER_HEADER'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'Populate staging wpl owners'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DELETE FROM spi_song_regist_details_stg WHERE session_id = V(''APP_SESSION'');',
'INSERT INTO spi_song_regist_details_stg',
'SELECT x.*, V(''APP_SESSION'') FROM(',
'WITH writers AS',
'	(',
'    SELECT pir.publisher_ip_id',
'    ,wri.* ',
'    FROM',
'    (SELECT ',
'		ainv.ip_id ip_id_wri,',
'		ainv.full_name full_name_wri,',
'		ainv.ip_number ip_number_wri,',
'		ainv.ipi_base_num ipi_base_num_wri,',
'		ainv.ipi_name_number ipi_name_number_wri,',
'		psc.role_id role_id_wri,',
'		psc.right_type_id right_type_id_wri,',
'		psc.collector_id collector_id_wri,',
'		psc.property_id property_id_wri,',
'		psc.share_percentage share_percentage_wri,',
'		psc.affiliated_society_id affiliated_society_id_wri,',
'        NVL2(rep.aff_category_hfa, rep.aff_category||''<HR>''||rep.aff_category_hfa, rep.aff_category)  derived_status_wri,',
'		spi_screen_utility.membership_society(psc.share_collector_id) current_society_code_wri,',
'		DECODE((SELECT SUMMARY_ROLE_CODE FROM aff_roles  WHERE  aff_roles.ROLE_ID=psc.ROLE_ID),''P'',''Publisher'',''W'',''Writer'',psc.ROLE_ID) Role_Type_wri,',
'		(SELECT ROLE_CODE FROM aff_roles  WHERE  aff_roles.ROLE_ID=psc.ROLE_ID) ROLE_CODE_wri,',
'		(SELECT ROLE_DESC FROM aff_roles  WHERE  aff_roles.ROLE_ID=psc.ROLE_ID) ROLE_DESC_wri,',
'		(select RIGHT_TYPE from aff_right_types where  aff_right_types.RIGHT_TYPE_ID = psc.RIGHT_TYPE_ID) RIGHT_TYPE_wri,',
'		(select RIGHT_DESCRIPTION from aff_right_types where  aff_right_types.RIGHT_TYPE_ID = psc.RIGHT_TYPE_ID) RIGHT_TYPE_Desc_wri,',
'		aff_ip.Get_Current_IPI_Society(psc.COLLECTOR_ID) ipi_society_code_wri,',
'        ''    '' AS blak_wri',
'    FROM prop_properties pp,',
'		prop_share_collectors psc,',
'		(SELECT ip.IP_ID,',
'				full_name,',
'				ip.IP_NUMBER,',
'				ip.IPI_BASE_NUMBER ipi_base_num,',
'				ain.name_number ipi_name_number,',
'				ip.group_number',
'			  FROM aff_interested_parties ip,',
'				aff_ip_names ain,',
'				aff_name_types nt',
'			  WHERE ain.ip_id               = ip.ip_id',
'			  AND nt.name_Type_id           = ain.name_type_id',
'			  AND ain.name_type_id          = 6',
'			  ) ainv,',
'       aff_societies afs,',
'       apcts.rep_affiliates rep',
'    WHERE psc.property_id                            = pp.property_id',
'    AND psc.right_type_id = 1 ',
'    AND psc.role_id IN(SELECT role_id FROM aff_roles WHERE summary_role_code = ''W'')',
'    AND psc.affiliated_society_id = afs.society_id(+)',
'    AND pp.property_id                               = :P310_PROP_ID',
'    AND ainv.ip_id                                   = psc.COLLECTOR_ID',
'    AND ainv.ip_id                                   = rep.ip_id(+)',
'    AND TRUNC(nvl(START_DATE_ACTIVE, sysdate))<=TRUNC(SYSDATE)',
'    AND NVL(psc.end_date_active,TRUNC(sysdate + 1)) >= TRUNC(sysdate +1)',
'    AND     psc.share_percentage >= 0) wri,',
'    Prop_Ip_Relations pir',
'    WHERE pir.writer_ip_id(+) = wri.collector_id_wri',
'    AND pir.property_id(+) = wri.property_id_wri',
'/*    AND pir.publisher_ip_id IN(SELECT psc1.collector_id ',
'                                FROM prop_share_collectors psc1',
'                                WHERE psc1.property_id = :P310_PROP_ID ',
'                                AND psc1.right_type_id = 1 ',
'                                AND psc1.role_id IN(SELECT role_id FROM aff_roles WHERE summary_role_code = ''P'')',
'                                AND psc1.end_date_active IS NULL) */',
'    ),',
'publishers AS    ',
'   (SELECT ',
'		ainv.ip_id ip_id_pub,',
'		ainv.full_name full_name_pub,',
'		ainv.ip_number ip_number_pub,',
'		ainv.ipi_base_num ipi_base_num_pub,',
'		ainv.ipi_name_number ipi_name_number_pub,',
'		psc.role_id role_id_pub,',
'		psc.right_type_id right_type_id_pub,',
'		psc.collector_id collector_id_pub,',
'		psc.property_id property_id_pub,',
'		psc.share_percentage share_percentage_pub,',
'		psc.affiliated_society_id affiliated_society_id_pub,',
'        NVL2(rep.aff_category_hfa, rep.aff_category||''<HR>''||rep.aff_category_hfa, rep.aff_category)  derived_status_pub,',
'		spi_screen_utility.membership_society(psc.share_collector_id) current_society_code_pub,',
'		DECODE((SELECT SUMMARY_ROLE_CODE FROM aff_roles  WHERE  aff_roles.ROLE_ID=psc.ROLE_ID),''P'',''Publisher'',''W'',''Writer'',psc.ROLE_ID) Role_Type_pub,',
'		(SELECT ROLE_CODE FROM aff_roles  WHERE  aff_roles.ROLE_ID=psc.ROLE_ID) ROLE_CODE_pub,',
'		(SELECT ROLE_DESC FROM aff_roles  WHERE  aff_roles.ROLE_ID=psc.ROLE_ID) ROLE_DESC_pub,',
'		(select RIGHT_TYPE from aff_right_types where  aff_right_types.RIGHT_TYPE_ID = psc.RIGHT_TYPE_ID) RIGHT_TYPE_pub,',
'		(select RIGHT_DESCRIPTION from aff_right_types where  aff_right_types.RIGHT_TYPE_ID = psc.RIGHT_TYPE_ID) RIGHT_TYPE_Desc_pub,',
'		aff_ip.Get_Current_IPI_Society(psc.COLLECTOR_ID) ipi_society_code_pub,',
'        '' '' AS blak_pub',
'	FROM prop_properties pp,',
'		prop_share_collectors psc,',
'	  (SELECT   ip.IP_ID,',
'				full_name,',
'				ip.IP_NUMBER,',
'				ip.IPI_BASE_NUMBER ipi_base_num,',
'				ain.name_number ipi_name_number,',
'				ip.group_number',
'			  FROM aff_interested_parties ip,',
'				aff_ip_names ain,',
'				aff_name_types nt',
'			  WHERE ain.ip_id               = ip.ip_id',
'			  AND nt.name_Type_id           = ain.name_type_id',
'			  AND ain.name_type_id          = 6',
'	  ) ainv,',
'    aff_societies afs,',
'    apcts.rep_affiliates rep',
'	WHERE psc.property_id                            = pp.property_id',
'	AND psc.right_type_id = 1 ',
'	AND psc.role_id IN(SELECT role_id FROM aff_roles WHERE summary_role_code = ''P'')',
'	AND psc.affiliated_society_id = afs.society_id(+)',
'	AND pp.property_id                               = :P310_PROP_ID',
'	AND ainv.ip_id                                   = psc.COLLECTOR_ID',
'	AND ainv.ip_id                                   = rep.ip_id(+)',
'	AND TRUNC(nvl(START_DATE_ACTIVE, sysdate))<=TRUNC(SYSDATE)',
'	AND NVL(psc.end_date_active,TRUNC(sysdate + 1)) >= TRUNC(sysdate +1)',
'	AND     psc.share_percentage >= 0) ',
'SELECT * ',
'FROM writers FULL OUTER JOIN publishers',
'ON writers.publisher_ip_id = publishers.ip_id_pub',
'--ORDER BY ip_id_wri, ip_id_pub',
') x;'))
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(39617396530964382)
,p_process_sequence=>60
,p_process_point=>'AFTER_HEADER'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'Generate report data wpl ownership'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'    m_wri		CLOB;',
'	m_pub		CLOB;',
'    m_type  	VARCHAR2(5);',
'	m_rows		CLOB;',
'    m_wri_ips   VARCHAR2(4000); ',
'    m_pub_ips   VARCHAR2(4000); ',
'	f_session_id NUMBER := V(''APP_SESSION'');',
'	m_wri_count  NUMBER := 0;',
'	m_pub_count  NUMBER := 0;',
'    m_count      NUMBER;',
'BEGIN',
'    DEBUG_WRITE(''START'', ''PG144''); -- Sachin',
'    DELETE FROM spi_song_regist_report_stg WHERE session_id = f_session_id;',
'	m_wri := ''<div class="divTable" style="border: 1px solid #000;">',
'			<div class="divTableBody">',
'			<div class="divTableRow">',
'			<div class="divTableCell" style="color: #337ac0"><b>IP Number</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Name</b></div>',
'            <div class="divTableCell" style="color: #337ac0"><b>Role Type</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>IPI Base Number</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>IPI Name Number</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Derived Status</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Role Code</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Share %</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Membership Society Code</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>IPI Society Code</b></div>',
'			</div>''; ',
'	m_pub := ''<div class="divTable" style="border: 1px solid #000;">',
'			<div class="divTableBody">',
'			<div class="divTableRow">',
'			<div class="divTableCell" style="color: #337ac0"><b>IP Number</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Name</b></div>',
'            <div class="divTableCell" style="color: #337ac0"><b>Role Type</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>IPI Base Number</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>IPI Name Number</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Derived Status</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Role Code</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Share %</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Membership Society Code</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>IPI Society Code</b></div>',
'			</div>''; 	',
'    BEGIN',
'        SELECT type INTO m_type',
'                    FROM(SELECT * FROM(SELECT ip_id_wri ip_id, ''WRI'' type, COUNT(*) cnt FROM spi_song_regist_details_stg WHERE session_id = f_session_id AND ip_id_wri IS NOT NULL GROUP BY ip_id_wri HAVING COUNT(*) > 1',
'                                      UNION ALL',
'                                      SELECT ip_id_pub ip_id, ''PUB'' type, COUNT(*) cnt FROM spi_song_regist_details_stg WHERE session_id = f_session_id AND ip_id_pub IS NOT NULL GROUP BY ip_id_pub HAVING COUNT(*) > 1)',
'                        ORDER BY type)',
'        WHERE ROWNUM <= 1; ',
'    EXCEPTION WHEN others THEN    ',
'        m_type := ''ALL'';',
'    END;',
'    ',
'    DEBUG_WRITE(''m_type = ''||m_type, ''PG144''); -- Sachin',
'    FOR cur_owners IN(SELECT * FROM(SELECT ip_id_wri ip_id, ''WRI'' type, COUNT(*) cnt FROM spi_song_regist_details_stg WHERE session_id = f_session_id GROUP BY ip_id_wri HAVING COUNT(*) > 1',
'									UNION ALL',
'									SELECT ip_id_pub ip_id, ''PUB'' type, COUNT(*) cnt FROM spi_song_regist_details_stg WHERE session_id = f_session_id GROUP BY ip_id_pub HAVING COUNT(*) > 1)',
'					  WHERE type = m_type',
'                      AND ip_id IS NOT NULL',
'                      ORDER BY cnt DESC)',
'    LOOP',
'        DEBUG_WRITE(''IN OWNERS cur_owners.type = ''||cur_owners.type, ''PG144''); -- Sachin',
'		IF(cur_owners.type = ''WRI'') THEN -- Writers',
'			FOR cur_wri IN(SELECT ip_id_wri,ip_number_wri,full_name_wri,role_type_wri,ipi_base_num_wri,ipi_name_number_wri,derived_status_wri,role_code_wri,share_percentage_wri,current_society_code_wri,ipi_society_code_wri,NULL',
'							FROM spi_song_regist_details_stg',
'							WHERE ip_id_wri = cur_owners.ip_id',
'							AND session_id = f_session_id ',
'							AND ROWNUM <= 1)',
'			LOOP',
'				m_rows := ''',
'			<div class="divTableRow">',
'			<div class="divTableCell">''||cur_wri.ip_number_wri||''</div>',
'			<div class="divTableCell">''||cur_wri.full_name_wri||''</div>',
'			<div class="divTableCell">''||cur_wri.role_type_wri||''</div>',
'			<div class="divTableCell">''||cur_wri.ipi_base_num_wri||''</div>',
'			<div class="divTableCell">''||cur_wri.ipi_name_number_wri||''</div>',
'			<div class="divTableCell">''||cur_wri.derived_status_wri||''</div>',
'			<div class="divTableCell">''||cur_wri.role_code_wri||''</div>',
'			<div class="divTableCell">''||cur_wri.share_percentage_wri||''</div>',
'			<div class="divTableCell">''||cur_wri.current_society_code_wri||''</div>',
'			<div class="divTableCell">''||cur_wri.ipi_society_code_wri||''</div>',
'			</div>'';',
'			END LOOP;',
'',
'			m_wri := m_wri || m_rows;',
'			',
'			FOR cur_pub IN(SELECT ip_id_pub,ip_number_pub,full_name_pub,role_type_pub,ipi_base_num_pub,ipi_name_number_pub,REPLACE(derived_status_pub,''<HR>'','' || '') derived_status_pub,role_code_pub,share_percentage_pub,current_society_code_pub,ipi_society_cod'
||'e_pub',
'							FROM spi_song_regist_details_stg',
'							WHERE ip_id_wri = cur_owners.ip_id',
'							AND session_id = f_session_id)',
'			LOOP',
'				m_rows := ''',
'			<div class="divTableRow">',
'			<div class="divTableCell">''||cur_pub.ip_number_pub||''</div>',
'			<div class="divTableCell">''||cur_pub.full_name_pub||''</div>',
'			<div class="divTableCell">''||cur_pub.role_type_pub||''</div>',
'			<div class="divTableCell">''||cur_pub.ipi_base_num_pub||''</div>',
'			<div class="divTableCell">''||cur_pub.ipi_name_number_pub||''</div>',
'			<div class="divTableCell">''||cur_pub.derived_status_pub||''</div>',
'			<div class="divTableCell">''||cur_pub.role_code_pub||''</div>',
'			<div class="divTableCell">''||cur_pub.share_percentage_pub||''</div>',
'			<div class="divTableCell">''||cur_pub.current_society_code_pub||''</div>',
'			<div class="divTableCell">''||cur_pub.ipi_society_code_pub||''</div>',
'			</div>'';',
'			',
'				m_pub := m_pub || m_rows;',
'			END LOOP;',
'		ELSE -- Publishers',
'			FOR cur_pub IN(SELECT ip_id_pub,ip_number_pub,full_name_pub,role_type_pub,ipi_base_num_pub,ipi_name_number_pub,REPLACE(derived_status_pub,''<HR>'','' || '') derived_status_pub,role_code_pub,share_percentage_pub,current_society_code_pub,ipi_society_cod'
||'e_pub',
'							FROM spi_song_regist_details_stg',
'							WHERE ip_id_pub = cur_owners.ip_id',
'							AND session_id = f_session_id ',
'							AND ROWNUM <= 1)',
'			LOOP',
'				m_rows := ''',
'			<div class="divTableRow">',
'			<div class="divTableCell">''||cur_pub.ip_number_pub||''</div>',
'			<div class="divTableCell">''||cur_pub.full_name_pub||''</div>',
'			<div class="divTableCell">''||cur_pub.role_type_pub||''</div>',
'			<div class="divTableCell">''||cur_pub.ipi_base_num_pub||''</div>',
'			<div class="divTableCell">''||cur_pub.ipi_name_number_pub||''</div>',
'			<div class="divTableCell">''||cur_pub.derived_status_pub||''</div>',
'			<div class="divTableCell">''||cur_pub.role_code_pub||''</div>',
'			<div class="divTableCell">''||cur_pub.share_percentage_pub||''</div>',
'			<div class="divTableCell">''||cur_pub.current_society_code_pub||''</div>',
'			<div class="divTableCell">''||cur_pub.ipi_society_code_pub||''</div>',
'			</div>'';',
'			END LOOP;',
'',
'			m_pub := m_pub || m_rows;',
'			',
'			FOR cur_wri IN(SELECT ip_id_wri,ip_number_wri,full_name_wri,role_type_wri,ipi_base_num_wri,ipi_name_number_wri,derived_status_wri,role_code_wri,share_percentage_wri,current_society_code_wri,ipi_society_code_wri,NULL',
'							FROM spi_song_regist_details_stg',
'							WHERE ip_id_pub = cur_owners.ip_id',
'							AND session_id = f_session_id)',
'			LOOP',
'			',
'			',
'				m_rows := ''',
'			<div class="divTableRow">',
'			<div class="divTableCell">''||cur_wri.ip_number_wri||''</div>',
'			<div class="divTableCell">''||cur_wri.full_name_wri||''</div>',
'			<div class="divTableCell">''||cur_wri.role_type_wri||''</div>',
'			<div class="divTableCell">''||cur_wri.ipi_base_num_wri||''</div>',
'			<div class="divTableCell">''||cur_wri.ipi_name_number_wri||''</div>',
'			<div class="divTableCell">''||cur_wri.derived_status_wri||''</div>',
'			<div class="divTableCell">''||cur_wri.role_code_wri||''</div>',
'			<div class="divTableCell">''||cur_wri.share_percentage_wri||''</div>',
'			<div class="divTableCell">''||cur_wri.current_society_code_wri||''</div>',
'			<div class="divTableCell">''||cur_wri.ipi_society_code_wri||''</div>',
'			</div>'';',
'				m_wri := m_wri || m_rows;',
'			END LOOP;',
'		END IF;',
'		',
'		m_wri := m_wri || ''',
'			</div>',
'			</div>'';',
'		m_pub := m_pub || ''',
'			</div>',
'			</div>'';	',
'	',
'		INSERT INTO spi_song_regist_report_stg(writer_data,publisher_data,session_id,stg_id)',
'		VALUES(m_wri,m_pub,f_session_id,spi_song_reg_s.NEXTVAL);',
'--        DEBUG_WRITE(m_wri, ''PG144''); -- Sachin',
'--        DEBUG_WRITE(m_pub, ''PG144''); -- Sachin',
'        ',
'        SELECT COUNT(*) INTO m_count FROM spi_song_regist_report_stg;',
'        DEBUG_WRITE(''m_count = ''||m_count, ''PG144''); -- Sachin',
'        ',
'    m_wri := ''<div class="divTable" style="border: 1px solid #000;">',
'			<div class="divTableBody">',
'			<div class="divTableRow">',
'			<div class="divTableCell" style="color: #337ac0"><b>IP Number</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Name</b></div>',
'            <div class="divTableCell" style="color: #337ac0"><b>Role Type</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>IPI Base Number</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>IPI Name Number</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Derived Status</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Role Code</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Share %</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Membership Society Code</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>IPI Society Code</b></div>',
'			</div>''; ',
'	m_pub := ''<div class="divTable" style="border: 1px solid #000;">',
'			<div class="divTableBody">',
'			<div class="divTableRow">',
'			<div class="divTableCell" style="color: #337ac0"><b>IP Number</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Name</b></div>',
'            <div class="divTableCell" style="color: #337ac0"><b>Role Type</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>IPI Base Number</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>IPI Name Number</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Derived Status</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Role Code</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Share %</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Membership Society Code</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>IPI Society Code</b></div>',
'			</div>''; 	',
'	END LOOP;',
'    ',
'	IF(m_type = ''WRI'' OR m_type = ''ALL'') THEN -- Writers',
'		FOR cur_wri IN(SELECT * FROM spi_song_regist_details_stg WHERE ip_id_wri IN(',
'                        SELECT ip_id_wri FROM spi_song_regist_details_stg WHERE session_id = f_session_id AND ip_id_wri IS NOT NULL GROUP BY ip_id_wri HAVING COUNT(*) <= 1)',
'                       AND session_id = f_session_id)',
'		LOOP',
'            m_wri_ips := cur_wri.ip_id_wri;',
'			m_rows := ''',
'			<div class="divTableRow">',
'			<div class="divTableCell">''||cur_wri.ip_number_wri||''</div>',
'			<div class="divTableCell">''||cur_wri.full_name_wri||''</div>',
'			<div class="divTableCell">''||cur_wri.role_type_wri||''</div>',
'			<div class="divTableCell">''||cur_wri.ipi_base_num_wri||''</div>',
'			<div class="divTableCell">''||cur_wri.ipi_name_number_wri||''</div>',
'			<div class="divTableCell">''||cur_wri.derived_status_wri||''</div>',
'			<div class="divTableCell">''||cur_wri.role_code_wri||''</div>',
'			<div class="divTableCell">''||cur_wri.share_percentage_wri||''</div>',
'			<div class="divTableCell">''||cur_wri.current_society_code_wri||''</div>',
'			<div class="divTableCell">''||cur_wri.ipi_society_code_wri||''</div>',
'			</div>'';',
'			m_wri := m_wri || m_rows;',
'			m_wri := m_wri || ''',
'			</div>',
'			</div>'';',
'			',
'			IF(cur_wri.ip_number_pub IS NOT NULL) THEN',
'                m_pub_ips := cur_wri.ip_id_pub;',
'				m_rows := ''',
'			<div class="divTableRow">',
'			<div class="divTableCell">''||cur_wri.ip_number_pub||''</div>',
'			<div class="divTableCell">''||cur_wri.full_name_pub||''</div>',
'			<div class="divTableCell">''||cur_wri.role_type_pub||''</div>',
'			<div class="divTableCell">''||cur_wri.ipi_base_num_pub||''</div>',
'			<div class="divTableCell">''||cur_wri.ipi_name_number_pub||''</div>',
'			<div class="divTableCell">''||cur_wri.derived_status_pub||''</div>',
'			<div class="divTableCell">''||cur_wri.role_code_pub||''</div>',
'			<div class="divTableCell">''||cur_wri.share_percentage_pub||''</div>',
'			<div class="divTableCell">''||cur_wri.current_society_code_pub||''</div>',
'			<div class="divTableCell">''||cur_wri.ipi_society_code_pub||''</div>',
'			</div>'';',
'			m_pub := m_pub || m_rows;',
'			m_pub := m_pub || ''',
'			</div>',
'			</div>'';',
'			ELSE',
'				m_pub := NULL;',
'			END IF;',
'			',
'			INSERT INTO spi_song_regist_report_stg(writer_ip_ids,publisher_ip_ids,writer_data,publisher_data,session_id,stg_id)',
'			VALUES(m_wri_ips,m_pub_ips,m_wri,m_pub,f_session_id,spi_song_reg_s.NEXTVAL);',
'            ',
'            m_wri_ips := NULL;',
'            m_pub_ips := NULL;',
'			',
'			m_wri := ''<div class="divTable" style="border: 1px solid #000;">',
'			<div class="divTableBody">',
'			<div class="divTableRow">',
'			<div class="divTableCell" style="color: #337ac0"><b>IP Number</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Name</b></div>',
'            <div class="divTableCell" style="color: #337ac0"><b>Role Type</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>IPI Base Number</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>IPI Name Number</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Derived Status</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Role Code</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Share %</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Membership Society Code</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>IPI Society Code</b></div>',
'			</div>''; ',
'			m_pub := ''<div class="divTable" style="border: 1px solid #000;">',
'			<div class="divTableBody">',
'			<div class="divTableRow">',
'			<div class="divTableCell" style="color: #337ac0"><b>IP Number</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Name</b></div>',
'            <div class="divTableCell" style="color: #337ac0"><b>Role Type</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>IPI Base Number</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>IPI Name Number</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Derived Status</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Role Code</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Share %</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Membership Society Code</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>IPI Society Code</b></div>',
'			</div>'';',
'		END LOOP;',
'    END IF;    ',
'	IF((m_type = ''PUB'' OR m_type = ''ALL'')) THEN',
'		FOR cur_pub IN(SELECT * FROM spi_song_regist_details_stg WHERE ip_id_pub IN(',
'                        SELECT ip_id_pub FROM spi_song_regist_details_stg WHERE session_id = f_session_id AND ip_id_pub IS NOT NULL GROUP BY ip_id_pub HAVING COUNT(*) <= 1)',
'                       AND session_id = f_session_id)',
'		LOOP',
'			IF(cur_pub.ip_number_wri IS NOT NULL) THEN',
'                m_wri_ips := cur_pub.ip_id_wri;',
'				m_rows := ''',
'			<div class="divTableRow">',
'			<div class="divTableCell">''||cur_pub.ip_number_wri||''</div>',
'			<div class="divTableCell">''||cur_pub.full_name_wri||''</div>',
'			<div class="divTableCell">''||cur_pub.role_type_wri||''</div>',
'			<div class="divTableCell">''||cur_pub.ipi_base_num_wri||''</div>',
'			<div class="divTableCell">''||cur_pub.ipi_name_number_wri||''</div>',
'			<div class="divTableCell">''||cur_pub.derived_status_wri||''</div>',
'			<div class="divTableCell">''||cur_pub.role_code_wri||''</div>',
'			<div class="divTableCell">''||cur_pub.share_percentage_wri||''</div>',
'			<div class="divTableCell">''||cur_pub.current_society_code_wri||''</div>',
'			<div class="divTableCell">''||cur_pub.ipi_society_code_wri||''</div>',
'			</div>'';',
'			m_wri := m_wri || m_rows;',
'			m_wri := m_wri || ''',
'			</div>',
'			</div>'';',
'			ELSE',
'				m_wri := NULL;',
'			END IF;',
'			',
'            m_pub_ips := cur_pub.ip_id_pub;',
'			m_rows := ''',
'			<div class="divTableRow">',
'			<div class="divTableCell">''||cur_pub.ip_number_pub||''</div>',
'			<div class="divTableCell">''||cur_pub.full_name_pub||''</div>',
'			<div class="divTableCell">''||cur_pub.role_type_pub||''</div>',
'			<div class="divTableCell">''||cur_pub.ipi_base_num_pub||''</div>',
'			<div class="divTableCell">''||cur_pub.ipi_name_number_pub||''</div>',
'			<div class="divTableCell">''||cur_pub.derived_status_pub||''</div>',
'			<div class="divTableCell">''||cur_pub.role_code_pub||''</div>',
'			<div class="divTableCell">''||cur_pub.share_percentage_pub||''</div>',
'			<div class="divTableCell">''||cur_pub.current_society_code_pub||''</div>',
'			<div class="divTableCell">''||cur_pub.ipi_society_code_pub||''</div>',
'			</div>'';',
'			m_pub := m_pub || m_rows;',
'			m_pub := m_pub || ''',
'			</div>',
'			</div>'';',
'            ',
'			INSERT INTO spi_song_regist_report_stg(writer_ip_ids,publisher_ip_ids,writer_data,publisher_data,session_id,stg_id)',
'			VALUES(m_wri_ips,m_pub_ips,m_wri,m_pub,f_session_id,spi_song_reg_s.NEXTVAL);',
'			',
'            m_wri_ips := NULL;',
'            m_pub_ips := NULL;',
'            ',
'			m_wri := ''<div class="divTable" style="border: 1px solid #000;">',
'			<div class="divTableBody">',
'			<div class="divTableRow">',
'			<div class="divTableCell" style="color: #337ac0"><b>IP Number</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Name</b></div>',
'            <div class="divTableCell" style="color: #337ac0"><b>Role Type</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>IPI Base Number</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>IPI Name Number</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Derived Status</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Role Code</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Share %</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Membership Society Code</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>IPI Society Code</b></div>',
'			</div>''; ',
'			m_pub := ''<div class="divTable" style="border: 1px solid #000;">',
'			<div class="divTableBody">',
'			<div class="divTableRow">',
'			<div class="divTableCell" style="color: #337ac0"><b>IP Number</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Name</b></div>',
'            <div class="divTableCell" style="color: #337ac0"><b>Role Type</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>IPI Base Number</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>IPI Name Number</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Derived Status</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Role Code</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Share %</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>Membership Society Code</b></div>',
'			<div class="divTableCell" style="color: #337ac0"><b>IPI Society Code</b></div>',
'			</div>'';',
'		END LOOP;		',
'	END IF;',
'',
'    DELETE FROM spi_song_regist_report_stg stg1',
'    WHERE stg1.publisher_ip_ids IS NOT NULL',
'    AND stg1.writer_ip_ids IS NOT NULL',
'    AND stg_id NOT IN(SELECT MIN(stg_id)',
'                        FROM spi_song_regist_report_stg stg2',
'                        WHERE stg1.writer_ip_ids = stg2.writer_ip_ids',
'                        AND stg1.publisher_ip_ids = stg2.publisher_ip_ids',
'                        AND stg2.publisher_ip_ids IS NOT NULL',
'                        AND stg2.writer_ip_ids IS NOT NULL',
'                        AND stg2.session_id = stg1.session_id);',
'                        ',
'    SELECT COUNT(*) INTO m_count FROM spi_song_regist_report_stg;',
'    DEBUG_WRITE(''m_count2 = ''||m_count, ''PG144''); -- Sachin                     ',
'END;'))
,p_process_error_message=>'Error'
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
);
end;
/
begin
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(11646813249834308)
,p_process_sequence=>10
,p_process_point=>'AFTER_SUBMIT'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'Delete_Recordings'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
' v_rec_property_id NUMBER;',
' v_coll_count NUMBER;',
'BEGIN',
'',
' for j in (select seq_id,c001 from apex_collections',
'                                       where collection_name = ''P310_RECORDINGS_COLL''',
'                                           and c023 = ''Y''',
'                                           and c002 = :P310_PROP_ID)loop',
'   SELECT property_id ',
'              INTO v_rec_property_id ',
'              FROM prop_recordings',
'             WHERE recording_id =  j.c001;',
'                                           ',
'',
'            UPDATE prop_properties ',
'            SET status = ''I''',
'            ,   last_updated_by = v(''APP_USER'')',
'            ,   last_update_date = SYSDATE',
'            WHERE property_id = v_rec_property_id ',
'            AND property_type_id=2;',
' ',
' apex_collection.UPDATE_MEMBER_ATTRIBUTE(''P310_RECORDINGS_COLL'',p_attr_number => ''24'', p_attr_value => ''DELETED'',p_seq => j.seq_id);',
'END loop;',
'',
'commit;',
'exception WHEN no_data_found THEN',
'null;',
'end;',
''))
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
,p_process_when_button_id=>wwv_flow_api.id(11646946572834309)
,p_process_when=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select c001 from apex_collections',
'                           where collection_name = ''P310_RECORDINGS_COLL''',
'                               and c023 = ''Y''',
'                               and c002 = :P310_PROP_ID;'))
,p_process_when_type=>'EXISTS'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(64497475721550822)
,p_process_sequence=>5
,p_process_point=>'BEFORE_BOX_BODY'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'Link Recording to BDS Song'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'declare',
'v_cnt NUMBER := 0;',
'begin',
'--htp.p(''Process Run-310'');',
'',
' IF :P310_BDS_SONG_ID IS NOT NULL and :P310_REC_PROPERTY_ID is not null THEN',
'--htp.p(''Test1'');',
'	BEGIN',
'		SELECT 1',
'		INTO v_cnt',
'		FROM prop_akas',
'		WHERE aka_name = to_char(:P310_BDS_SONG_ID)',
'                     AND property_id = :P310_REC_PROPERTY_ID',
'                     and aka_type_id = 3 /* BDS_Encore */',
'                      AND rownum = 1;',
'	EXCEPTION',
'		WHEN others THEN',
'		v_cnt := 0;',
'	END;',
'	IF NVL(v_cnt,0) = 0 THEN',
'',
'		BEGIN',
'			 INSERT INTO prop_akas',
'			 (aka_id          ,',
'			source_id       ,',
'			aka_name        ,',
'			aka_type_id     ,',
'			property_id     ,',
'			created_by      ,',
'			creation_date   ,',
'			last_updated_by ,',
'			last_update_date)',
'			VALUES',
'			(prop_aka_s.nextval,',
'			7,',
'			to_char(:P310_BDS_SONG_ID),',
'			3,',
'			:P310_REC_PROPERTY_ID,',
'			v(''APP_USER''),',
'			SYSDATE,',
'			v(''APP_USER''),',
'			SYSDATE);',
'			COMMIT;',
'		EXCEPTION',
'			WHEN others THEN',
'			NULL;',
'		END;',
'	END IF;',
'	',
'	SELECT source_property_id ',
'	INTO :P310_PROP_ID',
'	FROM prop_properties',
'	WHERE property_id =  :P310_REC_PROPERTY_ID;',
' END IF;',
'',
'end;'))
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(64496617689550822)
,p_process_sequence=>10
,p_process_point=>'BEFORE_BOX_BODY'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'Populate IPI Data'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'begin',
'select FOUNDATION_BIRTH_PLACE, FOUNDATION_BIRTH_STATE',
', to_char(to_date(to_char(AMMENDMENT_DATE,''DD-MON-YYYY'') ||'' ''||AMMENDMENT_TIME,''DD-MON-YYYY HH24MISS''),''MM/DD/YYYY HH24:MI:SS'') ammend_datetime',
', trim(itnt.TISAN) territory, iip.interested_party_id',
'into',
' :P310_IPI_BIRTH_PLACE, :P310_IPI_BIRTH_STATE',
', :P310_IPI_ammend_date',
', :P310_IPI_territory, :P310_interested_party_id',
'from IPI_INTERESTED_PARTY iip, IPI_TERRITORY_NAMES_TISN itnt',
'where itnt.territory_names_tisn_id(+) = iip.territory_names_tisn_id',
'and iip.base_number = nvl(:P310_IPI_BASE_NUMBER,0)',
';',
'exception when OTHERS then ',
'null;',
'end;'))
,p_process_when_type=>'NEVER'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(64497074712550822)
,p_process_sequence=>10
,p_process_point=>'BEFORE_BOX_BODY'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'Populate Data'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'BEGIN',
'    IF NVL(:P310_PROP_ID,0) <> 0 THEN',
'        SELECT ',
'            property_number, ',
'            title, ',
'            decode(status ,''A'',''Active'',''I'',''Inactive'') , ',
'            iswc, ',
'            copyright_reg_number,',
'            spi_screen_utility.get_right_type(:P310_PROP_ID,''PERF_RIGHT_FLAG'') Perf_right_flag,',
'            spi_screen_utility.get_right_type(:P310_PROP_ID,''SYNC_RIGHT_FLAG'') Sync_right_flag,',
'            spi_screen_utility.get_right_type(:P310_PROP_ID,''MECH_RIGHT_FLAG'') Mech_right_flag,',
'            ''N'' Grand_right_flag,',
'            LPAD( duration,6,''0'')duration',
'            , NVL(public_domain_flag,''N''), entry_date, hold_type_id, pp.CREATION_DATE,',
'            NVL(PUBLIC_DOMAIN_FLAG,''N''),',
'            (',
'                SELECT warning_code||''-->''||warning_description ',
'                FROM prop_song_warning_lookup ',
'                WHERE prop_song_warning_lookup.warning_id=sw.warning_id',
'            ) warning_code,',
'            first_use_refusal,',
'            --warning_effective_date,',
'            sw.warning_effective_dt,    ---Anuj : 13-APR-2020 for Statutory Termination (MSPI-90)',
'			pp.source_id,',
'            sng.copyright_date,',
'            pp.language_id',
'        INTO ',
'            :P310_PROP_NUMBER, ',
'            :P310_PROP_NAME, ',
'            :P310_STATUS,',
'            :P310_ISWC_NUM,',
'            :P310_COPYRIGHT_NUM,',
'            :P310_PERF_RIGHT_FLAG,',
'            :P310_SYNC_RIGHT_FLAG,',
'            :P310_MECH_RIGHT_FLAG,',
'            :P310_GRAND_RIGHT_FLAG,',
'            :P310_DURATION, ',
'            :P310_PUBLIC_DOMAIN_FLAG, ',
'            :P310_ENTRY_DATE, ',
'            :P310_HOLD_REASON_ID, ',
'            :P310_CREATION_DATE,',
'            :P310_PUBLIC_DOMAIN_FLAG2,',
'            :P310_WARNING_CODE,',
'            :P310_FIRST_USE_REFUSAL,',
'            :P310_WARNING_DATE,',
'			:P310_PROP_SOURCE_ID,',
'            :P310_COPYRIGHT_DATE,',
'            :P310_LANGUAGE_CODE',
'        FROM prop_properties pp, prop_songs sng,(SELECT * from mc_song_warnings WHERE END_DATE IS NULL) sw',
'        WHERE pp.property_id = :P310_PROP_ID',
'        AND pp.property_id = sng.property_id(+)',
'        AND pp.property_id = sw.property_id(+);',
'',
'    /*  SELECT COUNT(1) ',
'        INTO  :P310_MEC_LICENSE_COUNT',
'        FROM prop_akas aka ',
'        JOIN mc_licenses mcl ON ( mcl.song_code = aka.aka_name )  ',
'        WHERE aka.aka_type_id = 51  -- HFA song code',
'        AND aka.property_id =  :P310_PROP_ID; */',
'        ',
'        SELECT spi_screen_utility.get_MEC_licensee_count(:P310_PROP_ID)',
'        INTO  :P310_MEC_LICENSE_COUNT  ',
'        FROM dual;',
'          ',
'        BEGIN',
'            SELECT aka_name',
'            INTO  :p310_hfa_song_code',
'            FROM prop_akas aka ',
'            WHERE aka.aka_type_id = 51 ',
'            AND aka.property_id =  :P310_PROP_ID ',
'            AND ROWNUM=1;',
'        EXCEPTION',
'            WHEN OTHERS THEN',
'                :P310_HFA_SONG_CODE:=NULL;',
'        END;',
'',
'		-------------------------',
'		-- Fetch Source Name',
'		------------------------',
'		',
'		SELECT description',
'		INTO :P310_PROP_SOURCE',
'		FROM prop_sources',
'		WHERE source_id = :P310_PROP_SOURCE_ID;',
'		',
'        --delete from PROP_SHARE_COLLECTORS_tmp where session_id = v(''SESSION'');',
'        --prop_property.SHOW_PROP_EARN_PAY(v(''SESSION''), 1, :P310_PROP_ID,1);',
'        ',
'        ----Anuj Kumar : MSPI-90 - Statutory Termination - 09-APR-2020',
'        BEGIN',
'          SELECT ''<a href="f?p=&APP_ID.:310:&APP_SESSION.:::310:P310_PROP_ID,P310_CALLING_PAGE_ID:''||mst.new_derivative_prop_id||'',300">''',
'                ||pa.aka_name',
'                ||''</a>''',
'                ,mst.termination_effective_date',
'            INTO :P310_NEW_DERIVATIVE_PROP_ID',
'                ,:P310_TERMINATION_EFF_DATE',
'            FROM prop_mlc_stat_terminations mst',
'                ,prop_akas pa',
'           WHERE mst.old_derivative_prop_id = :P310_PROP_ID',
'             AND mst.end_date IS NULL',
'             AND pa.aka_type_id             = 51',
'             AND pa.source_id               = 7',
'             AND pa.property_id             = mst.new_derivative_prop_id;',
'',
'          :P310_STATUTORY_TERMINATION := ''Y'';',
'',
'        EXCEPTION',
'          WHEN OTHERS THEN',
'            :P310_NEW_DERIVATIVE_PROP_ID := NULL;',
'            :P310_TERMINATION_EFF_DATE   := NULL;',
'            :P310_STATUTORY_TERMINATION  := ''N'';',
'',
'        END;        ',
'',
'    END IF;',
'NULL;',
'END;'))
,p_process_when=>'P310_PROP_ID'
,p_process_when_type=>'ITEM_IS_NOT_NULL'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(4538872645564123)
,p_process_sequence=>20
,p_process_point=>'BEFORE_BOX_BODY'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'POPULATE_FLAGS'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'/*:P310_QUERY_HISTORY := ''N'';',
':P310_QUERY_AGREEMENTS := ''N'';',
':P310_QUERY_PERMISSIONS := ''N''; */',
'BEGIN',
'if :P310_PROPERTY_TAB=2 then',
'  :P310_QUERY_TITLE:= ''Y''; ',
'end if;',
'',
'if :P310_PROPERTY_TAB=3 then',
'	:P310_QUERY_PUBREL:= ''Y'';',
'end if;',
'',
'if :P310_PROPERTY_TAB=4 then',
'  :P310_QUERY_WORK:= ''Y'';',
'end if;',
'',
'if :P310_PROPERTY_TAB=5 then',
'  :P310_QUERY_AGREEMENTS:= ''Y'';',
'end if;',
'',
'if :P310_PROPERTY_TAB=6 then',
'  :P310_QUERY_PERMISSIONS := ''Y'';',
'end if;',
'',
'if :P310_PROPERTY_TAB=7 then',
'  :P310_QUERY_REG_HISTORY := ''Y'';',
'end if;',
'',
'if :P310_PROPERTY_TAB=8 then',
'  :P310_QUERY_REC:= ''Y'';',
'end if;',
'',
'if :P310_PROPERTY_TAB=9 then',
'  :P310_QUERY_COMM:= ''Y'';',
'end if;',
'',
'if :P310_PROPERTY_TAB=10 then',
'  :P310_QUERY_CUESHEET:= ''Y'';',
'end if;',
'',
'--Proc_debug(''P310_PROPERTY_TAB'' || :P310_PROPERTY_TAB);',
'',
'',
'if :P310_PROPERTY_TAB = 11 then',
' :P310_QUERY_NOTE:= ''Y'';',
'end if; ',
'',
'--Proc_debug(''P310_PROPERTY_TAB'' || :P310_PROPERTY_TAB);',
'',
'',
'if :P310_PROPERTY_TAB = 12 then',
' :P310_QUERY_HISTORY := ''Y'';',
'end if; ',
'',
'if :P310_PROPERTY_TAB = 13 then',
':P310_QUERY_CONSOLIADTED_HOLDS := ''Y'';',
'end if;',
'',
'if :P310_PROPERTY_TAB = 14 then',
':P310_QUERY_VESRIONS := ''Y'';',
'end if;',
'',
'end;',
''))
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(34044397812522836)
,p_process_sequence=>30
,p_process_point=>'BEFORE_BOX_BODY'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'Populate Property Holds'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'BEGIN',
'select 	PHT.DESCRIPTION',
'INTO    :P310_PROPERTY_HOLDS',
'from 	prop_properties PP,pay_hold_types pht ',
'where 	PP.HOLD_TYPE_ID=pht.HOLD_TYPE_ID    ',
'AND      pp.property_type_id =1',
'AND     PROPERTY_ID =:P310_PROP_ID  and source_id = 7;   ',
'EXCEPTION when OTHERS then ',
'null;',
'END;'))
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(11643742627815327)
,p_process_sequence=>40
,p_process_point=>'BEFORE_BOX_BODY'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'CREATE_COLL_RECORDINGS'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'v_rec_coll long;',
'BEGIN    ',
'   IF(APEX_COLLECTION.COLLECTION_EXISTS (p_collection_name => ''P310_RECORDINGS_COLL'') )THEN',
'                APEX_COLLECTION.DELETE_COLLECTION (p_collection_name=>''P310_RECORDINGS_COLL'');',
'    END IF;',
'',
'v_rec_coll := ''SELECT DISTINCT pr.RECORDING_ID, pp.source_property_id,pp.property_id,src.description source,pp.title rec_title,pp.property_number,pp.status,pp.hold_reason_id,pr.isrc,pr.ean,',
'trim(artist_last_name||'''' ''''||artist_first_name) artist,pc.collection_title Album,pc.label_name Recording_Label,pc.release_date release_date,',
'pr.recording_format,pa.aka_name Bds_Song_Id,pa.creation_date link_date,DECODE(DECODE(NVL((pa.aka_name),''''''''),'''''''', ''''Link'''',''''Unlink''''),''''Link'''', ''''<a href="f?p=518:341:''''|| v (''''APP_SESSION'''')|| ''''::::P341_REC_PROPERTY_ID,P310_BDS_SONG_ID_UNLINK,P34'
||'1_BDS_SONG_ID,P341_ARTIST,P341_ARTIST_DISP,P341_BDS_SONG_TITLE,P341_BDS_SONG_TITLE_DISP,P341_CAME_FROM,P341_SOURCE_PROPERTY_ID:''''|| pp.property_id',
'|| '''',,,''''||TRIM (artist_last_name||'''' '''' ||artist_first_name)||'''',''''||TRIM(artist_last_name||'''' ''''||artist_first_name)',
'                        || '''',''''',
'                        || trim(pp.title)',
'                        || '''',''''',
'                        || trim(pp.title)',
'                        || '''',''''',
'                        || 310',
'                        || '''','''' ',
'                        || '' ||:P310_PROP_ID || ''',
'                        || ''''">''''',
'                        || ''''Link''''',
'                        || ''''</a>'''',',
'                   ''''<a href="f?p=518:310:''''',
'                || v (''''APP_SESSION'''')',
'                || ''''::::P310_PAGE_ID,P310_REC_PROPERTY_ID,P310_BDS_SONG_ID_UNLINK:''''',
'                || ''''310''''',
'                || '''',''''',
'                || pp.property_id',
'                || '''',''''',
'                || pa.aka_name',
'                || ''''">''''',
'                || ''''UnLink''''',
'                || ''''</a>'''')',
'                Action,',
'                pp.CREATED_BY CREATED_BY,',
'                pp.CREATION_DATE CREATION_DATE,',
'                pp.LAST_UPDATED_BY LAST_UPDATED_BY,',
'                pp.LAST_UPDATE_DATE LAST_UPDATE_DATE,',
'                ''''N'''',',
'                ''''NOT_DELETED''''',
'    FROM prop_performers ppr,',
'      prop_recordings pr,',
'      prop_properties pp,',
'      prop_songs ps,',
'      prop_sources src,',
'      (SELECT * ',
'      FROM prop_akas pa ',
'      WHERE aka_type_id in (select pat.aka_type_id from prop_aka_types pat where upper(pat.aka_type_code)=''''BDS_ENCORE'''')',
'      ) pa,',
'      prop_collection_records pcr,',
'      prop_collections pc',
'    WHERE pr.property_id      = pp.property_id',
'    and ps.property_id        = '' || :P310_PROP_ID ||',
'   ''AND ps.song_id            = pr.song_id',
'    AND pr.property_id       = ppr.property_id(+)',
'    AND pp.source_property_id	 = '' || :P310_PROP_ID ||',
'    '' AND pp.property_type_id   =  2',
'     AND pp.source_id = '' || :P310_PROP_SOURCE_ID ||',
'   '' AND pp.status = ''''A''''',
'    AND ppr.property_id (+)      = pp.property_id',
'    AND src.source_id(+)         = pp.source_id',
'   /*AND (pa.aka_type_id  is null or pa.aka_type_id = (select pat.aka_type_id from prop_aka_types pat where upper(pat.aka_type_code)=''''BDS_ENCORE''''))',
'   Commented by Saurav on 21-APR-2020',
'   */',
'    AND pa.property_id(+)        = pr.property_id',
'    AND pr.recording_id = pcr.recording_id(+)',
'    AND pcr.collection_id = pc.collection_id(+)   and  '' || '''''''' || :P310_QUERY_REC || '''''''' || ''  =''''Y''''''; ',
'',
'-- PROC_LOGS(v_rec_coll);',
'',
'    APEX_COLLECTION.CREATE_COLLECTION_FROM_QUERY( p_collection_name => ''P310_RECORDINGS_COLL'',',
'                                                  p_query => v_rec_coll );',
'                         ',
'       exception when others then',
'            null;',
'            end;'))
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
,p_process_when=>'P310_PROPERTY_TAB'
,p_process_when_type=>'VAL_OF_ITEM_IN_COND_EQ_COND2'
,p_process_when2=>'8'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(39621166896099103)
,p_process_sequence=>70
,p_process_point=>'BEFORE_BOX_BODY'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'Link Recording to BDS Song'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'declare',
'v_cnt NUMBER := 0;',
'begin',
'--htp.p(''Process Run-310'');',
'',
' IF :P310_BDS_SONG_ID IS NOT NULL and :P310_REC_PROPERTY_ID is not null THEN',
'--htp.p(''Test1'');',
'	BEGIN',
'		SELECT 1',
'		INTO v_cnt',
'		FROM prop_akas',
'		WHERE aka_name = to_char(:P310_BDS_SONG_ID)',
'                     AND property_id = :P310_REC_PROPERTY_ID',
'                     and aka_type_id = 3 /* BDS_Encore */',
'                      AND rownum = 1;',
'	EXCEPTION',
'		WHEN others THEN',
'		v_cnt := 0;',
'	END;',
'	IF NVL(v_cnt,0) = 0 THEN',
'',
'		BEGIN',
'			 INSERT INTO prop_akas',
'			 (aka_id          ,',
'			source_id       ,',
'			aka_name        ,',
'			aka_type_id     ,',
'			property_id     ,',
'			created_by      ,',
'			creation_date   ,',
'			last_updated_by ,',
'			last_update_date)',
'			VALUES',
'			(prop_aka_s.nextval,',
'			7,',
'			to_char(:P310_BDS_SONG_ID),',
'			3,',
'			:P310_REC_PROPERTY_ID,',
'			v(''APP_USER''),',
'			SYSDATE,',
'			v(''APP_USER''),',
'			SYSDATE);',
'			COMMIT;',
'		EXCEPTION',
'			WHEN others THEN',
'			NULL;',
'		END;',
'	END IF;',
'	',
'	SELECT source_property_id ',
'	INTO :P310_PROP_ID',
'	FROM prop_properties',
'	WHERE property_id =  :P310_REC_PROPERTY_ID;',
' END IF;',
'',
'end;'))
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(64497897652550823)
,p_process_sequence=>20
,p_process_point=>'BEFORE_HEADER'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'UNLINKING_RECORDING_FROM_BDS'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'    declare',
'    v_property_num number;',
'    v_status varchar2(10);',
'    BEGIN',
'',
'      IF  :P310_PAGE_ID = ''310'' AND :P310_BDS_SONG_ID_UNLINK IS NOT NULL and nvl(:P310_BDS_SONG_ID_UNLINK,''~'') <> ''Link''',
'      THEN ',
'',
'',
'    select pp.property_number  into v_property_num ',
'    from prop_recordings  pr, prop_properties pp',
'    where pr.property_id = pp.property_id',
'    and pr.property_id = :P310_REC_PROPERTY_ID;',
'',
'',
'',
'       DELETE FROM PROP_AKAS',
'       WHERE AKA_NAME = :P310_BDS_SONG_ID_UNLINK AND PROPERTY_ID = :P310_REC_PROPERTY_ID;',
'',
'    BEGIN',
'',
'         Prop_Api.Create_song_reg_note',
'            (ip_property_id=> :P310_REC_PROPERTY_ID, ',
'            ip_notes=> ''Property Number ''||v_property_num||'' unlinked from BDS songs number ''||:P310_BDS_SONG_ID_UNLINK  ||'' by  ''||:APP_USER||'' on ''||sysdate,',
'            ip_user =>:APP_USER,',
'            op_success_flag=> V_STATUS',
'            );',
'    EXCEPTION WHEN OTHERS THEN',
'        NULL;',
'    END;    ',
'    commit;',
'    :P310_PAGE_ID:='''';',
'    :P310_BDS_SONG_ID_UNLINK:= null;',
'    :P310_BDS_SONG_ID := null;',
'',
'        COMMIT;',
'      END IF;',
'    null;',
'    END;'))
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(11647007489834310)
,p_process_sequence=>10
,p_process_point=>'ON_DEMAND'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'DELRECORDINGS'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'v_seq_id NUMBER;',
'v_recid NUMBER;',
'BEGIN',
'',
' IF apex_application.g_x02 = ''A'' THEN',
' ',
' ',
' for rec in ( SELECT seq_id , c001',
'         INTO v_seq_id , v_recid',
'         FROM apex_collections',
'            WHERE collection_name = ''P310_RECORDINGS_COLL''',
'            and c001 = apex_application.g_x01)',
'            loop',
'            ',
'        apex_collection.UPDATE_MEMBER_ATTRIBUTE(''P310_RECORDINGS_COLL'',p_attr_number => ''23'', p_attr_value => ''Y'',p_seq => rec.seq_id);',
'        ',
'        apex_collection.UPDATE_MEMBER_ATTRIBUTE(''P310_RECORDINGS_COLL'',p_attr_number => ''25'', p_attr_value => v_recid,p_seq => rec.seq_id);',
'        ',
'        end loop;',
'        ',
'        ',
'  END IF;',
'  ',
'   IF apex_application.g_x02 = ''R'' THEN',
' for rec in ( SELECT seq_id , c001',
'         INTO v_seq_id , v_recid',
'         FROM apex_collections',
'            WHERE collection_name = ''P310_RECORDINGS_COLL''',
'            and c001 = apex_application.g_x01)',
'            loop',
'            ',
'        apex_collection.UPDATE_MEMBER_ATTRIBUTE(''P310_RECORDINGS_COLL'',p_attr_number => ''23'', p_attr_value => ''N'',p_seq => rec.seq_id);',
'        ',
'        apex_collection.UPDATE_MEMBER_ATTRIBUTE(''P310_RECORDINGS_COLL'',p_attr_number => ''25'', p_attr_value => null ,p_seq => rec.seq_id);',
'        ',
'        end loop;',
'',
'  END IF;',
' END;'))
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(11647187744834311)
,p_process_sequence=>20
,p_process_point=>'ON_DEMAND'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'DEL_REC'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'v_seq_id NUMBER;',
'v_recid NUMBER;',
'v_action NUMBER;',
'BEGIN',
' v_action := TO_NUMBER(wwv_flow.g_x04);',
'',
'  IF  v_action = 2 then',
'        for j in ( SELECT seq_id , c001 --, c010,c011,C028',
'            from apex_collections ',
'            where collection_name = ''P310_RECORDINGS_COLL''',
'                     ) loop',
'              apex_collection.UPDATE_MEMBER_ATTRIBUTE(''P310_RECORDINGS_COLL'',p_attr_number => ''23'', p_attr_value => ''Y'',p_seq => j.seq_id);',
'              ',
'              apex_collection.UPDATE_MEMBER_ATTRIBUTE(''P310_RECORDINGS_COLL'',p_attr_number => ''25'', p_attr_value => j.c001 ,p_seq => j.seq_id);',
'',
'               end loop;',
'             ',
'    ELSIF v_action = 3 then',
'        for j in ( SELECT seq_id,c001 --, c010,c011,C028',
'            from apex_collections ',
'            where collection_name = ''P310_RECORDINGS_COLL''',
'                    ) loop',
'              apex_collection.UPDATE_MEMBER_ATTRIBUTE(''P310_RECORDINGS_COLL'',p_attr_number => ''23'', p_attr_value => ''N'',p_seq => j.seq_id);',
'              ',
'              apex_collection.UPDATE_MEMBER_ATTRIBUTE(''P310_RECORDINGS_COLL'',p_attr_number => ''25'', p_attr_value => null ,p_seq => j.seq_id);',
'',
'               end loop;',
'             end if;',
'         end;'))
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
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
