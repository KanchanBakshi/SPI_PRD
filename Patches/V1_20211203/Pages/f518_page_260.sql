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
--   Date and Time:   05:52 Wednesday November 17, 2021
--   Exported By:     SCHOPRA
--   Flashback:       0
--   Export Type:     Page Export
--   Version:         18.2.0.00.12
--   Instance ID:     69431613733717
--

prompt --application/pages/delete_00260
begin
wwv_flow_api.remove_page (p_flow_id=>wwv_flow.g_flow_id, p_page_id=>260);
end;
/
prompt --application/pages/page_00260
begin
wwv_flow_api.create_page(
 p_id=>260
,p_user_interface_id=>wwv_flow_api.id(51875501499240850)
,p_name=>'IP Details: &P260_IP_NUMBER.'
,p_step_title=>'IP Details: &P260_IP_NUMBER.'
,p_reload_on_submit=>'A'
,p_warn_on_unsaved_changes=>'N'
,p_step_sub_title=>'IP Details'
,p_step_sub_title_type=>'TEXT_WITH_SUBSTITUTIONS'
,p_first_item=>'AUTO_FIRST_ITEM'
,p_autocomplete_on_off=>'ON'
,p_group_id=>wwv_flow_api.id(68037809351097992)
,p_javascript_code=>wwv_flow_string.join(wwv_flow_t_varchar2(
'var htmldb_delete_message=''"DELETE_CONFIRM_MSG"'';',
'var waitPopup ;',
'',
'function removeRole(IP_CONTACT_POINT_ID) ',
'{',
'    if(confirm(''Would you like to perform this delete action?''))',
'    {',
'       // $x(''P2_ROLE_ID'').value = IP_CONTACT_POINT_ID;',
'        apex.submit({request:"DELETE_ONE"});',
'    }    ',
'}',
'',
'',
'function resetTab() ',
'{',
'        ',
'    if("&APP_SESSION_NUM."=='''')',
'    {',
'        $(''#NOC_tab a'').trigger("click");',
'        apex.region( "RDS" ).refresh();',
'    }',
'    var ajaxRequest = new htmldb_Get(null,&APP_ID.,''APPLICATION_PROCESS=RESETTAB'',260);',
'    var ajaxResult = ajaxRequest.get(); ',
'}  ',
'',
'$(function(){',
'    setTimeout(function(){',
' resetTab();',
'}, 500);',
'    ',
'});',
''))
,p_javascript_code_onload=>wwv_flow_string.join(wwv_flow_t_varchar2(
'$(''ul.apex-rds > li:eq(1) > a'').trigger(''click'');',
'',
'',
'',
'',
'',
''))
,p_inline_css=>wwv_flow_string.join(wwv_flow_t_varchar2(
'#P260_PAY_HOLD_REASON_HDR_DISPLAY { ',
'    color: red;',
'}',
'',
' #P260_REMIT_TO_DISPLAY {',
'    color: red;',
'    font-weight: bold;',
' ',
'}'))
,p_page_template_options=>'#DEFAULT#'
,p_required_role=>wwv_flow_api.id(69303254223531622)
,p_help_text=>'No help is available for this page.'
,p_last_updated_by=>'SCHOPRA'
,p_last_upd_yyyymmddhh24miss=>'20211101074008'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(2445906509167107)
,p_name=>'Old Hierarchy Agr.'
,p_region_name=>'HIAG'
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>140
,p_include_in_reg_disp_sel_yn=>'Y'
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#:t-Report--stretch:t-Report--altRowsDefault:t-Report--rowHighlight'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT ',
'       CASE ',
'         WHEN ah.hier_level = 0',
'         THEN ''<img src="/i/menu/pencil16x16.gif" alt="">''',
'--         THEN ''<span class="t-Icon fa fa-pencil" aria-hidden="true"></span>''',
'         ELSE',
'           '' ''',
'        END EDIT,',
'        CASE ',
'         WHEN ah.hier_level = 0',
'         THEN ''<img src="/i/menu/pencil16x16.gif" alt="">''',
'--         THEN ''<span class="t-Icon fa fa-pencil" aria-hidden="true"></span>''',
'         ELSE',
'           '' ''',
'        END NOW,',
'       CASE ',
'         WHEN (ah.hier_level = 0 AND ah.relate_account_number IS NOT NULL)',
'         THEN ''<a href="''||APEX_UTIL.PREPARE_URL (''f?p=&APP_ID.:480:&APP_SESSION.::::P480_PARTY_ID,P480_HIERARCHY_ID,P480_COLL_FOR,P480_HIER_LEVEL:'' ||:P260_IP_ID||'',''||aff_hier_id||'',''||parent_party_id||'','')||''"><img src="/i/menu/pencil16x16.gif" cl'
||'ass="apex-edit-pencil" alt=""></a>'' ',
'         WHEN (ah.hier_level = 0 AND ah.relate_account_number IS NULL)',
'         THEN ''<a href="''||APEX_UTIL.PREPARE_URL (''f?p=&APP_ID.:480:&APP_SESSION.::::P480_PARTY_ID,P480_HIERARCHY_ID,P480_COLL_FOR,P480_HIER_LEVEL:'' ||:P260_IP_ID||'',''||aff_hier_id||'',''||child_party_id||'','')||''"><img src="/i/menu/pencil16x16.gif" cla'
||'ss="apex-edit-pencil" alt=""></a>''',
'         ELSE',
'           '' ''',
'        END DEFAULT_COLLECTION_AGREEMENT, ',
'        CASE ',
'         WHEN ah.hier_level = 0',
'         THEN ''<img src="/i/menu/pencil16x16.gif" alt="">''',
'--         THEN ''<span class="t-Icon fa fa-pencil" aria-hidden="true"></span>''',
'         ELSE',
'           '' ''',
'        END EDIT_RIGHT_TYPE',
'      ,ah.aff_hier_id',
'      ,ah.hier_level',
'      ,ah.child_party_id',
'      ,(SELECT ip_number FROM aff_interested_parties WHERE ip_id = ah.child_party_id) CHILD_IP_NUMBER',
'      ,spi_screen_utility.get_party_name(ah.child_party_id) "Child Name"',
'      ,(SELECT NVL2(ra.aff_category_hfa, ra.aff_category||'' / ''||ra.aff_category_hfa, ra.aff_category) ',
'						 FROM rep_affiliates ra',
'						 WHERE ra.ip_id = ah.child_party_id)c_Derived_Status',
'      ,ah.parent_party_id',
'      ,(SELECT ip_number FROM aff_interested_parties WHERE ip_id = ah.parent_party_id) PARENT_IP_NUMBER',
'      ,spi_screen_utility.get_party_name(ah.parent_party_id) "Parent Name"',
'      ,(SELECT NVL2(ra.aff_category_hfa, ra.aff_category||'' / ''||ra.aff_category_hfa, ra.aff_category) ',
'						 FROM rep_affiliates ra',
'						 WHERE ra.ip_id = ah.parent_party_id) p_Derived_Status',
'      ,ah.RELATE_ACCOUNT_NUMBER',
'      ,ah.group_number',
'      ,CASE ',
'         WHEN ah.hier_level = 0',
'         THEN spi_screen_utility.get_ip_song_count(ah.child_party_id,ah.parent_party_id,NULL,ah.aff_hier_id) ',
'         ELSE',
'           '' ''',
'        END "SONG COUNT" ',
'      ,spi_screen_utility.GET_HIER_RIGHT_TYPE(ah.aff_hier_id) RIGHT_TYPE',
'      ,spi_screen_utility.get_hfa_number(ah.child_party_id) "Child HFA #"',
'      ,spi_screen_utility.get_hfa_number(ah.parent_party_id) "Parent HFA #"',
'      , CASE ',
'            WHEN ah.hier_level = 0 AND ah.child_party_id <> ah.parent_party_id AND ah.relate_account_number IS NOT NULL THEN',
'                CASE ',
'                    WHEN ah.hold_type_id IS NULL THEN ',
'                        ''<i class="fa fa-plus" style="color:red"></i>''  ',
'                    ELSE ',
'                        ''<i style="color:red">''||',
'                        (',
'                            SELECT pht.description ',
'                            FROM  pay_hold_types pht ',
'                            WHERE pht.hold_type_id = ah.hold_type_id',
'                        )||''</i>''',
'                END',
'            ELSE',
'                NULL',
'        END hold_type,',
'        ah.hold_type_id',
'FROM  aff_hierarchy ah',
'WHERE ah.GROUP_NUMBER  IN (SELECT group_number',
'                        FROM   aff_hierarchy',
'                        WHERE  child_party_id  = :P260_IP_ID --3546918',
'                        AND    hier_level  = 0',
'                        AND   end_date IS NULL                        ',
'                        ) ',
'   and :P260_QUERY_HIE=''Y''',
'   AND ah.end_date IS NULL  --Added by RR on 03-Sep-2018 ',
'ORDER BY ah.group_number,ah.hier_level '))
,p_display_condition_type=>'NEVER'
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_num_rows_type=>'ROW_RANGES_WITH_LINKS'
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_csv_output=>'Y'
,p_csv_output_link_text=>'Download'
,p_prn_output=>'N'
,p_sort_null=>'L'
,p_plug_query_strip_html=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(2450091088167148)
,p_query_column_id=>1
,p_column_alias=>'EDIT'
,p_column_display_sequence=>1
,p_column_link=>'f?p=&APP_ID.:400:&SESSION.::&DEBUG.:RP,400:P400_IP_ID,P400_AFF_HIER_ID,P400_GRP_NUM,P400_IP_NUMBER:&P260_IP_ID.,#AFF_HIER_ID#,#GROUP_NUMBER#,&P260_IP_NUMBER.'
,p_column_linktext=>'#EDIT#'
,p_column_alignment=>'CENTER'
,p_display_when_cond_type=>'NEVER'
,p_report_column_required_role=>wwv_flow_api.id(17200066639506016)
,p_display_as=>'WITHOUT_MODIFICATION'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(31796676246484833)
,p_query_column_id=>2
,p_column_alias=>'NOW'
,p_column_display_sequence=>2
,p_column_heading=>'Edit Nowing'
,p_column_link=>'f?p=&APP_ID.:11:&SESSION.::&DEBUG.:RP,11:P11_GROUP_NUMBER,P11_OLD_AFF_HIERARCHY_ID,P11_IP_ID:#GROUP_NUMBER#,#AFF_HIER_ID#,&P260_IP_ID.'
,p_column_linktext=>'#NOW#'
,p_report_column_required_role=>wwv_flow_api.id(33811989881355859)
,p_display_as=>'WITHOUT_MODIFICATION'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(31796704494484834)
,p_query_column_id=>3
,p_column_alias=>'DEFAULT_COLLECTION_AGREEMENT'
,p_column_display_sequence=>3
,p_column_heading=>'Edit Default Collection Agreement'
,p_column_link=>'f?p=&APP_ID.:480:&SESSION.::&DEBUG.:RP,480:P480_PARTY_ID,P480_HIERARCHY_ID,P480_COLL_FOR,P480_SEL_HIER_LEVEL,P480_HIER_LEVEL:&P260_IP_ID.,#AFF_HIER_ID#,#PARENT_PARTY_ID#,,'
,p_column_linktext=>'#DEFAULT_COLLECTION_AGREEMENT#'
,p_report_column_required_role=>wwv_flow_api.id(17200066639506016)
,p_display_as=>'WITHOUT_MODIFICATION'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(31796801827484835)
,p_query_column_id=>4
,p_column_alias=>'EDIT_RIGHT_TYPE'
,p_column_display_sequence=>4
,p_column_heading=>'Edit Right Type'
,p_column_link=>'f?p=&APP_ID.:16:&SESSION.::&DEBUG.:RP,16:P16_AFF_HIER_ID,P16_IP_ID:#AFF_HIER_ID#,&P260_IP_ID.'
,p_column_linktext=>'#EDIT_RIGHT_TYPE#'
,p_report_column_required_role=>wwv_flow_api.id(69665980648432325)
,p_display_as=>'WITHOUT_MODIFICATION'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(2450123284167149)
,p_query_column_id=>5
,p_column_alias=>'AFF_HIER_ID'
,p_column_display_sequence=>6
,p_column_heading=>'Hierarchy ID'
,p_use_as_row_header=>'N'
,p_column_alignment=>'RIGHT'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(2447677186167124)
,p_query_column_id=>6
,p_column_alias=>'HIER_LEVEL'
,p_column_display_sequence=>9
,p_column_heading=>'Hierarchy Level'
,p_use_as_row_header=>'N'
,p_column_alignment=>'CENTER'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(2447710101167125)
,p_query_column_id=>7
,p_column_alias=>'CHILD_PARTY_ID'
,p_column_display_sequence=>17
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(5436886475109027)
,p_query_column_id=>8
,p_column_alias=>'CHILD_IP_NUMBER'
,p_column_display_sequence=>19
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(2447891634167126)
,p_query_column_id=>9
,p_column_alias=>'Child Name'
,p_column_display_sequence=>10
,p_column_heading=>'Child Name'
,p_column_link=>'f?p=&APP_ID.:260:&SESSION.::&DEBUG.:RP,50210:P260_IP_ID,P260_CALLING_PAGE_ID,P260_IP_NUMBER,P260_PREV_IP_ID,P260_PREV_IP_NUMBER:#CHILD_PARTY_ID#,260,#CHILD_IP_NUMBER#,&P260_IP_ID.,&P260_IP_NUMBER.'
,p_column_linktext=>'#Child Name#'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(29509787054115521)
,p_query_column_id=>10
,p_column_alias=>'C_DERIVED_STATUS'
,p_column_display_sequence=>12
,p_column_heading=>'Child Derived Status'
,p_use_as_row_header=>'N'
,p_display_as=>'WITHOUT_MODIFICATION'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(2447969909167127)
,p_query_column_id=>11
,p_column_alias=>'PARENT_PARTY_ID'
,p_column_display_sequence=>18
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(5436968949109028)
,p_query_column_id=>12
,p_column_alias=>'PARENT_IP_NUMBER'
,p_column_display_sequence=>20
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(2448087191167128)
,p_query_column_id=>13
,p_column_alias=>'Parent Name'
,p_column_display_sequence=>13
,p_column_heading=>'Parent Name'
,p_column_link=>'f?p=&APP_ID.:260:&SESSION.::&DEBUG.:RP,50210:P260_IP_ID,P260_CALLING_PAGE_ID,P260_IP_NUMBER,P260_PREV_IP_ID,P260_PREV_IP_NUMBER:#PARENT_PARTY_ID#,260,#PARENT_IP_NUMBER#,&P260_IP_ID.,&P260_IP_NUMBER.'
,p_column_linktext=>'#Parent Name#'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(29509862794115522)
,p_query_column_id=>14
,p_column_alias=>'P_DERIVED_STATUS'
,p_column_display_sequence=>15
,p_column_heading=>'Parent Derived Status'
,p_use_as_row_header=>'N'
,p_display_as=>'WITHOUT_MODIFICATION'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(2446277687167110)
,p_query_column_id=>15
,p_column_alias=>'RELATE_ACCOUNT_NUMBER'
,p_column_display_sequence=>16
,p_column_heading=>'HFA Relate Account #'
,p_use_as_row_header=>'N'
,p_column_alignment=>'CENTER'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(6149190071415122)
,p_query_column_id=>16
,p_column_alias=>'GROUP_NUMBER'
,p_column_display_sequence=>7
,p_column_heading=>'Group Number'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(18297536324538223)
,p_query_column_id=>17
,p_column_alias=>'SONG COUNT'
,p_column_display_sequence=>21
,p_column_heading=>'Song Count'
,p_column_link=>'f?p=&APP_ID.:490:&SESSION.::&DEBUG.:RP,490:P490_CHILD_PARTY_ID,P490_PARENT_PARTY_ID,P490_RELATE_ACC_NUM,P490_AFF_HIER_ID:#CHILD_PARTY_ID#,#PARENT_PARTY_ID#,#RELATE_ACCOUNT_NUMBER#,#AFF_HIER_ID#'
,p_column_linktext=>'#SONG COUNT#'
,p_column_alignment=>'RIGHT'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(18296914364538217)
,p_query_column_id=>18
,p_column_alias=>'RIGHT_TYPE'
,p_column_display_sequence=>8
,p_column_heading=>'Right Type'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(34374348174973305)
,p_query_column_id=>19
,p_column_alias=>'Child HFA #'
,p_column_display_sequence=>11
,p_column_heading=>'Child hfa #'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(34374483555973306)
,p_query_column_id=>20
,p_column_alias=>'Parent HFA #'
,p_column_display_sequence=>14
,p_column_heading=>'Parent hfa #'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(36262448468352929)
,p_query_column_id=>21
,p_column_alias=>'HOLD_TYPE'
,p_column_display_sequence=>5
,p_column_heading=>'Add/Edit Hold Type'
,p_column_link=>'f?p=&APP_ID.:50420:&SESSION.:IL:&DEBUG.:RP,50420:P50420_AFF_HIER_ID,P50420_CHILD,P50420_PARENT,P50420_HOLD_TYPE_ID,P50420_IP_TYPE:#AFF_HIER_ID#,#Child Name#,#Parent Name#,#HOLD_TYPE_ID#,&P260_IP_TYPE.'
,p_column_linktext=>'#HOLD_TYPE#'
,p_column_link_attr=>'title="Click to add/edit"'
,p_column_alignment=>'CENTER'
,p_report_column_required_role=>wwv_flow_api.id(71705400949038146)
,p_display_as=>'WITHOUT_MODIFICATION'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(36263175344352936)
,p_query_column_id=>22
,p_column_alias=>'HOLD_TYPE_ID'
,p_column_display_sequence=>22
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(19406575483337105)
,p_plug_name=>'View Used Hierarchy'
,p_parent_plug_id=>wwv_flow_api.id(2445906509167107)
,p_region_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51848808455240825)
,p_plug_display_sequence=>10
,p_plug_display_point=>'BODY'
,p_query_type=>'SQL'
,p_plug_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT ',
'       aff_hier_id',
'      --,NULL "Group Name"',
'      ,hier_level hier_lev',
'      ,child_party_id',
'      ,(SELECT ip_number FROM aff_interested_parties WHERE ip_id = child_party_id) CHILD_IP_NUMBER',
'      ,spi_screen_utility.get_party_name(child_party_id) "Child Name"',
'      ,parent_party_id',
'      ,(SELECT ip_number FROM aff_interested_parties WHERE ip_id = parent_party_id) PARENT_IP_NUMBER',
'      ,spi_screen_utility.get_party_name(parent_party_id) "Parent Name"',
'      ,RELATE_ACCOUNT_NUMBER',
'      ,group_number',
'      ,spi_screen_utility.GET_HIER_RIGHT_TYPE(aff_hier_id) RIGHT_TYPE',
'      ,spi_screen_utility.get_hfa_number(child_party_id) Child_HFA',
'      ,spi_screen_utility.get_hfa_number(parent_party_id) Party_HFA',
'FROM   aff_hierarchy ',
'WHERE parent_party_id = :P260_IP_ID',
'AND hier_level = 0',
'AND child_party_id <> parent_party_id',
'AND end_date IS NULL',
'AND nowed_aff_hier_id IS NULL and :P260_QUERY_HIE=''Y''',
'--ORDER BY "Child Name"',
'/*',
'WHERE  group_number IN ( SELECT DISTINCT group_number ',
'                         FROM   aff_hierarchy ',
'                         WHERE  parent_party_id = :P260_IP_ID',
'                         AND    hier_level = 0',
'                         AND    end_date IS NULL',
'                       --  OR      child_party_id = :P260_IP_ID',
'                       ) and hier_level=0 and child_party_id <> parent_party_id;',
'*/                       '))
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
 p_id=>wwv_flow_api.id(28661483053759544)
,p_max_row_count=>'1000000'
,p_show_nulls_as=>'-'
,p_pagination_type=>'ROWS_X_TO_Y'
,p_pagination_display_pos=>'BOTTOM_RIGHT'
,p_report_list_mode=>'TABS'
,p_show_detail_link=>'N'
,p_show_notify=>'Y'
,p_download_formats=>'CSV:HTML:EMAIL:XLS:PDF:RTF'
,p_owner=>'SCHOPRA'
,p_internal_uid=>28661483053759544
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(28661594361759545)
,p_db_column_name=>'AFF_HIER_ID'
,p_display_order=>10
,p_column_identifier=>'A'
,p_column_label=>'Hierarchy ID'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(30772910010713705)
,p_db_column_name=>'RIGHT_TYPE'
,p_display_order=>20
,p_column_identifier=>'K'
,p_column_label=>'Right type'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(28661651155759546)
,p_db_column_name=>'HIER_LEV'
,p_display_order=>30
,p_column_identifier=>'B'
,p_column_label=>'Hier lev'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(28661750327759547)
,p_db_column_name=>'CHILD_PARTY_ID'
,p_display_order=>40
,p_column_identifier=>'C'
,p_column_label=>'Child party id'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(28661853198759548)
,p_db_column_name=>'CHILD_IP_NUMBER'
,p_display_order=>50
,p_column_identifier=>'D'
,p_column_label=>'Child IP #'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(28661925398759549)
,p_db_column_name=>'Child Name'
,p_display_order=>60
,p_column_identifier=>'E'
,p_column_label=>'Child Name'
,p_column_link=>'f?p=&APP_ID.:260:&SESSION.::&DEBUG.:RP:P260_IP_ID,P260_IP_NUMBER:#CHILD_PARTY_ID#,#CHILD_IP_NUMBER#'
,p_column_linktext=>'#Child Name#'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(28662003078759550)
,p_db_column_name=>'PARENT_PARTY_ID'
,p_display_order=>70
,p_column_identifier=>'F'
,p_column_label=>'Parent party id'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(30772502962713701)
,p_db_column_name=>'PARENT_IP_NUMBER'
,p_display_order=>80
,p_column_identifier=>'G'
,p_column_label=>'Parent IP #'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(30772678616713702)
,p_db_column_name=>'Parent Name'
,p_display_order=>90
,p_column_identifier=>'H'
,p_column_label=>'Parent Name'
,p_column_link=>'f?p=&APP_ID.:260:&SESSION.::&DEBUG.:RP:P260_IP_ID,P260_IP_NUMBER:#PARENT_PARTY_ID#,#PARENT_IP_NUMBER#'
,p_column_linktext=>'#Parent Name#'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(30772855952713704)
,p_db_column_name=>'GROUP_NUMBER'
,p_display_order=>100
,p_column_identifier=>'J'
,p_column_label=>'Group number'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(30772731301713703)
,p_db_column_name=>'RELATE_ACCOUNT_NUMBER'
,p_display_order=>110
,p_column_identifier=>'I'
,p_column_label=>'Relate Account #'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(30773009275713706)
,p_db_column_name=>'CHILD_HFA'
,p_display_order=>120
,p_column_identifier=>'L'
,p_column_label=>'Child HFA #'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(30773181014713707)
,p_db_column_name=>'PARTY_HFA'
,p_display_order=>130
,p_column_identifier=>'M'
,p_column_label=>'Parent HFA#'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(30784843261714887)
,p_application_user=>'APXWS_DEFAULT'
,p_report_seq=>10
,p_report_alias=>'307849'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'AFF_HIER_ID:RIGHT_TYPE:CHILD_IP_NUMBER:CHILD_HFA:Child Name:PARENT_IP_NUMBER:PARTY_HFA:Parent Name:RELATE_ACCOUNT_NUMBER:'
,p_sort_column_1=>'AFF_HIER_ID'
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
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(33806569131587639)
,p_name=>'Hierarchy History'
,p_parent_plug_id=>wwv_flow_api.id(2445906509167107)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>30
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#:t-Report--altRowsDefault:t-Report--rowHighlight'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT  /*',
'        CASE ',
'         WHEN (hier_level = 0 AND nowed_aff_hier_id IS NOT NULL)',
'         THEN ''<img src="/i/menu/pencil16x16.gif" alt="">''',
'--         THEN ''<span class="t-Icon fa fa-pencil" aria-hidden="true"></span>''',
'         ELSE',
'           '' ''',
'        END NOW',
'        */',
'      CASE WHEN TO_CHAR(nowed_aff_hier_id) IS NULL THEN '' '' ',
'          ELSE TO_CHAR(nowed_aff_hier_id) ',
'      END nowed_aff_hier_id',
'      ,aff_hier_id',
'      ,hier_level',
'      ,child_party_id',
'      ,(SELECT ip_number FROM aff_interested_parties WHERE ip_id = child_party_id) CHILD_IP_NUMBER',
'      ,spi_screen_utility.get_party_name(child_party_id) "Child Name"',
'      ,(SELECT NVL2(ra.aff_category_hfa, ra.aff_category||'' / ''||ra.aff_category_hfa, ra.aff_category) ',
'						 FROM rep_affiliates ra',
'						 WHERE ra.ip_id = child_party_id)c_Derived_Status',
'      ,parent_party_id',
'      ,(SELECT ip_number FROM aff_interested_parties WHERE ip_id = parent_party_id) PARENT_IP_NUMBER',
'      ,spi_screen_utility.get_party_name(parent_party_id) "Parent Name"',
'      ,(SELECT NVL2(ra.aff_category_hfa, ra.aff_category||'' / ''||ra.aff_category_hfa, ra.aff_category) ',
'						 FROM rep_affiliates ra',
'						 WHERE ra.ip_id = parent_party_id) p_Derived_Status',
'      ,RELATE_ACCOUNT_NUMBER',
'      ,group_number',
'      ,CASE ',
'         WHEN hier_level = 0',
'         --THEN spi_screen_utility.GET_SONG_COUNT(child_party_id,parent_party_id) ',
'         THEN spi_screen_utility.get_ip_song_count(ah.child_party_id,ah.parent_party_id,NULL,ah.aff_hier_id) ',
'         ELSE',
'           '' ''',
'        END "SONG COUNT" ',
'      ,spi_screen_utility.GET_HIER_RIGHT_TYPE(aff_hier_id) RIGHT_TYPE',
'      ,spi_screen_utility.get_hfa_number(child_party_id) "Child HFA #"',
'      ,spi_screen_utility.get_hfa_number(parent_party_id) "Parent HFA #"',
'FROM  aff_hierarchy AH',
'WHERE GROUP_NUMBER  IN (SELECT group_number',
'                        FROM   aff_hierarchy',
'                        WHERE  (child_party_id  = :P260_IP_ID)',
'                        AND    hier_level  = 0                      ',
'                        )',
'AND end_date IS NOT NULL and :P260_QUERY_HIE=''Y''',
'ORDER BY group_number,hier_level'))
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
 p_id=>wwv_flow_api.id(29512404890115548)
,p_query_column_id=>1
,p_column_alias=>'NOWED_AFF_HIER_ID'
,p_column_display_sequence=>1
,p_column_heading=>'Nowed Hier ID'
,p_column_link=>'f?p=&APP_ID.:24:&SESSION.::&DEBUG.:RP:P24_AFF_HIER_ID:#NOWED_AFF_HIER_ID#'
,p_column_linktext=>'#NOWED_AFF_HIER_ID#'
,p_column_alignment=>'RIGHT'
,p_display_as=>'WITHOUT_MODIFICATION'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(33806760425587641)
,p_query_column_id=>2
,p_column_alias=>'AFF_HIER_ID'
,p_column_display_sequence=>2
,p_column_heading=>'Hierarchy ID'
,p_use_as_row_header=>'N'
,p_column_alignment=>'RIGHT'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(33806813941587642)
,p_query_column_id=>3
,p_column_alias=>'HIER_LEVEL'
,p_column_display_sequence=>4
,p_column_heading=>'Hier Level'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(33806915101587643)
,p_query_column_id=>4
,p_column_alias=>'CHILD_PARTY_ID'
,p_column_display_sequence=>14
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(33807070115587644)
,p_query_column_id=>5
,p_column_alias=>'CHILD_IP_NUMBER'
,p_column_display_sequence=>15
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(33807138301587645)
,p_query_column_id=>6
,p_column_alias=>'Child Name'
,p_column_display_sequence=>5
,p_column_heading=>'Child Name'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(33807230756587646)
,p_query_column_id=>7
,p_column_alias=>'C_DERIVED_STATUS'
,p_column_display_sequence=>7
,p_column_heading=>'Child Derived Status'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(33807336542587647)
,p_query_column_id=>8
,p_column_alias=>'PARENT_PARTY_ID'
,p_column_display_sequence=>16
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(33807436864587648)
,p_query_column_id=>9
,p_column_alias=>'PARENT_IP_NUMBER'
,p_column_display_sequence=>17
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(33807546837587649)
,p_query_column_id=>10
,p_column_alias=>'Parent Name'
,p_column_display_sequence=>8
,p_column_heading=>'Parent Name'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(33807699425587650)
,p_query_column_id=>11
,p_column_alias=>'P_DERIVED_STATUS'
,p_column_display_sequence=>10
,p_column_heading=>'Parent Derived Status'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(35017304357952201)
,p_query_column_id=>12
,p_column_alias=>'RELATE_ACCOUNT_NUMBER'
,p_column_display_sequence=>11
,p_column_heading=>'HFA Relate Account #'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(35017440217952202)
,p_query_column_id=>13
,p_column_alias=>'GROUP_NUMBER'
,p_column_display_sequence=>12
,p_column_heading=>'Group Number'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(35017573174952203)
,p_query_column_id=>14
,p_column_alias=>'SONG COUNT'
,p_column_display_sequence=>13
,p_column_heading=>'Song Count'
,p_use_as_row_header=>'N'
,p_column_alignment=>'RIGHT'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(35017698577952204)
,p_query_column_id=>15
,p_column_alias=>'RIGHT_TYPE'
,p_column_display_sequence=>3
,p_column_heading=>'Right Type'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(35017758375952205)
,p_query_column_id=>16
,p_column_alias=>'Child HFA #'
,p_column_display_sequence=>6
,p_column_heading=>'Child hfa #'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(35017812915952206)
,p_query_column_id=>17
,p_column_alias=>'Parent HFA #'
,p_column_display_sequence=>9
,p_column_heading=>'Parent hfa #'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(6149592920415126)
,p_name=>'Collection Agr.'
,p_region_name=>'CLAG'
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>160
,p_include_in_reg_disp_sel_yn=>'Y'
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#:t-Report--stretch:t-Report--altRowsDefault:t-Report--rowHighlight'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select rn, the_level,',
'      collector_agreement_id,',
'      collector_id,',
'      collector,',
'      collectornum,',
'       parent,',
'       parentnum,',
'       parentid,',
'      collects_on_agreement_id,',
'     share_percent,',
'     heir_path,',
'     right_type_id,',
'     COLL_AGREEMENT_TYPE_ID,',
'     CLASSIFICATION,',
'     CREATION_DATE,',
'     CREATED_BY,',
'     spi_screen_utility.get_hfa_number(collector_id) "Owner HFA #",',
'     spi_screen_utility.get_hfa_number(parentid) "Parent HFA #",',
'     "Add" from',
'(select rownum rn,',
'    level the_level, ',
'    collector_agreement_id,',
'    collector_id,',
'    --(select UPPER(group_name) from Prop_Groups where collector_agreement_id = pca1.collector_agreement_id) Group_Name,',
'    (select primary_name from aff_ip_name_v where ip_id = collector_id ) collector,',
'    (select ip_number from aff_ip_name_v where ip_id = collector_id ) collectornum,',
'    (select primary_name from aff_ip_name_v where ip_id = ( select collector_id  from PROP_COLLECTOR_AGREEMENTS pca2 where pca2.collector_agreement_id = pca1.collects_on_agreement_id and pca2.right_type_id = pca1.right_type_id) ) parent,',
'    (select ip_number from aff_ip_name_v where ip_id = ( select collector_id  from PROP_COLLECTOR_AGREEMENTS pca2 where pca2.collector_agreement_id = pca1.collects_on_agreement_id and pca2.right_type_id = pca1.right_type_id) ) parentnum,',
'    (select collector_id  from PROP_COLLECTOR_AGREEMENTS pca2 where pca2.collector_agreement_id = pca1.collects_on_agreement_id and pca2.right_type_id = pca1.right_type_id)  parentid,',
'    collects_on_agreement_id,',
'    share_percent,',
'    heir_path,',
'    right_type_id,',
'    COLL_AGREEMENT_TYPE_ID,',
'    CLASSIFICATION, ',
'    CREATION_DATE,',
'    CREATED_BY,',
'    NULL "Add"',
'    --DECODE(level, 1, '' '',''<i class="fa fa-plus" aria-hidden="true"></i>'') "Add"',
'FROM prop_collector_agreements pca1',
'--WHERE right_type_id = 1',
'WHERE 1=1',
'--AND SYSDATE BETWEEN NVL(START_DATE,SYSDATE) AND NVL(END_DATE,SYSDATE) ',
'AND END_DATE IS NULL',
'and :P260_QUERY_COLL=''Y''',
'START WITH (collector_id = :P260_IP_ID AND collects_on_agreement_id IS NULL)',
'CONNECT BY collects_on_agreement_id = PRIOR collector_agreement_id',
'ORDER SIBLINGS BY collector_id --Added by Saurav on 28-FEB-2020',
')',
'order by rn',
'/* COmmented by Saurav Chopra on 03-mar-2018',
'SELECT REGEXP_COUNT(heir_path, ''/'', 1, ''i'') the_level,',
'           collector_agreement_id,',
'           collector_id,',
'           (select UPPER(group_name) from Prop_Groups where collector_agreement_id = pca1.collector_agreement_id) Group_Name,',
'           (select ip_number||'' - ''||primary_name from aff_ip_name_v where ip_id = collector_id ) collector,',
'           (select ip_number||'' - ''||primary_name from aff_ip_name_v where ip_id = ( select collector_id  from PROP_COLLECTOR_AGREEMENTS pca2 where pca2.collector_agreement_id = pca1.collects_on_agreement_id ) ) parent,',
'           collects_on_agreement_id,',
'           share_percent,',
'           heir_path,',
'           right_type_id',
'           --spi_screen_utility.get_song_count(pca1.collector_agreement_id) SONG_COUNT_LINK,',
'           --spi_screen_utility.get_song_count(pca1.collector_agreement_id) SONG_COUNT',
'      FROM PROP_COLLECTOR_AGREEMENTS pca1',
'where (heir_path like ''%/''||:P260_ip_NUMBER||''_________'' or heir_path like ''%/''||:P260_ip_NUMBER||''________'')',
'--and  RIGHT_TYPE_ID = 2',
'*/'))
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>'-'
,p_query_num_rows_type=>'ROW_RANGES_IN_SELECT_LIST'
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_csv_output=>'Y'
,p_csv_output_link_text=>'Download'
,p_prn_output=>'N'
,p_sort_null=>'L'
,p_plug_query_strip_html=>'N'
);
end;
/
begin
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(275192399253437702)
,p_query_column_id=>1
,p_column_alias=>'RN'
,p_column_display_sequence=>18
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(7806044585522422)
,p_query_column_id=>2
,p_column_alias=>'THE_LEVEL'
,p_column_display_sequence=>3
,p_column_heading=>'The Level'
,p_use_as_row_header=>'N'
,p_column_alignment=>'RIGHT'
,p_heading_alignment=>'LEFT'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(14810265262295127)
,p_query_column_id=>3
,p_column_alias=>'COLLECTOR_AGREEMENT_ID'
,p_column_display_sequence=>1
,p_use_as_row_header=>'N'
,p_column_link=>'f?p=&APP_ID.:415:&SESSION.::&DEBUG.:RP,415:P415_COLLECTOR_AGREEMENT_ID:#COLLECTOR_AGREEMENT_ID#'
,p_column_linktext=>'#COLLECTOR_AGREEMENT_ID#'
,p_heading_alignment=>'LEFT'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(16696649403235410)
,p_query_column_id=>4
,p_column_alias=>'COLLECTOR_ID'
,p_column_display_sequence=>4
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(34950802966093430)
,p_query_column_id=>5
,p_column_alias=>'COLLECTOR'
,p_column_display_sequence=>8
,p_column_heading=>'Payee'
,p_use_as_row_header=>'N'
,p_column_link=>'f?p=&APP_ID.:260:&SESSION.:APEX_CLONE_SESSION:&DEBUG.:RP:P260_IP_ID,P260_AFFILIATE_NO,P260_CALLING_PAGE_ID:#COLLECTOR_ID#,#COLLECTOR2#,260'
,p_column_linktext=>'#COLLECTOR#'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(23440255501268108)
,p_query_column_id=>6
,p_column_alias=>'COLLECTORNUM'
,p_column_display_sequence=>13
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(34951612710093438)
,p_query_column_id=>7
,p_column_alias=>'PARENT'
,p_column_display_sequence=>10
,p_column_heading=>'Collects on behalf of'
,p_use_as_row_header=>'N'
,p_column_link=>'f?p=&APP_ID.:260:&SESSION.::&DEBUG.:RP:P260_IP_ID,P260_AFFILIATE_NO,P260_CALLING_PAGE_ID:#PARENTID#,#PARENTNUM#,260'
,p_column_linktext=>'#PARENT#'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(23440300477268109)
,p_query_column_id=>8
,p_column_alias=>'PARENTNUM'
,p_column_display_sequence=>14
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(23440487518268110)
,p_query_column_id=>9
,p_column_alias=>'PARENTID'
,p_column_display_sequence=>15
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(7806432195522426)
,p_query_column_id=>10
,p_column_alias=>'COLLECTS_ON_AGREEMENT_ID'
,p_column_display_sequence=>7
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(7806522408522427)
,p_query_column_id=>11
,p_column_alias=>'SHARE_PERCENT'
,p_column_display_sequence=>5
,p_column_heading=>'Share Percent'
,p_use_as_row_header=>'N'
,p_column_format=>'999G999G999G999G990D00'
,p_column_alignment=>'RIGHT'
,p_heading_alignment=>'RIGHT'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(7806691241522428)
,p_query_column_id=>12
,p_column_alias=>'HEIR_PATH'
,p_column_display_sequence=>6
,p_column_heading=>'Heir Path'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(19448199258981338)
,p_query_column_id=>13
,p_column_alias=>'RIGHT_TYPE_ID'
,p_column_display_sequence=>12
,p_column_heading=>'Right type'
,p_use_as_row_header=>'N'
,p_display_as=>'TEXT_FROM_LOV'
,p_named_lov=>wwv_flow_api.id(62952354574980407)
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(29619869733493522)
,p_query_column_id=>14
,p_column_alias=>'COLL_AGREEMENT_TYPE_ID'
,p_column_display_sequence=>16
,p_column_heading=>'Agreement Type'
,p_use_as_row_header=>'N'
,p_display_as=>'TEXT_FROM_LOV'
,p_named_lov=>wwv_flow_api.id(64834507396304316)
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(29619999638493523)
,p_query_column_id=>15
,p_column_alias=>'CLASSIFICATION'
,p_column_display_sequence=>17
,p_column_heading=>'Classification'
,p_use_as_row_header=>'N'
,p_display_as=>'TEXT_FROM_LOV'
,p_named_lov=>wwv_flow_api.id(31448311584450840)
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(44907305552138602)
,p_query_column_id=>16
,p_column_alias=>'CREATION_DATE'
,p_column_display_sequence=>19
,p_column_heading=>'Creation Date'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(44907437128138603)
,p_query_column_id=>17
,p_column_alias=>'CREATED_BY'
,p_column_display_sequence=>20
,p_column_heading=>'Created By'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(2456428541447926)
,p_query_column_id=>18
,p_column_alias=>'Owner HFA #'
,p_column_display_sequence=>9
,p_column_heading=>'Payee Hfa #'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(2456599438447927)
,p_query_column_id=>19
,p_column_alias=>'Parent HFA #'
,p_column_display_sequence=>11
,p_column_heading=>'Collects on behalf of HFA #'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(29621607495493540)
,p_query_column_id=>20
,p_column_alias=>'Add'
,p_column_display_sequence=>2
,p_column_heading=>'Add Level'
,p_use_as_row_header=>'N'
,p_column_link=>'f?p=&APP_ID.:415:&SESSION.::&DEBUG.:RP,415:P415_COLLECTOR_ID,P415_COLLECTS_ON_AGR_ID,P415_RIGHT_TYPE_ID,P415_COLL_AGREEMENT_TYPE_ID:#COLLECTOR_ID#,#COLLECTOR_AGREEMENT_ID#,#RIGHT_TYPE_ID#,#COLL_AGREEMENT_TYPE_ID#'
,p_column_linktext=>'<i class="fa fa-plus" aria-hidden="true"></i>'
,p_column_alignment=>'CENTER'
,p_display_as=>'WITHOUT_MODIFICATION'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(29620144102493525)
,p_name=>'Party as Payee'
,p_parent_plug_id=>wwv_flow_api.id(6149592920415126)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>10
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#:t-Report--altRowsDefault:t-Report--rowHighlight'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT * FROM (select ',
'    level the_level, ',
'    collector_agreement_id,',
'    collector_id,',
'    --(select UPPER(group_name) from Prop_Groups where collector_agreement_id = pca1.collector_agreement_id) Group_Name,',
'    (select primary_name from aff_ip_name_v where ip_id = collector_id ) collector,',
'    (select ip_number from aff_ip_name_v where ip_id = collector_id ) collectornum,',
'    (select primary_name from aff_ip_name_v where ip_id = ( select collector_id  from PROP_COLLECTOR_AGREEMENTS pca2 where pca2.collector_agreement_id = pca1.collects_on_agreement_id and pca2.right_type_id = pca1.right_type_id) ) parent,',
'    (select ip_number from aff_ip_name_v where ip_id = ( select collector_id  from PROP_COLLECTOR_AGREEMENTS pca2 where pca2.collector_agreement_id = pca1.collects_on_agreement_id and pca2.right_type_id = pca1.right_type_id) ) parentnum,',
'    (select collector_id  from PROP_COLLECTOR_AGREEMENTS pca2 where pca2.collector_agreement_id = pca1.collects_on_agreement_id and pca2.right_type_id = pca1.right_type_id)  parentid,',
'    collects_on_agreement_id,',
'    share_percent,',
'    heir_path,',
'    right_type_id,',
'    COLL_AGREEMENT_TYPE_ID,',
'    CLASSIFICATION',
'FROM prop_collector_agreements pca1',
'--WHERE right_type_id = 1 ',
'WHERE SYSDATE BETWEEN NVL(pca1.START_DATE,SYSDATE) AND NVL(pca1.END_DATE,SYSDATE)               ',
'START WITH collector_id = :P260_IP_ID',
'CONNECT BY collects_on_agreement_id = PRIOR collector_agreement_id) temp',
'WHERE TEMP.COLLECTORNUM = :P260_IP_NUMBER and :P260_QUERY_COLL=''Y''',
'AND temp.PARENT IS NOT NULL',
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
 p_id=>wwv_flow_api.id(29620298840493526)
,p_query_column_id=>1
,p_column_alias=>'THE_LEVEL'
,p_column_display_sequence=>2
,p_column_heading=>'The level'
,p_use_as_row_header=>'N'
,p_column_alignment=>'RIGHT'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(29620353612493527)
,p_query_column_id=>2
,p_column_alias=>'COLLECTOR_AGREEMENT_ID'
,p_column_display_sequence=>1
,p_column_heading=>'Collector Agreement ID'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(29620436908493528)
,p_query_column_id=>3
,p_column_alias=>'COLLECTOR_ID'
,p_column_display_sequence=>5
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(29620508584493529)
,p_query_column_id=>4
,p_column_alias=>'COLLECTOR'
,p_column_display_sequence=>6
,p_column_heading=>'Payee'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(29620690917493530)
,p_query_column_id=>5
,p_column_alias=>'COLLECTORNUM'
,p_column_display_sequence=>7
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(29620721928493531)
,p_query_column_id=>6
,p_column_alias=>'PARENT'
,p_column_display_sequence=>8
,p_column_heading=>'Collects on behalf of'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(29620893269493532)
,p_query_column_id=>7
,p_column_alias=>'PARENTNUM'
,p_column_display_sequence=>9
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(29620940496493533)
,p_query_column_id=>8
,p_column_alias=>'PARENTID'
,p_column_display_sequence=>10
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(29621076526493534)
,p_query_column_id=>9
,p_column_alias=>'COLLECTS_ON_AGREEMENT_ID'
,p_column_display_sequence=>11
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(29621117768493535)
,p_query_column_id=>10
,p_column_alias=>'SHARE_PERCENT'
,p_column_display_sequence=>3
,p_column_heading=>'Share percent'
,p_use_as_row_header=>'N'
,p_column_format=>'999G999G999G999G990D00'
,p_column_alignment=>'RIGHT'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(29621280153493536)
,p_query_column_id=>11
,p_column_alias=>'HEIR_PATH'
,p_column_display_sequence=>4
,p_column_heading=>'Heir path'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(29621345372493537)
,p_query_column_id=>12
,p_column_alias=>'RIGHT_TYPE_ID'
,p_column_display_sequence=>12
,p_column_heading=>'Right Type'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_display_as=>'TEXT_FROM_LOV'
,p_named_lov=>wwv_flow_api.id(62952354574980407)
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(29621442057493538)
,p_query_column_id=>13
,p_column_alias=>'COLL_AGREEMENT_TYPE_ID'
,p_column_display_sequence=>13
,p_column_heading=>'Agreement Type'
,p_use_as_row_header=>'N'
,p_display_as=>'TEXT_FROM_LOV'
,p_named_lov=>wwv_flow_api.id(64834507396304316)
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(29621537751493539)
,p_query_column_id=>14
,p_column_alias=>'CLASSIFICATION'
,p_column_display_sequence=>14
,p_column_heading=>'Classification'
,p_use_as_row_header=>'N'
,p_display_as=>'TEXT_FROM_LOV'
,p_named_lov=>wwv_flow_api.id(31448311584450840)
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(37796228300097447)
,p_plug_name=>'History'
,p_parent_plug_id=>wwv_flow_api.id(6149592920415126)
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>20
,p_plug_display_point=>'BODY'
,p_query_type=>'SQL'
,p_plug_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select  * from PROP_COLLECTOR_AGREMNT_HISTORY',
'where substr(HEIR_PATH,2,instr(HEIR_PATH,''-'',2)-2) = :P260_IP_NUMBER',
'and :P260_QUERY_COLL=''Y''',
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
);
wwv_flow_api.create_worksheet(
 p_id=>wwv_flow_api.id(37796316301097448)
,p_max_row_count=>'1000000'
,p_show_nulls_as=>'-'
,p_pagination_type=>'ROWS_X_TO_Y'
,p_pagination_display_pos=>'BOTTOM_RIGHT'
,p_report_list_mode=>'TABS'
,p_show_detail_link=>'N'
,p_show_notify=>'Y'
,p_download_formats=>'CSV:HTML:EMAIL:XLS:PDF:RTF'
,p_owner=>'KBAKSHI'
,p_internal_uid=>37796316301097448
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37796448187097449)
,p_db_column_name=>'COLLECTOR_AGREEMENT_ID'
,p_display_order=>10
,p_column_identifier=>'A'
,p_column_label=>'Collector Agreement ID'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37796596546097450)
,p_db_column_name=>'COLLECTOR_ID'
,p_display_order=>20
,p_column_identifier=>'B'
,p_column_label=>'Collector ID'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38887186119446401)
,p_db_column_name=>'COLLECTS_ON_AGREEMENT_ID'
,p_display_order=>30
,p_column_identifier=>'C'
,p_column_label=>'Collects On Agreement ID'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38887283635446402)
,p_db_column_name=>'SHARE_PERCENT'
,p_display_order=>40
,p_column_identifier=>'D'
,p_column_label=>'Share Percent'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38887316240446403)
,p_db_column_name=>'COLL_AGREEMENT_TYPE_ID'
,p_display_order=>50
,p_column_identifier=>'E'
,p_column_label=>'Coll Agreement Type ID'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38887422050446404)
,p_db_column_name=>'REMIT_TO_ID'
,p_display_order=>60
,p_column_identifier=>'F'
,p_column_label=>'Remit To ID'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38887560728446405)
,p_db_column_name=>'CREATION_CLASS_ID'
,p_display_order=>70
,p_column_identifier=>'G'
,p_column_label=>'Creation Class ID'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38887615471446406)
,p_db_column_name=>'RIGHT_TYPE_ID'
,p_display_order=>80
,p_column_identifier=>'H'
,p_column_label=>'Right Type ID'
,p_column_type=>'NUMBER'
,p_display_text_as=>'LOV_ESCAPE_SC'
,p_column_alignment=>'RIGHT'
,p_rpt_named_lov=>wwv_flow_api.id(62952354574980407)
,p_rpt_show_filter_lov=>'1'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38887703565446407)
,p_db_column_name=>'ROLE_ID'
,p_display_order=>90
,p_column_identifier=>'I'
,p_column_label=>'Role ID'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38887815832446408)
,p_db_column_name=>'START_DATE'
,p_display_order=>100
,p_column_identifier=>'J'
,p_column_label=>'Start Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38887956858446409)
,p_db_column_name=>'END_DATE'
,p_display_order=>110
,p_column_identifier=>'K'
,p_column_label=>'End Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38888004484446410)
,p_db_column_name=>'HEIR_PATH'
,p_display_order=>120
,p_column_identifier=>'L'
,p_column_label=>'Heir Path'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38888168458446411)
,p_db_column_name=>'CREATED_BY'
,p_display_order=>130
,p_column_identifier=>'M'
,p_column_label=>'Created By'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38888257833446412)
,p_db_column_name=>'CREATION_DATE'
,p_display_order=>140
,p_column_identifier=>'N'
,p_column_label=>'Creation Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38888332191446413)
,p_db_column_name=>'LAST_UPDATED_BY'
,p_display_order=>150
,p_column_identifier=>'O'
,p_column_label=>'Last Updated By'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38888477775446414)
,p_db_column_name=>'LAST_UPDATE_DATE'
,p_display_order=>160
,p_column_identifier=>'P'
,p_column_label=>'Last Update Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38888542201446415)
,p_db_column_name=>'EFFECTIVE_DATE'
,p_display_order=>170
,p_column_identifier=>'Q'
,p_column_label=>'Effective Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38888699284446416)
,p_db_column_name=>'SIGNATURE_DATE'
,p_display_order=>180
,p_column_identifier=>'R'
,p_column_label=>'Signature Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38888773555446417)
,p_db_column_name=>'AMMENDMENT_DATE'
,p_display_order=>190
,p_column_identifier=>'S'
,p_column_label=>'Ammendment Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38888879980446418)
,p_db_column_name=>'RENEW_ALLOWED_FLAG'
,p_display_order=>200
,p_column_identifier=>'T'
,p_column_label=>'Renew Allowed Flag'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38888926760446419)
,p_db_column_name=>'RENEWAL_TERM_MONTHS'
,p_display_order=>210
,p_column_identifier=>'U'
,p_column_label=>'Renewal Term Months'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38889075883446420)
,p_db_column_name=>'STATUS'
,p_display_order=>220
,p_column_identifier=>'V'
,p_column_label=>'Status'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38889114698446421)
,p_db_column_name=>'ALERT_DATE'
,p_display_order=>230
,p_column_identifier=>'W'
,p_column_label=>'Alert Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38889298784446422)
,p_db_column_name=>'DOMESTIC_PAY_THRU_DATE'
,p_display_order=>240
,p_column_identifier=>'X'
,p_column_label=>'Domestic Pay Thru Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38889382889446423)
,p_db_column_name=>'FOREIGN_PAY_THRU_DATE'
,p_display_order=>250
,p_column_identifier=>'Y'
,p_column_label=>'Foreign Pay Thru Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38889429979446424)
,p_db_column_name=>'IGNORE_EARN_DATE_FLAG'
,p_display_order=>260
,p_column_identifier=>'Z'
,p_column_label=>'Ignore Earn Date Flag'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38889562853446425)
,p_db_column_name=>'CLASSIFICATION'
,p_display_order=>270
,p_column_identifier=>'AA'
,p_column_label=>'Classification'
,p_column_type=>'STRING'
,p_display_text_as=>'LOV_ESCAPE_SC'
,p_rpt_named_lov=>wwv_flow_api.id(31448311584450840)
,p_rpt_show_filter_lov=>'1'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38908810782902107)
,p_db_column_name=>'HIST_ID'
,p_display_order=>280
,p_column_identifier=>'AB'
,p_column_label=>'History ID'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(42435090795815750)
,p_db_column_name=>'HIST_CREATION_DATE'
,p_display_order=>290
,p_column_identifier=>'AC'
,p_column_label=>'Hist Creation Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(44907218711138601)
,p_db_column_name=>'HIST_CREATED_BY'
,p_display_order=>300
,p_column_identifier=>'AD'
,p_column_label=>'Hist Created By'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(44907605121138605)
,p_db_column_name=>'AGR_END_DATE'
,p_display_order=>310
,p_column_identifier=>'AF'
,p_column_label=>'Agr End Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(38901193162459012)
,p_application_user=>'APXWS_DEFAULT'
,p_report_seq=>10
,p_report_alias=>'389012'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'HIST_ID:HIST_CREATED_BY:HIST_CREATION_DATE:AGR_END_DATE:COLLECTOR_AGREEMENT_ID:COLLECTOR_ID:COLLECTS_ON_AGREEMENT_ID:SHARE_PERCENT:COLL_AGREEMENT_TYPE_ID:REMIT_TO_ID:CREATION_CLASS_ID:RIGHT_TYPE_ID:ROLE_ID:START_DATE:END_DATE:HEIR_PATH:CREATED_BY:CRE'
||'ATION_DATE:LAST_UPDATED_BY:LAST_UPDATE_DATE:EFFECTIVE_DATE:SIGNATURE_DATE:AMMENDMENT_DATE:RENEW_ALLOWED_FLAG:RENEWAL_TERM_MONTHS:STATUS:ALERT_DATE:DOMESTIC_PAY_THRU_DATE:FOREIGN_PAY_THRU_DATE:IGNORE_EARN_DATE_FLAG:CLASSIFICATION:'
,p_sort_column_1=>'HIST_ID'
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
 p_id=>wwv_flow_api.id(23950324549820744)
,p_plug_name=>'Name/Contact Overview'
,p_region_name=>'NOC'
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--scrollBody'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>35
,p_include_in_reg_disp_sel_yn=>'Y'
,p_plug_display_point=>'BODY'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_attribute_01=>'N'
,p_attribute_02=>'HTML'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(23950464724820745)
,p_plug_name=>'Names'
,p_parent_plug_id=>wwv_flow_api.id(23950324549820744)
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>220
,p_plug_display_point=>'BODY'
,p_query_type=>'SQL'
,p_plug_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT CASE --n.NAME_TYPE_ID',
'	WHEN n.NAME_TYPE_ID IN(10081,10061,10021,10041) THEN',
'		NULL',
'	ELSE',
'	    ''<a href="''||APEX_UTIL.PREPARE_URL (''f?p=&APP_ID.:50290:&APP_SESSION.::::P50290_IP_NAME_ID:'' ||n.IP_NAME_ID)||''"><img src="/i/menu/pencil16x16.gif" class="apex-edit-pencil" alt=""></a>'' ',
'	END edit_link,',
'IP_NAME_ID,',
'NAME_TYPE_DESC name_type,',
'  NULL prefix,',
'  FIRST_NAME,',
'  NAME primary_lastname,',
'  NULL suffix,',
'  FULL_NAME,',
'  NAME_NUMBER IPI_name_number,',
'  n.DATE_OF_CREATION,',
'  n.last_update_date,',
'  n.second_last_name',
'FROM AFF_NAME_TYPES nt,',
'  aff_ip_names n',
'WHERE NT.NAME_TYPE_ID = N.NAME_TYPE_ID',
'AND N.IP_ID           = :P260_IP_ID',
'AND :P260_ip_id      IS NOT NULL'))
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
 p_id=>wwv_flow_api.id(23950536821820746)
,p_max_row_count=>'3000000'
,p_show_nulls_as=>'-'
,p_pagination_type=>'ROWS_X_TO_Y'
,p_pagination_display_pos=>'BOTTOM_RIGHT'
,p_report_list_mode=>'TABS'
,p_show_detail_link=>'N'
,p_show_notify=>'Y'
,p_show_flashback=>'N'
,p_download_formats=>'CSV:HTML'
,p_owner=>'VYADAV'
,p_internal_uid=>23950536821820746
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(23520090797313135)
,p_db_column_name=>'EDIT_LINK'
,p_display_order=>10
,p_column_identifier=>'M'
,p_column_label=>'Edit link'
,p_column_type=>'STRING'
,p_display_text_as=>'WITHOUT_MODIFICATION'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(23950624417820747)
,p_db_column_name=>'IP_NAME_ID'
,p_display_order=>20
,p_column_identifier=>'A'
,p_column_label=>'&nbsp;'
,p_column_link=>'f?p=&APP_ID.:50290:&SESSION.::&DEBUG.:RP,50290:P50290_IP_NAME_ID:#IP_NAME_ID#'
,p_column_linktext=>'<img src="#IMAGE_PREFIX#menu/pencil16x16.gif" alt="">'
,p_column_type=>'NUMBER'
,p_heading_alignment=>'LEFT'
,p_display_condition_type=>'NEVER'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(23950778974820748)
,p_db_column_name=>'NAME_TYPE'
,p_display_order=>30
,p_column_identifier=>'B'
,p_column_label=>'Name Type'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(23950898366820749)
,p_db_column_name=>'PREFIX'
,p_display_order=>40
,p_column_identifier=>'C'
,p_column_label=>'Prefix'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(23950983570820750)
,p_db_column_name=>'FIRST_NAME'
,p_display_order=>50
,p_column_identifier=>'D'
,p_column_label=>'First Name'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26245689722751701)
,p_db_column_name=>'PRIMARY_LASTNAME'
,p_display_order=>60
,p_column_identifier=>'E'
,p_column_label=>'Primary Last Name'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26245758026751702)
,p_db_column_name=>'SUFFIX'
,p_display_order=>70
,p_column_identifier=>'F'
,p_column_label=>'Suffix'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26245877238751703)
,p_db_column_name=>'FULL_NAME'
,p_display_order=>80
,p_column_identifier=>'G'
,p_column_label=>'Full Name'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26245908139751704)
,p_db_column_name=>'IPI_NAME_NUMBER'
,p_display_order=>90
,p_column_identifier=>'H'
,p_column_label=>'IPI Name Number'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26246046734751705)
,p_db_column_name=>'DATE_OF_CREATION'
,p_display_order=>100
,p_column_identifier=>'I'
,p_column_label=>'Created On'
,p_column_type=>'DATE'
,p_heading_alignment=>'LEFT'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(28316724528272603)
,p_db_column_name=>'LAST_UPDATE_DATE'
,p_display_order=>110
,p_column_identifier=>'L'
,p_column_label=>'Last Update Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26246274914751707)
,p_db_column_name=>'SECOND_LAST_NAME'
,p_display_order=>120
,p_column_identifier=>'K'
,p_column_label=>'Second Last Name'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(26254779903767063)
,p_application_user=>'APXWS_DEFAULT'
,p_report_seq=>10
,p_report_alias=>'262548'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'EDIT_LINK:NAME_TYPE:PREFIX:FIRST_NAME:PRIMARY_LASTNAME:SUFFIX:FULL_NAME:IPI_NAME_NUMBER:DATE_OF_CREATION:SECOND_LAST_NAME:LAST_UPDATE_DATE:'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(26246719289751712)
,p_plug_name=>'Contacts'
,p_parent_plug_id=>wwv_flow_api.id(23950324549820744)
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>240
,p_plug_display_point=>'BODY'
,p_query_type=>'SQL'
,p_plug_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'--FINAL QUERY',
'',
'SELECT ',
'--distinct aic.ip_contact_id,',
'--MSPI-178 Naveen',
'    xref.relationship_id,',
'    aic.ip_contact_id,',
'    CASE ',
'        WHEN ict.contact_type NOT IN (''MLC_SELF'') ',
'        THEN ''<a href="''||APEX_UTIL.PREPARE_URL (''f?p=&APP_ID.:556:&APP_SESSION.:::556:P556_IP_CONTACT_ID,P556_IP_ID:''||aic.ip_contact_id||'',''||xref.ip_id)||''">''||aic.ip_contact_id||''</a>'' ',
'    ELSE NULL ',
'    END AS ip_contact_id_old,',
'    ict.contact_type,',
'	xref.display_flag,',
'  xref.primary_flag,',
'  full_name,',
'   aic.status,',
'  xref.ip_id,',
'   (SELECT PHONE_NUMBER',
'  FROM',
'    ( SELECT DISTINCT DECODE(PHONE_LINE_TYPE,''FAX'', DECODE(PHONE_COUNTRY_CODE,NULL,NULL,''(''',
'      ||PHONE_COUNTRY_CODE',
'      ||'')'' )',
'      ||DECODE(PHONE_AREA_CODE,NULL,NULL,''(''',
'      ||PHONE_AREA_CODE',
'      ||'')'' )',
'      || DECODE(PHONE_NUMBER,NULL,NULL,''(''',
'      ||SUBSTR(PHONE_NUMBER,1,3)',
'      ||''-''',
'      ||SUBSTR(PHONE_NUMBER,4)',
'      ||'')'')) "PHONE_NUMBER"',
'    FROM aff_ip_contact_points',
'    WHERE status = ''ACTIVE''',
'    and primary_flag = ''Y''',
'	and rownum=1	',
'    AND ip_contact_id = aic.ip_contact_id and phone_line_type=''FAX'' ',
'     )',
'  WHERE PHONE_NUMBER IS NOT NULL',
'  ) FAX,',
'  (SELECT PHONE_NUMBER',
'  FROM',
'    ( SELECT DISTINCT DECODE(PHONE_LINE_TYPE,''WORK'', DECODE(PHONE_COUNTRY_CODE,NULL,NULL,''(''',
'      ||PHONE_COUNTRY_CODE',
'      ||'')'' )',
'      ||DECODE(PHONE_AREA_CODE,NULL,NULL,''(''',
'      ||PHONE_AREA_CODE',
'      ||'')'' )',
'      || DECODE(PHONE_NUMBER,NULL,NULL,''(''',
'      ||SUBSTR(PHONE_NUMBER,1,3)',
'      ||''-''',
'      ||SUBSTR(PHONE_NUMBER,4)',
'      ||'')'')) "PHONE_NUMBER"',
'    FROM aff_ip_contact_points',
'    WHERE status = ''ACTIVE''',
'    and primary_flag = ''Y''',
'	and rownum=1	',
'    AND ip_contact_id = aic.ip_contact_id and phone_line_type=''WORK'' ',
'    ',
'   )',
'  WHERE PHONE_NUMBER IS NOT NULL',
'  ) BUSINESS_PHONE_NUMBER,',
'  ',
'  (SELECT PHONE_NUMBER',
'  FROM',
'    ( SELECT DISTINCT DECODE(PHONE_LINE_TYPE,''GEN'', DECODE(PHONE_COUNTRY_CODE,NULL,NULL,''(''',
'      ||PHONE_COUNTRY_CODE',
'      ||'')'' )',
'      ||DECODE(PHONE_AREA_CODE,NULL,NULL,''(''',
'      ||PHONE_AREA_CODE',
'      ||'')'' )',
'      || DECODE(PHONE_NUMBER,NULL,NULL,''(''',
'      ||SUBSTR(PHONE_NUMBER,1,3)',
'      ||''-''',
'      ||SUBSTR(PHONE_NUMBER,4)',
'      ||'')''),''GEN'', DECODE(PHONE_COUNTRY_CODE,NULL,NULL,''(''',
'      ||PHONE_COUNTRY_CODE',
'      ||'')'' )',
'      ||DECODE(PHONE_AREA_CODE,NULL,NULL,''(''',
'      ||PHONE_AREA_CODE',
'      ||'')'' )',
'      || DECODE(PHONE_NUMBER,NULL,NULL,''(''',
'      ||SUBSTR(PHONE_NUMBER,1,3)',
'      ||''-''',
'      ||SUBSTR(PHONE_NUMBER,4)',
'      ||'')'')) "PHONE_NUMBER"',
'    FROM aff_ip_contact_points',
'   WHERE status = ''ACTIVE''',
'    and primary_flag = ''Y''',
'	and rownum=1	',
'    AND ip_contact_id = aic.ip_contact_id and phone_line_type=''GEN'' ',
'   )',
'  WHERE PHONE_NUMBER IS NOT NULL',
'  ) PHONE_NUMBER,',
'  (SELECT PHONE_NUMBER',
'  FROM',
'    ( SELECT DISTINCT DECODE(PHONE_LINE_TYPE,''MOBILE'', DECODE(PHONE_COUNTRY_CODE,NULL,NULL,''(''',
'      ||PHONE_COUNTRY_CODE',
'      ||'')'' )',
'      ||DECODE(PHONE_AREA_CODE,NULL,NULL,''(''',
'      ||PHONE_AREA_CODE',
'      ||'')'' )',
'      || DECODE(PHONE_NUMBER,NULL,NULL,''(''',
'      ||SUBSTR(PHONE_NUMBER,1,3)',
'      ||''-''',
'      ||SUBSTR(PHONE_NUMBER,4)',
'      ||'')'')) "PHONE_NUMBER"',
'    FROM aff_ip_contact_points',
'    WHERE status = ''ACTIVE''',
'    and primary_flag = ''Y''',
'	and rownum=1	',
'    AND ip_contact_id = aic.ip_contact_id and phone_line_type=''MOBILE'' ',
'       )',
'  WHERE PHONE_NUMBER IS NOT NULL',
'  ) CELL_PHONE_NUMBER,',
'  (SELECT EMAIL_ADDRESS',
'  FROM aff_ip_contact_points',
'    WHERE status = ''ACTIVE''',
'    and primary_flag = ''Y''',
'	and rownum=1	',
'    AND ip_contact_id = aic.ip_contact_id and email_address is not null ',
'      AND rownum         = 1',
'  ) "EMAIL_ADDRESS",',
'  (SELECT URL',
'  FROM aff_ip_contact_points',
'    WHERE status = ''ACTIVE''',
'    and primary_flag = ''Y''',
'	and rownum=1	',
'    AND ip_contact_id = aic.ip_contact_id and url is not null ',
'     ) "URL",',
'     (select tab.sfdc_contact_id from aff_ip_contacts tab where tab.ip_contact_id=aic.ip_contact_id) sfdc_contact_id,',
'     xref.sfdc_relationship_id',
'     ',
'FROM ',
'    AFF_IP_CONTACTS_V aic,',
'    aff_ip_contact_relationship xref ,',
'	AFF_IP_CONTACT_TYPES ict',
'WHERE ICT.CONTACT_TYPE_ID(+) = xref.CONT_TYPE_ID',
'--AND  aic.ip_id           = xref.ip_id',
'AND aic.ip_contact_id     = xref.ip_contact_id',
'AND  xref.ip_id              = :P260_ip_id',
'AND NVL(aic.status, ''A'')   = ''A''',
'AND NVL(xref.status, ''A'')  = ''A''',
'AND :P260_ip_id          IS NOT NULL',
'--Commented by Naveen on 21-07-2020 As per MSPI-178 ',
'--AND ict.contact_type <> ''MLC_SELF'' -- Added by Vignesh Kumar on 7-May-2020 As per MSPI-109',
';'))
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
end;
/
begin
wwv_flow_api.create_worksheet(
 p_id=>wwv_flow_api.id(26246872153751713)
,p_max_row_count=>'3000000'
,p_show_nulls_as=>'-'
,p_pagination_type=>'ROWS_X_TO_Y'
,p_pagination_display_pos=>'BOTTOM_LEFT'
,p_report_list_mode=>'TABS'
,p_show_detail_link=>'N'
,p_show_notify=>'Y'
,p_show_flashback=>'N'
,p_download_formats=>'CSV:HTML:EMAIL'
,p_owner=>'VYADAV'
,p_internal_uid=>26246872153751713
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(23099819753243702)
,p_db_column_name=>'IP_CONTACT_ID'
,p_display_order=>10
,p_column_identifier=>'R'
,p_column_label=>'IP Contact ID'
,p_column_link=>'f?p=&APP_ID.:556:&SESSION.::&DEBUG.:RP,556:P556_IP_CONTACT_ID,P556_IP_ID,P556_RELATIONSHIP_ID:#IP_CONTACT_ID#,#IP_ID#,#RELATIONSHIP_ID#'
,p_column_linktext=>'#IP_CONTACT_ID#'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26247069658751715)
,p_db_column_name=>'CONTACT_TYPE'
,p_display_order=>20
,p_column_identifier=>'B'
,p_column_label=>'Contact Type'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26247188024751716)
,p_db_column_name=>'DISPLAY_FLAG'
,p_display_order=>30
,p_column_identifier=>'C'
,p_column_label=>'Display Flag'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26247207421751717)
,p_db_column_name=>'PRIMARY_FLAG'
,p_display_order=>40
,p_column_identifier=>'D'
,p_column_label=>'Primary Flag'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26247338785751718)
,p_db_column_name=>'FULL_NAME'
,p_display_order=>50
,p_column_identifier=>'E'
,p_column_label=>'Full Name'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26247412783751719)
,p_db_column_name=>'STATUS'
,p_display_order=>60
,p_column_identifier=>'F'
,p_column_label=>'Status'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26247534728751720)
,p_db_column_name=>'IP_ID'
,p_display_order=>70
,p_column_identifier=>'G'
,p_column_label=>'Ip id'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(28659343755759523)
,p_db_column_name=>'FAX'
,p_display_order=>80
,p_column_identifier=>'N'
,p_column_label=>'Fax'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26247642994751721)
,p_db_column_name=>'BUSINESS_PHONE_NUMBER'
,p_display_order=>90
,p_column_identifier=>'H'
,p_column_label=>'Business Phone Number'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26247894630751723)
,p_db_column_name=>'CELL_PHONE_NUMBER'
,p_display_order=>100
,p_column_identifier=>'J'
,p_column_label=>'Cell Phone Number'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26247763955751722)
,p_db_column_name=>'PHONE_NUMBER'
,p_display_order=>110
,p_column_identifier=>'I'
,p_column_label=>'Phone Number'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26247946729751724)
,p_db_column_name=>'EMAIL_ADDRESS'
,p_display_order=>120
,p_column_identifier=>'K'
,p_column_label=>'Email Address'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26248003296751725)
,p_db_column_name=>'URL'
,p_display_order=>130
,p_column_identifier=>'L'
,p_column_label=>'URL'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(17291707270721943)
,p_db_column_name=>'SFDC_CONTACT_ID'
,p_display_order=>140
,p_column_identifier=>'P'
,p_column_label=>'SFDC Contact ID'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(17291882679721944)
,p_db_column_name=>'SFDC_RELATIONSHIP_ID'
,p_display_order=>150
,p_column_identifier=>'Q'
,p_column_label=>'SFDC Relationship ID'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(23099920162243703)
,p_db_column_name=>'IP_CONTACT_ID_OLD'
,p_display_order=>160
,p_column_identifier=>'S'
,p_column_label=>'Ip Contact Id Old'
,p_column_type=>'STRING'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(28184509832944508)
,p_db_column_name=>'RELATIONSHIP_ID'
,p_display_order=>170
,p_column_identifier=>'T'
,p_column_label=>'Relationship Id'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(26293839704207280)
,p_application_user=>'APXWS_DEFAULT'
,p_report_seq=>10
,p_report_alias=>'262939'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'IP_CONTACT_ID:CONTACT_TYPE:DISPLAY_FLAG:PRIMARY_FLAG:FULL_NAME:STATUS:BUSINESS_PHONE_NUMBER:PHONE_NUMBER:CELL_PHONE_NUMBER:EMAIL_ADDRESS:URL:FAX:SFDC_CONTACT_ID:SFDC_:RELATIONSHIP_ID'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(26248445354751729)
,p_plug_name=>'Notes'
,p_parent_plug_id=>wwv_flow_api.id(23950324549820744)
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>250
,p_plug_display_point=>'BODY'
,p_query_type=>'SQL'
,p_plug_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select	note_purpose,			',
'notes	note_display,			',
'note_priority				',
',	created_by			',
',	creation_date	',
', last_updated_by',
', last_update_date',
',	ip_note_id			',
'from	AFF_IP_NOTES			',
'where	ip_id	=	:P260_ip_id	',
'and	nvl(wp_only_flag,''N'')	<>	''Y''	',
'and	:P260_ip_id	is	not	null',
'ORDER BY ip_note_id ,creation_date desc'))
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
 p_id=>wwv_flow_api.id(26248595613751730)
,p_max_row_count=>'3000000'
,p_show_nulls_as=>'-'
,p_pagination_type=>'ROWS_X_TO_Y'
,p_pagination_display_pos=>'BOTTOM_LEFT'
,p_report_list_mode=>'TABS'
,p_show_detail_link=>'N'
,p_show_notify=>'Y'
,p_show_flashback=>'N'
,p_download_formats=>'CSV:HTML:EMAIL'
,p_owner=>'VYADAV'
,p_internal_uid=>26248595613751730
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26249340912751738)
,p_db_column_name=>'IP_NOTE_ID'
,p_display_order=>10
,p_column_identifier=>'H'
,p_column_label=>'&nbsp;'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26248690678751731)
,p_db_column_name=>'NOTE_PURPOSE'
,p_display_order=>20
,p_column_identifier=>'A'
,p_column_label=>'Purpose'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26248742080751732)
,p_db_column_name=>'NOTE_DISPLAY'
,p_display_order=>30
,p_column_identifier=>'B'
,p_column_label=>'Text'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26248847546751733)
,p_db_column_name=>'NOTE_PRIORITY'
,p_display_order=>40
,p_column_identifier=>'C'
,p_column_label=>'Priority'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26248904745751734)
,p_db_column_name=>'CREATED_BY'
,p_display_order=>50
,p_column_identifier=>'D'
,p_column_label=>'Created By'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26249093013751735)
,p_db_column_name=>'CREATION_DATE'
,p_display_order=>60
,p_column_identifier=>'E'
,p_column_label=>'Creation Date'
,p_column_type=>'DATE'
,p_heading_alignment=>'LEFT'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26249198047751736)
,p_db_column_name=>'LAST_UPDATED_BY'
,p_display_order=>70
,p_column_identifier=>'F'
,p_column_label=>'Last Updated By'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26249253405751737)
,p_db_column_name=>'LAST_UPDATE_DATE'
,p_display_order=>80
,p_column_identifier=>'G'
,p_column_label=>'Last Update Date'
,p_column_type=>'DATE'
,p_heading_alignment=>'LEFT'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(26304537577364569)
,p_application_user=>'APXWS_DEFAULT'
,p_report_seq=>10
,p_report_alias=>'263046'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'NOTE_PURPOSE:NOTE_DISPLAY:NOTE_PRIORITY:CREATED_BY:CREATION_DATE:'
,p_sort_column_1=>'CREATION_DATE'
,p_sort_direction_1=>'DESC'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(26249528201751740)
,p_plug_name=>'Change History'
,p_parent_plug_id=>wwv_flow_api.id(23950324549820744)
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>260
,p_plug_display_point=>'BODY'
,p_query_type=>'SQL'
,p_plug_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select * from ',
'( select column_name, date_changed, changed_by,',
'CASE COLUMN_NAME ',
' WHEN ''TAX_ID'' THEN regexp_replace (substr(PREV_VALUE,1,length(PREV_VALUE)-4), ''\d'', ''X'')||substr(PREV_VALUE,-4) ',
' WHEN ''ORGANIZATION_TYPE'' THEN NVL((SELECT description FROM aff_lookups WHERE UPPER(lookup_type) =  ''MC_ORGANIZATION_TYPE'' AND lookup_code = prev_value), prev_value) ',
' else PREV_VALUE END prev_value, ',
' CASE COLUMN_NAME WHEN ''TAX_ID'' THEN regexp_replace (substr(new_value ,1,length(new_value )-4), ''\d'', ''X'')||substr(new_value ,-4) ',
' WHEN ''ORGANIZATION_TYPE'' THEN NVL((SELECT description FROM aff_lookups WHERE UPPER(lookup_type) =  ''MC_ORGANIZATION_TYPE'' AND lookup_code = NEW_VALUE), NEW_VALUE)',
' else NEW_VALUE  END new_value',
'from apcts.hist_change_logs',
'where SQL_CODE =''AFFILIATE''',
'and table_name in ( ''AFF_INTERESTED_PARTIES'',',
'                    ''AFF_IP_ADDRESSES'',',
'                    ''AFF_IP_CONTACTS'',',
'                    ''AFF_IP_CONTACT_POINTS'',',
'                    ''AFF_IP_NAMES'',',
'                    ''AFF_IP_NOTES'',',
'                    ''AFF_MEMBERSHIP_AGREEMENTS''',
'                   )',
'and key_value1 = :P260_IP_ID',
'--and column_name <> ''TAX_ID''',
')',
'--where :P260_SHOW_RESULT = ''SHOW_RESULT'''))
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
 p_id=>wwv_flow_api.id(26249647089751741)
,p_max_row_count=>'1000000'
,p_show_nulls_as=>'-'
,p_pagination_type=>'ROWS_X_TO_Y'
,p_pagination_display_pos=>'BOTTOM_LEFT'
,p_report_list_mode=>'TABS'
,p_show_detail_link=>'N'
,p_show_notify=>'Y'
,p_show_flashback=>'N'
,p_download_formats=>'CSV:HTML:EMAIL:XLS:PDF:RTF'
,p_owner=>'VYADAV'
,p_internal_uid=>26249647089751741
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26249737043751742)
,p_db_column_name=>'COLUMN_NAME'
,p_display_order=>10
,p_column_identifier=>'A'
,p_column_label=>'Column Name'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26249812113751743)
,p_db_column_name=>'DATE_CHANGED'
,p_display_order=>20
,p_column_identifier=>'B'
,p_column_label=>'Date Changed'
,p_column_type=>'DATE'
,p_heading_alignment=>'LEFT'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26249941979751744)
,p_db_column_name=>'CHANGED_BY'
,p_display_order=>30
,p_column_identifier=>'C'
,p_column_label=>'Changed By'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26250023236751745)
,p_db_column_name=>'PREV_VALUE'
,p_display_order=>40
,p_column_identifier=>'D'
,p_column_label=>'Prev Value'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26250184503751746)
,p_db_column_name=>'NEW_VALUE'
,p_display_order=>50
,p_column_identifier=>'E'
,p_column_label=>'New Value'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(26309348403440527)
,p_application_user=>'APXWS_DEFAULT'
,p_report_seq=>10
,p_report_alias=>'263094'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'COLUMN_NAME:DATE_CHANGED:CHANGED_BY:PREV_VALUE:NEW_VALUE'
,p_sort_column_1=>'DATE_CHANGED'
,p_sort_direction_1=>'DESC'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(26273367519107401)
,p_plug_name=>'Addresses'
,p_parent_plug_id=>wwv_flow_api.id(23950324549820744)
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>230
,p_plug_display_point=>'BODY'
,p_query_type=>'SQL'
,p_plug_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT	',
'    --MSPI-177',
'    CASE ',
'        WHEN iat.address_type NOT IN (''MLC_MAILING'') ',
'        THEN ''<a href="''||APEX_UTIL.PREPARE_URL (''f?p=&APP_ID.:50241:&APP_SESSION.:::50241:P50241_IP_ADDRESS_ID,P50241_IP_ID:''||IA.ip_address_id||'',''||IA.ip_id)||''"><img src="/i/menu/pencil16x16.gif" class="apex-edit-pencil" alt=""></a>'' ',
'    ELSE NULL ',
'    END AS ip_address_id,',
'    iat.description as address_type,				',
'	decode(IA.DISPLAY_FLAG,''Y'',''Yes'',''No'') DISPLAY_FLAG,				',
'	IA.ADDRESS1				',
'	||	'',	''		',
'	||	IA.ADDRESS2			',
'	||	'',	''		',
'	||	IA.ADDRESS3			',
'	||	'',	''		',
'	||	IA.ADDRESS4			',
'	address_lines,				',
'	IA.CITY,				',
'	(SELECT meaning FROM aff_lookups WHERE UPPER(lookup_code) = UPPER(IA.STATE) AND lookup_type =''AFF_STATE_CODES'') STATE,			',
'    IA.PROVINCE,',
'	IA.POSTAL_CODE,				',
'--	ia.County,				',
'--	ia.Province,				',
'	(SELECT tisan FROM   aff_territories_v WHERE territory_id = ia.Country) COUNTRY,				',
'	IA.ADDRESS	Display_address,			',
'	decode(ia.Status,''I'',''Inactive'',''B'',''Bad Address'',''Active'') Status,',
'	ia.ip_id				',
'	FROM	AFF_IP_ADDRESS_TYPES	iat,	AFF_IP_ADDRESSES	ia',
'	WHERE	IA.ADDRESS_TYPE_ID	=	iat.address_type_id	',
'	AND	IA.IP_ID	=	:P260_ip_id',
'and :P260_ip_id is not null'))
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
 p_id=>wwv_flow_api.id(26273446925107402)
,p_max_row_count=>'3000000'
,p_show_nulls_as=>'-'
,p_pagination_type=>'ROWS_X_TO_Y'
,p_pagination_display_pos=>'BOTTOM_LEFT'
,p_report_list_mode=>'TABS'
,p_show_detail_link=>'N'
,p_show_notify=>'Y'
,p_show_flashback=>'N'
,p_download_formats=>'CSV:HTML:EMAIL'
,p_owner=>'VYADAV'
,p_internal_uid=>26273446925107402
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(20005834694757009)
,p_db_column_name=>'IP_ADDRESS_ID'
,p_display_order=>10
,p_column_identifier=>'L'
,p_column_label=>'Edit link'
,p_column_type=>'STRING'
,p_display_text_as=>'WITHOUT_MODIFICATION'
,p_column_alignment=>'CENTER'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26273624818107404)
,p_db_column_name=>'ADDRESS_TYPE'
,p_display_order=>20
,p_column_identifier=>'B'
,p_column_label=>'Address Type'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26273773604107405)
,p_db_column_name=>'DISPLAY_FLAG'
,p_display_order=>30
,p_column_identifier=>'C'
,p_column_label=>'Display Flag'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26273812794107406)
,p_db_column_name=>'ADDRESS_LINES'
,p_display_order=>40
,p_column_identifier=>'D'
,p_column_label=>'Address Lines'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26273932736107407)
,p_db_column_name=>'CITY'
,p_display_order=>50
,p_column_identifier=>'E'
,p_column_label=>'City'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26274018909107408)
,p_db_column_name=>'STATE'
,p_display_order=>60
,p_column_identifier=>'F'
,p_column_label=>'State'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26274170323107409)
,p_db_column_name=>'POSTAL_CODE'
,p_display_order=>70
,p_column_identifier=>'G'
,p_column_label=>'Postal Code'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26274208934107410)
,p_db_column_name=>'COUNTRY'
,p_display_order=>80
,p_column_identifier=>'H'
,p_column_label=>'Country'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26274322115107411)
,p_db_column_name=>'DISPLAY_ADDRESS'
,p_display_order=>90
,p_column_identifier=>'I'
,p_column_label=>'Display Address'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26274413876107412)
,p_db_column_name=>'STATUS'
,p_display_order=>100
,p_column_identifier=>'J'
,p_column_label=>'Status'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(26274532367107413)
,p_db_column_name=>'IP_ID'
,p_display_order=>110
,p_column_identifier=>'K'
,p_column_label=>'Ip id'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(39045858321278450)
,p_db_column_name=>'PROVINCE'
,p_display_order=>120
,p_column_identifier=>'M'
,p_column_label=>'Province'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(26283309974149578)
,p_application_user=>'APXWS_DEFAULT'
,p_report_seq=>10
,p_report_alias=>'262834'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'IP_ADDRESS_ID:ADDRESS_TYPE:DISPLAY_FLAG:ADDRESS_LINES:CITY:STATE:PROVINCE:POSTAL_CODE:COUNTRY:DISPLAY_ADDRESS:STATUS:'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(28659957399759529)
,p_name=>'Unlinked Contact Points'
,p_parent_plug_id=>wwv_flow_api.id(23950324549820744)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>245
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#:t-Report--altRowsDefault:t-Report--rowHighlight'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select	icpt.contact_point_type,	icp.display_flag,	icp.primary_flag	',
',	icp.email_address,	icp.phone_country_code||'' ''||	icp.phone_area_code||''-''||icp.phone_number Phone_number',
',	icp.phone_extension,	icp.phone_line_type,	icp.url,	icp.status',
',	icp.ip_id			',
',   APEX_ITEM.SELECT_LIST_FROM_QUERY(1,NULL,''select full_name, ip_contact_id from AFF_IP_CONTACTS_V where ip_id = ''|| :P260_ip_id,  NULL, ''YES'', NULL,''-Select-'') AS Link_to',
',   APEX_ITEM.HIDDEN(2, ip_contact_point_id) as ip_cp_id',
', ''<a class="deleteAccButton" contact-point-id=''||icp.IP_CONTACT_POINT_ID||'' onclick="removeRole(''''''||icp.ip_contact_point_id||'''''');" href="javascript:void(0);"><img title="Remove" alt="Remove" src="/i/ws/redx.gif"></a>'' DEL_ONE',
'from	AFF_IP_CONTACT_POINTS	icp,	AFF_IP_CONT_POINT_TYPES	icpt',
'where	ICP.CONT_POINT_TYPE_ID	=	ICPT.CONT_POINT_TYPE_ID	',
'and	icp.ip_id	=	:P260_ip_id',
'and icp.status = ''ACTIVE''',
'and :P260_ip_id is not null',
'and ip_contact_id is NULL '))
,p_display_when_condition=>'select 1 from AFF_IP_CONTACT_POINTS where ip_id = :P260_ip_id and ip_contact_id is NULL'
,p_display_condition_type=>'EXISTS'
,p_header=>'Choose a contact from "Link To" drop down and press "Link" button'
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
 p_id=>wwv_flow_api.id(28660038001759530)
,p_query_column_id=>1
,p_column_alias=>'CONTACT_POINT_TYPE'
,p_column_display_sequence=>3
,p_column_heading=>'Contact Point Type'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(28660190844759531)
,p_query_column_id=>2
,p_column_alias=>'DISPLAY_FLAG'
,p_column_display_sequence=>1
,p_column_heading=>'Display Flag'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(28660216223759532)
,p_query_column_id=>3
,p_column_alias=>'PRIMARY_FLAG'
,p_column_display_sequence=>2
,p_column_heading=>'Primary Flag'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(28660345109759533)
,p_query_column_id=>4
,p_column_alias=>'EMAIL_ADDRESS'
,p_column_display_sequence=>7
,p_column_heading=>'Email Address'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(28660459929759534)
,p_query_column_id=>5
,p_column_alias=>'PHONE_NUMBER'
,p_column_display_sequence=>5
,p_column_heading=>'Phone Number'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(28660534928759535)
,p_query_column_id=>6
,p_column_alias=>'PHONE_EXTENSION'
,p_column_display_sequence=>6
,p_column_heading=>'Phone Extension'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(28660686056759536)
,p_query_column_id=>7
,p_column_alias=>'PHONE_LINE_TYPE'
,p_column_display_sequence=>4
,p_column_heading=>'Phone Line Type'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(28660718804759537)
,p_query_column_id=>8
,p_column_alias=>'URL'
,p_column_display_sequence=>8
,p_column_heading=>'URL'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(28660885378759538)
,p_query_column_id=>9
,p_column_alias=>'STATUS'
,p_column_display_sequence=>9
,p_column_heading=>'Status'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(28660970982759539)
,p_query_column_id=>10
,p_column_alias=>'IP_ID'
,p_column_display_sequence=>10
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(28661020724759540)
,p_query_column_id=>11
,p_column_alias=>'LINK_TO'
,p_column_display_sequence=>11
,p_column_heading=>'Link To'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_display_as=>'WITHOUT_MODIFICATION'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(28661362258759543)
,p_query_column_id=>12
,p_column_alias=>'IP_CP_ID'
,p_column_display_sequence=>13
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_display_as=>'WITHOUT_MODIFICATION'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(60167223905808439)
,p_query_column_id=>13
,p_column_alias=>'DEL_ONE'
,p_column_display_sequence=>12
,p_column_heading=>'&nbsp;'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_display_as=>'WITHOUT_MODIFICATION'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(34040844461522801)
,p_plug_name=>'Consolidated Holds'
,p_region_name=>'CONSHOLD'
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>230
,p_include_in_reg_disp_sel_yn=>'Y'
,p_plug_display_point=>'BODY'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_attribute_01=>'N'
,p_attribute_02=>'HTML'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(34040902188522802)
,p_plug_name=>'Party Holds'
,p_parent_plug_id=>wwv_flow_api.id(34040844461522801)
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>10
,p_plug_display_point=>'BODY'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_attribute_01=>'N'
,p_attribute_02=>'HTML'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(34042375590522816)
,p_plug_name=>'Share Level Holds'
,p_parent_plug_id=>wwv_flow_api.id(34040844461522801)
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>20
,p_plug_display_point=>'BODY'
,p_query_type=>'SQL'
,p_plug_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select PP.PROPERTY_ID, psp.COLLECTOR_ID ,pp.property_number,psp.SHARE_PERCENTAGE,AFT.RIGHT_DESCRIPTION,AR.ROLE_DESC,PHT.DESCRIPTION',
'from 	prop_properties pp , ',
'       prop_share_collectors psp , ',
'       pay_hold_types pht,',
'       AFF_RIGHT_TYPES AFT,',
'       AFF_ROLES  AR',
'where  pp.PROPERTY_ID = psp.PROPERTY_ID ',
'and    psp.HOLD_TYPE_ID =  pht.HOLD_TYPE_ID',
'and    psp.COLLECTOR_ID = :P260_IP_ID and		pp.source_id = 7 and pp.property_type_id =1',
'and    AFT.RIGHT_TYPE_ID= psp.RIGHT_TYPE_ID',
'AND    AR.ROLE_ID = PSP.ROLE_ID',
'AND     PSP.END_DATE_ACTIVE is NULL',
'AND :P260_QUERY_CONSOLIDATED =''Y''',
'group by psp.COLLECTOR_ID,psp.SHARE_PERCENTAGE,AFT.RIGHT_DESCRIPTION,pp.property_number,AR.ROLE_DESC,PHT.DESCRIPTION,PP.PROPERTY_ID;'))
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
 p_id=>wwv_flow_api.id(34042427377522817)
,p_max_row_count=>'1000000'
,p_show_nulls_as=>'-'
,p_pagination_type=>'ROWS_X_TO_Y'
,p_pagination_display_pos=>'BOTTOM_RIGHT'
,p_report_list_mode=>'TABS'
,p_show_detail_link=>'N'
,p_show_notify=>'Y'
,p_download_formats=>'CSV:HTML:EMAIL:XLS:PDF:RTF'
,p_owner=>'RGARG'
,p_internal_uid=>34042427377522817
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34042858164522821)
,p_db_column_name=>'PROPERTY_NUMBER'
,p_display_order=>10
,p_column_identifier=>'D'
,p_column_label=>'Property number'
,p_column_link=>'f?p=&APP_ID.:310:&SESSION.::&DEBUG.:RP:P310_PROP_ID:#PROPERTY_ID#'
,p_column_linktext=>'#PROPERTY_NUMBER#'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34042582411522818)
,p_db_column_name=>'COLLECTOR_ID'
,p_display_order=>20
,p_column_identifier=>'A'
,p_column_label=>'Collector id'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34042704536522820)
,p_db_column_name=>'RIGHT_DESCRIPTION'
,p_display_order=>30
,p_column_identifier=>'C'
,p_column_label=>'Right description'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34042967192522822)
,p_db_column_name=>'DESCRIPTION'
,p_display_order=>40
,p_column_identifier=>'E'
,p_column_label=>'Hold Description'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34043251882522825)
,p_db_column_name=>'SHARE_PERCENTAGE'
,p_display_order=>50
,p_column_identifier=>'F'
,p_column_label=>'Share percentage'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34043352287522826)
,p_db_column_name=>'ROLE_DESC'
,p_display_order=>60
,p_column_identifier=>'G'
,p_column_label=>'Role desc'
,p_column_type=>'STRING'
);
end;
/
begin
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34295292252208914)
,p_db_column_name=>'PROPERTY_ID'
,p_display_order=>70
,p_column_identifier=>'H'
,p_column_label=>'Property id'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(34070693535319487)
,p_application_user=>'APXWS_DEFAULT'
,p_report_seq=>10
,p_report_alias=>'340707'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'DESCRIPTION:PROPERTY_NUMBER:SHARE_PERCENTAGE:RIGHT_DESCRIPTION:ROLE_DESC::PROPERTY_ID'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(34294360991208905)
,p_plug_name=>'Memo Adjustment'
,p_parent_plug_id=>wwv_flow_api.id(34040844461522801)
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>30
,p_plug_display_point=>'BODY'
,p_query_type=>'SQL'
,p_plug_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT MEMO_NUM,FROM_PUBLISHER_NUM,SONG_CODE,PUBLISHER_SEQ_NUM,PUBLISHER_NUM,PUBLISHER_SPLIT ',
'FROM   MC_DIST_MEMO_ADJ',
'WHERE  MEMO_TYPE_CODE=''PS'' ',
'AND     :P260_QUERY_CONSOLIDATED =''Y''',
'AND		(FROM_PUBLISHER_NUM_IP_ID= :P260_IP_ID OR PUBLISHER_IP_ID =:P260_IP_ID);'))
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
 p_id=>wwv_flow_api.id(34294472822208906)
,p_max_row_count=>'1000000'
,p_show_nulls_as=>'-'
,p_pagination_type=>'ROWS_X_TO_Y'
,p_pagination_display_pos=>'BOTTOM_RIGHT'
,p_report_list_mode=>'TABS'
,p_show_detail_link=>'N'
,p_show_notify=>'Y'
,p_download_formats=>'CSV:HTML:EMAIL:XLS:PDF:RTF'
,p_owner=>'RGARG'
,p_internal_uid=>34294472822208906
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34294568031208907)
,p_db_column_name=>'MEMO_NUM'
,p_display_order=>10
,p_column_identifier=>'A'
,p_column_label=>'Memo num'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34294677438208908)
,p_db_column_name=>'FROM_PUBLISHER_NUM'
,p_display_order=>20
,p_column_identifier=>'B'
,p_column_label=>'HFA CODE'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34294788262208909)
,p_db_column_name=>'SONG_CODE'
,p_display_order=>30
,p_column_identifier=>'C'
,p_column_label=>'Song code'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34294892828208910)
,p_db_column_name=>'PUBLISHER_SEQ_NUM'
,p_display_order=>40
,p_column_identifier=>'D'
,p_column_label=>'Publisher seq num'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34294927506208911)
,p_db_column_name=>'PUBLISHER_NUM'
,p_display_order=>50
,p_column_identifier=>'E'
,p_column_label=>'Publisher num'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(34295083922208912)
,p_db_column_name=>'PUBLISHER_SPLIT'
,p_display_order=>60
,p_column_identifier=>'F'
,p_column_label=>'Publisher split'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(34314447185633242)
,p_application_user=>'APXWS_DEFAULT'
,p_report_seq=>10
,p_report_alias=>'343145'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'MEMO_NUM:FROM_PUBLISHER_NUM:SONG_CODE:PUBLISHER_SEQ_NUM:PUBLISHER_NUM:PUBLISHER_SPLIT'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(37332744869575513)
,p_plug_name=>'Hierarchy Agr.'
,p_region_name=>'HIAGH'
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>150
,p_include_in_reg_disp_sel_yn=>'Y'
,p_plug_display_point=>'BODY'
,p_query_type=>'SQL'
,p_plug_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT ',
'       CASE ',
'         WHEN ah.hier_level = 0',
'         THEN ''<img src="/i/menu/pencil16x16.gif" alt="">''',
'--         THEN ''<span class="t-Icon fa fa-pencil" aria-hidden="true"></span>''',
'         ELSE',
'           '' ''',
'        END EDIT,',
'        CASE ',
'         WHEN ah.hier_level = 0 AND jns.status IN(''QUEUED'',''QUEUED_FAST'')',
'         THEN ''<a >Scheduled</a>''',
'         WHEN ah.hier_level = 0 AND jns.status=''IN PROGRESS''        --Adding as part of SPI-2670',
'         THEN ''<a >In Progress</a>''',
'         WHEN ah.hier_level = 0 AND jns.status=''PROCESSED''',
'         THEN ''<img src="/i/menu/pencil16x16.gif" alt="">''',
'         WHEN ah.hier_level = 0 AND jns.status=''ERRORED''',
'         THEN ''<a >ERRORED</a>''',
'         WHEN ah.hier_level = 0 AND jns.schedule_id IS NULL',
'         THEN ''<img src="/i/menu/pencil16x16.gif" alt="">''',
'         ELSE',
'           '' ''',
'       END NOW,',
'        /*',
'        CASE',
'        WHEN ah.hier_level = 0 AND jns.schedule_id IS NULL',
'         THEN ''<img src="/i/menu/pencil16x16.gif" alt="">''',
'         WHEN ah.hier_level = 0 AND jns.schedule_id IS NOT NULL',
'         THEN ''<a >Scheduled</a>''',
'         ELSE',
'           '' ''',
'        END NOW,',
'        */',
'       CASE ',
'         WHEN (ah.hier_level = 0 AND ah.relate_account_number IS NOT NULL)',
'         THEN ''<a href="''||APEX_UTIL.PREPARE_URL (''f?p=&APP_ID.:480:&APP_SESSION.::::P480_PARTY_ID,P480_HIERARCHY_ID,P480_COLL_FOR,P480_HIER_LEVEL:'' ||:P260_IP_ID||'',''||aff_hier_id||'',''||parent_party_id||'','')||''"><img src="/i/menu/pencil16x16.gif" cl'
||'ass="apex-edit-pencil" alt=""></a>'' ',
'         WHEN (ah.hier_level = 0 AND ah.relate_account_number IS NULL)',
'         THEN ''<a href="''||APEX_UTIL.PREPARE_URL (''f?p=&APP_ID.:480:&APP_SESSION.::::P480_PARTY_ID,P480_HIERARCHY_ID,P480_COLL_FOR,P480_HIER_LEVEL:'' ||:P260_IP_ID||'',''||aff_hier_id||'',''||child_party_id||'','')||''"><img src="/i/menu/pencil16x16.gif" cla'
||'ss="apex-edit-pencil" alt=""></a>''',
'         ELSE',
'           '' ''',
'        END DEFAULT_COLLECTION_AGREEMENT, ',
'        CASE ',
'         WHEN ah.hier_level = 0',
'         THEN ''<img src="/i/menu/pencil16x16.gif" alt="">''',
'--         THEN ''<span class="t-Icon fa fa-pencil" aria-hidden="true"></span>''',
'         ELSE',
'           '' ''',
'        END EDIT_RIGHT_TYPE',
'      ,ah.aff_hier_id',
'      ,ah.hier_level',
'      ,ah.child_party_id',
'      ,(SELECT ip_number FROM aff_interested_parties WHERE ip_id = ah.child_party_id) CHILD_IP_NUMBER',
'      ,spi_screen_utility.get_party_name(ah.child_party_id) "Child Name"',
'      ,(SELECT NVL2(ra.aff_category_hfa, ra.aff_category||'' / ''||ra.aff_category_hfa, ra.aff_category) ',
'						 FROM rep_affiliates ra',
'						 WHERE ra.ip_id = ah.child_party_id)c_Derived_Status',
'      ,ah.parent_party_id',
'      ,(SELECT ip_number FROM aff_interested_parties WHERE ip_id = ah.parent_party_id) PARENT_IP_NUMBER',
'      ,spi_screen_utility.get_party_name(ah.parent_party_id) "Parent Name"',
'      ,(SELECT NVL2(ra.aff_category_hfa, ra.aff_category||'' / ''||ra.aff_category_hfa, ra.aff_category) ',
'						 FROM rep_affiliates ra',
'						 WHERE ra.ip_id = ah.parent_party_id) p_Derived_Status',
'      ,ah.RELATE_ACCOUNT_NUMBER',
'      ,ah.group_number',
'      ,CASE ',
'         WHEN ah.hier_level = 0',
'         THEN spi_screen_utility.get_ip_song_count(ah.child_party_id,ah.parent_party_id,1,ah.aff_hier_id)  ',
'         ELSE',
'           '' ''',
'        END "Perf Count" ',
'      ,CASE ',
'         WHEN ah.hier_level = 0',
'         THEN spi_screen_utility.get_ip_song_count(ah.child_party_id,ah.parent_party_id,2,ah.aff_hier_id)  ',
'         ELSE',
'           '' ''',
'        END "Mech Count" ',
'      ,CASE ',
'         WHEN ah.hier_level = 0',
'         THEN spi_screen_utility.get_ip_song_count(ah.child_party_id,ah.parent_party_id,NULL,ah.aff_hier_id)  ',
'         ELSE',
'           '' ''',
'        END "Total Song Count" ',
'      ,spi_screen_utility.GET_HIER_RIGHT_TYPE(ah.aff_hier_id) RIGHT_TYPE',
'      ,spi_screen_utility.get_hfa_number(ah.child_party_id) "Child HFA #"',
'      ,spi_screen_utility.get_hfa_number(ah.parent_party_id) "Parent HFA #"',
'      , CASE ',
'            WHEN ah.hier_level = 0 AND ah.child_party_id <> ah.parent_party_id AND ah.relate_account_number IS NOT NULL THEN',
'                CASE ',
'                    WHEN ah.hold_type_id IS NULL THEN ',
'                        ''<i class="fa fa-plus" style="color:red"></i>''  ',
'                    ELSE ',
'                        ''<i style="color:red">''||',
'                        (',
'                            SELECT pht.description ',
'                            FROM  pay_hold_types pht ',
'                            WHERE pht.hold_type_id = ah.hold_type_id',
'                        )||''</i>''',
'                END',
'            ELSE',
'                NULL',
'        END hold_type,',
'        ah.hold_type_id,',
'        CASE WHEN ah.hier_level = 0 THEN ',
'         mc_utility_pkg.get_payee(spi_screen_utility.get_hfa_number(ah.child_party_id),null,sysdate,ah.group_number) ',
'        ELSE null END Payee',
'FROM  aff_hierarchy ah,',
'      job_nowing_schedules jns',
'WHERE ah.GROUP_NUMBER  IN (',
'                            SELECT DISTINCT group_number',
'                              FROM aff_hierarchy',
'                             WHERE child_party_id  = :P260_IP_ID --3546918',
'                               AND hier_level  = 0',
'                               AND end_date IS NULL                        ',
'                           ) ',
'   and :P260_QUERY_HIE=''Y''',
'   AND ah.end_date IS NULL  --Added by RR on 03-Sep-2018 ',
'   AND ah.aff_hier_id  =  jns.old_aff_hier_id(+)',
'   --AND NVL(jns.status,''QUEUED'')= ''QUEUED'' --With this table, we performed left join so this col will be null when no data on jns table.',
'ORDER BY ah.group_number,ah.hier_level '))
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
 p_id=>wwv_flow_api.id(37335306440575539)
,p_max_row_count=>'1000000'
,p_show_nulls_as=>'-'
,p_pagination_type=>'ROWS_X_TO_Y'
,p_pagination_display_pos=>'BOTTOM_RIGHT'
,p_report_list_mode=>'TABS'
,p_show_detail_link=>'N'
,p_show_notify=>'Y'
,p_download_formats=>'CSV:HTML:EMAIL:XLS:PDF:RTF'
,p_owner=>'KBAKSHI'
,p_internal_uid=>37335306440575539
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37335453164575540)
,p_db_column_name=>'EDIT'
,p_display_order=>10
,p_column_identifier=>'A'
,p_column_label=>'Edit'
,p_column_link=>'f?p=&APP_ID.:400:&SESSION.::&DEBUG.:RP,400:P400_IP_ID,P400_AFF_HIER_ID,P400_GRP_NUM,P400_IP_NUMBER:&P260_IP_ID.,#AFF_HIER_ID#,#GROUP_NUMBER#,&P260_IP_NUMBER.'
,p_column_linktext=>'#EDIT#'
,p_column_type=>'STRING'
,p_display_condition_type=>'NEVER'
,p_security_scheme=>wwv_flow_api.id(17200066639506016)
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37335535002575541)
,p_db_column_name=>'NOW'
,p_display_order=>20
,p_column_identifier=>'B'
,p_column_label=>'Edit Nowing'
,p_column_link=>'f?p=&APP_ID.:11:&SESSION.::&DEBUG.:RP,11:P11_GROUP_NUMBER,P11_OLD_AFF_HIERARCHY_ID,P11_IP_ID:#GROUP_NUMBER#,#AFF_HIER_ID#,&P260_IP_ID.'
,p_column_linktext=>'#NOW#'
,p_column_type=>'STRING'
,p_display_text_as=>'WITHOUT_MODIFICATION'
,p_security_scheme=>wwv_flow_api.id(33811989881355859)
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37335690061575542)
,p_db_column_name=>'DEFAULT_COLLECTION_AGREEMENT'
,p_display_order=>30
,p_column_identifier=>'C'
,p_column_label=>'Edit Default Collection Agreement'
,p_column_link=>'f?p=&APP_ID.:480:&SESSION.::&DEBUG.:RP,480:P480_PARTY_ID,P480_HIERARCHY_ID,P480_COLL_FOR,P480_SEL_HIER_LEVEL,P480_HIER_LEVEL:&P260_IP_ID.,#AFF_HIER_ID#,#PARENT_PARTY_ID#,,'
,p_column_linktext=>'#DEFAULT_COLLECTION_AGREEMENT#'
,p_column_type=>'STRING'
,p_display_text_as=>'WITHOUT_MODIFICATION'
,p_security_scheme=>wwv_flow_api.id(17200066639506016)
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37335730253575543)
,p_db_column_name=>'EDIT_RIGHT_TYPE'
,p_display_order=>40
,p_column_identifier=>'D'
,p_column_label=>'Edit Right Type'
,p_column_link=>'f?p=&APP_ID.:16:&SESSION.::&DEBUG.:RP,16:P16_AFF_HIER_ID,P16_IP_ID:#AFF_HIER_ID#,&P260_IP_ID.'
,p_column_linktext=>'#EDIT_RIGHT_TYPE#'
,p_column_type=>'STRING'
,p_display_text_as=>'WITHOUT_MODIFICATION'
,p_security_scheme=>wwv_flow_api.id(69665980648432325)
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37337458655580010)
,p_db_column_name=>'HOLD_TYPE'
,p_display_order=>50
,p_column_identifier=>'U'
,p_column_label=>'Add/Edit Hold Type'
,p_column_link=>'f?p=&APP_ID.:50420:&SESSION.::&DEBUG.:RP,50420:P50420_AFF_HIER_ID,P50420_CHILD,P50420_PARENT,P50420_HOLD_TYPE_ID,P50420_IP_TYPE:#AFF_HIER_ID#,#Child Name#,#Parent Name#,#HOLD_TYPE_ID#,&P260_IP_TYPE.'
,p_column_linktext=>'#HOLD_TYPE#'
,p_column_link_attr=>'title="Click to add/edit"'
,p_column_type=>'STRING'
,p_display_text_as=>'WITHOUT_MODIFICATION'
,p_security_scheme=>wwv_flow_api.id(71705400949038146)
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37335817590575544)
,p_db_column_name=>'AFF_HIER_ID'
,p_display_order=>60
,p_column_identifier=>'E'
,p_column_label=>'Hierarchy ID'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37336921455580005)
,p_db_column_name=>'GROUP_NUMBER'
,p_display_order=>70
,p_column_identifier=>'P'
,p_column_label=>'Group Number'
,p_column_type=>'NUMBER'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37337190398580007)
,p_db_column_name=>'RIGHT_TYPE'
,p_display_order=>80
,p_column_identifier=>'R'
,p_column_label=>'Right Type'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37335927489575545)
,p_db_column_name=>'HIER_LEVEL'
,p_display_order=>90
,p_column_identifier=>'F'
,p_column_label=>'Hierarchy Level'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37336238097575548)
,p_db_column_name=>'Child Name'
,p_display_order=>100
,p_column_identifier=>'I'
,p_column_label=>'Child Name'
,p_column_link=>'f?p=&APP_ID.:260:&SESSION.:APEX_CLONE_SESSION:&DEBUG.:RP,50210:P260_IP_ID,P260_CALLING_PAGE_ID,P260_IP_NUMBER,P260_PREV_IP_ID,P260_PREV_IP_NUMBER:#CHILD_PARTY_ID#,260,#CHILD_IP_NUMBER#,&P260_IP_ID.,&P260_IP_NUMBER.'
,p_column_linktext=>'#Child Name#'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37337238070580008)
,p_db_column_name=>'Child HFA #'
,p_display_order=>110
,p_column_identifier=>'S'
,p_column_label=>'Child hfa #'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37336364990575549)
,p_db_column_name=>'C_DERIVED_STATUS'
,p_display_order=>120
,p_column_identifier=>'J'
,p_column_label=>'Child Derived Status'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37336663049580002)
,p_db_column_name=>'Parent Name'
,p_display_order=>130
,p_column_identifier=>'M'
,p_column_label=>'Parent Name'
,p_column_link=>'f?p=&APP_ID.:260:&SESSION.:APEX_CLONE_SESSION:&DEBUG.:RP,50210:P260_IP_ID,P260_CALLING_PAGE_ID,P260_IP_NUMBER,P260_PREV_IP_ID,P260_PREV_IP_NUMBER:#PARENT_PARTY_ID#,260,#PARENT_IP_NUMBER#,&P260_IP_ID.,&P260_IP_NUMBER.'
,p_column_linktext=>'#Parent Name#'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37337314163580009)
,p_db_column_name=>'Parent HFA #'
,p_display_order=>140
,p_column_identifier=>'T'
,p_column_label=>'Parent hfa #'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37336731921580003)
,p_db_column_name=>'P_DERIVED_STATUS'
,p_display_order=>150
,p_column_identifier=>'N'
,p_column_label=>'Parent Derived Status'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37336857578580004)
,p_db_column_name=>'RELATE_ACCOUNT_NUMBER'
,p_display_order=>160
,p_column_identifier=>'O'
,p_column_label=>'HFA Relate Account #'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37336088835575546)
,p_db_column_name=>'CHILD_PARTY_ID'
,p_display_order=>170
,p_column_identifier=>'G'
,p_column_label=>'Child party id'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37336479468575550)
,p_db_column_name=>'PARENT_PARTY_ID'
,p_display_order=>180
,p_column_identifier=>'K'
,p_column_label=>'Parent party id'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37336100954575547)
,p_db_column_name=>'CHILD_IP_NUMBER'
,p_display_order=>190
,p_column_identifier=>'H'
,p_column_label=>'Child ip number'
,p_column_type=>'STRING'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37336532713580001)
,p_db_column_name=>'PARENT_IP_NUMBER'
,p_display_order=>200
,p_column_identifier=>'L'
,p_column_label=>'Parent ip number'
,p_column_type=>'STRING'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37337549130580011)
,p_db_column_name=>'HOLD_TYPE_ID'
,p_display_order=>220
,p_column_identifier=>'V'
,p_column_label=>'Hold type id'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(89133548067626304)
,p_db_column_name=>'Perf Count'
,p_display_order=>230
,p_column_identifier=>'W'
,p_column_label=>'Perf count'
,p_column_type=>'STRING'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(89133608352626305)
,p_db_column_name=>'Mech Count'
,p_display_order=>240
,p_column_identifier=>'X'
,p_column_label=>'Mech count'
,p_column_type=>'STRING'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(89133747786626306)
,p_db_column_name=>'Total Song Count'
,p_display_order=>250
,p_column_identifier=>'Y'
,p_column_label=>'All Right types Count'
,p_column_link=>'f?p=&APP_ID.:490:&SESSION.::&DEBUG.:RP:P490_CHILD_PARTY_ID,P490_PARENT_PARTY_ID,P490_RELATE_ACC_NUM,P490_AFF_HIER_ID:#CHILD_PARTY_ID#,#PARENT_PARTY_ID#,#RELATE_ACCOUNT_NUMBER#,#AFF_HIER_ID#'
,p_column_linktext=>'#Total Song Count#'
,p_column_type=>'STRING'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(2456792789447929)
,p_db_column_name=>'PAYEE'
,p_display_order=>260
,p_column_identifier=>'AA'
,p_column_label=>'Payee'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(37349565511581543)
,p_application_user=>'APXWS_DEFAULT'
,p_report_seq=>10
,p_report_alias=>'373496'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'NOW:DEFAULT_COLLECTION_AGREEMENT:EDIT_RIGHT_TYPE:HOLD_TYPE:AFF_HIER_ID:GROUP_NUMBER:RIGHT_TYPE:HIER_LEVEL:Child Name:Child HFA #:C_DERIVED_STATUS:Parent Name:Parent HFA #:P_DERIVED_STATUS:RELATE_ACCOUNT_NUMBER:PAYEE:Perf Count:Mech Count:Total Song C'
||'ount:'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(37338287862580018)
,p_plug_name=>'View Used Hierarchy'
,p_parent_plug_id=>wwv_flow_api.id(37332744869575513)
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>10
,p_plug_display_point=>'BODY'
,p_query_type=>'SQL'
,p_plug_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT ',
'       aff_hier_id',
'      ,hier_level hier_lev',
'      ,child_party_id',
'      ,(SELECT ip_number FROM aff_interested_parties WHERE ip_id = child_party_id) CHILD_IP_NUMBER',
'      ,spi_screen_utility.get_party_name(child_party_id) "Child Name"',
'      ,parent_party_id',
'      ,(SELECT ip_number FROM aff_interested_parties WHERE ip_id = parent_party_id) PARENT_IP_NUMBER',
'      ,spi_screen_utility.get_party_name(parent_party_id) "Parent Name"',
'      ,RELATE_ACCOUNT_NUMBER',
'      ,group_number',
'      ,case when hier_level  = 0 THEN spi_screen_utility.GET_HIER_RIGHT_TYPE(aff_hier_id)  ELSE NULL END RIGHT_TYPE',
'      ,CASE WHEN child_party_id = :P260_IP_ID THEN ''<B>''||spi_screen_utility.get_hfa_number(child_party_id)||''</B>'' ELSE spi_screen_utility.get_hfa_number(child_party_id) END  Child_HFA',
'      ,CASE WHEN parent_party_id = :P260_IP_ID THEN ''<B>''||spi_screen_utility.get_hfa_number(parent_party_id)||''</B>'' ELSE spi_screen_utility.get_hfa_number(parent_party_id) END Party_HFA',
'FROM   aff_hierarchy outr',
'WHERE 1=1',
'AND child_party_id <> parent_party_id',
'AND end_date IS NULL',
'AND nowed_aff_hier_id IS NULL ',
'AND NOT EXISTS',
'(--Ignoring end dated hierarchies.',
'    SELECT NULL ',
'    FROM aff_hierarchy innr',
'    WHERE outr.group_number = innr.group_number',
'    AND innr.hier_level     = 0',
'    AND innr.end_date IS NOT NULL',
'    AND innr.nowed_aff_hier_id IS NOT NULL',
')',
'AND EXISTS',
'(',
'    SELECT 1 ',
'    FROM aff_hierarchy innr_e',
'    WHERE parent_party_id = :P260_IP_ID',
' --   AND innr_e.hier_level > 0 ',
'    AND innr_e.group_number =  outr.group_number',
'    AND innr_e.end_date IS NULL',
'    AND innr_e.nowed_aff_hier_id IS NULL ',
')',
'and :P260_QUERY_HIE=''Y''',
'order by group_number, hier_level'))
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
 p_id=>wwv_flow_api.id(37338316328580019)
,p_max_row_count=>'1000000'
,p_show_nulls_as=>'-'
,p_pagination_type=>'ROWS_X_TO_Y'
,p_pagination_display_pos=>'BOTTOM_RIGHT'
,p_report_list_mode=>'TABS'
,p_show_detail_link=>'N'
,p_show_notify=>'Y'
,p_download_formats=>'CSV:HTML:EMAIL:XLS:PDF:RTF'
,p_owner=>'KBAKSHI'
,p_internal_uid=>37338316328580019
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37338448139580020)
,p_db_column_name=>'CHILD_IP_NUMBER'
,p_display_order=>10
,p_column_identifier=>'A'
,p_column_label=>'Child IP #'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37338503589580021)
,p_db_column_name=>'AFF_HIER_ID'
,p_display_order=>20
,p_column_identifier=>'B'
,p_column_label=>'Hierarchy ID'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37338664476580022)
,p_db_column_name=>'HIER_LEV'
,p_display_order=>30
,p_column_identifier=>'C'
,p_column_label=>'Hier level'
,p_column_type=>'NUMBER'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37338790422580023)
,p_db_column_name=>'CHILD_PARTY_ID'
,p_display_order=>40
,p_column_identifier=>'D'
,p_column_label=>'Child party id'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37338853782580024)
,p_db_column_name=>'Child Name'
,p_display_order=>50
,p_column_identifier=>'E'
,p_column_label=>'Child Name'
,p_column_link=>'f?p=&APP_ID.:260:&SESSION.:APEX_CLONE_SESSION:&DEBUG.:RP:P260_IP_ID,P260_IP_NUMBER:#CHILD_PARTY_ID#,#CHILD_IP_NUMBER#'
,p_column_linktext=>'#Child Name#'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37338945343580025)
,p_db_column_name=>'PARENT_PARTY_ID'
,p_display_order=>60
,p_column_identifier=>'F'
,p_column_label=>'Parent party id'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37339009636580026)
,p_db_column_name=>'PARENT_IP_NUMBER'
,p_display_order=>70
,p_column_identifier=>'G'
,p_column_label=>'Parent IP #'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37339126091580027)
,p_db_column_name=>'Parent Name'
,p_display_order=>80
,p_column_identifier=>'H'
,p_column_label=>'Parent Name'
,p_column_link=>'f?p=&APP_ID.:260:&SESSION.:APEX_CLONE_SESSION:&DEBUG.:RP:P260_IP_ID,P260_IP_NUMBER:#PARENT_PARTY_ID#,#PARENT_IP_NUMBER#'
,p_column_linktext=>'#Parent Name#'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37339251976580028)
,p_db_column_name=>'RELATE_ACCOUNT_NUMBER'
,p_display_order=>90
,p_column_identifier=>'I'
,p_column_label=>'Relate Account #'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37339387784580029)
,p_db_column_name=>'GROUP_NUMBER'
,p_display_order=>100
,p_column_identifier=>'J'
,p_column_label=>'Group number'
,p_column_type=>'NUMBER'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37339472009580030)
,p_db_column_name=>'RIGHT_TYPE'
,p_display_order=>110
,p_column_identifier=>'K'
,p_column_label=>'Right type'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37339531930580031)
,p_db_column_name=>'CHILD_HFA'
,p_display_order=>120
,p_column_identifier=>'L'
,p_column_label=>'Child HFA #'
,p_column_type=>'STRING'
,p_display_text_as=>'WITHOUT_MODIFICATION'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37339607131580032)
,p_db_column_name=>'PARTY_HFA'
,p_display_order=>130
,p_column_identifier=>'M'
,p_column_label=>'Parent HFA#'
,p_column_type=>'STRING'
,p_display_text_as=>'WITHOUT_MODIFICATION'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(37383992644185847)
,p_application_user=>'APXWS_DEFAULT'
,p_report_seq=>10
,p_report_alias=>'373840'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'CHILD_IP_NUMBER:AFF_HIER_ID:GROUP_NUMBER:HIER_LEV:Child Name:PARENT_IP_NUMBER:Parent Name:RELATE_ACCOUNT_NUMBER:RIGHT_TYPE:CHILD_HFA:PARTY_HFA:'
,p_sort_column_1=>'GROUP_NUMBER'
,p_sort_direction_1=>'ASC'
,p_sort_column_2=>'HIER_LEV'
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
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(37339724360580033)
,p_name=>'Nowed Hierarchy History'
,p_parent_plug_id=>wwv_flow_api.id(37332744869575513)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>20
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#:t-Report--altRowsDefault:t-Report--rowHighlight'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT  /*',
'        CASE ',
'         WHEN (hier_level = 0 AND nowed_aff_hier_id IS NOT NULL)',
'         THEN ''<img src="/i/menu/pencil16x16.gif" alt="">''',
'--         THEN ''<span class="t-Icon fa fa-pencil" aria-hidden="true"></span>''',
'         ELSE',
'           '' ''',
'        END NOW',
'        */',
'      CASE WHEN TO_CHAR(nowed_aff_hier_id) IS NULL THEN '' '' ',
'          ELSE TO_CHAR(nowed_aff_hier_id) ',
'      END nowed_aff_hier_id',
'      ,aff_hier_id',
'      ,hier_level',
'      ,child_party_id',
'      ,(SELECT ip_number FROM aff_interested_parties WHERE ip_id = child_party_id) CHILD_IP_NUMBER',
'      ,spi_screen_utility.get_party_name(child_party_id) "Child Name"',
'      ,(SELECT NVL2(ra.aff_category_hfa, ra.aff_category||'' / ''||ra.aff_category_hfa, ra.aff_category) ',
'						 FROM rep_affiliates ra',
'						 WHERE ra.ip_id = child_party_id)c_Derived_Status',
'      ,parent_party_id',
'      ,(SELECT ip_number FROM aff_interested_parties WHERE ip_id = parent_party_id) PARENT_IP_NUMBER',
'      ,spi_screen_utility.get_party_name(parent_party_id) "Parent Name"',
'      ,(SELECT NVL2(ra.aff_category_hfa, ra.aff_category||'' / ''||ra.aff_category_hfa, ra.aff_category) ',
'						 FROM rep_affiliates ra',
'						 WHERE ra.ip_id = parent_party_id) p_Derived_Status',
'      ,RELATE_ACCOUNT_NUMBER',
'      ,group_number',
'      ,CASE ',
'         WHEN hier_level = 0',
'         --THEN spi_screen_utility.GET_SONG_COUNT(child_party_id,parent_party_id) ',
'         THEN spi_screen_utility.get_ip_song_count(ah.child_party_id,ah.parent_party_id,NULL,ah.aff_hier_id) ',
'         ELSE',
'           '' ''',
'        END "SONG COUNT" ',
'      ,spi_screen_utility.GET_HIER_RIGHT_TYPE(aff_hier_id) RIGHT_TYPE',
'      ,spi_screen_utility.get_hfa_number(child_party_id) "Child HFA #"',
'      ,spi_screen_utility.get_hfa_number(parent_party_id) "Parent HFA #"',
'FROM  aff_hierarchy AH',
'WHERE GROUP_NUMBER  IN (SELECT group_number',
'                        FROM   aff_hierarchy',
'                        WHERE  (child_party_id  = :P260_IP_ID)',
'                        AND    hier_level  = 0                      ',
'                        )',
'AND end_date IS NOT NULL and :P260_QUERY_HIE=''Y''',
'ORDER BY group_number,hier_level'))
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
end;
/
begin
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(37339864978580034)
,p_query_column_id=>1
,p_column_alias=>'NOWED_AFF_HIER_ID'
,p_column_display_sequence=>1
,p_column_heading=>'Nowed Hier ID'
,p_column_link=>'f?p=&APP_ID.:24:&SESSION.::&DEBUG.:RP:P24_AFF_HIER_ID:#NOWED_AFF_HIER_ID#'
,p_column_linktext=>'#NOWED_AFF_HIER_ID#'
,p_column_alignment=>'RIGHT'
,p_display_as=>'WITHOUT_MODIFICATION'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(37339992714580035)
,p_query_column_id=>2
,p_column_alias=>'AFF_HIER_ID'
,p_column_display_sequence=>2
,p_column_heading=>'Hierarchy ID'
,p_use_as_row_header=>'N'
,p_column_alignment=>'RIGHT'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(37340079345580036)
,p_query_column_id=>3
,p_column_alias=>'HIER_LEVEL'
,p_column_display_sequence=>3
,p_column_heading=>'Hier Level'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(37340182963580037)
,p_query_column_id=>4
,p_column_alias=>'CHILD_PARTY_ID'
,p_column_display_sequence=>4
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(37340263662580038)
,p_query_column_id=>5
,p_column_alias=>'CHILD_IP_NUMBER'
,p_column_display_sequence=>5
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(37340335424580039)
,p_query_column_id=>6
,p_column_alias=>'Child Name'
,p_column_display_sequence=>6
,p_column_heading=>'Child Name'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(37340423155580040)
,p_query_column_id=>7
,p_column_alias=>'C_DERIVED_STATUS'
,p_column_display_sequence=>7
,p_column_heading=>'Child Derived Status'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(37340578511580041)
,p_query_column_id=>8
,p_column_alias=>'PARENT_PARTY_ID'
,p_column_display_sequence=>8
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(37340630548580042)
,p_query_column_id=>9
,p_column_alias=>'PARENT_IP_NUMBER'
,p_column_display_sequence=>9
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(37340740998580043)
,p_query_column_id=>10
,p_column_alias=>'Parent Name'
,p_column_display_sequence=>10
,p_column_heading=>'Parent Name'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(37340842109580044)
,p_query_column_id=>11
,p_column_alias=>'P_DERIVED_STATUS'
,p_column_display_sequence=>11
,p_column_heading=>'Parent Derived Status'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(37340998951580045)
,p_query_column_id=>12
,p_column_alias=>'RELATE_ACCOUNT_NUMBER'
,p_column_display_sequence=>12
,p_column_heading=>'HFA Relate Account #'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(37341061683580046)
,p_query_column_id=>13
,p_column_alias=>'GROUP_NUMBER'
,p_column_display_sequence=>13
,p_column_heading=>'Group Number'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(37341115516580047)
,p_query_column_id=>14
,p_column_alias=>'SONG COUNT'
,p_column_display_sequence=>14
,p_column_heading=>'Song Count'
,p_use_as_row_header=>'N'
,p_column_alignment=>'RIGHT'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(37341216181580048)
,p_query_column_id=>15
,p_column_alias=>'RIGHT_TYPE'
,p_column_display_sequence=>15
,p_column_heading=>'Right Type'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(37341382688580049)
,p_query_column_id=>16
,p_column_alias=>'Child HFA #'
,p_column_display_sequence=>16
,p_column_heading=>'Child hfa #'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(37341409123580050)
,p_query_column_id=>17
,p_column_alias=>'Parent HFA #'
,p_column_display_sequence=>17
,p_column_heading=>'Parent hfa #'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(37793925872097424)
,p_plug_name=>'History'
,p_parent_plug_id=>wwv_flow_api.id(37332744869575513)
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>30
,p_plug_display_point=>'BODY'
,p_query_type=>'SQL'
,p_plug_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT  ahrth.AFF_HIERARCHY_RIGHT_TYPE_ID,ahh.AFF_HIER_ID,',
'      ahh.CHILD_PARTY_ID,',
'      (SELECT ip_number FROM aff_interested_parties WHERE ip_id = ahh.child_party_id) CHILD_IP_NUMBER,',
'      spi_screen_utility.get_party_name(ahh.child_party_id) Child_Name,',
'      ahh.PARENT_PARTY_ID,',
'      (SELECT ip_number FROM aff_interested_parties WHERE ip_id = ahh.parent_party_id) PARENT_IP_NUMBER,',
'      spi_screen_utility.get_party_name(ahh.parent_party_id) Parent_Name,',
'      spi_screen_utility.get_hfa_number(ahh.child_party_id) "Child HFA #",',
'      (SELECT NVL2(ra.aff_category_hfa, ra.aff_category||'' / ''||ra.aff_category_hfa, ra.aff_category) ',
'						 FROM rep_affiliates ra',
'						 WHERE ra.ip_id = ahh.child_party_id)c_Derived_Status,',
'      spi_screen_utility.get_hfa_number(ahh.parent_party_id) "Parent HFA #",',
'      (SELECT NVL2(ra.aff_category_hfa, ra.aff_category||'' / ''||ra.aff_category_hfa, ra.aff_category) ',
'						 FROM rep_affiliates ra',
'						 WHERE ra.ip_id = ahh.parent_party_id) p_Derived_Status,',
'      ahh.RELATE_ACCOUNT_NUMBER,',
'      ahh.HIER_LEVEL,',
'      ahh.START_DATE,',
'      ahh.END_DATE,',
'      ahh.CREATED_BY,',
'      ahh.CREATION_DATE,',
'      ahh.LAST_UPDATED_BY,',
'      ahh.LAST_UPDATE_DATE,',
'      ahh.GROUP_NUMBER,',
'      ahh.NOWED_AFF_HIER_ID,',
'      ahh.HOLD_TYPE_ID,',
'      (SELECT pht.description ',
'                            FROM  pay_hold_types pht ',
'                            WHERE pht.hold_type_id = ahh.hold_type_id) hold_type,',
'      ahrth.right_type,',
'      to_char(ahrth.HIST_CREATED_DATETIME,''YYYY-MM-DD HH:MI:SS AM'') history_creation_date,',
'      ahrth.Action,',
'      ahrth.Hist_created_by',
'      from AFF_HIERARCHY_RIGHT_TYPES_HIST ahrth, aff_hierarchy_hist ahh',
'      where ahrth.history_id=ahh.hist_id --order by ahh.hist_id desc',
'      and history_ID in (select max(ahrth.history_ID) from AFF_HIERARCHY_RIGHT_TYPES_HIST ahrth,aff_hierarchy_hist ahh',
'      where ahrth.history_id=ahh.hist_id',
'      and ahrth.flag = ''Y'' group by ahrth.group_id)',
'      and child_party_id=:P260_IP_ID',
'      union',
'     SELECT  ahrth.AFF_HIERARCHY_RIGHT_TYPE_ID,ahh.AFF_HIER_ID,',
'      ahh.CHILD_PARTY_ID,',
'      (SELECT ip_number FROM aff_interested_parties WHERE ip_id = ahh.child_party_id) CHILD_IP_NUMBER,',
'      spi_screen_utility.get_party_name(ahh.child_party_id) Child_Name,',
'      ahh.PARENT_PARTY_ID,',
'      (SELECT ip_number FROM aff_interested_parties WHERE ip_id = ahh.parent_party_id) PARENT_IP_NUMBER,',
'      spi_screen_utility.get_party_name(ahh.parent_party_id) Parent_Name,',
'      spi_screen_utility.get_hfa_number(ahh.child_party_id) "Child HFA #",',
'      (SELECT NVL2(ra.aff_category_hfa, ra.aff_category||'' / ''||ra.aff_category_hfa, ra.aff_category) ',
'						 FROM rep_affiliates ra',
'						 WHERE ra.ip_id = ahh.child_party_id)c_Derived_Status,',
'      spi_screen_utility.get_hfa_number(ahh.parent_party_id) "Parent HFA #",',
'      (SELECT NVL2(ra.aff_category_hfa, ra.aff_category||'' / ''||ra.aff_category_hfa, ra.aff_category) ',
'						 FROM rep_affiliates ra',
'						 WHERE ra.ip_id = ahh.parent_party_id) p_Derived_Status,',
'      ahh.RELATE_ACCOUNT_NUMBER,',
'      ahh.HIER_LEVEL,',
'      ahh.START_DATE,',
'      ahh.END_DATE,',
'      ahh.CREATED_BY,',
'      ahh.CREATION_DATE,',
'      ahh.LAST_UPDATED_BY,',
'      ahh.LAST_UPDATE_DATE,',
'      ahh.GROUP_NUMBER,',
'      ahh.NOWED_AFF_HIER_ID,',
'      ahh.HOLD_TYPE_ID,',
'      (SELECT pht.description ',
'                            FROM  pay_hold_types pht ',
'                            WHERE pht.hold_type_id = ahh.hold_type_id) hold_type,',
'      ahrth.right_type,',
'       to_char(ahh.HIST_CREATED_DATETIME,''YYYY-MM-DD HH:MI:SS AM'') history_creation_date,',
'      ahh.Action,',
'      ahh.Hist_created_by',
'      from AFF_HIERARCHY_RIGHT_TYPES_HIST ahrth, aff_hierarchy_hist ahh',
'      where ahrth.history_id=ahh.hist_id --order by ahh.hist_id desc',
'      and history_ID in (select max(ahh.hist_ID) from AFF_HIERARCHY_RIGHT_TYPES_HIST ahrth,aff_hierarchy_hist ahh',
'      where ahrth.history_id=ahh.hist_id and ahh.flag = ''Y'' and :P260_QUERY_HIE=''Y''',
'      group by ahh.group_id)  ',
'      and child_party_id=:P260_IP_ID;'))
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
 p_id=>wwv_flow_api.id(37794096705097425)
,p_max_row_count=>'1000000'
,p_show_nulls_as=>'-'
,p_pagination_type=>'ROWS_X_TO_Y'
,p_pagination_display_pos=>'BOTTOM_RIGHT'
,p_report_list_mode=>'TABS'
,p_show_detail_link=>'N'
,p_show_notify=>'Y'
,p_download_formats=>'CSV:HTML:EMAIL:XLS:PDF:RTF'
,p_owner=>'KBAKSHI'
,p_internal_uid=>37794096705097425
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37794180791097426)
,p_db_column_name=>'AFF_HIER_ID'
,p_display_order=>10
,p_column_identifier=>'A'
,p_column_label=>'Aff Hier ID'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37794236179097427)
,p_db_column_name=>'CHILD_PARTY_ID'
,p_display_order=>20
,p_column_identifier=>'B'
,p_column_label=>'Child Party ID'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37794364239097428)
,p_db_column_name=>'PARENT_PARTY_ID'
,p_display_order=>30
,p_column_identifier=>'C'
,p_column_label=>'Parent Party ID'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37794405358097429)
,p_db_column_name=>'RELATE_ACCOUNT_NUMBER'
,p_display_order=>40
,p_column_identifier=>'D'
,p_column_label=>'HFA Relate Account #'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37794577765097430)
,p_db_column_name=>'HIER_LEVEL'
,p_display_order=>50
,p_column_identifier=>'E'
,p_column_label=>'Hier Level'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37794681299097431)
,p_db_column_name=>'START_DATE'
,p_display_order=>60
,p_column_identifier=>'F'
,p_column_label=>'Start Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37794722977097432)
,p_db_column_name=>'END_DATE'
,p_display_order=>70
,p_column_identifier=>'G'
,p_column_label=>'End Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37794815209097433)
,p_db_column_name=>'CREATED_BY'
,p_display_order=>80
,p_column_identifier=>'H'
,p_column_label=>'Created By'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37794912465097434)
,p_db_column_name=>'CREATION_DATE'
,p_display_order=>90
,p_column_identifier=>'I'
,p_column_label=>'Creation Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37795068736097435)
,p_db_column_name=>'LAST_UPDATED_BY'
,p_display_order=>100
,p_column_identifier=>'J'
,p_column_label=>'Last Updated By'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37795166250097436)
,p_db_column_name=>'LAST_UPDATE_DATE'
,p_display_order=>110
,p_column_identifier=>'K'
,p_column_label=>'Last Update Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37795237462097437)
,p_db_column_name=>'GROUP_NUMBER'
,p_display_order=>120
,p_column_identifier=>'L'
,p_column_label=>'Group Number'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37795382563097438)
,p_db_column_name=>'NOWED_AFF_HIER_ID'
,p_display_order=>130
,p_column_identifier=>'M'
,p_column_label=>'Nowed Aff Hier ID'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37795435102097439)
,p_db_column_name=>'HOLD_TYPE_ID'
,p_display_order=>140
,p_column_identifier=>'N'
,p_column_label=>'Hold Type ID'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(47641913100622809)
,p_db_column_name=>'AFF_HIERARCHY_RIGHT_TYPE_ID'
,p_display_order=>160
,p_column_identifier=>'P'
,p_column_label=>'Aff hierarchy right type id'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(47642089730622810)
,p_db_column_name=>'RIGHT_TYPE'
,p_display_order=>170
,p_column_identifier=>'Q'
,p_column_label=>'Right type'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(47642233119622812)
,p_db_column_name=>'HISTORY_CREATION_DATE'
,p_display_order=>180
,p_column_identifier=>'S'
,p_column_label=>'History creation date'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(47642322992622813)
,p_db_column_name=>'CHILD_IP_NUMBER'
,p_display_order=>190
,p_column_identifier=>'T'
,p_column_label=>'Child ip number'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(47642409525622814)
,p_db_column_name=>'CHILD_NAME'
,p_display_order=>200
,p_column_identifier=>'U'
,p_column_label=>'Child Name'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(47642511282622815)
,p_db_column_name=>'PARENT_IP_NUMBER'
,p_display_order=>210
,p_column_identifier=>'V'
,p_column_label=>'Parent ip number'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(47642683767622816)
,p_db_column_name=>'PARENT_NAME'
,p_display_order=>220
,p_column_identifier=>'W'
,p_column_label=>'Parent Name'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(47642792233622817)
,p_db_column_name=>'Child HFA #'
,p_display_order=>230
,p_column_identifier=>'X'
,p_column_label=>'Child hfa #'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(47642837310622818)
,p_db_column_name=>'Parent HFA #'
,p_display_order=>240
,p_column_identifier=>'Y'
,p_column_label=>'Parent hfa #'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(47642944210622819)
,p_db_column_name=>'P_DERIVED_STATUS'
,p_display_order=>250
,p_column_identifier=>'Z'
,p_column_label=>'Parent Derived Status'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(47643008757622820)
,p_db_column_name=>'HOLD_TYPE'
,p_display_order=>260
,p_column_identifier=>'AA'
,p_column_label=>'Hold type'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(47643182631622821)
,p_db_column_name=>'C_DERIVED_STATUS'
,p_display_order=>270
,p_column_identifier=>'AB'
,p_column_label=>'Child Derived Status'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(4379652051140846)
,p_db_column_name=>'ACTION'
,p_display_order=>280
,p_column_identifier=>'AC'
,p_column_label=>'Action'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(4379796831140847)
,p_db_column_name=>'HIST_CREATED_BY'
,p_display_order=>290
,p_column_identifier=>'AD'
,p_column_label=>'Hist Created By'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(38880826143045197)
,p_application_user=>'APXWS_DEFAULT'
,p_report_seq=>10
,p_report_alias=>'388809'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'HISTORY_CREATION_DATE:AFF_HIER_ID:GROUP_NUMBER:RIGHT_TYPE:HIER_LEVEL:CHILD_NAME:Child HFA #:C_DERIVED_STATUS:PARENT_NAME:Parent HFA #:P_DERIVED_STATUS:RELATE_ACCOUNT_NUMBER:HOLD_TYPE:START_DATE:END_DATE:CREATED_BY:CREATION_DATE:LAST_UPDATED_BY:LAST_U'
||'PDATE_DATE:NOWED_AFF_HIER_ID::ACTION:HIST_CREATED_BY'
,p_sort_column_1=>'HISTORY_CREATION_DATE'
,p_sort_direction_1=>'DESC'
,p_sort_column_2=>'LAST_UPDATE_DATE'
,p_sort_direction_2=>'ASC'
,p_sort_column_3=>'HIST_ID'
,p_sort_direction_3=>'DESC'
,p_sort_column_4=>'0'
,p_sort_direction_4=>'ASC'
,p_sort_column_5=>'0'
,p_sort_direction_5=>'ASC'
,p_sort_column_6=>'0'
,p_sort_direction_6=>'ASC'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(63368308632030607)
,p_name=>'Contacts'
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>80
,p_include_in_reg_disp_sel_yn=>'Y'
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#:t-Report--altRowsDefault:t-Report--rowHighlight'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT DISTINCT ic.IP_CONTACT_ID, ',
'ict.contact_type,',
'  ic.display_flag,',
'  ic.primary_flag,',
'  full_name,',
'  ic.status,',
'  ic.ip_id,',
'(SELECT PHONE_NUMBER',
'  FROM',
'    ( SELECT DISTINCT DECODE(PHONE_LINE_TYPE,''WORK'', DECODE(PHONE_COUNTRY_CODE,NULL,NULL,''(''',
'      ||PHONE_COUNTRY_CODE',
'      ||'')'' )',
'      ||DECODE(PHONE_AREA_CODE,NULL,NULL,''(''',
'      ||PHONE_AREA_CODE',
'      ||'')'' )',
'      || DECODE(PHONE_NUMBER,NULL,NULL,''(''',
'      ||SUBSTR(PHONE_NUMBER,1,3)',
'      ||''-''',
'      ||SUBSTR(PHONE_NUMBER,4)',
'      ||'')'')) "PHONE_NUMBER"',
'    FROM aff_ip_contact_points',
'    WHERE ip_id       = :P260_ip_id',
'    AND ip_contact_id = ic.ip_contact_id',
'    )',
'  WHERE PHONE_NUMBER IS NOT NULL',
'  ) BUSINESS_PHONE_NUMBER,',
'  (SELECT PHONE_NUMBER',
'  FROM',
'    ( SELECT DISTINCT DECODE(PHONE_LINE_TYPE,''GEN'', DECODE(PHONE_COUNTRY_CODE,NULL,NULL,''(''',
'      ||PHONE_COUNTRY_CODE',
'      ||'')'' )',
'      ||DECODE(PHONE_AREA_CODE,NULL,NULL,''(''',
'      ||PHONE_AREA_CODE',
'      ||'')'' )',
'      || DECODE(PHONE_NUMBER,NULL,NULL,''(''',
'      ||SUBSTR(PHONE_NUMBER,1,3)',
'      ||''-''',
'      ||SUBSTR(PHONE_NUMBER,4)',
'      ||'')'')) "PHONE_NUMBER"',
'    FROM aff_ip_contact_points',
'    WHERE ip_id       = :P260_ip_id',
'    AND ip_contact_id = ic.ip_contact_id',
'    )',
'  WHERE PHONE_NUMBER IS NOT NULL',
'  ) PHONE_NUMBER,',
'  (SELECT PHONE_NUMBER',
'  FROM',
'    ( SELECT DISTINCT DECODE(PHONE_LINE_TYPE,''MOBILE'', DECODE(PHONE_COUNTRY_CODE,NULL,NULL,''(''',
'      ||PHONE_COUNTRY_CODE',
'      ||'')'' )',
'      ||DECODE(PHONE_AREA_CODE,NULL,NULL,''(''',
'      ||PHONE_AREA_CODE',
'      ||'')'' )',
'      || DECODE(PHONE_NUMBER,NULL,NULL,''(''',
'      ||SUBSTR(PHONE_NUMBER,1,3)',
'      ||''-''',
'      ||SUBSTR(PHONE_NUMBER,4)',
'      ||'')'')) "PHONE_NUMBER"',
'    FROM aff_ip_contact_points',
'    WHERE ip_id       = :P260_ip_id',
'    AND ip_contact_id = ic.ip_contact_id',
'    )',
'  WHERE PHONE_NUMBER IS NOT NULL',
'  ) CELL_PHONE_NUMBER,',
'  (SELECT EMAIL_ADDRESS',
'  FROM aff_ip_contact_points',
'  WHERE ip_id        = :P260_ip_id',
'  AND ip_contact_id  = ic.ip_contact_id',
'  AND EMAIL_ADDRESS IS NOT NULL',
'  AND rownum         = 1',
'  ) "EMAIL_ADDRESS",',
'  (SELECT URL',
'  FROM aff_ip_contact_points ',
'  WHERE ip_id       = :P260_ip_id',
'  AND ip_contact_id = ic.ip_contact_id',
'  AND URL          IS NOT NULL',
'  AND rownum        = 1',
'  ) "URL",',
'DECODE(aia.address1,NULL,NULL,aia.address1||'',''||aia.address2||'',''||aia.city||'',''||aia.state||'',''||aia.postal_code) Address',
'FROM AFF_IP_CONTACT_TYPES ict,',
'  AFF_IP_CONTACTS_V ic,',
'  AFF_IP_CONTACT_POINTS Aic,',
'  aff_ip_addresses aia',
'WHERE ICT.CONTACT_TYPE_ID = IC.CONT_TYPE_ID',
'AND ic.ip_contact_id = aic.ip_contact_id(+)',
'AND aic.ip_address_id = aia.ip_address_id(+)',
'AND ic.ip_id              = :P260_ip_id',
'AND :P260_ip_id          IS NOT NULL'))
,p_display_condition_type=>'NEVER'
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>'-'
,p_query_num_rows_type=>'ROW_RANGES_WITH_LINKS'
,p_pagination_display_position=>'TOP_AND_BOTTOM_LEFT'
,p_csv_output=>'Y'
,p_csv_output_link_text=>'Download'
,p_prn_output=>'N'
,p_sort_null=>'L'
,p_query_asc_image=>'apex/builder/dup.gif'
,p_query_asc_image_attr=>'width="16" height="16" alt="" '
,p_query_desc_image=>'apex/builder/ddown.gif'
,p_query_desc_image_attr=>'width="16" height="16" alt="" '
,p_plug_query_strip_html=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(66914507575500804)
,p_query_column_id=>1
,p_column_alias=>'IP_CONTACT_ID'
,p_column_display_sequence=>1
,p_use_as_row_header=>'N'
,p_column_link=>'f?p=&APP_ID.:556:&SESSION.::&DEBUG.:RP,556:P556_IP_CONTACT_ID:#IP_CONTACT_ID#'
,p_column_linktext=>'<img src="#IMAGE_PREFIX#menu/pencil16x16.gif" alt="">'
,p_disable_sort_column=>'N'
,p_report_column_required_role=>wwv_flow_api.id(69665980648432325)
,p_lov_show_nulls=>'YES'
,p_derived_column=>'N'
,p_lov_display_extra=>'YES'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63866568348176332)
,p_query_column_id=>2
,p_column_alias=>'CONTACT_TYPE'
,p_column_display_sequence=>3
,p_column_heading=>'Contact Type'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63866675334176333)
,p_query_column_id=>3
,p_column_alias=>'DISPLAY_FLAG'
,p_column_display_sequence=>4
,p_column_heading=>'Display Flag'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63866729846176334)
,p_query_column_id=>4
,p_column_alias=>'PRIMARY_FLAG'
,p_column_display_sequence=>5
,p_column_heading=>'Primary Flag'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63866873478176335)
,p_query_column_id=>5
,p_column_alias=>'FULL_NAME'
,p_column_display_sequence=>2
,p_column_heading=>'Full Name'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63866944469176336)
,p_query_column_id=>6
,p_column_alias=>'STATUS'
,p_column_display_sequence=>6
,p_column_heading=>'Status'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63867088197176337)
,p_query_column_id=>7
,p_column_alias=>'IP_ID'
,p_column_display_sequence=>7
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63867175805176338)
,p_query_column_id=>8
,p_column_alias=>'BUSINESS_PHONE_NUMBER'
,p_column_display_sequence=>11
,p_column_heading=>'Business Phone Number'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63867242729176339)
,p_query_column_id=>9
,p_column_alias=>'PHONE_NUMBER'
,p_column_display_sequence=>12
,p_column_heading=>'Phone Number'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63867395516176340)
,p_query_column_id=>10
,p_column_alias=>'CELL_PHONE_NUMBER'
,p_column_display_sequence=>13
,p_column_heading=>'Cell PhoneNumber'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63867404345176341)
,p_query_column_id=>11
,p_column_alias=>'EMAIL_ADDRESS'
,p_column_display_sequence=>8
,p_column_heading=>'Email Address'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63867531824176342)
,p_query_column_id=>12
,p_column_alias=>'URL'
,p_column_display_sequence=>9
,p_column_heading=>'URL'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63867639095176343)
,p_query_column_id=>13
,p_column_alias=>'ADDRESS'
,p_column_display_sequence=>10
,p_column_heading=>'Address'
,p_use_as_row_header=>'N'
,p_heading_alignment=>'LEFT'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(63373557462030613)
,p_name=>'Non Collection Agr Permissions'
,p_parent_plug_id=>wwv_flow_api.id(63368308632030607)
,p_template=>wwv_flow_api.id(51848808455240825)
,p_display_sequence=>451
,p_region_template_options=>'#DEFAULT#'
,p_component_template_options=>'#DEFAULT#'
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
'where ip_id = pcp.earner_id',
'AND name_type_id = 6) Owner,',
'(SELECT full_name from aff_ip_names ',
'where ip_id = pcp.admin_acct',
'AND name_type_id = 6) admin_acct,',
'(SELECT full_name from aff_ip_names ',
'where ip_id = pcp.acct_admined',
'AND name_type_id = 6) acct_admined,',
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
'where collector_agreement_id = -1',
'AND earner_id = :P260_IP_ID'))
,p_display_condition_type=>'NEVER'
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>' - '
,p_query_no_data_found=>'no data found'
,p_query_num_rows_type=>'NEXT_PREVIOUS_LINKS'
,p_query_row_count_max=>500
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_csv_output=>'Y'
,p_csv_output_link_text=>'Download'
,p_prn_output=>'N'
,p_sort_null=>'L'
,p_query_asc_image=>'apex/builder/dup.gif'
,p_query_asc_image_attr=>'width="16" height="16" alt="" '
,p_query_desc_image=>'apex/builder/ddown.gif'
,p_query_desc_image_attr=>'width="16" height="16" alt="" '
,p_plug_query_strip_html=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63373926352030613)
,p_query_column_id=>1
,p_column_alias=>'AGR_PERMISSION_ID'
,p_column_display_sequence=>1
,p_column_heading=>'Agr Permission Id'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63374323685030614)
,p_query_column_id=>2
,p_column_alias=>'COLLECTOR_AGREEMENT_ID'
,p_column_display_sequence=>2
,p_column_heading=>'Collector Agreement Id'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63374757203030614)
,p_query_column_id=>3
,p_column_alias=>'COLLECTOR_ID'
,p_column_display_sequence=>3
,p_column_heading=>'Collector Id'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63375176060030614)
,p_query_column_id=>4
,p_column_alias=>'COLLECTOR'
,p_column_display_sequence=>4
,p_column_heading=>'Collector'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63375528647030615)
,p_query_column_id=>5
,p_column_alias=>'EARNER_ID'
,p_column_display_sequence=>5
,p_column_heading=>'Earner Id'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63375934115030615)
,p_query_column_id=>6
,p_column_alias=>'OWNER'
,p_column_display_sequence=>6
,p_column_heading=>'Owner'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63376357541030615)
,p_query_column_id=>7
,p_column_alias=>'ADMIN_ACCT'
,p_column_display_sequence=>7
,p_column_heading=>'Admin Acct'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63376793136030615)
,p_query_column_id=>8
,p_column_alias=>'ACCT_ADMINED'
,p_column_display_sequence=>8
,p_column_heading=>'Acct Admined'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63377160068030616)
,p_query_column_id=>9
,p_column_alias=>'AGREEMENT_TYPE'
,p_column_display_sequence=>9
,p_column_heading=>'Agreement Type'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63377527364030616)
,p_query_column_id=>10
,p_column_alias=>'CAN_VIEW_STATEMTS'
,p_column_display_sequence=>10
,p_column_heading=>'Can View Statemts'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63377993618030616)
,p_query_column_id=>11
,p_column_alias=>'CAN_VIEW_CATALOG'
,p_column_display_sequence=>11
,p_column_heading=>'Can View Catalog'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63378365813030616)
,p_query_column_id=>12
,p_column_alias=>'CAN_ADD_WORKS'
,p_column_display_sequence=>12
,p_column_heading=>'Can Add Works'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63378731190030617)
,p_query_column_id=>13
,p_column_alias=>'CAN_ADD_RECORDING'
,p_column_display_sequence=>13
,p_column_heading=>'Can Add Recording'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63379139384030617)
,p_query_column_id=>14
,p_column_alias=>'CAN_ADD_COMMERCIAL'
,p_column_display_sequence=>14
,p_column_heading=>'Can Add Commercial'
);
end;
/
begin
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63379598976030617)
,p_query_column_id=>15
,p_column_alias=>'CAN_ADD_DIRECT_DEPOSIT'
,p_column_display_sequence=>15
,p_column_heading=>'Can Add Direct Deposit'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63379907584030618)
,p_query_column_id=>16
,p_column_alias=>'CAN_VIEW_ADD_LIVE_PERF'
,p_column_display_sequence=>16
,p_column_heading=>'Can View Add Live Perf'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63380384753030618)
,p_query_column_id=>17
,p_column_alias=>'CAN_APPROVE_MYSPACE'
,p_column_display_sequence=>17
,p_column_heading=>'Can Approve Myspace'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63380793818030618)
,p_query_column_id=>18
,p_column_alias=>'CAN_UPLOAD_DIGITAL_RECORDGS'
,p_column_display_sequence=>18
,p_column_heading=>'Can Upload Digital Recordgs'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63381121113030619)
,p_query_column_id=>19
,p_column_alias=>'CAN_UPDATE_ACCT_INFO'
,p_column_display_sequence=>19
,p_column_heading=>'Can Update Acct Info'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63381550833030619)
,p_query_column_id=>20
,p_column_alias=>'CAN_OPT_OUT_OF_PAPERLESS'
,p_column_display_sequence=>20
,p_column_heading=>'Can Opt Out Of Paperless'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(63386355177030623)
,p_plug_name=>'Party Songs'
,p_region_name=>'SONG'
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>220
,p_include_in_reg_disp_sel_yn=>'Y'
,p_plug_display_point=>'BODY'
,p_query_type=>'SQL'
,p_plug_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'Select *',
'  From (  ',
'With Agr_Soc As',
'(',
'Select Pgp.Property_Id         Property_Id,',
'   Ama.Ip_Id                   Ip_Id,',
'   S.Society_Long_Name         Agreement_Society,',
'   S.Society_Name',
'From Prop_Group_Props          Pgp,',
'   Prop_Groups                 Pg,',
'   Aff_Membership_Agreements   Ama,',
'   Aff_Societies_V             S',
'Where     Pg.Group_Id = Pgp.Group_Id',
'   And Ama.Ip_Id = :P260_Ip_Id',
'   And Pgp.Group_Id = Ama.Group_Id ',
'   And Ama.Society_Id = S.Society_Id(+) ',
')                         ',
'  Select /*+ PARALLEL(16)*/ Distinct',
'               P.Property_Id,',
'               Ar.Role_Code Song_Role,',
'               Ra.Aff_Name As Full_Name,',
'               Ra.Aff_Num As',
'                   Ip_Number,',
'               P.Title,',
'               P.Property_Number,',
'               Aka.Aka_Name',
'                   As Hfa_Song_Code,',
'               P.Status,',
'               So.Share_Percentage',
'                   As Default_Share_Percent,',
'               Agreement_Society,',
'               Spi_Screen_Utility.Get_Writers (P.Property_Id)',
'                   Writers,',
'               So.Right_Type_Id',
'                   As Right_Type,',
'               So.Authoritative_Flag,',
'               P.Entry_Date',
'                   As Reg_Date,',
'               Ps.Iswc,',
'               Agr_Soc.Society_Name,',
'               Ar.Summary_Role_Code As Pub_Wtr',
'          From Prop_Share_Collectors  Partition (Ed_Active) So,',
'               Prop_Akas              Aka,',
'               Prop_Properties        P,',
'               Prop_Songs             Ps,',
'               Aff_Roles              Ar,',
'               Rep_Affiliates         Ra,',
'               Agr_Soc   ',
'         Where P.Property_Id = Aka.Property_Id',
'               And Aka.Source_Id = P.Source_Id',
'               And Aka.Aka_Type_Id = 51                -- hfa song code)',
'               And So.Collector_Id = :P260_IP_ID',
'               And So.Role_Id = Ar.Role_Id',
'               And So.Collector_Id = Ra.Ip_Id',
'               And So.Collector_Id Not In (7089410, 602200)',
'               And So.End_Date_Active Is Null',
'               And Agr_Soc.Property_Id(+) = P.Property_Id',
'               And Agr_Soc.Ip_Id(+) = So.Collector_Id',
'               And P.Property_Id = So.Property_Id',
'               And P.Source_Id = 7                                    -- SESAC',
'               And P.Property_Type_Id = 1',
'               And P.Property_Id = Ps.Property_Id',
')',
'Where :P260_QUERY_SONG = ''Y''',
''))
,p_plug_source_type=>'NATIVE_IR'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_plug_display_condition_type=>'VAL_OF_ITEM_IN_COND_NOT_EQ_COND2'
,p_plug_display_when_condition=>'P260_IP_ID'
,p_plug_display_when_cond2=>'7089410'
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
 p_id=>wwv_flow_api.id(63879120562205808)
,p_max_row_count=>'1000000'
,p_show_nulls_as=>'-'
,p_pagination_type=>'ROWS_X_TO_Y_OF_Z'
,p_pagination_display_pos=>'TOP_AND_BOTTOM_LEFT'
,p_show_display_row_count=>'Y'
,p_report_list_mode=>'TABS'
,p_show_detail_link=>'N'
,p_show_notify=>'Y'
,p_show_flashback=>'N'
,p_download_formats=>'CSV:HTML:EMAIL'
,p_owner=>'DCOLLINS'
,p_internal_uid=>63879120562205808
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(63879329249205810)
,p_db_column_name=>'SONG_ROLE'
,p_display_order=>90
,p_column_identifier=>'B'
,p_column_label=>'Song Role'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(63879263033205809)
,p_db_column_name=>'PROPERTY_ID'
,p_display_order=>110
,p_column_identifier=>'A'
,p_column_label=>'Property ID'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(33670223266694117)
,p_db_column_name=>'FULL_NAME'
,p_display_order=>120
,p_column_identifier=>'C'
,p_column_label=>'Full Name'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(33670321800694118)
,p_db_column_name=>'IP_NUMBER'
,p_display_order=>130
,p_column_identifier=>'D'
,p_column_label=>'IP Number'
,p_column_type=>'STRING'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(33670433619694119)
,p_db_column_name=>'TITLE'
,p_display_order=>140
,p_column_identifier=>'E'
,p_column_label=>'Title'
,p_column_link=>'f?p=&APP_ID.:310:&SESSION.:APEX_CLONE_SESSION:&DEBUG.:RP,310:P310_PROP_ID,P310_CALLING_PAGE_ID:#PROPERTY_ID#,260'
,p_column_linktext=>'#TITLE#'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(33670506341694120)
,p_db_column_name=>'PROPERTY_NUMBER'
,p_display_order=>150
,p_column_identifier=>'F'
,p_column_label=>'Property Number'
,p_column_link=>'f?p=&APP_ID.:310:&SESSION.:APEX_CLONE_SESSION:&DEBUG.:RP,310:P310_PROP_ID,P310_CALLING_PAGE_ID:#PROPERTY_ID#,260'
,p_column_linktext=>'#PROPERTY_NUMBER#'
,p_column_type=>'STRING'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(33670664080694121)
,p_db_column_name=>'HFA_SONG_CODE'
,p_display_order=>160
,p_column_identifier=>'G'
,p_column_label=>'HFA Song Code'
,p_column_type=>'STRING'
,p_column_alignment=>'CENTER'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(33670799429694122)
,p_db_column_name=>'STATUS'
,p_display_order=>170
,p_column_identifier=>'H'
,p_column_label=>'Status'
,p_column_type=>'STRING'
,p_column_alignment=>'CENTER'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(33670847986694123)
,p_db_column_name=>'DEFAULT_SHARE_PERCENT'
,p_display_order=>180
,p_column_identifier=>'I'
,p_column_label=>'Default Share Percent'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(33670913847694124)
,p_db_column_name=>'AGREEMENT_SOCIETY'
,p_display_order=>190
,p_column_identifier=>'J'
,p_column_label=>'Agreement Society'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(33671063067694125)
,p_db_column_name=>'WRITERS'
,p_display_order=>200
,p_column_identifier=>'K'
,p_column_label=>'Writers'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(19711905827749003)
,p_db_column_name=>'RIGHT_TYPE'
,p_display_order=>210
,p_column_identifier=>'M'
,p_column_label=>'Right type'
,p_column_type=>'NUMBER'
,p_display_text_as=>'LOV_ESCAPE_SC'
,p_rpt_named_lov=>wwv_flow_api.id(43965500146362099)
,p_rpt_show_filter_lov=>'1'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(36262375676352928)
,p_db_column_name=>'AUTHORITATIVE_FLAG'
,p_display_order=>220
,p_column_identifier=>'N'
,p_column_label=>'Authoritative Flag'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(51306620264501128)
,p_db_column_name=>'ISWC'
,p_display_order=>240
,p_column_identifier=>'P'
,p_column_label=>'ISWC'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(51306767791501129)
,p_db_column_name=>'SOCIETY_NAME'
,p_display_order=>250
,p_column_identifier=>'Q'
,p_column_label=>'PRO'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(51306838582501130)
,p_db_column_name=>'PUB_WTR'
,p_display_order=>260
,p_column_identifier=>'R'
,p_column_label=>'Pub_Wtr'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(46228142373526108)
,p_db_column_name=>'REG_DATE'
,p_display_order=>270
,p_column_identifier=>'S'
,p_column_label=>'Reg Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(63950862392217373)
,p_application_user=>'APXWS_DEFAULT'
,p_report_seq=>10
,p_report_alias=>'639509'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'SONG_ROLE:FULL_NAME:TITLE:PROPERTY_NUMBER:STATUS:DEFAULT_SHARE_PERCENT:AGREEMENT_SOCIETY:HFA_SONG_CODE:WRITERS:RIGHT_TYPE:AUTHORITATIVE_FLAG:ISWC:PUB_WTR:SOCIETY_NAME'
,p_sort_column_1=>'TITLE'
,p_sort_direction_1=>'ASC'
);
wwv_flow_api.create_worksheet_condition(
 p_id=>wwv_flow_api.id(87494843632285634)
,p_report_id=>wwv_flow_api.id(63950862392217373)
,p_condition_type=>'FILTER'
,p_allow_delete=>'Y'
,p_column_name=>'STATUS'
,p_operator=>'='
,p_expr=>'A'
,p_condition_sql=>'"STATUS" = #APXWS_EXPR#'
,p_condition_display=>'#APXWS_COL_NAME# = ''A''  '
,p_enabled=>'Y'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(63393586162030628)
,p_plug_name=>'IP Header Display'
,p_region_template_options=>'#DEFAULT#:t-Region--noPadding:is-expanded:t-Region--scrollBody:t-Form--slimPadding'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51846539847240824)
,p_plug_display_sequence=>20
,p_plug_display_point=>'BODY'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_attribute_01=>'N'
,p_attribute_02=>'HTML'
,p_attribute_03=>'N'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(63403184423030635)
,p_name=>'Names'
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>50
,p_include_in_reg_disp_sel_yn=>'Y'
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#:t-Report--altRowsDefault:t-Report--rowHighlight'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT IP_NAME_ID,',
'NAME_TYPE_DESC name_type,',
'  NULL prefix,',
'  FIRST_NAME,',
'  NAME primary_lastname,',
'  NULL suffix,',
'  FULL_NAME,',
'  NAME_NUMBER IPI_name_number,',
'  DATE_OF_CREATION,',
'  DATE_OF_LAST_AMMENDMENT ,',
'  second_last_name',
'FROM AFF_NAME_TYPES nt,',
'  aff_ip_names n',
'WHERE NT.NAME_TYPE_ID = N.NAME_TYPE_ID',
'AND N.IP_ID           = :P260_IP_ID',
'AND :P260_ip_id      IS NOT NULL'))
,p_display_condition_type=>'NEVER'
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>'-'
,p_query_num_rows_type=>'ROW_RANGES_WITH_LINKS'
,p_pagination_display_position=>'BOTTOM_LEFT'
,p_csv_output=>'Y'
,p_csv_output_link_text=>'Download'
,p_prn_output=>'N'
,p_sort_null=>'L'
,p_query_asc_image=>'apex/builder/dup.gif'
,p_query_asc_image_attr=>'width="16" height="16" alt="" '
,p_query_desc_image=>'apex/builder/ddown.gif'
,p_query_desc_image_attr=>'width="16" height="16" alt="" '
,p_plug_query_strip_html=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(66914210214500801)
,p_query_column_id=>1
,p_column_alias=>'IP_NAME_ID'
,p_column_display_sequence=>1
,p_column_link=>'f?p=&APP_ID.:50290:&SESSION.::&DEBUG.:RP,50290:P50290_IP_NAME_ID:#IP_NAME_ID#'
,p_column_linktext=>'<img src="#IMAGE_PREFIX#menu/pencil16x16.gif" alt="">'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63880271842205819)
,p_query_column_id=>2
,p_column_alias=>'NAME_TYPE'
,p_column_display_sequence=>2
,p_column_heading=>'Name Type'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63880348623205820)
,p_query_column_id=>3
,p_column_alias=>'PREFIX'
,p_column_display_sequence=>3
,p_column_heading=>'Prefix'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63880421038205821)
,p_query_column_id=>4
,p_column_alias=>'FIRST_NAME'
,p_column_display_sequence=>4
,p_column_heading=>'First Name'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63880530187205822)
,p_query_column_id=>5
,p_column_alias=>'PRIMARY_LASTNAME'
,p_column_display_sequence=>5
,p_column_heading=>'Primary Last Name'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63880675637205823)
,p_query_column_id=>6
,p_column_alias=>'SUFFIX'
,p_column_display_sequence=>6
,p_column_heading=>'Suffix'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63880722022205824)
,p_query_column_id=>7
,p_column_alias=>'FULL_NAME'
,p_column_display_sequence=>7
,p_column_heading=>'Full Name'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63880833312205825)
,p_query_column_id=>8
,p_column_alias=>'IPI_NAME_NUMBER'
,p_column_display_sequence=>8
,p_column_heading=>'IPI Name Number'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63880932470205826)
,p_query_column_id=>9
,p_column_alias=>'DATE_OF_CREATION'
,p_column_display_sequence=>9
,p_column_heading=>'Created On'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63881087782205827)
,p_query_column_id=>10
,p_column_alias=>'DATE_OF_LAST_AMMENDMENT'
,p_column_display_sequence=>10
,p_column_heading=>'Last Amended On'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63881188205205828)
,p_query_column_id=>11
,p_column_alias=>'SECOND_LAST_NAME'
,p_column_display_sequence=>11
,p_column_heading=>'Second Last Name'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(63407589302030638)
,p_plug_name=>'Notes'
,p_region_name=>'NOTE'
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>100
,p_include_in_reg_disp_sel_yn=>'Y'
,p_plug_grid_column_css_classes=>'cl-ir-dynamic-display'
,p_plug_display_point=>'BODY'
,p_query_type=>'SQL'
,p_plug_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select	',
'note_purpose,			',
'notes	note_display,			',
'note_priority				',
',	created_by			',
',	creation_date	',
', last_updated_by',
', last_update_date',
',	ip_note_id			',
'from	AFF_IP_NOTES			',
'where	ip_id	=	:P260_ip_id	',
'and	nvl(wp_only_flag,''N'')	<>	''Y''	',
'and	:P260_ip_id	is	not	null and :P260_QUERY_NOTES=''Y''',
'ORDER BY ip_note_id desc, note_priority desc nulls last'))
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
 p_id=>wwv_flow_api.id(66914902023500808)
,p_max_row_count=>'1000000'
,p_show_nulls_as=>'-'
,p_pagination_type=>'ROWS_X_TO_Y'
,p_pagination_display_pos=>'BOTTOM_RIGHT'
,p_report_list_mode=>'TABS'
,p_show_detail_link=>'N'
,p_show_notify=>'Y'
,p_show_flashback=>'N'
,p_download_formats=>'CSV:HTML:EMAIL'
,p_owner=>'DCOLLINS'
,p_internal_uid=>66914902023500808
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(66915515018500814)
,p_db_column_name=>'IP_NOTE_ID'
,p_display_order=>10
,p_column_identifier=>'F'
,p_column_label=>'Note ID'
,p_column_type=>'NUMBER'
,p_display_text_as=>'HIDDEN'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(66915014180500809)
,p_db_column_name=>'NOTE_PURPOSE'
,p_display_order=>20
,p_column_identifier=>'A'
,p_column_label=>'Purpose'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(66915182175500810)
,p_db_column_name=>'NOTE_DISPLAY'
,p_display_order=>30
,p_column_identifier=>'B'
,p_column_label=>'Text'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(66915268481500811)
,p_db_column_name=>'NOTE_PRIORITY'
,p_display_order=>40
,p_column_identifier=>'C'
,p_column_label=>'Priority'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(66915313925500812)
,p_db_column_name=>'CREATED_BY'
,p_display_order=>50
,p_column_identifier=>'D'
,p_column_label=>'Created By'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(66915432058500813)
,p_db_column_name=>'CREATION_DATE'
,p_display_order=>60
,p_column_identifier=>'E'
,p_column_label=>'Creation Date'
,p_column_type=>'DATE'
,p_heading_alignment=>'LEFT'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(66916329190500822)
,p_db_column_name=>'LAST_UPDATED_BY'
,p_display_order=>70
,p_column_identifier=>'G'
,p_column_label=>'Last Updated By'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(66916458936500823)
,p_db_column_name=>'LAST_UPDATE_DATE'
,p_display_order=>80
,p_column_identifier=>'H'
,p_column_label=>'Last Update Date'
,p_column_type=>'DATE'
,p_heading_alignment=>'LEFT'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(66970263604784714)
,p_application_user=>'APXWS_DEFAULT'
,p_report_seq=>10
,p_report_alias=>'669703'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'NOTE_PURPOSE:NOTE_DISPLAY:NOTE_PRIORITY:CREATED_BY:CREATION_DATE:'
,p_sort_column_1=>'CREATION_DATE'
,p_sort_direction_1=>'DESC'
,p_sort_column_2=>'0'
,p_sort_direction_2=>'DESC'
,p_sort_column_3=>'0'
,p_sort_direction_3=>'DESC'
,p_sort_column_4=>'0'
,p_sort_direction_4=>'ASC'
,p_sort_column_5=>'0'
,p_sort_direction_5=>'ASC'
,p_sort_column_6=>'0'
,p_sort_direction_6=>'ASC'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(63424359871030651)
,p_plug_name=>'Royalties & Holds'
,p_region_name=>'ROYL'
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--scrollBody'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>110
,p_include_in_reg_disp_sel_yn=>'Y'
,p_plug_display_point=>'BODY'
,p_plug_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_plug_query_num_rows_type=>'NEXT_PREVIOUS_LINKS'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_plug_query_show_nulls_as=>' - '
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_attribute_01=>'N'
,p_attribute_02=>'HTML'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(20922090990266319)
,p_name=>'Royalties/Threshhold '
,p_parent_plug_id=>wwv_flow_api.id(63424359871030651)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>10
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#:t-Report--altRowsDefault:t-Report--rowHighlight'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>'select IP_THRESHOLD_ID,THRESHOLD_TYPE, THRESHOLD_VALUE from AFF_IP_THRESHOLDS where ip_id=:P260_IP_ID and :P260_QUERY_ROYAL=''Y'';'
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
 p_id=>wwv_flow_api.id(20923051984266329)
,p_query_column_id=>1
,p_column_alias=>'IP_THRESHOLD_ID'
,p_column_display_sequence=>1
,p_column_heading=>'Edit'
,p_column_link=>'f?p=&APP_ID.:50256:&SESSION.::&DEBUG.:RP:P50256_IP_ID,P50256_IP_THRESHOLD_ID,P50256_THRESHOLD_TYPE,P50256_THRESHOLD_VALUE:&P260_IP_ID.,#IP_THRESHOLD_ID#,#THRESHOLD_TYPE#,#THRESHOLD_VALUE#'
,p_column_linktext=>'<img src="#IMAGE_PREFIX#app_ui/img/icons/apex-edit-pencil.png" class="apex-edit-pencil" alt="">'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(20922146065266320)
,p_query_column_id=>2
,p_column_alias=>'THRESHOLD_TYPE'
,p_column_display_sequence=>2
,p_column_heading=>'Threshold Type'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(20922216510266321)
,p_query_column_id=>3
,p_column_alias=>'THRESHOLD_VALUE'
,p_column_display_sequence=>3
,p_column_heading=>'Threshold Value'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(37791860935097403)
,p_plug_name=>'Performance Royalty Earnings'
,p_parent_plug_id=>wwv_flow_api.id(63424359871030651)
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>5
,p_plug_display_point=>'BODY'
,p_query_type=>'SQL'
,p_plug_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT * FROM perf_royality_earnings_mv where affiliate_number=:P260_IP_NUMBER and :P260_QUERY_ROYAL=''Y''',
'ORDER BY affiliate_number,',
'         payout_quarter_CCYYQ,',
'         perf_category_desc ;'))
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
 p_id=>wwv_flow_api.id(37791934955097404)
,p_max_row_count=>'1000000'
,p_max_rows_per_page=>'20'
,p_show_nulls_as=>'-'
,p_pagination_type=>'ROWS_X_TO_Y'
,p_pagination_display_pos=>'BOTTOM_RIGHT'
,p_report_list_mode=>'TABS'
,p_show_detail_link=>'N'
,p_show_notify=>'Y'
,p_download_formats=>'CSV:HTML:EMAIL:XLS:PDF:RTF'
,p_owner=>'KBAKSHI'
,p_internal_uid=>37791934955097404
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37792098515097405)
,p_db_column_name=>'AFFILIATE_NUMBER'
,p_display_order=>10
,p_column_identifier=>'A'
,p_column_label=>'Affiliate Number'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37792140807097406)
,p_db_column_name=>'AFFILIATE_NAME'
,p_display_order=>20
,p_column_identifier=>'B'
,p_column_label=>'Affiliate Name'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37792256419097407)
,p_db_column_name=>'PAYOUT_QUARTER_CCYYQ'
,p_display_order=>30
,p_column_identifier=>'C'
,p_column_label=>'Payout Quarter CCYYQ'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37792378330097408)
,p_db_column_name=>'PERFORMANCE_CATEGORY'
,p_display_order=>40
,p_column_identifier=>'D'
,p_column_label=>'Performance Category'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37792408376097409)
,p_db_column_name=>'EARNINGS_SOURCE'
,p_display_order=>50
,p_column_identifier=>'E'
,p_column_label=>'Earnings Source'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37792570396097410)
,p_db_column_name=>'EARNING_AMOUNT'
,p_display_order=>60
,p_column_identifier=>'F'
,p_column_label=>'Earning Amount'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(37792635525097411)
,p_db_column_name=>'PERF_CATEGORY_DESC'
,p_display_order=>70
,p_column_identifier=>'G'
,p_column_label=>'Perf Category'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(46228265804526109)
,p_db_column_name=>'EARNINGS_APPLIED'
,p_display_order=>80
,p_column_identifier=>'H'
,p_column_label=>'Earnings Applied'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(38117020132906720)
,p_application_user=>'APXWS_DEFAULT'
,p_report_seq=>10
,p_report_alias=>'381171'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'AFFILIATE_NUMBER:AFFILIATE_NAME:PAYOUT_QUARTER_CCYYQ:PERFORMANCE_CATEGORY:EARNINGS_SOURCE:EARNING_AMOUNT:PERF_CATEGORY_DESC:EARNINGS_APPLIED'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(69228625468028223)
,p_name=>'HFA Holds'
,p_parent_plug_id=>wwv_flow_api.id(63424359871030651)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>20
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#:t-Report--altRowsDefault:t-Report--rowHighlight'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select LICENSEE_NUM,',
'DECODE(HOLD_TYPE,''SH'',''Song Hold'',''RH'',''Regular Hold'',''I'',''Invalid Address Hold'',''IRH'',''Regular Hold & Invalid Address Hold'',HOLD_TYPE) HOLD_TYPE,',
'HOLD_CODE,DECODE(STATUS ,''Y'',''Active'',''N'',''Inactive'',STATUS) STATUS',
'from mc_licensee_holds ',
'where PUBLISHER_IP_ID=:P260_IP_ID and ',
'STATUS=''Y'' and :P260_QUERY_ROYAL=''Y'';'))
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>'-'
,p_query_num_rows_type=>'ROW_RANGES_IN_SELECT_LIST'
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_csv_output=>'Y'
,p_csv_output_link_text=>'Download'
,p_prn_output=>'N'
,p_sort_null=>'L'
,p_plug_query_strip_html=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(69228875160028225)
,p_query_column_id=>1
,p_column_alias=>'LICENSEE_NUM'
,p_column_display_sequence=>1
,p_column_heading=>'Licensee Number'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(69228968053028226)
,p_query_column_id=>2
,p_column_alias=>'HOLD_TYPE'
,p_column_display_sequence=>2
,p_column_heading=>'Hold Type'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(69229031296028227)
,p_query_column_id=>3
,p_column_alias=>'HOLD_CODE'
,p_column_display_sequence=>3
,p_column_heading=>'Hold Code'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(69229157981028228)
,p_query_column_id=>4
,p_column_alias=>'STATUS'
,p_column_display_sequence=>4
,p_column_heading=>'Status'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(69228743323028224)
,p_name=>'HFA Adjustments'
,p_parent_plug_id=>wwv_flow_api.id(63424359871030651)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>30
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#:t-Report--altRowsDefault:t-Report--rowHighlight'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select FROM_PUBLISHER_NUM,MEMO_TYPE_CODE,SONG_CODE,SONG_TITLE,PUBLISHER_SEQ_NUM,PUBLISHER_NUM,PUBLISHER_IP_ID,PUBLISHER_SPLIT ',
'from mc_dist_memo_adj where FROM_PUBLISHER_NUM_IP_ID=:P260_IP_ID and memo_deposition = ''A'' and memo_type_code=''SS'' and :P260_QUERY_ROYAL=''Y'';',
''))
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>'-'
,p_query_num_rows_type=>'ROW_RANGES_IN_SELECT_LIST'
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_csv_output=>'Y'
,p_csv_output_link_text=>'Download'
,p_prn_output=>'N'
,p_sort_null=>'L'
,p_plug_query_strip_html=>'N'
);
end;
/
begin
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(69229212833028229)
,p_query_column_id=>1
,p_column_alias=>'FROM_PUBLISHER_NUM'
,p_column_display_sequence=>1
,p_column_heading=>'From Publisher Number'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(69229303921028230)
,p_query_column_id=>2
,p_column_alias=>'MEMO_TYPE_CODE'
,p_column_display_sequence=>2
,p_column_heading=>'Memo Type Code'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(69229456100028231)
,p_query_column_id=>3
,p_column_alias=>'SONG_CODE'
,p_column_display_sequence=>3
,p_column_heading=>'Song Code'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(69229522944028232)
,p_query_column_id=>4
,p_column_alias=>'SONG_TITLE'
,p_column_display_sequence=>4
,p_column_heading=>'Song Title'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(69229609213028233)
,p_query_column_id=>5
,p_column_alias=>'PUBLISHER_SEQ_NUM'
,p_column_display_sequence=>5
,p_column_heading=>'Publisher Seq Number'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(69229743455028234)
,p_query_column_id=>6
,p_column_alias=>'PUBLISHER_NUM'
,p_column_display_sequence=>6
,p_column_heading=>'Publisher Number'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(69229834548028235)
,p_query_column_id=>7
,p_column_alias=>'PUBLISHER_IP_ID'
,p_column_display_sequence=>7
,p_column_heading=>'Publisher IP ID'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(69229988975028236)
,p_query_column_id=>8
,p_column_alias=>'PUBLISHER_SPLIT'
,p_column_display_sequence=>8
,p_column_heading=>'Publisher Split'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(63433904888030657)
,p_plug_name=>'IPI Header&Agr'
,p_region_name=>'HIPH'
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--scrollBody'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>120
,p_include_in_reg_disp_sel_yn=>'Y'
,p_plug_display_point=>'BODY'
,p_plug_query_num_rows_type=>'NEXT_PREVIOUS_LINKS'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_plug_query_show_nulls_as=>' - '
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_attribute_01=>'N'
,p_attribute_02=>'HTML'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(63437118105030659)
,p_name=>'IPI Names'
,p_parent_plug_id=>wwv_flow_api.id(63433904888030657)
,p_template=>wwv_flow_api.id(51848808455240825)
,p_display_sequence=>10
,p_region_template_options=>'#DEFAULT#'
,p_component_template_options=>'#DEFAULT#'
,p_new_grid_row=>false
,p_display_column=>1
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select trim(name_type) name_type, trim(name_type_desc) name_type_desc, trim(name) name, trim(first_name) first_name, trim(name_number) name_number  ',
', to_char(to_date(to_char(date_of_creation,''DD-MON-YYYY'') ||'' ''||time_of_creation,''DD-MON-YYYY HH24MISS''),''MM/DD/YYYY HH24:MI:SS'') creation_datetime',
', to_char(to_date(to_char(date_of_last_ammendment,''DD-MON-YYYY'') ||'' ''||time_of_last_ammendment,''DD-MON-YYYY HH24MISS''),''MM/DD/YYYY HH24:MI:SS'') ammend_datetime',
'from IPI_NAME_CONN inc, ipi_name_type int',
'where INT.IP_NAME_TYPE_ID = INC.NAME_TYPE_ID',
'and inc.interested_party_id = :P260_INTERESTED_PARTY_ID',
'and  :P260_INTERESTED_PARTY_ID is not null and :P260_QUERY_IPIHED=''Y''',
''))
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
 p_id=>wwv_flow_api.id(63437585998030660)
,p_query_column_id=>1
,p_column_alias=>'NAME_TYPE'
,p_column_display_sequence=>1
,p_column_heading=>'Name Type'
,p_default_sort_column_sequence=>1
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63437906120030660)
,p_query_column_id=>2
,p_column_alias=>'NAME_TYPE_DESC'
,p_column_display_sequence=>2
,p_column_heading=>'Name Type Desc'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63438340357030660)
,p_query_column_id=>3
,p_column_alias=>'NAME'
,p_column_display_sequence=>3
,p_column_heading=>'Name'
,p_default_sort_column_sequence=>2
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63438739627030661)
,p_query_column_id=>4
,p_column_alias=>'FIRST_NAME'
,p_column_display_sequence=>4
,p_column_heading=>'First Name'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63439118360030661)
,p_query_column_id=>5
,p_column_alias=>'NAME_NUMBER'
,p_column_display_sequence=>5
,p_column_heading=>'Name Number'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63439551171030661)
,p_query_column_id=>6
,p_column_alias=>'CREATION_DATETIME'
,p_column_display_sequence=>6
,p_column_heading=>'Creation Datetime'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63439969967030661)
,p_query_column_id=>7
,p_column_alias=>'AMMEND_DATETIME'
,p_column_display_sequence=>7
,p_column_heading=>'Ammend Datetime'
,p_disable_sort_column=>'N'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(63440345180030662)
,p_name=>'IPI Agreements'
,p_parent_plug_id=>wwv_flow_api.id(63433904888030657)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>20
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#:t-Report--altRowsDefault:t-Report--rowHighlight'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select RC.ROLE_CODE_ID, interested_party_id, trim(soc.society_name)||'' ''||soc.society_country society, cc.creation_class_code||'' ''||CC.CREATION_CLASS_DESC Creation_class, RC.ROLE_CODE||'' ''||RC.ROLE_DESC role',
', ima.signature_date, To_char(ima.membership_share',
'/100,''009.99'') membership_share, to_char(to_date(to_char(ima.valid_from_date,''DD-MON-YYYY'') ||'' ''||ima.valid_from_time,''DD-MON-YYYY HH24MISS''),''MM/DD/YYYY HH24:MI:SS'') valid_from_datetime',
', to_char(to_date(to_char(ima.valid_to_date,''DD-MON-YYYY'') ||'' ''||ima.valid_to_time,''DD-MON-YYYY HH24MISS''),''MM/DD/YYYY HH24:MI:SS'') valid_to_datetime',
', max(to_char(to_date(to_char(AMMENDMENT_DATE,''DD-MON-YYYY'') ||'' ''||AMMENDMENT_TIME,''DD-MON-YYYY HH24MISS''),''MM/DD/YYYY HH24:MI:SS'')) ammend_datetime',
', max(ima.membership_agreement_id) membership_agreement_id, count(1) count_rows',
'from ipi_membership_agreement ima, IPI_SOCIETIES soc, IPI_ROLE_CODES rc ',
', IPI_CREATION_CLASSES cc',
'where interested_party_id = :P260_INTERESTED_PARTY_ID',
'and SOC.SOCIETY_CODE_ID = IMA.SOCIETY_CODE_ID',
'and CC.CREATION_CLASS_ID = IMA.CREATION_CLASS_ID',
'and RC.ROLE_CODE_ID = IMA.ROLE_CODE_ID and  :P260_QUERY_IPIHED=''Y''',
'group by RC.ROLE_CODE_ID, interested_party_id, soc.society_name, soc.society_country, cc.creation_class_code, CC.CREATION_CLASS_DESC, RC.ROLE_CODE, RC.ROLE_DESC',
', ima.signature_date, ima.membership_share',
', to_char(to_date(to_char(ima.valid_from_date,''DD-MON-YYYY'') ||'' ''||ima.valid_from_time,''DD-MON-YYYY HH24MISS''),''MM/DD/YYYY HH24:MI:SS'') ',
', to_char(to_date(to_char(ima.valid_to_date,''DD-MON-YYYY'') ||'' ''||ima.valid_to_time,''DD-MON-YYYY HH24MISS''),''MM/DD/YYYY HH24:MI:SS'')'))
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>'-'
,p_query_num_rows_type=>'ROW_RANGES_WITH_LINKS'
,p_pagination_display_position=>'TOP_AND_BOTTOM_LEFT'
,p_csv_output=>'Y'
,p_csv_output_link_text=>'Download'
,p_prn_output=>'N'
,p_sort_null=>'L'
,p_query_asc_image=>'apex/builder/dup.gif'
,p_query_asc_image_attr=>'width="16" height="16" alt="" '
,p_query_desc_image=>'apex/builder/ddown.gif'
,p_query_desc_image_attr=>'width="16" height="16" alt="" '
,p_plug_query_strip_html=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(7575397807132009)
,p_query_column_id=>1
,p_column_alias=>'ROLE_CODE_ID'
,p_column_display_sequence=>12
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(7575295502132008)
,p_query_column_id=>2
,p_column_alias=>'INTERESTED_PARTY_ID'
,p_column_display_sequence=>11
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63881291864205829)
,p_query_column_id=>3
,p_column_alias=>'SOCIETY'
,p_column_display_sequence=>1
,p_column_heading=>'Society'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63881332135205830)
,p_query_column_id=>4
,p_column_alias=>'CREATION_CLASS'
,p_column_display_sequence=>2
,p_column_heading=>'Creation Class'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63881421484205831)
,p_query_column_id=>5
,p_column_alias=>'ROLE'
,p_column_display_sequence=>3
,p_column_heading=>'Role'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63881560458205832)
,p_query_column_id=>6
,p_column_alias=>'SIGNATURE_DATE'
,p_column_display_sequence=>5
,p_column_heading=>'Signature Date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63881694536205833)
,p_query_column_id=>7
,p_column_alias=>'MEMBERSHIP_SHARE'
,p_column_display_sequence=>4
,p_column_heading=>'Membership Share'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63881778804205834)
,p_query_column_id=>8
,p_column_alias=>'VALID_FROM_DATETIME'
,p_column_display_sequence=>6
,p_column_heading=>'Valid From Datetime'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63881861346205835)
,p_query_column_id=>9
,p_column_alias=>'VALID_TO_DATETIME'
,p_column_display_sequence=>7
,p_column_heading=>'Valid To Datetime'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63881918845205836)
,p_query_column_id=>10
,p_column_alias=>'AMMEND_DATETIME'
,p_column_display_sequence=>8
,p_column_heading=>'Ammend Datetime'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63882080288205837)
,p_query_column_id=>11
,p_column_alias=>'MEMBERSHIP_AGREEMENT_ID'
,p_column_display_sequence=>9
,p_column_heading=>'Membership Agreement ID'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63882163686205838)
,p_query_column_id=>12
,p_column_alias=>'COUNT_ROWS'
,p_column_display_sequence=>10
,p_column_heading=>'Count rows'
,p_column_link=>'f?p=&APP_ID.:6:&SESSION.::&DEBUG.:RP,6:P6_PARTY_ID,P6_ROLE_CODE_ID:#INTERESTED_PARTY_ID#,#ROLE_CODE_ID#'
,p_column_linktext=>'#COUNT_ROWS#'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(63450024162030678)
,p_name=>'Affiliation Agr. Never'
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>190
,p_include_in_reg_disp_sel_yn=>'Y'
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#:t-Report--altRowsDefault:t-Report--rowHighlight'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT trim(soc.society_name)',
'  ||'' ''',
'  ||soc.society_country society ,',
'  RC.ROLE_CODE',
'  ||'' ''',
'  ||RC.ROLE_DESC role_type ,',
'  RT.RIGHT_TYPE',
'  ||'' ''',
'  ||RT.RIGHT_DESCription right_type,',
'  ama.signature_date,',
' -- to_char(ama.membership_share/100,''009.99'') ',
'ama.membership_share membership_share ,',
'  valid_from_date,',
'  valid_to_date,',
'  AMMENDMENT_DATE,',
'  effective_date ,',
'  RENEW_ALLOWED_FLAG,',
'  RENEWAL_TERM_MONTHS ,',
'Aff_Ip.Get_Renewal_Date(ama.membership_agreement_id) renewal_date,',
'  ama.membership_agreement_id',
'FROM AFF_MEMBERSHIP_AGREEMENTS ama,',
'  AFF_SOCIETIES soc,',
'  AFF_ROLES rc,',
'  AFF_RIGHT_TYPES rt',
'WHERE ip_id          = :P260_IP_ID',
'AND SOC.SOCIETY_ID   = AMA.SOCIETY_ID',
'AND RC.ROLE_ID       = AMA.ROLE_ID',
'AND RT.RIGHT_TYPE_ID = AMA.RIGHT_TYPE_ID',
'AND :P260_IP_ID     IS NOT NULL',
'AND AMA.SOCIETY_ID   = NVL( :P260_Society_Filter,AMA.SOCIETY_ID)'))
,p_display_condition_type=>'NEVER'
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>'-'
,p_query_num_rows_type=>'ROW_RANGES_WITH_LINKS'
,p_pagination_display_position=>'TOP_AND_BOTTOM_LEFT'
,p_csv_output=>'Y'
,p_csv_output_link_text=>'Download'
,p_prn_output=>'N'
,p_sort_null=>'L'
,p_query_asc_image=>'apex/builder/dup.gif'
,p_query_asc_image_attr=>'width="16" height="16" alt="" '
,p_query_desc_image=>'apex/builder/ddown.gif'
,p_query_desc_image_attr=>'width="16" height="16" alt="" '
,p_plug_query_strip_html=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63872428886184841)
,p_query_column_id=>1
,p_column_alias=>'SOCIETY'
,p_column_display_sequence=>1
,p_column_heading=>'Society'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63872518523184842)
,p_query_column_id=>2
,p_column_alias=>'ROLE_TYPE'
,p_column_display_sequence=>2
,p_column_heading=>'Role type'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63872691305184843)
,p_query_column_id=>3
,p_column_alias=>'RIGHT_TYPE'
,p_column_display_sequence=>3
,p_column_heading=>'Right type'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63872733857184844)
,p_query_column_id=>4
,p_column_alias=>'SIGNATURE_DATE'
,p_column_display_sequence=>4
,p_column_heading=>'Signature date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63872882119184845)
,p_query_column_id=>5
,p_column_alias=>'MEMBERSHIP_SHARE'
,p_column_display_sequence=>5
,p_column_heading=>'Membership share'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63872910196184846)
,p_query_column_id=>6
,p_column_alias=>'VALID_FROM_DATE'
,p_column_display_sequence=>6
,p_column_heading=>'Valid from date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63873006040184847)
,p_query_column_id=>7
,p_column_alias=>'VALID_TO_DATE'
,p_column_display_sequence=>7
,p_column_heading=>'Valid to date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63873109239184848)
,p_query_column_id=>8
,p_column_alias=>'AMMENDMENT_DATE'
,p_column_display_sequence=>8
,p_column_heading=>'Ammendment date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63873210208184849)
,p_query_column_id=>9
,p_column_alias=>'EFFECTIVE_DATE'
,p_column_display_sequence=>9
,p_column_heading=>'Effective date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63873374540184850)
,p_query_column_id=>10
,p_column_alias=>'RENEW_ALLOWED_FLAG'
,p_column_display_sequence=>10
,p_column_heading=>'Renew allowed flag'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63873468879198301)
,p_query_column_id=>11
,p_column_alias=>'RENEWAL_TERM_MONTHS'
,p_column_display_sequence=>11
,p_column_heading=>'Renewal term months'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63873557556198302)
,p_query_column_id=>12
,p_column_alias=>'RENEWAL_DATE'
,p_column_display_sequence=>12
,p_column_heading=>'Renewal date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63873636682198303)
,p_query_column_id=>13
,p_column_alias=>'MEMBERSHIP_AGREEMENT_ID'
,p_column_display_sequence=>13
,p_column_heading=>'Membership agreement id'
,p_column_link=>'f?p=&APP_ID.:7240:&SESSION.::&DEBUG.:RP::'
,p_column_linktext=>'#MEMBERSHIP_AGREEMENT_ID#'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(63457273523030684)
,p_name=>'Collection Agr(NEVER)'
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>170
,p_include_in_reg_disp_sel_yn=>'Y'
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#:t-Report--stretch:t-Report--altRowsDefault:t-Report--rowHighlight'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT * FROM (SELECT REGEXP_COUNT(heir_path, ''/'', 1, ''i'') the_level,',
'           collector_agreement_id,',
'           collector_id,',
'           (select ip_number||'' - ''||primary_name from aff_ip_name_v where ip_id = collector_id ) collector,',
'           collects_on_agreement_id,',
'           share_percent,',
'           heir_path,',
'          RIGHT_TYPE_ID',
'      FROM PROP_COLLECTOR_AGREEMENTS',
'CONNECT BY PRIOR collector_Agreement_id = collects_on_Agreement_id',
'START WITH collector_id = :P260_ip_id',
'AND collects_on_agreement_id IS NULL',
'union',
'SELECT REGEXP_COUNT(heir_path, ''/'', 1, ''i'') the_level,',
'           collector_agreement_id,',
'           collector_id,',
'           (select ip_number||'' - ''||primary_name from aff_ip_name_v where ip_id = collector_id ) collector,',
'           collects_on_agreement_id,',
'           share_percent,',
'           heir_path ,',
'          RIGHT_TYPE_ID',
'FROM rep_affiliates ra, PROP_COLLECTOR_AGREEMENTS pca',
'WHERE ra.ip_id = :P260_ip_id',
'AND instr(heir_path,ra.aff_num,1) > 0 ',
'AND collects_on_agreement_id IS NOT NULL) order by RIGHT_TYPE_ID',
''))
,p_display_condition_type=>'NEVER'
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>'-'
,p_query_num_rows_type=>'ROW_RANGES_WITH_LINKS'
,p_pagination_display_position=>'TOP_AND_BOTTOM_LEFT'
,p_csv_output=>'Y'
,p_csv_output_link_text=>'Download'
,p_prn_output=>'N'
,p_sort_null=>'L'
,p_query_asc_image=>'apex/builder/dup.gif'
,p_query_asc_image_attr=>'width="16" height="16" alt="" '
,p_query_desc_image=>'apex/builder/ddown.gif'
,p_query_desc_image_attr=>'width="16" height="16" alt="" '
,p_plug_query_strip_html=>'N'
,p_comment=>wwv_flow_string.join(wwv_flow_t_varchar2(
'',
'',
'SELECT * FROM (SELECT REGEXP_COUNT(heir_path, ''/'', 1, ''i'') the_level,',
'           collector_agreement_id,',
'           collector_id,',
'           (select ip_number||'' - ''||primary_name from aff_ip_name_v where ip_id = collector_id ) collector,',
'           collects_on_agreement_id,',
'           share_percent,',
'           heir_path,',
'          RIGHT_TYPE_ID',
'      FROM PROP_COLLECTOR_AGREEMENTS',
'CONNECT BY PRIOR collector_Agreement_id = collects_on_Agreement_id',
'START WITH collector_id = :P260_ip_id',
'AND collects_on_agreement_id IS NULL',
'union',
'SELECT REGEXP_COUNT(heir_path, ''/'', 1, ''i'') the_level,',
'           collector_agreement_id,',
'           collector_id,',
'           (select ip_number||'' - ''||primary_name from aff_ip_name_v where ip_id = collector_id ) collector,',
'           collects_on_agreement_id,',
'           share_percent,',
'           heir_path ,',
'          RIGHT_TYPE_ID',
'FROM rep_affiliates ra, PROP_COLLECTOR_AGREEMENTS pca',
'WHERE ra.ip_id = :P260_ip_id',
'AND instr(heir_path,ra.aff_num,1) > 0 ',
'AND collects_on_agreement_id IS NOT NULL) order by RIGHT_TYPE_ID',
''))
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63871276077184829)
,p_query_column_id=>1
,p_column_alias=>'THE_LEVEL'
,p_column_display_sequence=>1
,p_column_heading=>'The level'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63871312263184830)
,p_query_column_id=>2
,p_column_alias=>'COLLECTOR_AGREEMENT_ID'
,p_column_display_sequence=>7
,p_column_heading=>'Collector Agreement ID'
,p_column_link=>'f?p=&APP_ID.:7213:&SESSION.::&DEBUG.:RP:P7213_COLLECTOR_AGREEMENT_ID,P7213_CALLING_PAGE_ID,P7213_REQUEST:#COLLECTOR_AGREEMENT_ID#,&APP_PAGE_ID.,COLLAGR'
,p_column_linktext=>'#COLLECTOR_AGREEMENT_ID#'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63871459888184831)
,p_query_column_id=>3
,p_column_alias=>'COLLECTOR_ID'
,p_column_display_sequence=>2
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63871556870184832)
,p_query_column_id=>4
,p_column_alias=>'COLLECTOR'
,p_column_display_sequence=>3
,p_column_heading=>'Collector'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63871649387184833)
,p_query_column_id=>5
,p_column_alias=>'COLLECTS_ON_AGREEMENT_ID'
,p_column_display_sequence=>4
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63871710573184834)
,p_query_column_id=>6
,p_column_alias=>'SHARE_PERCENT'
,p_column_display_sequence=>5
,p_column_heading=>'Share percent'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63871862529184835)
,p_query_column_id=>7
,p_column_alias=>'HEIR_PATH'
,p_column_display_sequence=>6
,p_column_heading=>'Heir path'
,p_use_as_row_header=>'N'
,p_lov_show_nulls=>'YES'
,p_derived_column=>'N'
,p_lov_display_extra=>'YES'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(25407402487142438)
,p_query_column_id=>8
,p_column_alias=>'RIGHT_TYPE_ID'
,p_column_display_sequence=>8
,p_column_heading=>'Right Type'
,p_use_as_row_header=>'N'
,p_display_as=>'TEXT_FROM_LOV'
,p_named_lov=>wwv_flow_api.id(62952354574980407)
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(63460421216030686)
,p_name=>'Bands'
,p_region_name=>'BAND'
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>60
,p_include_in_reg_disp_sel_yn=>'Y'
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#:t-Report--altRowsDefault:t-Report--rowHighlight'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select * from APCTS.AFF_IP_BANDS',
'where ip_id = :P260_IP_ID and :P260_QUERY_BANDS=''Y'''))
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>'-'
,p_query_num_rows_type=>'ROW_RANGES_WITH_LINKS'
,p_pagination_display_position=>'TOP_AND_BOTTOM_LEFT'
,p_csv_output=>'Y'
,p_csv_output_link_text=>'Download'
,p_prn_output=>'N'
,p_sort_null=>'L'
,p_query_asc_image=>'apex/builder/dup.gif'
,p_query_asc_image_attr=>'width="16" height="16" alt="" '
,p_query_desc_image=>'apex/builder/ddown.gif'
,p_query_desc_image_attr=>'width="16" height="16" alt="" '
,p_plug_query_strip_html=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(62547484545590448)
,p_query_column_id=>1
,p_column_alias=>'IP_BAND_ID'
,p_column_display_sequence=>1
,p_use_as_row_header=>'N'
,p_column_link=>'f?p=&APP_ID.:50300:&SESSION.::&DEBUG.:RP,50300:P50300_IP_BAND_ID:#IP_BAND_ID#'
,p_column_linktext=>'<img src="#IMAGE_PREFIX#menu/pencil16x16.gif" alt="">'
,p_disable_sort_column=>'N'
,p_report_column_required_role=>wwv_flow_api.id(69665980648432325)
,p_lov_show_nulls=>'YES'
,p_derived_column=>'N'
,p_lov_display_extra=>'YES'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(62547571827590449)
,p_query_column_id=>2
,p_column_alias=>'IP_ID'
,p_column_display_sequence=>2
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(62547644448590450)
,p_query_column_id=>3
,p_column_alias=>'IP_NUMBER'
,p_column_display_sequence=>3
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63863422581176301)
,p_query_column_id=>4
,p_column_alias=>'BAND_NAME'
,p_column_display_sequence=>4
,p_column_heading=>'Band name'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63863566207176302)
,p_query_column_id=>5
,p_column_alias=>'START_DATE'
,p_column_display_sequence=>5
,p_column_heading=>'Start date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63863611112176303)
,p_query_column_id=>6
,p_column_alias=>'END_DATE'
,p_column_display_sequence=>6
,p_column_heading=>'End date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63863726622176304)
,p_query_column_id=>7
,p_column_alias=>'CREATED_BY'
,p_column_display_sequence=>7
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63863867017176305)
,p_query_column_id=>8
,p_column_alias=>'CREATION_DATE'
,p_column_display_sequence=>8
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63863974863176306)
,p_query_column_id=>9
,p_column_alias=>'LAST_UPDATED_BY'
,p_column_display_sequence=>9
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63864048472176307)
,p_query_column_id=>10
,p_column_alias=>'LAST_UPDATE_DATE'
,p_column_display_sequence=>10
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(63464898628030689)
,p_plug_name=>'Change History'
,p_region_name=>'HIST'
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>210
,p_include_in_reg_disp_sel_yn=>'Y'
,p_plug_display_point=>'BODY'
,p_query_type=>'SQL'
,p_plug_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select * from ',
'( ',
'select column_name, ',
'date_changed, changed_by, ',
' CASE COLUMN_NAME ',
' WHEN ''TAX_ID'' THEN regexp_replace (substr(PREV_VALUE,1,length(PREV_VALUE)-4), ''\d'', ''X'')||substr(PREV_VALUE,-4) ',
' WHEN ''ORGANIZATION_TYPE'' THEN NVL((SELECT description FROM aff_lookups WHERE UPPER(lookup_type) =  ''MC_ORGANIZATION_TYPE'' AND lookup_code = prev_value), prev_value) ',
' else PREV_VALUE END prev_value, ',
' CASE COLUMN_NAME WHEN ''TAX_ID'' THEN regexp_replace (substr(new_value ,1,length(new_value )-4), ''\d'', ''X'')||substr(new_value ,-4) ',
' WHEN ''ORGANIZATION_TYPE'' THEN NVL((SELECT description FROM aff_lookups WHERE UPPER(lookup_type) =  ''MC_ORGANIZATION_TYPE'' AND lookup_code = NEW_VALUE), NEW_VALUE)',
' else NEW_VALUE  END new_value ',
'from apcts.hist_change_logs',
'where SQL_CODE =''AFFILIATE''',
'and table_name in ( ''AFF_INTERESTED_PARTIES'',',
'                    ''AFF_IP_ADDRESSES'',',
'                    ''AFF_IP_CONTACTS'',',
'                    ''AFF_IP_CONTACT_POINTS'',',
'                    ''AFF_IP_NAMES'',',
'                    ''AFF_IP_NOTES'',',
'                    ''AFF_IP_NOTES'',',
'                    ''AFF_MEMBERSHIP_AGREEMENTS''',
'                    )',
'and key_value1 = :P260_IP_ID',
'--and column_name <> ''TAX_ID''',
')',
'where :P260_QUERY_HISTORY = ''Y'''))
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
,p_plug_header=>'Please enter search criteria and press go to retrieve data '
);
end;
/
begin
wwv_flow_api.create_worksheet(
 p_id=>wwv_flow_api.id(67582526364106027)
,p_max_row_count=>'1000000'
,p_show_nulls_as=>'-'
,p_pagination_type=>'ROWS_X_TO_Y_OF_Z'
,p_pagination_display_pos=>'TOP_AND_BOTTOM_LEFT'
,p_show_display_row_count=>'Y'
,p_report_list_mode=>'TABS'
,p_show_detail_link=>'N'
,p_show_notify=>'Y'
,p_show_flashback=>'N'
,p_download_formats=>'CSV:HTML:EMAIL:XLS:PDF:RTF'
,p_owner=>'DCOLLINS'
,p_internal_uid=>67582526364106027
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(67582666730106028)
,p_db_column_name=>'COLUMN_NAME'
,p_display_order=>10
,p_column_identifier=>'A'
,p_column_label=>'Column name'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(67582969928106031)
,p_db_column_name=>'PREV_VALUE'
,p_display_order=>20
,p_column_identifier=>'D'
,p_column_label=>'Prev value'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(67583019961106032)
,p_db_column_name=>'NEW_VALUE'
,p_display_order=>30
,p_column_identifier=>'E'
,p_column_label=>'New value'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(67582728213106029)
,p_db_column_name=>'DATE_CHANGED'
,p_display_order=>40
,p_column_identifier=>'B'
,p_column_label=>'Date changed'
,p_column_type=>'DATE'
,p_heading_alignment=>'LEFT'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(67582838779106030)
,p_db_column_name=>'CHANGED_BY'
,p_display_order=>50
,p_column_identifier=>'C'
,p_column_label=>'Changed by'
,p_column_type=>'STRING'
,p_heading_alignment=>'LEFT'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(67699176387803509)
,p_application_user=>'APXWS_DEFAULT'
,p_report_seq=>10
,p_report_alias=>'676992'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'COLUMN_NAME:DATE_CHANGED:CHANGED_BY:PREV_VALUE:NEW_VALUE'
,p_sort_column_1=>'DATE_CHANGED'
,p_sort_direction_1=>'DESC'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(63474031644030696)
,p_plug_name=>'Account'
,p_region_name=>'ACCOUNTS'
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--scrollBody'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>40
,p_include_in_reg_disp_sel_yn=>'Y'
,p_plug_display_point=>'BODY'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_attribute_01=>'N'
,p_attribute_02=>'HTML'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(9815489076107247)
,p_name=>'Prospect Opportunities'
,p_parent_plug_id=>wwv_flow_api.id(63474031644030696)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>20
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#:t-Report--altRowsDefault:t-Report--rowHighlight'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select SFDC_OPPORTUNITY_ID, opp_status, to_char(last_update_date, ''YYYY-MM-DD HH24:MI:SS'') last_changed ',
'from aff_prospect_opportunities',
'where ip_id = :P260_IP_ID',
'AND :P260_QUERY_ACCOUNT=''Y''',
'order by aff_prospect_opportunity_id desc',
''))
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
 p_id=>wwv_flow_api.id(23099792662243701)
,p_query_column_id=>1
,p_column_alias=>'SFDC_OPPORTUNITY_ID'
,p_column_display_sequence=>1
,p_column_heading=>'Sfdc Opportunity ID'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(9815576528107248)
,p_query_column_id=>2
,p_column_alias=>'OPP_STATUS'
,p_column_display_sequence=>2
,p_column_heading=>'Opp Status'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(9815658241107249)
,p_query_column_id=>3
,p_column_alias=>'LAST_CHANGED'
,p_column_display_sequence=>3
,p_column_heading=>'Last Changed'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(63381954351030619)
,p_name=>'Contact Points'
,p_parent_plug_id=>wwv_flow_api.id(63474031644030696)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>70
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#:t-Report--altRowsDefault:t-Report--rowHighlight'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select	icp.IP_CONTACT_POINT_ID, icpt.contact_point_type,	icp.display_flag,	icp.primary_flag	',
',	icp.email_address,	icp.phone_country_code||'' ''||	icp.phone_area_code||''-''||icp.phone_number Phone_number',
',	icp.phone_extension,	decode(icp.phone_line_type,''GEN'',''GENERAL'',icp.phone_line_type) phone_line_type,	icp.url,	icp.status',
',	icp.ip_id			',
'from	AFF_IP_CONTACT_POINTS	icp,	AFF_IP_CONT_POINT_TYPES	icpt',
'where	ICP.CONT_POINT_TYPE_ID	=	ICPT.CONT_POINT_TYPE_ID	 ',
'--and (PHONE_LINE_TYPE in (''GEN'',''MOBILE'' ,''FAX'')  or PHONE_LINE_TYPE is null) and  CONTACT_PURPOSE in (''PHONE'',''EMAIL'',''URL'')',
'and	icp.ip_id	=	:P260_ip_id and ip_contact_id is null',
'and :P260_ip_id is not null'))
,p_display_condition_type=>'NEVER'
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>'-'
,p_query_num_rows_type=>'ROW_RANGES_WITH_LINKS'
,p_pagination_display_position=>'TOP_AND_BOTTOM_LEFT'
,p_csv_output=>'Y'
,p_csv_output_link_text=>'Download'
,p_prn_output=>'N'
,p_sort_null=>'L'
,p_query_asc_image=>'apex/builder/dup.gif'
,p_query_asc_image_attr=>'width="16" height="16" alt="" '
,p_query_desc_image=>'apex/builder/ddown.gif'
,p_query_desc_image_attr=>'width="16" height="16" alt="" '
,p_plug_query_strip_html=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(66914810682500807)
,p_query_column_id=>1
,p_column_alias=>'IP_CONTACT_POINT_ID'
,p_column_display_sequence=>1
,p_column_link=>'f?p=&APP_ID.:557:&SESSION.::&DEBUG.:RP:P557_IP_CONTACT_POINT_ID:#IP_CONTACT_POINT_ID#'
,p_column_linktext=>'<img src="#IMAGE_PREFIX#app_ui/img/icons/apex-edit-pencil.png" class="apex-edit-pencil" alt="">'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63867770142176344)
,p_query_column_id=>2
,p_column_alias=>'CONTACT_POINT_TYPE'
,p_column_display_sequence=>2
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63867878818176345)
,p_query_column_id=>3
,p_column_alias=>'DISPLAY_FLAG'
,p_column_display_sequence=>9
,p_column_heading=>'Display Flag'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63867952406176346)
,p_query_column_id=>4
,p_column_alias=>'PRIMARY_FLAG'
,p_column_display_sequence=>8
,p_column_heading=>'Primary Flag'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63868022520176347)
,p_query_column_id=>5
,p_column_alias=>'EMAIL_ADDRESS'
,p_column_display_sequence=>6
,p_column_heading=>'Email Address'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63868146308176348)
,p_query_column_id=>6
,p_column_alias=>'PHONE_NUMBER'
,p_column_display_sequence=>4
,p_column_heading=>'Phone Number'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63868222030176349)
,p_query_column_id=>7
,p_column_alias=>'PHONE_EXTENSION'
,p_column_display_sequence=>5
,p_column_heading=>'Phone Extension'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63868383236176350)
,p_query_column_id=>8
,p_column_alias=>'PHONE_LINE_TYPE'
,p_column_display_sequence=>3
,p_column_heading=>'Phone Line Type'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63868427365184801)
,p_query_column_id=>9
,p_column_alias=>'URL'
,p_column_display_sequence=>7
,p_column_heading=>'URL'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63868576611184802)
,p_query_column_id=>10
,p_column_alias=>'STATUS'
,p_column_display_sequence=>10
,p_column_heading=>'Status'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63868646545184803)
,p_query_column_id=>11
,p_column_alias=>'IP_ID'
,p_column_display_sequence=>11
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(63433124679030657)
,p_name=>'Email/URL'
,p_parent_plug_id=>wwv_flow_api.id(63474031644030696)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>80
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#:t-Report--altRowsDefault:t-Report--rowHighlight'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select email_address contact_point ',
'from aff_ip_contact_points',
'where cont_point_type_id = 2',
'and ip_contact_id is null',
'and ip_id = :P260_ip_id',
'union',
'select url contact_point from aff_ip_contact_points',
'where cont_point_type_id = 3',
'and ip_contact_id is null',
'and ip_id = :P260_ip_id'))
,p_display_condition_type=>'NEVER'
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>' - '
,p_query_no_data_found=>'No Email/URL data found'
,p_query_num_rows_type=>'ROW_RANGES_WITH_LINKS'
,p_query_row_count_max=>500
,p_pagination_display_position=>'TOP_AND_BOTTOM_LEFT'
,p_csv_output=>'Y'
,p_csv_output_link_text=>'Download'
,p_prn_output=>'N'
,p_sort_null=>'L'
,p_query_asc_image=>'apex/builder/dup.gif'
,p_query_asc_image_attr=>'width="16" height="16" alt="" '
,p_query_desc_image=>'apex/builder/ddown.gif'
,p_query_desc_image_attr=>'width="16" height="16" alt="" '
,p_plug_query_strip_html=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63433559752030657)
,p_query_column_id=>1
,p_column_alias=>'CONTACT_POINT'
,p_column_display_sequence=>1
,p_column_heading=>'Email/URL'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(63478970726030700)
,p_name=>'Contact'
,p_parent_plug_id=>wwv_flow_api.id(63474031644030696)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>40
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT aic.last_name||'' ''||aic.first_name Name,',
'acp.phone_area_code||'' ''||acp.phone_number Phone_Number',
'FROM aff_ip_contacts aic,aff_ip_contact_points acp',
',AFF_IP_CONTACT_TYPES ict -- Added by Vignesh Kumar on 7-May-2020 As per MSPI-109',
'WHERE aic.ip_id = acp.ip_id',
'AND ict.CONTACT_TYPE_ID = aic.CONT_TYPE_ID -- Added by Vignesh Kumar on 7-May-2020 As per MSPI-109',
'AND ict.contact_type    <> ''MLC_SELF'' -- Added by Vignesh Kumar on 7-May-2020 As per MSPI-109',
'AND NVL(aic.status, ''A'') = ''A''',
'AND NVL(acp.status, ''ACTIVE'') = ''ACTIVE''',
'AND aic.ip_id = :P260_IP_ID',
'AND acp.cont_point_type_id = 1',
'AND acp.phone_line_type IS NOT NULL',
'AND rownum =1  AND :P260_QUERY_ACCOUNT=''Y'''))
,p_display_when_condition=>':P260_AFF_IPI_TYPE = ''L'''
,p_display_condition_type=>'SQL_EXPRESSION'
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_headings_type=>'QUERY_COLUMNS_INITCAP'
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>' - '
,p_query_no_data_found=>'No contact data found'
,p_query_num_rows_type=>'ROW_RANGES_WITH_LINKS'
,p_query_row_count_max=>500
,p_pagination_display_position=>'TOP_AND_BOTTOM_LEFT'
,p_csv_output=>'Y'
,p_csv_output_link_text=>'Download'
,p_prn_output=>'N'
,p_sort_null=>'L'
,p_query_asc_image=>'apex/builder/dup.gif'
,p_query_asc_image_attr=>'width="16" height="16" alt="" '
,p_query_desc_image=>'apex/builder/ddown.gif'
,p_query_desc_image_attr=>'width="16" height="16" alt="" '
,p_plug_query_strip_html=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63479306581030701)
,p_query_column_id=>1
,p_column_alias=>'NAME'
,p_column_display_sequence=>1
,p_column_heading=>'NAME'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63479764425030701)
,p_query_column_id=>2
,p_column_alias=>'PHONE_NUMBER'
,p_column_display_sequence=>2
,p_column_heading=>'PHONE_NUMBER'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(63480178688030701)
,p_name=>'Remit To'
,p_parent_plug_id=>wwv_flow_api.id(63474031644030696)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>50
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select * from spi_remit_info_mv',
'where ip_id = :P260_ip_id',
'and :P260_ip_id is not null AND :P260_QUERY_ACCOUNT=''Y'''))
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>' - '
,p_query_no_data_found=>'No Remit To data found'
,p_query_num_rows_type=>'ROW_RANGES_WITH_LINKS'
,p_query_row_count_max=>500
,p_pagination_display_position=>'TOP_AND_BOTTOM_LEFT'
,p_csv_output=>'Y'
,p_csv_output_link_text=>'Download'
,p_prn_output=>'N'
,p_sort_null=>'L'
,p_query_asc_image=>'apex/builder/dup.gif'
,p_query_asc_image_attr=>'width="16" height="16" alt="" '
,p_query_desc_image=>'apex/builder/ddown.gif'
,p_query_desc_image_attr=>'width="16" height="16" alt="" '
,p_plug_query_strip_html=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63480570222030702)
,p_query_column_id=>1
,p_column_alias=>'IP_ID'
,p_column_display_sequence=>1
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63480930559030702)
,p_query_column_id=>2
,p_column_alias=>'RELATIONSHIP_ID'
,p_column_display_sequence=>2
,p_column_heading=>'Relationship ID'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63481367748030702)
,p_query_column_id=>3
,p_column_alias=>'PARTY_ID'
,p_column_display_sequence=>3
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63481715722030703)
,p_query_column_id=>4
,p_column_alias=>'SUPPLIER_SITE_ID'
,p_column_display_sequence=>5
,p_column_heading=>'Supplier Site ID'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63482142300030703)
,p_query_column_id=>5
,p_column_alias=>'REMIT_PARTY_ID'
,p_column_display_sequence=>6
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63482550949030703)
,p_query_column_id=>6
,p_column_alias=>'REMIT_SUPPLIER_SITE_ID'
,p_column_display_sequence=>8
,p_column_heading=>'Remit Supplier Site ID'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63482973364030703)
,p_query_column_id=>7
,p_column_alias=>'FROM_DATE'
,p_column_display_sequence=>9
,p_column_heading=>'From Date'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63483313515030704)
,p_query_column_id=>8
,p_column_alias=>'TO_DATE'
,p_column_display_sequence=>10
,p_column_heading=>'To Date'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63483796654030704)
,p_query_column_id=>9
,p_column_alias=>'PRIMARY_FLAG'
,p_column_display_sequence=>11
,p_column_heading=>'Primary Flag'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63484141461030704)
,p_query_column_id=>10
,p_column_alias=>'ACTIVE'
,p_column_display_sequence=>12
,p_column_heading=>'Active'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63484580850030705)
,p_query_column_id=>11
,p_column_alias=>'VENDOR_ID'
,p_column_display_sequence=>13
,p_column_heading=>'Vendor ID'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63484951896030705)
,p_query_column_id=>12
,p_column_alias=>'VENDOR_SITE_CODE'
,p_column_display_sequence=>14
,p_column_heading=>'Vendor Site Code'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63485364500030705)
,p_query_column_id=>13
,p_column_alias=>'ADDRESS_LINE1'
,p_column_display_sequence=>15
,p_column_heading=>'Address Line1'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63485781431030705)
,p_query_column_id=>14
,p_column_alias=>'ADDRESS_LINE2'
,p_column_display_sequence=>16
,p_column_heading=>'Address Line2'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63486115848030706)
,p_query_column_id=>15
,p_column_alias=>'ADDRESS_LINE3'
,p_column_display_sequence=>17
,p_column_heading=>'Address Line3'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63486527561030706)
,p_query_column_id=>16
,p_column_alias=>'CITY'
,p_column_display_sequence=>18
,p_column_heading=>'City'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63486939057030706)
,p_query_column_id=>17
,p_column_alias=>'STATE'
,p_column_display_sequence=>19
,p_column_heading=>'State'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63487393812030707)
,p_query_column_id=>18
,p_column_alias=>'ZIP'
,p_column_display_sequence=>20
,p_column_heading=>'Zip'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63487751806030707)
,p_query_column_id=>19
,p_column_alias=>'PROVINCE'
,p_column_display_sequence=>21
,p_column_heading=>'Province'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63488100087030707)
,p_query_column_id=>20
,p_column_alias=>'COUNTRY'
,p_column_display_sequence=>22
,p_column_heading=>'Country'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63488518603030707)
,p_query_column_id=>21
,p_column_alias=>'TAX_REPORTING_SITE_FLAG'
,p_column_display_sequence=>23
,p_column_heading=>'Tax Reporting Site Flag'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63488942788030708)
,p_query_column_id=>22
,p_column_alias=>'PRIMARY_PAY_SITE_FLAG'
,p_column_display_sequence=>24
,p_column_heading=>'Primary Pay Site Flag'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63489384748030708)
,p_query_column_id=>23
,p_column_alias=>'LOCATION_ID'
,p_column_display_sequence=>25
,p_column_heading=>'Location Id'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63489781552030708)
,p_query_column_id=>24
,p_column_alias=>'PARTY_SITE_ID'
,p_column_display_sequence=>26
,p_column_heading=>'Party Site Id'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(22892842785692301)
,p_query_column_id=>25
,p_column_alias=>'PARTY_NAME'
,p_column_display_sequence=>4
,p_column_heading=>'Party Name'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(22892944220692302)
,p_query_column_id=>26
,p_column_alias=>'REMIT_PARTY_NAME'
,p_column_display_sequence=>7
,p_column_heading=>'Remit Party Name'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(63490129285030709)
,p_name=>'Non Collection Agr Permission'
,p_parent_plug_id=>wwv_flow_api.id(63474031644030696)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>60
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#'
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
'where ip_id = pcp.earner_id',
'AND name_type_id = 6) Owner,',
'(SELECT full_name from aff_ip_names ',
'where ip_id = pcp.admin_acct',
'AND name_type_id = 6) admin_acct,',
'(SELECT full_name from aff_ip_names ',
'where ip_id = pcp.acct_admined',
'AND name_type_id = 6) acct_admined,',
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
'where collector_agreement_id = -1',
'AND earner_id = :P260_IP_ID AND :P260_QUERY_ACCOUNT=''Y'''))
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>' - '
,p_query_no_data_found=>'No Non Collection Agreements Permissions data found'
,p_query_num_rows_type=>'ROW_RANGES_WITH_LINKS'
,p_query_row_count_max=>500
,p_pagination_display_position=>'TOP_AND_BOTTOM_LEFT'
,p_csv_output=>'Y'
,p_csv_output_link_text=>'Download'
,p_prn_output=>'N'
,p_sort_null=>'L'
,p_query_asc_image=>'apex/builder/dup.gif'
,p_query_asc_image_attr=>'width="16" height="16" alt="" '
,p_query_desc_image=>'apex/builder/ddown.gif'
,p_query_desc_image_attr=>'width="16" height="16" alt="" '
,p_plug_query_strip_html=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63490514076030709)
,p_query_column_id=>1
,p_column_alias=>'AGR_PERMISSION_ID'
,p_column_display_sequence=>1
,p_column_heading=>'Agr Permission Id'
,p_hidden_column=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63490950621030709)
,p_query_column_id=>2
,p_column_alias=>'COLLECTOR_AGREEMENT_ID'
,p_column_display_sequence=>2
,p_column_heading=>'Collector Agreement Id'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63491375422030710)
,p_query_column_id=>3
,p_column_alias=>'COLLECTOR_ID'
,p_column_display_sequence=>3
,p_column_heading=>'Collector Id'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63491767748030710)
,p_query_column_id=>4
,p_column_alias=>'COLLECTOR'
,p_column_display_sequence=>4
,p_column_heading=>'Collector'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63492150761030710)
,p_query_column_id=>5
,p_column_alias=>'EARNER_ID'
,p_column_display_sequence=>5
,p_column_heading=>'Earner Id'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63492540202030710)
,p_query_column_id=>6
,p_column_alias=>'OWNER'
,p_column_display_sequence=>6
,p_column_heading=>'Owner'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63492977719030711)
,p_query_column_id=>7
,p_column_alias=>'ADMIN_ACCT'
,p_column_display_sequence=>7
,p_column_heading=>'Admin Acct'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63493393319030711)
,p_query_column_id=>8
,p_column_alias=>'ACCT_ADMINED'
,p_column_display_sequence=>8
,p_column_heading=>'Acct Admined'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63493755125030712)
,p_query_column_id=>9
,p_column_alias=>'AGREEMENT_TYPE'
,p_column_display_sequence=>9
,p_column_heading=>'Agreement Type'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63494176850030712)
,p_query_column_id=>10
,p_column_alias=>'CAN_VIEW_STATEMTS'
,p_column_display_sequence=>10
,p_column_heading=>'Can View Statemts'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63494514189030712)
,p_query_column_id=>11
,p_column_alias=>'CAN_VIEW_CATALOG'
,p_column_display_sequence=>11
,p_column_heading=>'Can View Catalog'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63494913113030713)
,p_query_column_id=>12
,p_column_alias=>'CAN_ADD_WORKS'
,p_column_display_sequence=>12
,p_column_heading=>'Can Add Works'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63495328219030713)
,p_query_column_id=>13
,p_column_alias=>'CAN_ADD_RECORDING'
,p_column_display_sequence=>13
,p_column_heading=>'Can Add Recording'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63495764910030713)
,p_query_column_id=>14
,p_column_alias=>'CAN_ADD_COMMERCIAL'
,p_column_display_sequence=>14
,p_column_heading=>'Can Add Commercial'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63496131901030713)
,p_query_column_id=>15
,p_column_alias=>'CAN_ADD_DIRECT_DEPOSIT'
,p_column_display_sequence=>15
,p_column_heading=>'Can Add Direct Deposit'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63496584618030714)
,p_query_column_id=>16
,p_column_alias=>'CAN_VIEW_ADD_LIVE_PERF'
,p_column_display_sequence=>16
,p_column_heading=>'Can View Add Live Perf'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63496918137030714)
,p_query_column_id=>17
,p_column_alias=>'CAN_APPROVE_MYSPACE'
,p_column_display_sequence=>17
,p_column_heading=>'Can Approve Myspace'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63497352378030714)
,p_query_column_id=>18
,p_column_alias=>'CAN_UPLOAD_DIGITAL_RECORDGS'
,p_column_display_sequence=>18
,p_column_heading=>'Can Upload Digital Recordgs'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63497751224030714)
,p_query_column_id=>19
,p_column_alias=>'CAN_UPDATE_ACCT_INFO'
,p_column_display_sequence=>19
,p_column_heading=>'Can Update Acct Info'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63498162728030715)
,p_query_column_id=>20
,p_column_alias=>'CAN_OPT_OUT_OF_PAPERLESS'
,p_column_display_sequence=>20
,p_column_heading=>'Can Opt Out Of Paperless'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(90319608709072214)
,p_name=>'Bank Information'
,p_parent_plug_id=>wwv_flow_api.id(63474031644030696)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>10
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#:t-Report--altRowsDefault:t-Report--rowHighlight'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'  --CREATED BY NAVEEN AS PER TICKET SPI-2233',
'  SELECT PAYMENT_METHOD_CODE, ',
'      regexp_replace (substr(BANK_ACCOUNT_NUM,1,length(BANK_ACCOUNT_NUM)-4), ''\d'', ''X'')||substr(BANK_ACCOUNT_NUM,-4) as Bank_Account_Number,  ',
'      BRANCH_NUMBER,',
'      BANK_NAME,',
'      START_DATE',
'FROM XXSEC_SUP_BANK_INFO',
'WHERE IP_NUMBER =:P260_IP_NUMBER',
'AND end_date is null',
'AND :P260_QUERY_ACCOUNT=''Y'';',
''))
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>'-'
,p_csv_output=>'N'
,p_prn_output=>'N'
,p_sort_null=>'L'
,p_plug_query_strip_html=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(90319787900072215)
,p_query_column_id=>1
,p_column_alias=>'PAYMENT_METHOD_CODE'
,p_column_display_sequence=>1
,p_column_heading=>'Payment Method'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(90319825916072216)
,p_query_column_id=>2
,p_column_alias=>'BANK_ACCOUNT_NUMBER'
,p_column_display_sequence=>2
,p_column_heading=>'Bank Account Number'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(90319912151072217)
,p_query_column_id=>3
,p_column_alias=>'BRANCH_NUMBER'
,p_column_display_sequence=>3
,p_column_heading=>'Branch Number'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(346215367133506501)
,p_query_column_id=>4
,p_column_alias=>'BANK_NAME'
,p_column_display_sequence=>4
,p_column_heading=>'Bank Name'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(90320000854072218)
,p_query_column_id=>5
,p_column_alias=>'START_DATE'
,p_column_display_sequence=>5
,p_column_heading=>'Start Date'
,p_use_as_row_header=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(63498510122030715)
,p_name=>'Affiliation Agr.'
,p_region_name=>'AFAG'
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>180
,p_include_in_reg_disp_sel_yn=>'Y'
,p_region_template_options=>'#DEFAULT#:js-showMaximizeButton:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#:t-Report--altRowsDefault:t-Report--rowHighlight'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT * FROM (SELECT trim(soc.society_name)',
'  ||'' ''',
'  ||soc.society_country society ,',
'  RC.ROLE_CODE',
'  ||'' ''',
'  ||RC.ROLE_DESC role_type ,',
'  RT.RIGHT_TYPE',
'  ||'' ''',
'  ||RT.RIGHT_DESCription right_type,',
'  ama.signature_date,',
'  to_char(ama.membership_share,''009.99'') membership_share ,',
'  valid_from_date,',
'  valid_to_date,',
'  AMMENDMENT_DATE,',
'  effective_date ,',
'  RENEW_ALLOWED_FLAG,',
'  RENEWAL_TERM_MONTHS ,',
'  decode(NVL(ama.Status, ''A'') ,''I'',''Inactive'',''A'',''Active'',ama.status) Status,',
'Aff_Ip.Get_Renewal_Date(ama.membership_agreement_id) renewal_date,',
'  ama.membership_agreement_id',
'FROM AFF_MEMBERSHIP_AGREEMENTS ama,',
'  AFF_SOCIETIES soc,',
'  AFF_ROLES rc,',
'  AFF_RIGHT_TYPES rt',
'WHERE ip_id          = :P260_IP_ID',
'AND SOC.SOCIETY_ID   = AMA.SOCIETY_ID',
'AND RC.ROLE_ID       = AMA.ROLE_ID',
'AND RT.RIGHT_TYPE_ID = AMA.RIGHT_TYPE_ID',
'AND :P260_IP_ID     IS NOT NULL',
'AND AMA.SOCIETY_ID   = NVL( :P260_Society_Filter,AMA.SOCIETY_ID)) where  :P260_QUERY_AFF =''Y'''))
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>'-'
,p_query_num_rows_type=>'ROW_RANGES_IN_SELECT_LIST'
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_csv_output=>'Y'
,p_csv_output_link_text=>'Download'
,p_prn_output=>'N'
,p_sort_null=>'L'
,p_query_asc_image=>'apex/builder/dup.gif'
,p_query_asc_image_attr=>'width="16" height="16" alt="" '
,p_query_desc_image=>'apex/builder/ddown.gif'
,p_query_desc_image_attr=>'width="16" height="16" alt="" '
,p_plug_query_strip_html=>'N'
);
end;
/
begin
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63869996801184816)
,p_query_column_id=>1
,p_column_alias=>'SOCIETY'
,p_column_display_sequence=>1
,p_column_heading=>'Society'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63870064362184817)
,p_query_column_id=>2
,p_column_alias=>'ROLE_TYPE'
,p_column_display_sequence=>2
,p_column_heading=>'Role type'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63870149491184818)
,p_query_column_id=>3
,p_column_alias=>'RIGHT_TYPE'
,p_column_display_sequence=>3
,p_column_heading=>'Right type'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63870230028184819)
,p_query_column_id=>4
,p_column_alias=>'SIGNATURE_DATE'
,p_column_display_sequence=>5
,p_column_heading=>'Signature date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63870305719184820)
,p_query_column_id=>5
,p_column_alias=>'MEMBERSHIP_SHARE'
,p_column_display_sequence=>6
,p_column_heading=>'Membership share'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63870444471184821)
,p_query_column_id=>6
,p_column_alias=>'VALID_FROM_DATE'
,p_column_display_sequence=>7
,p_column_heading=>'Valid from date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63870593932184822)
,p_query_column_id=>7
,p_column_alias=>'VALID_TO_DATE'
,p_column_display_sequence=>8
,p_column_heading=>'Valid to date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63870648550184823)
,p_query_column_id=>8
,p_column_alias=>'AMMENDMENT_DATE'
,p_column_display_sequence=>9
,p_column_heading=>'Ammendment date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63870789840184824)
,p_query_column_id=>9
,p_column_alias=>'EFFECTIVE_DATE'
,p_column_display_sequence=>10
,p_column_heading=>'Effective date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63870852185184825)
,p_query_column_id=>10
,p_column_alias=>'RENEW_ALLOWED_FLAG'
,p_column_display_sequence=>11
,p_column_heading=>'Renew allowed flag'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63870929136184826)
,p_query_column_id=>11
,p_column_alias=>'RENEWAL_TERM_MONTHS'
,p_column_display_sequence=>12
,p_column_heading=>'Renewal term months'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(23241514182825317)
,p_query_column_id=>12
,p_column_alias=>'STATUS'
,p_column_display_sequence=>4
,p_column_heading=>'Status'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63871030111184827)
,p_query_column_id=>13
,p_column_alias=>'RENEWAL_DATE'
,p_column_display_sequence=>13
,p_column_heading=>'Renewal date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_display_when_cond_type=>'NEVER'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(63871136430184828)
,p_query_column_id=>14
,p_column_alias=>'MEMBERSHIP_AGREEMENT_ID'
,p_column_display_sequence=>14
,p_column_heading=>'Membership agreement id'
,p_column_link=>'f?p=&APP_ID.:7240:&SESSION.::&DEBUG.:RP:P7240_MEMBERSHIP_AGR_ID,P7240_CALLING_PAGE_ID,P7240_PROPERTY,P7240_TITLE:#MEMBERSHIP_AGREEMENT_ID#,&APP_PAGE_ID.,,'
,p_column_linktext=>'#MEMBERSHIP_AGREEMENT_ID#'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(2208531419471449)
,p_name=>'Parent Affiliation Agreements'
,p_parent_plug_id=>wwv_flow_api.id(63498510122030715)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>204
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#:t-Report--altRowsDefault:t-Report--rowHighlight'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT * FROM (SELECT   ip_id, mc_utility_pkg.get_ip_number(ip_id ) Parent_IP_Number,mc_utility_pkg.get_hfa_num(ip_id ) Parent_HFA_Number,mc_utility_pkg.get_party_name(ip_id) Party_Name,trim(soc.society_name)',
'  ||'' ''',
'  ||soc.society_country society ,',
'  RC.ROLE_CODE',
'  ||'' ''',
'  ||RC.ROLE_DESC role_type ,',
'  RT.RIGHT_TYPE',
'  ||'' ''',
'  ||RT.RIGHT_DESCription right_type,',
'  ama.signature_date,',
'  to_char(ama.membership_share,''009.99'') membership_share ,',
'  valid_from_date,',
'  valid_to_date,',
'  AMMENDMENT_DATE,',
'  effective_date ,',
'  RENEW_ALLOWED_FLAG,',
'  RENEWAL_TERM_MONTHS ,',
'  decode(NVL(ama.Status, ''A'') ,''I'',''Inactive'',''A'',''Active'',ama.status) Status,',
'Aff_Ip.Get_Renewal_Date(ama.membership_agreement_id) renewal_date,',
'  ama.membership_agreement_id',
'FROM AFF_MEMBERSHIP_AGREEMENTS ama,',
'  AFF_SOCIETIES soc,',
'  AFF_ROLES rc,',
'  AFF_RIGHT_TYPES rt',
' -- AFF_HIERARCHY AH',
'wHERE ip_id          IN  (SELECT PARENT_PARTY_ID FROM AFF_HIERARCHY WHERE CHILD_PARTY_ID = :P260_IP_ID AND HIER_LEVEL = 0 AND END_DATE IS NULL AND NOWED_AFF_HIER_ID IS NULL AND RELATE_ACCOUNT_NUMBER IS NOT NULL)',
'AND SOC.SOCIETY_ID   = AMA.SOCIETY_ID',
'AND RC.ROLE_ID       = AMA.ROLE_ID',
'AND RT.RIGHT_TYPE_ID = AMA.RIGHT_TYPE_ID',
'--AND AH.child_party_id  = 7264123   AND HIER_LEVEL = 0 AND END_DATE IS NULL AND NOWED_AFF_HIER_ID IS NULL',
'AND :P260_IP_ID    IS NOT NULL',
'AND AMA.SOCIETY_ID   = NVL( :P260_Society_Filter,AMA.SOCIETY_ID)) where  :P260_QUERY_AFF =''Y''',
';'))
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
 p_id=>wwv_flow_api.id(3148410664296418)
,p_query_column_id=>1
,p_column_alias=>'IP_ID'
,p_column_display_sequence=>1
,p_hidden_column=>'Y'
,p_derived_column=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(3148500268296419)
,p_query_column_id=>2
,p_column_alias=>'PARENT_IP_NUMBER'
,p_column_display_sequence=>2
,p_column_heading=>'Parent IP Number'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(3148674619296420)
,p_query_column_id=>3
,p_column_alias=>'PARENT_HFA_NUMBER'
,p_column_display_sequence=>3
,p_column_heading=>'Parent HFA Number'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(3150172315296435)
,p_query_column_id=>4
,p_column_alias=>'PARTY_NAME'
,p_column_display_sequence=>4
,p_column_heading=>'Party Name'
,p_use_as_row_header=>'N'
,p_column_link=>'f?p=&APP_ID.:260:&SESSION.:APEX_CLONE_SESSION:&DEBUG.:RP:P260_IP_ID,P260_IP_NUMBER:#IP_ID#,#PARENT_IP_NUMBER#'
,p_column_linktext=>'#PARTY_NAME#'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(3148748127296421)
,p_query_column_id=>5
,p_column_alias=>'SOCIETY'
,p_column_display_sequence=>5
,p_column_heading=>'Society'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(3148863591296422)
,p_query_column_id=>6
,p_column_alias=>'ROLE_TYPE'
,p_column_display_sequence=>6
,p_column_heading=>'Role Type'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(3148928882296423)
,p_query_column_id=>7
,p_column_alias=>'RIGHT_TYPE'
,p_column_display_sequence=>7
,p_column_heading=>'Right Type'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(3149045442296424)
,p_query_column_id=>8
,p_column_alias=>'SIGNATURE_DATE'
,p_column_display_sequence=>8
,p_column_heading=>'Signature Date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(3149101894296425)
,p_query_column_id=>9
,p_column_alias=>'MEMBERSHIP_SHARE'
,p_column_display_sequence=>9
,p_column_heading=>'Membership Share'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(3149251636296426)
,p_query_column_id=>10
,p_column_alias=>'VALID_FROM_DATE'
,p_column_display_sequence=>10
,p_column_heading=>'Valid From Date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(3149381523296427)
,p_query_column_id=>11
,p_column_alias=>'VALID_TO_DATE'
,p_column_display_sequence=>11
,p_column_heading=>'Valid To Date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(3149432186296428)
,p_query_column_id=>12
,p_column_alias=>'AMMENDMENT_DATE'
,p_column_display_sequence=>12
,p_column_heading=>'Ammendment Date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(3149590678296429)
,p_query_column_id=>13
,p_column_alias=>'EFFECTIVE_DATE'
,p_column_display_sequence=>13
,p_column_heading=>'Effective Date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(3149682106296430)
,p_query_column_id=>14
,p_column_alias=>'RENEW_ALLOWED_FLAG'
,p_column_display_sequence=>14
,p_column_heading=>'Renew Allowed Flag'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(3149723134296431)
,p_query_column_id=>15
,p_column_alias=>'RENEWAL_TERM_MONTHS'
,p_column_display_sequence=>15
,p_column_heading=>'Renewal Term Months'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(3149855695296432)
,p_query_column_id=>16
,p_column_alias=>'STATUS'
,p_column_display_sequence=>16
,p_column_heading=>'Status'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(3149935131296433)
,p_query_column_id=>17
,p_column_alias=>'RENEWAL_DATE'
,p_column_display_sequence=>17
,p_column_heading=>'Renewal Date'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(3150009297296434)
,p_query_column_id=>18
,p_column_alias=>'MEMBERSHIP_AGREEMENT_ID'
,p_column_display_sequence=>18
,p_column_heading=>'Membership Agreement Id'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_region(
 p_id=>wwv_flow_api.id(11420285810058749)
,p_name=>'HFA Managed Agreements'
,p_parent_plug_id=>wwv_flow_api.id(63498510122030715)
,p_template=>wwv_flow_api.id(51849320405240825)
,p_display_sequence=>205
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#:t-Report--altRowsDefault:t-Report--rowHighlight'
,p_display_point=>'BODY'
,p_source_type=>'NATIVE_SQL_REPORT'
,p_query_type=>'SQL'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT ',
'  AGREEMENT_CODE "Agreement Code",',
'  LICENSEE_NUM  "Licensee Number",',
'  OPT_STATUS   "Opt in Status",',
'  ADMIN_SERVICE "Admin Status",',
'  ENTRY_DATE "Created Date",',
'  ENTER_BY    "Created User"',
'FROM mc_pub_agreements_v ',
'WHERE publisher_num = :P260_HFA_NUMBER and  :P260_QUERY_AFF =''Y'''))
,p_ajax_enabled=>'Y'
,p_query_row_template=>wwv_flow_api.id(51858120200240832)
,p_query_num_rows=>15
,p_query_options=>'DERIVED_REPORT_COLUMNS'
,p_query_show_nulls_as=>'-'
,p_query_no_data_found=>'&P260_HFA_MAN_AGR_MSG.'
,p_query_num_rows_type=>'ROW_RANGES_IN_SELECT_LIST'
,p_pagination_display_position=>'BOTTOM_RIGHT'
,p_csv_output=>'Y'
,p_csv_output_link_text=>'Download'
,p_prn_output=>'N'
,p_sort_null=>'L'
,p_plug_query_strip_html=>'N'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(11420321057058750)
,p_query_column_id=>1
,p_column_alias=>'Agreement Code'
,p_column_display_sequence=>1
,p_column_heading=>'Agreement code'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(13584041060450101)
,p_query_column_id=>2
,p_column_alias=>'Licensee Number'
,p_column_display_sequence=>2
,p_column_heading=>'Licensee number'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(13584124046450102)
,p_query_column_id=>3
,p_column_alias=>'Opt in Status'
,p_column_display_sequence=>3
,p_column_heading=>'Opt in status'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(13584243280450103)
,p_query_column_id=>4
,p_column_alias=>'Admin Status'
,p_column_display_sequence=>4
,p_column_heading=>'Admin status'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(13584333136450104)
,p_query_column_id=>5
,p_column_alias=>'Created Date'
,p_column_display_sequence=>5
,p_column_heading=>'Created date'
,p_use_as_row_header=>'N'
,p_default_sort_column_sequence=>1
,p_default_sort_dir=>'desc'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_report_columns(
 p_id=>wwv_flow_api.id(13584428030450105)
,p_query_column_id=>6
,p_column_alias=>'Created User'
,p_column_display_sequence=>6
,p_column_heading=>'Created User'
,p_use_as_row_header=>'N'
,p_disable_sort_column=>'N'
,p_derived_column=>'N'
,p_include_in_export=>'Y'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(38889816560446428)
,p_plug_name=>'History'
,p_parent_plug_id=>wwv_flow_api.id(63498510122030715)
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>215
,p_plug_display_point=>'BODY'
,p_query_type=>'SQL'
,p_plug_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select  * from AFF_MEMBERSHIP_AGREEMENTS_HIST',
'where ip_id=:P260_IP_ID and  :P260_QUERY_AFF =''Y'''))
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
 p_id=>wwv_flow_api.id(38889923967446429)
,p_max_row_count=>'1000000'
,p_show_nulls_as=>'-'
,p_pagination_type=>'ROWS_X_TO_Y'
,p_pagination_display_pos=>'BOTTOM_RIGHT'
,p_report_list_mode=>'TABS'
,p_show_detail_link=>'N'
,p_show_notify=>'Y'
,p_download_formats=>'CSV:HTML:EMAIL:XLS:PDF:RTF'
,p_owner=>'KBAKSHI'
,p_internal_uid=>38889923967446429
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38890311257446433)
,p_db_column_name=>'EFFECTIVE_DATE'
,p_display_order=>40
,p_column_identifier=>'D'
,p_column_label=>'Effective date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38890459179446434)
,p_db_column_name=>'SIGNATURE_DATE'
,p_display_order=>50
,p_column_identifier=>'E'
,p_column_label=>'Signature date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38890695565446436)
,p_db_column_name=>'AMMENDMENT_DATE'
,p_display_order=>70
,p_column_identifier=>'G'
,p_column_label=>'Ammendment date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38890958936446439)
,p_db_column_name=>'CREATION_CLASS_ID'
,p_display_order=>100
,p_column_identifier=>'J'
,p_column_label=>'Creation Class'
,p_column_type=>'NUMBER'
,p_display_text_as=>'LOV_ESCAPE_SC'
,p_column_alignment=>'RIGHT'
,p_rpt_named_lov=>wwv_flow_api.id(66006905986375125)
,p_rpt_show_filter_lov=>'1'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38891082641446440)
,p_db_column_name=>'ROLE_ID'
,p_display_order=>110
,p_column_identifier=>'K'
,p_column_label=>'Role'
,p_column_type=>'NUMBER'
,p_display_text_as=>'LOV_ESCAPE_SC'
,p_column_alignment=>'RIGHT'
,p_rpt_named_lov=>wwv_flow_api.id(62720921871606139)
,p_rpt_show_filter_lov=>'1'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38891195896446441)
,p_db_column_name=>'RIGHT_TYPE_ID'
,p_display_order=>120
,p_column_identifier=>'L'
,p_column_label=>'Right type ID'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38891229285446442)
,p_db_column_name=>'RENEW_ALLOWED_FLAG'
,p_display_order=>130
,p_column_identifier=>'M'
,p_column_label=>'Renew allowed flag'
,p_column_type=>'STRING'
,p_display_text_as=>'LOV_ESCAPE_SC'
,p_rpt_named_lov=>wwv_flow_api.id(66009685797375127)
,p_rpt_show_filter_lov=>'1'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38891371486446443)
,p_db_column_name=>'RENEWAL_TERM_MONTHS'
,p_display_order=>140
,p_column_identifier=>'N'
,p_column_label=>'Renewal term months'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38891405907446444)
,p_db_column_name=>'STATUS'
,p_display_order=>150
,p_column_identifier=>'O'
,p_column_label=>'Status'
,p_column_type=>'STRING'
,p_display_text_as=>'LOV_ESCAPE_SC'
,p_rpt_named_lov=>wwv_flow_api.id(62613490312062601)
,p_rpt_show_filter_lov=>'1'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38891530600446445)
,p_db_column_name=>'CREATED_BY'
,p_display_order=>160
,p_column_identifier=>'P'
,p_column_label=>'Created by'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38891606129446446)
,p_db_column_name=>'CREATION_DATE'
,p_display_order=>170
,p_column_identifier=>'Q'
,p_column_label=>'Creation date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38891742310446447)
,p_db_column_name=>'LAST_UPDATED_BY'
,p_display_order=>180
,p_column_identifier=>'R'
,p_column_label=>'Last updated by'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38891857674446448)
,p_db_column_name=>'LAST_UPDATE_DATE'
,p_display_order=>190
,p_column_identifier=>'S'
,p_column_label=>'Last update date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38891902007446449)
,p_db_column_name=>'ALERT_DATE'
,p_display_order=>200
,p_column_identifier=>'T'
,p_column_label=>'Alert date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38892087337446450)
,p_db_column_name=>'DOMESTIC_PAY_THRU_DATE'
,p_display_order=>210
,p_column_identifier=>'U'
,p_column_label=>'Domestic pay thru date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(38908740429902106)
,p_db_column_name=>'HIST_ID'
,p_display_order=>240
,p_column_identifier=>'X'
,p_column_label=>'History ID'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(80694657802697337)
,p_db_column_name=>'MEMBERSHIP_AGREEMENT_ID'
,p_display_order=>250
,p_column_identifier=>'AK'
,p_column_label=>'Membership agreement id'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(80694740508697338)
,p_db_column_name=>'VALID_FROM_DATE'
,p_display_order=>260
,p_column_identifier=>'AL'
,p_column_label=>'Valid from date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(80694893008697339)
,p_db_column_name=>'VALID_TO_DATE'
,p_display_order=>270
,p_column_identifier=>'AM'
,p_column_label=>'Valid to date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(80694947117697340)
,p_db_column_name=>'MEMBERSHIP_SHARE'
,p_display_order=>280
,p_column_identifier=>'AN'
,p_column_label=>'Membership share'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(80695096728697341)
,p_db_column_name=>'IP_ID'
,p_display_order=>290
,p_column_identifier=>'AO'
,p_column_label=>'Ip id'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(80695163816697342)
,p_db_column_name=>'SOCIETY_ID'
,p_display_order=>300
,p_column_identifier=>'AP'
,p_column_label=>'Society id'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(80695231035697343)
,p_db_column_name=>'GROUP_ID'
,p_display_order=>310
,p_column_identifier=>'AQ'
,p_column_label=>'Group id'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(80695346614697344)
,p_db_column_name=>'SPECIAL_HANDLING'
,p_display_order=>320
,p_column_identifier=>'AR'
,p_column_label=>'Special handling'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(38924162332910882)
,p_application_user=>'APXWS_DEFAULT'
,p_report_seq=>10
,p_report_alias=>'389242'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'HIST_ID:EFFECTIVE_DATE:SIGNATURE_DATE:AMMENDMENT_DATE:CREATION_CLASS_ID:ROLE_ID:RIGHT_TYPE_ID:RENEW_ALLOWED_FLAG:RENEWAL_TERM_MONTHS:STATUS:CREATED_BY:CREATION_DATE:LAST_UPDATED_BY:LAST_UPDATE_DATE:ALERT_DATE:DOMESTIC_PAY_THRU_DATE::MEMBERSHIP_AGREEM'
||'ENT_ID:VALID_FROM_DATE:VALID_TO_DATE:MEMBERSHIP_SHARE:IP_ID:SOCIETY_ID:GROUP_ID:SPECIAL_HANDLING'
,p_sort_column_1=>'HIST_ID'
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
 p_id=>wwv_flow_api.id(63504112702030720)
,p_plug_name=>'ACMI Details'
,p_region_name=>'ACMI'
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>200
,p_include_in_reg_disp_sel_yn=>'Y'
,p_plug_grid_column_css_classes=>'cl-ir-dynamic-display'
,p_plug_display_point=>'BODY'
,p_query_type=>'SQL'
,p_plug_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT *',
'  FROM (SELECT ra.aff_name                                                 associated_affiliate,',
'               ra.aff_num                                                associated_ip_number,',
'               ain_own.full_name                                    detail_affiliate,',
'               ip_own.ip_number                                     detail_ip_number,',
'               NVL (d.money_paid, 0) + NVL (d.money_recouped, 0)    balance,',
'               d.*,',
'               aa.assoc_ip_id,',
'               NVL ((SELECT ''Y''',
'                       FROM acmi_incl_omit_songs a',
'                      WHERE a.detail_id = d.detail_id AND ROWNUM = 1),',
'                    ''N'')                                            Inclusions_Exclusions',
'          FROM acmi_detail             d,',
'               aff_ip_names            ain_own,',
'               aff_interested_parties  ip_own',
'               ,acmi_affiliates            aa',
'               , rep_affiliates ra',
'         WHERE     ip_own.ip_id = d.ip_id',
'               AND ain_own.ip_id = d.ip_id',
'               AND ain_own.name_Type_id = 6',
'               AND aa.detail_id = d.detail_id',
'               and ra.ip_id = aa.assoc_ip_id',
'               )',
' WHERE assoc_ip_id = :P260_IP_ID'))
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
 p_id=>wwv_flow_api.id(7477245813232108)
,p_max_row_count=>'1000000'
,p_allow_save_rpt_public=>'Y'
,p_save_rpt_public_auth_scheme=>wwv_flow_api.id(6185302949729668)
,p_show_nulls_as=>'-'
,p_pagination_type=>'ROWS_X_TO_Y_OF_Z'
,p_pagination_display_pos=>'BOTTOM_LEFT'
,p_report_list_mode=>'TABS'
,p_show_detail_link=>'N'
,p_show_notify=>'Y'
,p_show_flashback=>'N'
,p_download_formats=>'CSV:HTML:EMAIL'
,p_owner=>'DCOLLINS'
,p_internal_uid=>7477245813232108
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7477373807232109)
,p_db_column_name=>'ASSOCIATED_AFFILIATE'
,p_display_order=>10
,p_column_identifier=>'A'
,p_column_label=>'Associated Affiliate'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7477453099232110)
,p_db_column_name=>'ASSOCIATED_IP_NUMBER'
,p_display_order=>20
,p_column_identifier=>'B'
,p_column_label=>'Associated IP#'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7477538702232111)
,p_db_column_name=>'DETAIL_AFFILIATE'
,p_display_order=>30
,p_column_identifier=>'C'
,p_column_label=>'Detail Affiliate'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7477612184232112)
,p_db_column_name=>'DETAIL_IP_NUMBER'
,p_display_order=>40
,p_column_identifier=>'D'
,p_column_label=>'Detail IP#'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7477770452232113)
,p_db_column_name=>'BALANCE'
,p_display_order=>50
,p_column_identifier=>'E'
,p_column_label=>'Balance'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7477862076232114)
,p_db_column_name=>'DETAIL_ID'
,p_display_order=>60
,p_column_identifier=>'F'
,p_column_label=>'Detail ID'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7477988541232115)
,p_db_column_name=>'HEADER_ID'
,p_display_order=>70
,p_column_identifier=>'G'
,p_column_label=>'Header ID'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7478025228232116)
,p_db_column_name=>'AFFILIATE_NUMBER'
,p_display_order=>80
,p_column_identifier=>'H'
,p_column_label=>'Affiliate#'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7478134070232117)
,p_db_column_name=>'IP_ID'
,p_display_order=>90
,p_column_identifier=>'I'
,p_column_label=>'IP ID'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7478210404232118)
,p_db_column_name=>'SUPPLEMENT_ID'
,p_display_order=>100
,p_column_identifier=>'J'
,p_column_label=>'Supplement ID'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7478335567232119)
,p_db_column_name=>'PROVISION_ID'
,p_display_order=>110
,p_column_identifier=>'K'
,p_column_label=>'Provision ID'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7478482274232120)
,p_db_column_name=>'ANNOTATION'
,p_display_order=>120
,p_column_identifier=>'L'
,p_column_label=>'Annotation'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7478583933232121)
,p_db_column_name=>'AFFILIATE_REP'
,p_display_order=>130
,p_column_identifier=>'M'
,p_column_label=>'Affiliate Rep'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7478688341232122)
,p_db_column_name=>'PROVISION_DATE'
,p_display_order=>140
,p_column_identifier=>'N'
,p_column_label=>'Provision Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7478760506232123)
,p_db_column_name=>'ENTRY_DATE'
,p_display_order=>150
,p_column_identifier=>'O'
,p_column_label=>'Entry Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
end;
/
begin
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7478835479232124)
,p_db_column_name=>'EFFECTIVE_DATE'
,p_display_order=>160
,p_column_identifier=>'P'
,p_column_label=>'Effective Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7478993140232125)
,p_db_column_name=>'EXPIRATION_DATE'
,p_display_order=>170
,p_column_identifier=>'Q'
,p_column_label=>'Expiration Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7479077637232126)
,p_db_column_name=>'PROVISION_TYPE'
,p_display_order=>180
,p_column_identifier=>'R'
,p_column_label=>'Provision Type'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7479145309232127)
,p_db_column_name=>'LEGAL_CODE'
,p_display_order=>190
,p_column_identifier=>'S'
,p_column_label=>'Legal Code'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7479283483232128)
,p_db_column_name=>'STATUS_CODE'
,p_display_order=>200
,p_column_identifier=>'T'
,p_column_label=>'Status Code'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7479379486232129)
,p_db_column_name=>'EDIT_LOCKED'
,p_display_order=>210
,p_column_identifier=>'U'
,p_column_label=>'Edit Locked'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7479498935232130)
,p_db_column_name=>'RECOUP_CODE'
,p_display_order=>220
,p_column_identifier=>'V'
,p_column_label=>'Recoup Code'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7479500828232131)
,p_db_column_name=>'RECOUP_BAL_CF'
,p_display_order=>230
,p_column_identifier=>'W'
,p_column_label=>'Recoup Bal CF'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7479628249232132)
,p_db_column_name=>'RECOUP_EXP_DATE'
,p_display_order=>240
,p_column_identifier=>'X'
,p_column_label=>'Recoup Exp Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7479703709232133)
,p_db_column_name=>'OTHER_RECOUP_SRC'
,p_display_order=>250
,p_column_identifier=>'Y'
,p_column_label=>'Other Recoup Src.'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7479813044232134)
,p_db_column_name=>'INC_OMIT_SONGS'
,p_display_order=>260
,p_column_identifier=>'Z'
,p_column_label=>'Inc Omit Songs'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7479912092232135)
,p_db_column_name=>'INC_OMIT_SHOWS'
,p_display_order=>270
,p_column_identifier=>'AA'
,p_column_label=>'Inc Omit Shows'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7480033357232136)
,p_db_column_name=>'INC_OMIT_CAT_PTYPE'
,p_display_order=>280
,p_column_identifier=>'AB'
,p_column_label=>'Inc Omit Cat Ptype'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7480154789232137)
,p_db_column_name=>'PROVISION_AMOUNT'
,p_display_order=>290
,p_column_identifier=>'AC'
,p_column_label=>'Provision Amount'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7480297795232138)
,p_db_column_name=>'MONEY_PAID'
,p_display_order=>300
,p_column_identifier=>'AD'
,p_column_label=>'Money Paid'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7480375027232139)
,p_db_column_name=>'MONEY_RECOUPED'
,p_display_order=>310
,p_column_identifier=>'AE'
,p_column_label=>'Money Recouped'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7480449624232140)
,p_db_column_name=>'WRITEOFF_BALANCE'
,p_display_order=>320
,p_column_identifier=>'AF'
,p_column_label=>'Writeoff Balance'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7480569092232141)
,p_db_column_name=>'PAYMENT_FREQUENCY'
,p_display_order=>330
,p_column_identifier=>'AG'
,p_column_label=>'Payment Frequency'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7480645785232142)
,p_db_column_name=>'PAYMENT_QUANTITY'
,p_display_order=>340
,p_column_identifier=>'AH'
,p_column_label=>'Payment Quantity'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7480707584232143)
,p_db_column_name=>'CREATED_BY'
,p_display_order=>350
,p_column_identifier=>'AI'
,p_column_label=>'Created By'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7480811466232144)
,p_db_column_name=>'CREATION_DATE'
,p_display_order=>360
,p_column_identifier=>'AJ'
,p_column_label=>'Creation Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7480928244232145)
,p_db_column_name=>'LAST_UPDATED_BY'
,p_display_order=>370
,p_column_identifier=>'AK'
,p_column_label=>'Last Updated By'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7481013401232146)
,p_db_column_name=>'LAST_UPDATE_DATE'
,p_display_order=>380
,p_column_identifier=>'AL'
,p_column_label=>'Last Update Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7481131515232147)
,p_db_column_name=>'RECEIVABLE_CCID'
,p_display_order=>390
,p_column_identifier=>'AM'
,p_column_label=>'Receivable CCID'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7481220687232148)
,p_db_column_name=>'DOMESTIC_FOREIGN_FLAG'
,p_display_order=>400
,p_column_identifier=>'AN'
,p_column_label=>'Domestic Foreign fFag'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7481386102232149)
,p_db_column_name=>'RECONCILE_DATE'
,p_display_order=>410
,p_column_identifier=>'AO'
,p_column_label=>'Reconcile Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7481475973232150)
,p_db_column_name=>'MONEY_INTEREST'
,p_display_order=>420
,p_column_identifier=>'AP'
,p_column_label=>'Money Interest'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7574524436132001)
,p_db_column_name=>'MONEY_RECOUPWO'
,p_display_order=>430
,p_column_identifier=>'AQ'
,p_column_label=>'Money Recoupwo'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7574653019132002)
,p_db_column_name=>'MONEY_GLVAR'
,p_display_order=>440
,p_column_identifier=>'AR'
,p_column_label=>'Money GLVAR'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(7574784443132003)
,p_db_column_name=>'INCLUSIONS_EXCLUSIONS'
,p_display_order=>450
,p_column_identifier=>'AS'
,p_column_label=>'Inclusions Exclusions'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(39607885412660236)
,p_db_column_name=>'SORT_ORDER'
,p_display_order=>460
,p_column_identifier=>'AT'
,p_column_label=>'Sort order'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(39607956560660237)
,p_db_column_name=>'SFDC_ACMI_PROVISION_REL_ID'
,p_display_order=>470
,p_column_identifier=>'AU'
,p_column_label=>'Sfdc acmi provision rel id'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(28183942364944502)
,p_db_column_name=>'COLLECTOR_LVL_RECOUP'
,p_display_order=>480
,p_column_identifier=>'AV'
,p_column_label=>'Collector Lvl Recoup'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(28184005244944503)
,p_db_column_name=>'DOMESTIC_RECOUP_EXP_DATE'
,p_display_order=>490
,p_column_identifier=>'AW'
,p_column_label=>'Domestic Recoup Exp Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(28184169906944504)
,p_db_column_name=>'FOREIGN_RECOUP_EXP_DATE'
,p_display_order=>500
,p_column_identifier=>'AX'
,p_column_label=>'Foreign Recoup Exp Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(28184210145944505)
,p_db_column_name=>'SIGNED_OFF_BY'
,p_display_order=>510
,p_column_identifier=>'AY'
,p_column_label=>'Signed Off By'
,p_column_type=>'STRING'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(28184333066944506)
,p_db_column_name=>'SIGNED_OFF_DATE'
,p_display_order=>520
,p_column_identifier=>'AZ'
,p_column_label=>'Signed Off Date'
,p_column_type=>'DATE'
,p_column_alignment=>'CENTER'
,p_tz_dependent=>'N'
);
wwv_flow_api.create_worksheet_column(
 p_id=>wwv_flow_api.id(28184465369944507)
,p_db_column_name=>'ASSOC_IP_ID'
,p_display_order=>530
,p_column_identifier=>'BA'
,p_column_label=>'Assoc Ip Id'
,p_column_type=>'NUMBER'
,p_column_alignment=>'RIGHT'
);
wwv_flow_api.create_worksheet_rpt(
 p_id=>wwv_flow_api.id(7607341330132973)
,p_application_user=>'APXWS_DEFAULT'
,p_report_seq=>10
,p_report_alias=>'76074'
,p_status=>'PUBLIC'
,p_is_default=>'Y'
,p_report_columns=>'ASSOCIATED_AFFILIATE:ASSOCIATED_IP_NUMBER:DETAIL_AFFILIATE:DETAIL_IP_NUMBER:BALANCE:AFFILIATE_NUMBER:ANNOTATION:AFFILIATE_REP:PROVISION_DATE:ENTRY_DATE:EFFECTIVE_DATE:EXPIRATION_DATE:PROVISION_TYPE:LEGAL_CODE:STATUS_CODE:EDIT_LOCKED:RECOUP_CODE:RECOU'
||'P_BAL_CF:RECOUP_EXP_DATE:OTHER_RECOUP_SRC:INC_OMIT_SONGS:INC_OMIT_SHOWS:INC_OMIT_CAT_PTYPE:PROVISION_AMOUNT:MONEY_PAID:MONEY_RECOUPED:WRITEOFF_BALANCE:PAYMENT_FREQUENCY:PAYMENT_QUANTITY:RECEIVABLE_CCID:DOMESTIC_FOREIGN_FLAG:RECONCILE_DATE:MONEY_INTER'
||'EST:MONEY_RECOUPWO:MONEY_GLVAR:INCLUSIONS_EXCLUSIONS::SORT_ORDER:SFDC_ACMI_PROVISION_REL_ID:COLLECTOR_LVL_RECOUP:DOMESTIC_RECOUP_EXP_DATE:FOREIGN_RECOUP_EXP_DATE:SIGNED_OFF_BY:SIGNED_OFF_DATE:ASSOC_IP_ID'
);
wwv_flow_api.create_worksheet_condition(
 p_id=>wwv_flow_api.id(7842631802104278)
,p_report_id=>wwv_flow_api.id(7607341330132973)
,p_condition_type=>'FILTER'
,p_allow_delete=>'Y'
,p_column_name=>'STATUS_CODE'
,p_operator=>'='
,p_expr=>'A'
,p_condition_sql=>'"STATUS_CODE" = #APXWS_EXPR#'
,p_condition_display=>'#APXWS_COL_NAME# = ''A''  '
,p_enabled=>'Y'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(63523093888030733)
,p_plug_name=>'Breadcrumb'
,p_region_template_options=>'#DEFAULT#:t-BreadcrumbRegion--useBreadcrumbTitle'
,p_component_template_options=>'#DEFAULT#'
,p_plug_template=>wwv_flow_api.id(51852637782240827)
,p_plug_display_sequence=>10
,p_plug_display_point=>'REGION_POSITION_01'
,p_menu_id=>wwv_flow_api.id(51876846391240858)
,p_plug_source_type=>'NATIVE_BREADCRUMB'
,p_menu_template_id=>wwv_flow_api.id(51870806884240842)
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(63880148523205818)
,p_plug_name=>'RDS'
,p_region_name=>'RDS'
,p_region_template_options=>'#DEFAULT#:js-useLocalStorage:t-TabsRegion-mod--pill'
,p_plug_template=>wwv_flow_api.id(51851408341240826)
,p_plug_display_sequence=>30
,p_plug_display_point=>'BODY'
,p_plug_source_type=>'NATIVE_DISPLAY_SELECTOR'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_attribute_01=>'STANDARD'
,p_attribute_02=>'N'
,p_attribute_03=>'Y'
);
wwv_flow_api.create_page_plug(
 p_id=>wwv_flow_api.id(65211164049221609)
,p_plug_name=>'Parameters'
,p_region_template_options=>'#DEFAULT#:t-Region--scrollBody'
,p_plug_template=>wwv_flow_api.id(51849320405240825)
,p_plug_display_sequence=>10
,p_include_in_reg_disp_sel_yn=>'Y'
,p_plug_display_point=>'BODY'
,p_plug_query_options=>'DERIVED_REPORT_COLUMNS'
,p_plug_display_condition_type=>'NEVER'
,p_attribute_01=>'N'
,p_attribute_02=>'HTML'
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(2445622172167104)
,p_button_sequence=>10
,p_button_plug_id=>wwv_flow_api.id(2445906509167107)
,p_button_name=>'New_Hierarchy_Agreement'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'New Hierarchy Agreement '
,p_button_position=>'ABOVE_BOX'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:400:&SESSION.::&DEBUG.:RP,400:P400_IP_ID,P400_SHOW_PARENT_REP,P400_CAME_FROM_QUEUE:&P260_IP_ID.,Y,'
,p_button_condition=>':P260_IP_TYPE IN (1,4)'
,p_button_condition_type=>'PLSQL_EXPRESSION'
,p_icon_css_classes=>'fa-plus'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(17200066639506016)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(37333096277575516)
,p_button_sequence=>10
,p_button_plug_id=>wwv_flow_api.id(37332744869575513)
,p_button_name=>'SELF_AGREEMENT2_1'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Create Self Agreement'
,p_button_position=>'ABOVE_BOX'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:400:&SESSION.::&DEBUG.:RP,400:P400_IP_ID,P400_AGG_TYPE:&P260_IP_ID.,SELF'
,p_button_condition=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'	l_COUNT	NUMBER;',
'BEGIN ',
'',
'	IF :P260_HFA_NUMBER NOT LIKE ''P%'' THEN',
'		RETURN FALSE;',
'	ELSE',
'		SELECT COUNT(*) ',
'		INTO l_COUNT',
'		FROM aff_hierarchy ',
'		WHERE RELATE_ACCOUNT_NUMBER IS NULL ',
'		AND HIER_LEVEL=0 AND CHILD_PARTY_ID=:P260_IP_ID',
'		/*SPI-649*/',
'		AND end_date IS NULL AND NOWED_AFF_HIER_ID IS NULL;',
'		',
'		IF l_Count = 0 THEN',
'			RETURN TRUE;',
'		ELSE	',
'			RETURN FALSE;',
'		END IF;',
'	END IF;',
'END;	'))
,p_button_condition_type=>'FUNCTION_BODY'
,p_icon_css_classes=>'fa-plus'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(17200066639506016)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(66909209741387602)
,p_button_sequence=>10
,p_button_plug_id=>wwv_flow_api.id(63460421216030686)
,p_button_name=>'CREATE_BAND'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--iconLeft:t-Button--primary'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Add Band'
,p_button_position=>'ABOVE_BOX'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:50300:&SESSION.::&DEBUG.:50300:P50300_IP_ID:&P260_IP_ID.'
,p_icon_css_classes=>'fa-plus'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(69665980648432325)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(13586346594450124)
,p_button_sequence=>20
,p_button_plug_id=>wwv_flow_api.id(2445906509167107)
,p_button_name=>'SELF_AGREEMENT2'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Create Self Agreement'
,p_button_position=>'ABOVE_BOX'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:400:&SESSION.::&DEBUG.:RP,400:P400_IP_ID,P400_AGG_TYPE:&P260_IP_ID.,SELF'
,p_button_condition=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'	l_COUNT	NUMBER;',
'BEGIN ',
'',
'	IF :P260_IP_TYPE <> 1 THEN',
'		RETURN FALSE;',
'	ELSE',
'		SELECT COUNT(*) ',
'		INTO l_COUNT',
'		FROM aff_hierarchy ',
'		WHERE CHILD_PARTY_ID=PARENT_PARTY_ID ',
'		AND HIER_LEVEL=0 AND CHILD_PARTY_ID=:P260_IP_ID',
'		/*SPI-649*/',
'		AND end_date IS NULL AND NOWED_AFF_HIER_ID IS NULL;',
'		',
'		IF l_Count = 0 THEN',
'			RETURN TRUE;',
'		ELSE	',
'			RETURN FALSE;',
'		END IF;',
'	END IF;',
'END;'))
,p_button_condition_type=>'FUNCTION_BODY'
,p_icon_css_classes=>'fa-plus'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(17200066639506016)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(37332910113575515)
,p_button_sequence=>20
,p_button_plug_id=>wwv_flow_api.id(37332744869575513)
,p_button_name=>'New_Hierarchy_Agreement_1'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'New Hierarchy Agreement '
,p_button_position=>'ABOVE_BOX'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:400:&SESSION.::&DEBUG.:RP,400:P400_IP_ID,P400_SHOW_PARENT_REP,P400_CAME_FROM_QUEUE:&P260_IP_ID.,Y,'
,p_button_condition=>':P260_IP_TYPE IN (1,4)'
,p_button_condition_type=>'PLSQL_EXPRESSION'
,p_icon_css_classes=>'fa-plus'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(17200066639506016)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(28661121377759541)
,p_button_sequence=>10
,p_button_plug_id=>wwv_flow_api.id(28659957399759529)
,p_button_name=>'LINK'
,p_button_action=>'SUBMIT'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Link'
,p_button_position=>'BELOW_BOX'
,p_button_alignment=>'LEFT'
,p_icon_css_classes=>'fa-link'
,p_grid_new_grid=>false
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(63394368330030628)
,p_button_sequence=>10
,p_button_plug_id=>wwv_flow_api.id(63393586162030628)
,p_button_name=>'CANCEL'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Cancel'
,p_button_position=>'BELOW_BOX'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:&P260_CALLING_PAGE_ID.:&SESSION.::&DEBUG.:RP::'
,p_button_condition=>'P260_PREV_IP_ID'
,p_button_condition_type=>'ITEM_IS_NULL'
,p_icon_css_classes=>'fa-rotate-left'
,p_grid_new_grid=>false
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(63424700617030651)
,p_button_sequence=>10
,p_button_plug_id=>wwv_flow_api.id(63424359871030651)
,p_button_name=>'EARNINGS_SUMMARY'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#'
,p_button_template_id=>wwv_flow_api.id(51870385911240841)
,p_button_image_alt=>'Earnings Summary'
,p_button_position=>'BELOW_BOX'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:365:&SESSION.::&DEBUG.:RP:P365_IP_ID,P365_CALLING_PAGE_ID,P365_REQUEST:&P260_IP_ID.,&APP_PAGE_ID.,ROY'
,p_button_condition_type=>'NEVER'
,p_grid_new_grid=>false
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(19406449607337104)
,p_button_sequence=>20
,p_button_plug_id=>wwv_flow_api.id(63393586162030628)
,p_button_name=>'CANCEL2'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Cancel'
,p_button_position=>'BELOW_BOX'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:&P260_CALLING_PAGE_ID.:&SESSION.::&DEBUG.:RP:P260_IP_ID,P260_IP_NUMBER,P260_PREV_IP_ID,P260_PREV_IP_NUMBER,P260_CALLING_PAGE_ID:&P260_PREV_IP_ID.,&P260_PREV_IP_NUMBER.,,,250'
,p_button_condition=>'P260_PREV_IP_ID'
,p_button_condition_type=>'ITEM_IS_NOT_NULL'
,p_icon_css_classes=>'fa-rotate-left'
,p_grid_new_grid=>false
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(68361904218458390)
,p_button_sequence=>20
,p_button_plug_id=>wwv_flow_api.id(63498510122030715)
,p_button_name=>'MEMBERSHIP_AGREEMENTS'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Search Membership Agreements'
,p_button_position=>'BELOW_BOX'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:600:&SESSION.::&DEBUG.:600:P600_AFFILIATE_NO,P600_CALLING_PAGE_ID:&P260_IP_NUMBER.,&APP_PAGE_ID.'
,p_button_condition_type=>'NEVER'
,p_icon_css_classes=>'fa-search'
,p_grid_new_grid=>false
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(63395177212030629)
,p_button_sequence=>30
,p_button_plug_id=>wwv_flow_api.id(63393586162030628)
,p_button_name=>'EDIT_PROSPECT'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Edit Prospect '
,p_button_position=>'BELOW_BOX'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:50215:&SESSION.::&DEBUG.:RP,50215:P50215_IP_ID:&P260_IP_ID.'
,p_button_condition_type=>'NEVER'
,p_icon_css_classes=>'fa-pencil'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(69665980648432325)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(28318094651272616)
,p_button_sequence=>40
,p_button_plug_id=>wwv_flow_api.id(63393586162030628)
,p_button_name=>'SCANNED_DOCS'
,p_button_action=>'DEFINED_BY_DA'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Scanned Documents'
,p_button_position=>'BELOW_BOX'
,p_button_alignment=>'LEFT'
,p_warn_on_unsaved_changes=>null
,p_icon_css_classes=>'fa-picture-o'
,p_grid_new_grid=>false
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(65215007095221648)
,p_button_sequence=>50
,p_button_plug_id=>wwv_flow_api.id(63393586162030628)
,p_button_name=>'EDIT_INTERESTED_PARTY'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--iconLeft:t-Button--primary'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Edit Interested Party'
,p_button_position=>'BELOW_BOX'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:50210:&SESSION.::&DEBUG.:RP,50210:P50210_IP_ID,P50210_CALLING_PAGE_ID:&P260_IP_ID.,260'
,p_icon_css_classes=>'fa-pencil'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(69665980648432325)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(51304293076501104)
,p_button_sequence=>60
,p_button_plug_id=>wwv_flow_api.id(63393586162030628)
,p_button_name=>'COPY_TO_CTSTST'
,p_button_action=>'SUBMIT'
,p_button_template_options=>'#DEFAULT#:t-Button--iconLeft:t-Button--primary'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Copy To CTSTST'
,p_button_position=>'BELOW_BOX'
,p_button_alignment=>'LEFT'
,p_button_condition=>'P260_INSTANCE_NAME'
,p_button_condition2=>'CTSDEV:CTSPRD'
,p_button_condition_type=>'VALUE_OF_ITEM_IN_CONDITION_IN_COLON_DELIMITED_LIST'
,p_icon_css_classes=>'fa-pencil'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(69665980648432325)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(27136663064364108)
,p_button_sequence=>70
,p_button_plug_id=>wwv_flow_api.id(63393586162030628)
,p_button_name=>'Send_EAPP'
,p_button_action=>'SUBMIT'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Send EAPP'
,p_button_position=>'BELOW_BOX'
,p_button_alignment=>'LEFT'
,p_button_condition=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select 1 ',
'from dual ',
'where :P260_AFFILIATE_REP is not null and :P260_AFFILIATE_REP !=''*NONE''',
'and :P260_EMAIL is not null',
'and :P260_IP_ID is not null',
'and :P260_PROSPECT_FLAG=''Y''',
'and :app_user in (''SCHOPRA'',''SKLING'',''PMITRA'',''PKUMAR'',''DPARKHURST'')'))
,p_button_condition_type=>'EXISTS'
,p_icon_css_classes=>'fa-forumbee'
,p_grid_new_grid=>false
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(63455660263030682)
,p_button_sequence=>640
,p_button_plug_id=>wwv_flow_api.id(63450024162030678)
,p_button_name=>'P260_SEARCH_AA'
,p_button_static_id=>'P260_SEARCH_AA'
,p_button_action=>'SUBMIT'
,p_button_template_options=>'#DEFAULT#'
,p_button_template_id=>wwv_flow_api.id(51870385911240841)
,p_button_image_alt=>'Search'
,p_button_position=>'BODY'
,p_grid_new_grid=>false
,p_grid_new_row=>'N'
,p_grid_new_column=>'Y'
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(6150443484415135)
,p_button_sequence=>10
,p_button_plug_id=>wwv_flow_api.id(6149592920415126)
,p_button_name=>'ADD_COLLECTOR'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Add Collection Agreement '
,p_button_position=>'TOP'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:415:&SESSION.::&DEBUG.:RP,415:P415_COLLECTOR_ID:&P260_IP_ID.'
,p_icon_css_classes=>'fa-plus'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(17200066639506016)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(20922339242266322)
,p_button_sequence=>10
,p_button_plug_id=>wwv_flow_api.id(20922090990266319)
,p_button_name=>'Add_Royal_Threshhold'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Add Royalties/Threshhold'
,p_button_position=>'TOP'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:50256:&SESSION.::&DEBUG.:RP:P50256_IP_ID,P50256_IP_THRESHOLD_ID,P50256_THRESHOLD_TYPE,P50256_THRESHOLD_VALUE:&P260_IP_ID.,,,'
,p_icon_css_classes=>'fa-plus'
,p_grid_new_grid=>false
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(26246555571751710)
,p_button_sequence=>10
,p_button_plug_id=>wwv_flow_api.id(23950324549820744)
,p_button_name=>'ADD_NAME'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Add Name'
,p_button_position=>'TOP'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:50290:&SESSION.::&DEBUG.:RP,50290:P50290_IP_ID:&P260_IP_ID.'
,p_icon_css_classes=>'fa-plus'
,p_grid_new_grid=>false
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(28187018058944533)
,p_button_sequence=>10
,p_button_plug_id=>wwv_flow_api.id(63386355177030623)
,p_button_name=>'Get_Performing_Song_Catalog'
,p_button_action=>'DEFINED_BY_DA'
,p_button_template_options=>'#DEFAULT#'
,p_button_template_id=>wwv_flow_api.id(51870385911240841)
,p_button_image_alt=>'SESAC Performing Song Catalog'
,p_button_position=>'TOP'
,p_button_alignment=>'LEFT'
,p_warn_on_unsaved_changes=>null
,p_grid_new_grid=>false
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(32120254616480319)
,p_button_sequence=>10
,p_button_plug_id=>wwv_flow_api.id(63498510122030715)
,p_button_name=>'ADD_MEM_AGREE'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Add Membership Agreement '
,p_button_position=>'TOP'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:7240:&SESSION.::&DEBUG.:RP,7240:P7240_INTERESTED_PARTY,P7240_CALLING_PAGE_ID:&P260_IP_ID.,260'
,p_icon_css_classes=>'fa-plus'
,p_grid_new_grid=>false
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(66925438032572408)
,p_button_sequence=>10
,p_button_plug_id=>wwv_flow_api.id(63368308632030607)
,p_button_name=>'CREATE'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--iconLeft:t-Button--primary'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Add Contacts'
,p_button_position=>'TOP'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:50260:&SESSION.::&DEBUG.:50260:P50260_IP_ID:&P260_IP_ID.'
,p_icon_css_classes=>'fa-plus'
,p_grid_new_grid=>false
,p_grid_new_row=>'N'
,p_grid_new_column=>'N'
,p_security_scheme=>wwv_flow_api.id(69665980648432325)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(26246637509751711)
,p_button_sequence=>20
,p_button_plug_id=>wwv_flow_api.id(23950324549820744)
,p_button_name=>'ADD_ADDRESS'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Add Address'
,p_button_position=>'TOP'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:50241:&SESSION.::&DEBUG.:RP,50241:P50241_IP_ID:&P260_IP_ID.'
,p_icon_css_classes=>'fa-plus'
,p_grid_new_grid=>false
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(26248200301751727)
,p_button_sequence=>40
,p_button_plug_id=>wwv_flow_api.id(23950324549820744)
,p_button_name=>'ADD_CONTACT'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Add Contact'
,p_button_position=>'TOP'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:50260:&SESSION.::&DEBUG.:RP,50260:P50260_IP_ID:&P260_IP_ID.'
,p_icon_css_classes=>'fa-plus'
,p_grid_new_grid=>false
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(63456039226030683)
,p_button_sequence=>40
,p_button_plug_id=>wwv_flow_api.id(63450024162030678)
,p_button_name=>'CREATE'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#'
,p_button_template_id=>wwv_flow_api.id(51870385911240841)
,p_button_image_alt=>'Create'
,p_button_position=>'TOP'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:620:&SESSION.::&DEBUG.:RP,620:P620_MEMBERSHIP_AGREEMENT_ID,P620_GROUP_ID:,'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(69665980648432325)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(26249424615751739)
,p_button_sequence=>50
,p_button_plug_id=>wwv_flow_api.id(23950324549820744)
,p_button_name=>'ADD_NOTE'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Add Note'
,p_button_position=>'TOP'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:555:&SESSION.::&DEBUG.:RP,555:P555_IP_ID,P555_NOTE_PURPOSE:&P260_IP_ID.,AFFILIATE'
,p_icon_css_classes=>'fa-plus'
,p_grid_new_grid=>false
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(63410785949030640)
,p_button_sequence=>50
,p_button_plug_id=>wwv_flow_api.id(63407589302030638)
,p_button_name=>'ADD_NOTES'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--iconLeft:t-Button--primary'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Add Notes'
,p_button_position=>'TOP'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:555:&SESSION.::&DEBUG.:RP,555:P555_IP_ID,P555_NOTE_PURPOSE:&P260_IP_ID.,ACCOUNT'
,p_icon_css_classes=>'fa-plus'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(69665980648432325)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(66914023871493124)
,p_button_sequence=>650
,p_button_plug_id=>wwv_flow_api.id(63403184423030635)
,p_button_name=>'CREATE'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--iconLeft:t-Button--primary'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Add Name'
,p_button_position=>'TOP'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:50290:&SESSION.::&DEBUG.:RP,50290:P50290_IP_ID:&P260_IP_ID.'
,p_icon_css_classes=>'fa-plus'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(69665980648432325)
);
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(66956549498742132)
,p_button_sequence=>660
,p_button_plug_id=>wwv_flow_api.id(63381954351030619)
,p_button_name=>'CREATE'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#'
,p_button_template_id=>wwv_flow_api.id(51870385911240841)
,p_button_image_alt=>'Add'
,p_button_position=>'TOP'
,p_button_redirect_url=>'f?p=&APP_ID.:50009:&SESSION.::&DEBUG.:50009:P50009_IP_ID:&P50200_IP_ID.'
,p_button_condition_type=>'NEVER'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(69665980648432325)
);
end;
/
begin
wwv_flow_api.create_page_button(
 p_id=>wwv_flow_api.id(22323570117570405)
,p_button_sequence=>670
,p_button_plug_id=>wwv_flow_api.id(63381954351030619)
,p_button_name=>'ADD_CONTACT_POINT'
,p_button_action=>'REDIRECT_PAGE'
,p_button_template_options=>'#DEFAULT#:t-Button--primary:t-Button--iconLeft'
,p_button_template_id=>wwv_flow_api.id(51870469213240841)
,p_button_image_alt=>'Add contact point'
,p_button_position=>'TOP'
,p_button_alignment=>'LEFT'
,p_button_redirect_url=>'f?p=&APP_ID.:557:&SESSION.::&DEBUG.:RP,557:P557_IP_ID,P557_IP_CONTACT_ID:&P260_IP_ID.,'
,p_icon_css_classes=>'fa-plus'
,p_grid_new_grid=>false
,p_security_scheme=>wwv_flow_api.id(69665980648432325)
);
wwv_flow_api.create_page_branch(
 p_id=>wwv_flow_api.id(63527522278030738)
,p_branch_name=>'Go To Page 260'
,p_branch_action=>'f?p=&APP_ID.:260:&SESSION.:ADDR:&DEBUG.:::&success_msg=#SUCCESS_MSG#'
,p_branch_point=>'AFTER_PROCESSING'
,p_branch_type=>'REDIRECT_URL'
,p_branch_sequence=>1
);
wwv_flow_api.create_page_branch(
 p_id=>wwv_flow_api.id(43294514084079930)
,p_branch_name=>'gotodestination'
,p_branch_action=>'DECLARE'||wwv_flow.LF||
' v_ip_id_status NUMBER;'||wwv_flow.LF||
'BEGIN'||wwv_flow.LF||
'SELECT status_code_id '||wwv_flow.LF||
'		INTO v_ip_id_status'||wwv_flow.LF||
'	FROM aff_interested_parties'||wwv_flow.LF||
'	where ip_id = :P260_IP_ID;'||wwv_flow.LF||
'	'||wwv_flow.LF||
' IF v_ip_id_status <> 120'||wwv_flow.LF||
' THEN'||wwv_flow.LF||
'  RETURN ''Party is inactive, hierarchy creation not allowed.'';'||wwv_flow.LF||
' END IF;'||wwv_flow.LF||
'	END;'
,p_branch_point=>'AFTER_PROCESSING'
,p_branch_type=>'BRANCH_TO_FUNCTION_RETURNING_URL'
,p_branch_when_button_id=>wwv_flow_api.id(2445622172167104)
,p_branch_sequence=>10
);
wwv_flow_api.create_page_branch(
 p_id=>wwv_flow_api.id(63527924154030738)
,p_branch_name=>'Goto page 250'
,p_branch_action=>'f?p=&APP_ID.:250:&SESSION.::&DEBUG.::P250_TEST:100'
,p_branch_point=>'AFTER_PROCESSING'
,p_branch_type=>'REDIRECT_URL'
,p_branch_when_button_id=>wwv_flow_api.id(4152802147756377)
,p_branch_sequence=>11
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(4537733942564112)
,p_name=>'P260_QUERY_HISTORY'
,p_item_sequence=>350
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(4538301265564118)
,p_name=>'P260_QUERY_SONG'
,p_item_sequence=>360
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(9815740949107250)
,p_name=>'P260_REMIT_TO'
,p_item_sequence=>410
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_prompt=>'Remit To'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select ''Y'' from spi_remit_info_mv',
'where ip_id = :P260_ip_id'))
,p_source_type=>'QUERY'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_begin_on_new_line=>'N'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'Y'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(17291694445721942)
,p_name=>'P260_SFDC_ACCOUNT_ID'
,p_item_sequence=>310
,p_item_plug_id=>wwv_flow_api.id(63474031644030696)
,p_prompt=>'SFDC Account ID'
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
 p_id=>wwv_flow_api.id(19406119943337101)
,p_name=>'P260_PREV_IP_ID'
,p_item_sequence=>70
,p_item_plug_id=>wwv_flow_api.id(65211164049221609)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(19406222997337102)
,p_name=>'P260_PREV_IP_NUMBER'
,p_item_sequence=>80
,p_item_plug_id=>wwv_flow_api.id(65211164049221609)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(19448814371981345)
,p_name=>'P260_IP_TYPE'
,p_item_sequence=>70
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_prompt=>'IP Type'
,p_display_as=>'NATIVE_SELECT_LIST'
,p_named_lov=>'AFF_IP_TYPES'
,p_lov=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select ip_description d, ip_type_id r',
'from   AFF_IP_TYPES',
'order by 2'))
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
 p_id=>wwv_flow_api.id(19716354405749047)
,p_name=>'P260_HFA_MAN_AGR_MSG'
,p_item_sequence=>380
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(26246319953751708)
,p_name=>'P260_SHOW_RESULT'
,p_item_sequence=>10
,p_item_plug_id=>wwv_flow_api.id(23950324549820744)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(27939709208132920)
,p_name=>'P260_EAPP_SENT_FLAG'
,p_item_sequence=>390
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_prompt=>'E-App Sent Flag'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select password from  aff_interested_parties',
'        WHERE   ip_id =:p260_ip_id;'))
,p_source_type=>'QUERY'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_begin_on_new_line=>'N'
,p_colspan=>2
,p_grid_column=>4
,p_grid_label_column_span=>1
,p_display_when=>'P260_PROSPECT_FLAG'
,p_display_when2=>'Y'
,p_display_when_type=>'VAL_OF_ITEM_IN_COND_EQ_COND2'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'Y'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(30332296087365722)
,p_name=>'P260_TAX_ID_TYPE'
,p_item_sequence=>190
,p_item_plug_id=>wwv_flow_api.id(63474031644030696)
,p_source=>'select TAX_ID_TYPE from aff_interested_parties where ip_number = :P260_IP_NUMBER'
,p_source_type=>'QUERY'
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(30332550653369147)
,p_name=>'P260_TAX_REPORTING_NAME'
,p_item_sequence=>200
,p_item_plug_id=>wwv_flow_api.id(63474031644030696)
,p_source=>'select TAX_REPORTING_NAME from aff_interested_parties where ip_number = :P260_IP_NUMBER'
,p_source_type=>'QUERY'
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(33672916706694144)
,p_name=>'P260_EMAIL'
,p_item_sequence=>90
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_prompt=>'Email'
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
 p_id=>wwv_flow_api.id(33766822668980204)
,p_name=>'P260_CONTACT_PNT_ID'
,p_item_sequence=>10
,p_item_plug_id=>wwv_flow_api.id(63474031644030696)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(34043069091522823)
,p_name=>'P260_PARTY_HOLD'
,p_item_sequence=>10
,p_item_plug_id=>wwv_flow_api.id(34040902188522802)
,p_prompt=>'Party Hold'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_grid_column=>1
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'Y'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(34233577167807947)
,p_name=>'P260_W8_SIGNATURE_DATE'
,p_item_sequence=>280
,p_item_plug_id=>wwv_flow_api.id(63474031644030696)
,p_prompt=>'W8 Signature Date'
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
 p_id=>wwv_flow_api.id(34233613138807948)
,p_name=>'P260_W9_RECEIVED_FLAG'
,p_item_sequence=>300
,p_item_plug_id=>wwv_flow_api.id(63474031644030696)
,p_prompt=>'W9 Received Flag'
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
 p_id=>wwv_flow_api.id(34233755733807949)
,p_name=>'P260_W8_SIGNATURE_DATE2'
,p_item_sequence=>120
,p_item_plug_id=>wwv_flow_api.id(63424359871030651)
,p_prompt=>'W8 Signature Date'
,p_source=>'P260_W8_SIGNATURE_DATE'
,p_source_type=>'ITEM'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_colspan=>4
,p_grid_label_column_span=>2
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'Y'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(34233885454807950)
,p_name=>'P260_W9_RECEIVED_FLAG2'
,p_item_sequence=>130
,p_item_plug_id=>wwv_flow_api.id(63424359871030651)
,p_prompt=>'W9 Received Flag'
,p_source=>'P260_W9_RECEIVED_FLAG'
,p_source_type=>'ITEM'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_begin_on_new_line=>'N'
,p_colspan=>4
,p_grid_label_column_span=>2
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'Y'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(34612494692060025)
,p_name=>'P260_PAY_HOLD_REASON_HDR'
,p_item_sequence=>190
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_prompt=>'Pay Hold Type'
,p_source=>'P260_PAY_HOLD_ID'
,p_source_type=>'ITEM'
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
 p_id=>wwv_flow_api.id(34803587365762511)
,p_name=>'P260_PAY_HOLD_TYPE'
,p_item_sequence=>100
,p_item_plug_id=>wwv_flow_api.id(63474031644030696)
,p_prompt=>'Pay Hold Type'
,p_source=>'P260_PAY_HOLD_REASON_HDR'
,p_source_type=>'ITEM'
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
 p_id=>wwv_flow_api.id(37332871324575514)
,p_name=>'P260_TEST_1'
,p_item_sequence=>10
,p_item_plug_id=>wwv_flow_api.id(37332744869575513)
,p_prompt=>'Test'
,p_display_as=>'NATIVE_TEXT_FIELD'
,p_cSize=>30
,p_display_when_type=>'NEVER'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'N'
,p_attribute_04=>'TEXT'
,p_attribute_05=>'BOTH'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(39606048746660218)
,p_name=>'P260_WEB'
,p_item_sequence=>230
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_prompt=>'URL'
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
 p_id=>wwv_flow_api.id(39606933103660227)
,p_name=>'P260_QUERY_BANDS'
,p_item_sequence=>270
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(39607063929660228)
,p_name=>'P260_QUERY_NOTES'
,p_item_sequence=>280
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(39607184949660229)
,p_name=>'P260_QUERY_ROYAL'
,p_item_sequence=>290
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(39607298862660230)
,p_name=>'P260_QUERY_IPIHED'
,p_item_sequence=>300
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(39607495782660232)
,p_name=>'P260_QUERY_HIE'
,p_item_sequence=>310
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(39607525257660233)
,p_name=>'P260_QUERY_COLL'
,p_item_sequence=>320
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(39607627883660234)
,p_name=>'P260_QUERY_AFF'
,p_item_sequence=>330
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(39607776630660235)
,p_name=>'P260_QUERY_ACMI'
,p_item_sequence=>340
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(39608080599660238)
,p_name=>'P260_QUERY_ACCOUNT'
,p_item_sequence=>260
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(42864502315888303)
,p_name=>'P260_STATUS'
,p_item_sequence=>240
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_prompt=>'Status'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_colspan=>2
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'Y'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(51304571621501107)
,p_name=>'P260_INSTANCE_NAME'
,p_item_sequence=>400
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(56531367112292317)
,p_name=>'P260_CLOSED_RETURN_VAL'
,p_item_sequence=>10
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(60164004185808407)
,p_name=>'P260_TEST'
,p_item_sequence=>10
,p_item_plug_id=>wwv_flow_api.id(2445906509167107)
,p_prompt=>'Test'
,p_display_as=>'NATIVE_TEXT_FIELD'
,p_cSize=>30
,p_display_when_type=>'NEVER'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'N'
,p_attribute_04=>'TEXT'
,p_attribute_05=>'BOTH'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(63391959696030627)
,p_name=>'P260_IP_ID'
,p_item_sequence=>50
,p_item_plug_id=>wwv_flow_api.id(65211164049221609)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(63395510972030629)
,p_name=>'P260_IP_NUMBER'
,p_item_sequence=>20
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_prompt=>'IP Number'
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
 p_id=>wwv_flow_api.id(63395977064030629)
,p_name=>'P260_PRIMARY_NAME'
,p_item_sequence=>40
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_prompt=>'Primary Name'
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
 p_id=>wwv_flow_api.id(63396398888030630)
,p_name=>'P260_IPI_BASE_NUMBER'
,p_item_sequence=>50
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_prompt=>'IPI Base Number'
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
 p_id=>wwv_flow_api.id(63396735921030630)
,p_name=>'P260_AFF_IPI_TYPE'
,p_item_sequence=>60
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_prompt=>'IPI Party Type'
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
 p_id=>wwv_flow_api.id(63397580340030630)
,p_name=>'P260_AFFILIATE_REP'
,p_item_sequence=>140
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_item_default=>'*NONE'
,p_prompt=>'IP Rep'
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
 p_id=>wwv_flow_api.id(63397987499030631)
,p_name=>'P260_AFF_EARNINGS_RANK'
,p_item_sequence=>80
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_prompt=>'IP Rank'
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
 p_id=>wwv_flow_api.id(63398372593030631)
,p_name=>'P260_IPI_NAME_NUMBER'
,p_item_sequence=>120
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_prompt=>'IPI Name Number'
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
 p_id=>wwv_flow_api.id(63398783465030631)
,p_name=>'P260_DISPLAY_TAX_ID'
,p_item_sequence=>200
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_prompt=>'Tax ID Display'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'    l_tax_id            AFF_INTERESTED_PARTIES.TAX_ID%TYPE;',
'    l_masked_tax_id     VARCHAR2(100);',
'',
'BEGIN',
'    ',
'    SELECT tax_id, CASE WHEN length(tax_id)>= 4 THEN REGEXP_REPLACE (SUBSTR(tax_id,1,LENGTH(tax_id)-4), ''.'', ''X'')||SUBSTR(TAX_ID,-4) ELSE tax_id end as masked_tax_id',
'    INTO l_tax_id, l_masked_tax_id',
'    FROM aff_interested_parties ',
'    where ip_number = :P260_IP_NUMBER;',
'    ',
'    IF l_tax_id IS NOT NULL THEN',
'        IF APEX_AUTHORIZATION.IS_AUTHORIZED( p_authorization_name => ''PARTY_TAX_FULL_VIEW'') THEN',
'            RETURN ''<a href="f?p=&APP_ID.:265:&SESSION.::::P265_IP_ID,P265_AFFILIATE_NO:&P260_IP_ID.,&P260_AFFILIATE_NO.">''||l_tax_id||''</a>'';',
'        ELSIF APEX_AUTHORIZATION.IS_AUTHORIZED( p_authorization_name => (''PARTY_ADMIN_TAX_ID'')) THEN',
'            RETURN ''<a href="f?p=&APP_ID.:265:&SESSION.::::P265_IP_ID,P265_AFFILIATE_NO:&P260_IP_ID.,&P260_AFFILIATE_NO.">''||l_masked_tax_id||''</a>'';',
'        ELSE',
'            RETURN ''<a href="f?p=&APP_ID.:265:&SESSION.::::P265_IP_ID,P265_AFFILIATE_NO:&P260_IP_ID.,&P260_AFFILIATE_NO.">''||l_masked_tax_id||''</a>'';',
'      ',
'        END IF;',
'    END IF;',
'    ',
'EXCEPTION WHEN OTHERS THEN',
'    RETURN NULL;',
'    ',
'END;'))
,p_source_type=>'FUNCTION_BODY'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_label_column_span=>1
,p_display_when=>wwv_flow_string.join(wwv_flow_t_varchar2(
'BEGIN',
'    IF APEX_AUTHORIZATION.IS_AUTHORIZED( p_authorization_name => ''PARTY_TAX_FULL_VIEW'') OR',
'    APEX_AUTHORIZATION.IS_AUTHORIZED( p_authorization_name => ''PARTY_ADMIN_TAX_ID'') OR ',
'    APEX_AUTHORIZATION.IS_AUTHORIZED( p_authorization_name => NULL) ',
'    THEN',
'        RETURN TRUE;',
'    END IF;',
'END;'))
,p_display_when_type=>'FUNCTION_BODY'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_escape_on_http_output=>'N'
,p_help_text=>'Please click on TAX ID to see all the parties sharing the same TAX ID value.'
,p_attribute_01=>'N'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
,p_item_comment=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select ''<a href="f?p=&APP_ID.:265:&SESSION.::::P265_IP_ID,P265_AFFILIATE_NO:&P260_IP_ID.,&P260_AFFILIATE_NO.">''||tax_id||''</a>'' tax_id from (',
'select regexp_replace (substr(tax_id,1,length(tax_id)-4), ''\d'', ''X'')||substr(TAX_ID,-4) tax_id from aff_interested_parties where ip_number = :P260_IP_NUMBER',
'and length(tax_id)>=4',
'union',
'select TAX_ID tax_id from aff_interested_parties where ip_number = :P260_IP_NUMBER and length(tax_id)<4);',
'BEGIN',
'    IF APEX_AUTHORIZATION.IS_AUTHORIZED( p_authorization_name => ''PARTY_TAX_FULL_VIEW'') OR',
'    APEX_AUTHORIZATION.IS_AUTHORIZED( p_authorization_name => ''PARTY_ADMIN_TAX_ID'') ',
'    THEN',
'        RETURN TRUE;',
'    ELSE ',
'        RETURN FALSE;',
'    END IF;',
'END;'))
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(63399185522030632)
,p_name=>'P260_DERIVED_STATUS'
,p_item_sequence=>110
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_prompt=>'Derived Status'
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
 p_id=>wwv_flow_api.id(63399579239030632)
,p_name=>'P260_PARENT_IP'
,p_item_sequence=>220
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_prompt=>'Parent IP'
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
 p_id=>wwv_flow_api.id(63399935979030632)
,p_name=>'P260_PERF_RIGHT_FLAG'
,p_item_sequence=>150
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(63400393888030632)
,p_name=>'P260_SYNC_RIGHT_FLAG'
,p_item_sequence=>160
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(63400732478030633)
,p_name=>'P260_MECH_RIGHT_FLAG'
,p_item_sequence=>170
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(63401147574030633)
,p_name=>'P260_GRAND_RIGHT_FLAG'
,p_item_sequence=>180
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'N'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(63401506787030633)
,p_name=>'P260_PROSPECT_FLAG'
,p_item_sequence=>130
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_prompt=>'Prospect Flag'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_named_lov=>'YES_NO'
,p_lov=>'.'||wwv_flow_api.id(62410853427715388)||'.'
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_lov_display_extra=>'YES'
,p_attribute_01=>'N'
,p_attribute_02=>'LOV'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(63411576059030641)
,p_name=>'P260_TAX_ID'
,p_item_sequence=>210
,p_item_plug_id=>wwv_flow_api.id(63474031644030696)
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select regexp_replace (substr(tax_id,1,length(tax_id)-4), ''\d'', ''X'')||substr(TAX_ID,-4) tax_id from aff_interested_parties where ip_number = :P260_IP_NUMBER',
'and length(tax_id)>=4',
'union',
'select TAX_ID tax_id from aff_interested_parties where ip_number = :P260_IP_NUMBER and length(tax_id)<4;'))
,p_source_type=>'QUERY'
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
,p_item_comment=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select regexp_replace (substr(tax_id,1,length(tax_id)-4), ''\d'', ''X'')||substr(TAX_ID,-4) tax_id from aff_interested_parties where ip_number = :P260_IP_NUMBER',
'and length(tax_id)>=4',
'union',
'select TAX_ID tax_id from aff_interested_parties where ip_number = :P260_IP_NUMBER and length(tax_id)<4;'))
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(63411934132030641)
,p_name=>'P260_AFF_CITIZEN_TERR'
,p_item_sequence=>270
,p_item_plug_id=>wwv_flow_api.id(63474031644030696)
,p_prompt=>'Citizenship Territory'
,p_source=>'select aff_ip.get_ipi_nationality(:P260_INTERESTED_PARTY_ID) from dual'
,p_source_type=>'QUERY'
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
 p_id=>wwv_flow_api.id(63412352554030642)
,p_name=>'P260_FOREIGN_RESIDENCE'
,p_item_sequence=>250
,p_item_plug_id=>wwv_flow_api.id(63474031644030696)
,p_prompt=>'Foreign Residence'
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
 p_id=>wwv_flow_api.id(63412730690030642)
,p_name=>'P260_AFF_RESIDENCE_TERR'
,p_item_sequence=>260
,p_item_plug_id=>wwv_flow_api.id(63474031644030696)
,p_prompt=>'Residence Territory'
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
 p_id=>wwv_flow_api.id(63413144781030642)
,p_name=>'P260_PAY_HOLD_ID'
,p_item_sequence=>290
,p_item_plug_id=>wwv_flow_api.id(63474031644030696)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(63413577523030642)
,p_name=>'P260_PAY_HOLD_REASON'
,p_item_sequence=>90
,p_item_plug_id=>wwv_flow_api.id(63474031644030696)
,p_prompt=>'Pay Hold Reason'
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
 p_id=>wwv_flow_api.id(63413911777030643)
,p_name=>'P260_AP_OPEN_BALANCE'
,p_item_sequence=>60
,p_item_plug_id=>wwv_flow_api.id(63474031644030696)
,p_prompt=>'AP Open Balance'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT TO_CHAR ( AP_OPEN_BALANCE,',
'               ''fm9999999.90'') ',
'FROM Xxsec_Ap_Supplier_Info_Mv',
'WHERE ip_id = :P260_IP_ID'))
,p_source_type=>'QUERY'
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
 p_id=>wwv_flow_api.id(63425106075030651)
,p_name=>'P260_PAYMENT_FREQ'
,p_item_sequence=>10
,p_item_plug_id=>wwv_flow_api.id(63424359871030651)
,p_prompt=>'Payment Freq'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_colspan=>4
,p_grid_label_column_span=>2
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(63425567871030651)
,p_name=>'P260_ROY_TH_AMOUNT'
,p_item_sequence=>110
,p_item_plug_id=>wwv_flow_api.id(63424359871030651)
,p_prompt=>'Royalties Threshold Amount'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_begin_on_new_line=>'N'
,p_colspan=>4
,p_grid_label_column_span=>2
,p_display_when_type=>'NEVER'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(63425968831030652)
,p_name=>'P260_PREFERRED_LANG'
,p_item_sequence=>40
,p_item_plug_id=>wwv_flow_api.id(63424359871030651)
,p_prompt=>'Preferred Language'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_begin_on_new_line=>'N'
,p_colspan=>4
,p_grid_label_column_span=>2
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(63426321549030652)
,p_name=>'P260_PAPER_STMT_ONLY'
,p_item_sequence=>50
,p_item_plug_id=>wwv_flow_api.id(63424359871030651)
,p_prompt=>'Paper Stmt Only'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_begin_on_new_line=>'N'
,p_colspan=>4
,p_grid_label_column_span=>2
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(63426709098030652)
,p_name=>'P260_DIRECT_DEPOSIT_IND'
,p_item_sequence=>60
,p_item_plug_id=>wwv_flow_api.id(63424359871030651)
,p_use_cache_before_default=>'NO'
,p_item_default=>wwv_flow_string.join(wwv_flow_t_varchar2(
'declare',
'',
'l_par VARCHAR2(100);',
'l_direct_deposit_flag VARCHAR2(1) := ''N'';',
'begin',
'      begin',
'         l_par := aff_ip.get_payment_method(:P260_IP_ID);',
'       ',
'      exception',
'          when others then',
'          l_par := null;',
'     end;',
'     IF l_par = ''EFT'' THEN',
'	l_direct_deposit_flag := ''Y'';',
'      END IF;',
' return l_direct_deposit_flag;',
' end;'))
,p_item_default_type=>'PLSQL_FUNCTION_BODY'
,p_prompt=>'Direct Deposit'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_colspan=>4
,p_grid_label_column_span=>2
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(63427117473030653)
,p_name=>'P260_BANK_NAME'
,p_item_sequence=>70
,p_item_plug_id=>wwv_flow_api.id(63424359871030651)
,p_use_cache_before_default=>'NO'
,p_item_default=>wwv_flow_string.join(wwv_flow_t_varchar2(
'declare',
'',
'l_par VARCHAR2(100);',
'l_direct_deposit_flag VARCHAR2(1) := ''N'';',
'begin',
'      begin',
'         l_par := aff_ip.get_bank_name(:P260_AFFILIATE_NO);',
'            ',
'      exception',
'          when others then',
'          l_par := null;',
'     end;',
'     ',
' return l_par;',
' end;'))
,p_item_default_type=>'PLSQL_FUNCTION_BODY'
,p_prompt=>'Bank Name'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_begin_on_new_line=>'N'
,p_colspan=>4
,p_grid_label_column_span=>2
,p_display_when_type=>'NEVER'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(63427582274030653)
,p_name=>'P260_BANK_ACCT_NO'
,p_item_sequence=>80
,p_item_plug_id=>wwv_flow_api.id(63424359871030651)
,p_use_cache_before_default=>'NO'
,p_item_default=>wwv_flow_string.join(wwv_flow_t_varchar2(
'declare',
'',
'l_par VARCHAR2(100);',
'l_bank_acct_no VARCHAR2(100) := NULL;',
'begin',
'      begin',
'         l_bank_acct_no := aff_ip.get_bank_acct_no(:P260_AFFILIATE_NO);',
'          SELECT SUBSTR(l_bank_acct_no,LENGTH(l_bank_acct_no)-3 )',
'          INTO l_par',
'          FROM dual;',
'      ',
'      exception',
'          when others then',
'          l_par := null;',
'     end;',
'     ',
' return l_par;',
' end;'))
,p_item_default_type=>'PLSQL_FUNCTION_BODY'
,p_prompt=>'Bank Acct No'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_begin_on_new_line=>'N'
,p_colspan=>4
,p_grid_label_column_span=>2
,p_display_when_type=>'NEVER'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
end;
/
begin
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(63427928223030653)
,p_name=>'P260_PAY_HOLD_ID2'
,p_item_sequence=>90
,p_item_plug_id=>wwv_flow_api.id(63424359871030651)
,p_prompt=>'Pay Hold'
,p_source=>'P260_PAY_HOLD_ID'
,p_source_type=>'ITEM'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_begin_on_new_line=>'N'
,p_colspan=>4
,p_grid_label_column_span=>2
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'Y'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(63428386340030653)
,p_name=>'P260_DART_ROYALTIES'
,p_item_sequence=>100
,p_item_plug_id=>wwv_flow_api.id(63424359871030651)
,p_prompt=>'DART Royalties'
,p_display_as=>'NATIVE_TEXT_FIELD'
,p_cSize=>30
,p_cMaxlength=>4000
,p_begin_on_new_line=>'N'
,p_colspan=>4
,p_grid_label_column_span=>2
,p_display_when_type=>'NEVER'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'N'
,p_attribute_04=>'TEXT'
,p_attribute_05=>'NONE'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(63429519796030654)
,p_name=>'P260_ORIG_EFF_DATE'
,p_item_sequence=>220
,p_item_plug_id=>wwv_flow_api.id(63474031644030696)
,p_prompt=>'Orig Effective Date'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_colspan=>3
,p_grid_label_column_span=>1
,p_display_when_type=>'NEVER'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(63429987754030654)
,p_name=>'P260_CONTRACT_END_DATE'
,p_item_sequence=>230
,p_item_plug_id=>wwv_flow_api.id(63474031644030696)
,p_prompt=>'Contract End Date'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_label_column_span=>1
,p_display_when_type=>'NEVER'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(63430319778030655)
,p_name=>'P260_SEX'
,p_item_sequence=>140
,p_item_plug_id=>wwv_flow_api.id(63474031644030696)
,p_prompt=>'Sex'
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
 p_id=>wwv_flow_api.id(63430781694030655)
,p_name=>'P260_IPI_REGISTERED'
,p_item_sequence=>150
,p_item_plug_id=>wwv_flow_api.id(63474031644030696)
,p_prompt=>'IPI Registered Flag'
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
 p_id=>wwv_flow_api.id(63431509654030655)
,p_name=>'P260_AFF_PREV_SOC'
,p_item_sequence=>40
,p_item_plug_id=>wwv_flow_api.id(63474031644030696)
,p_item_default=>'Unknown'
,p_prompt=>'Previous Society'
,p_source=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select society_name from ipi_societies where society_code_id in (',
'select max(society_code_id) from IPI_MEMBERSHIP_AGREEMENT',
'where (interested_party_id, valid_to_date) in (',
'select * from (',
'select interested_party_id, max(valid_to_date) ',
'from IPI_MEMBERSHIP_AGREEMENT',
'where valid_to_date < sysdate',
'and interested_party_id = :P260_INTERESTED_PARTY_ID',
'group by interested_party_id',
'order by 2 desc)',
'where rownum < 2 ))'))
,p_source_type=>'QUERY'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_label_column_span=>1
,p_display_when=>wwv_flow_string.join(wwv_flow_t_varchar2(
'if nvl(:P260_PROSPECT_FLAG,''N'') != ''Y'' then ',
'  return true;',
'else',
'',
'return false;',
'end if;',
''))
,p_display_when_type=>'FUNCTION_BODY'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(63431984416030656)
,p_name=>'P260_BIRTH_DATE'
,p_item_sequence=>160
,p_item_plug_id=>wwv_flow_api.id(63474031644030696)
,p_prompt=>'Birth Date'
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
 p_id=>wwv_flow_api.id(63432339298030656)
,p_name=>'P260_DEATH_DATE'
,p_item_sequence=>170
,p_item_plug_id=>wwv_flow_api.id(63474031644030696)
,p_prompt=>'Death Date'
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
 p_id=>wwv_flow_api.id(63432798370030656)
,p_name=>'P260_RENEWAL_DATE'
,p_item_sequence=>240
,p_item_plug_id=>wwv_flow_api.id(63474031644030696)
,p_prompt=>'Renewal Date'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_begin_on_new_line=>'N'
,p_colspan=>3
,p_grid_label_column_span=>1
,p_display_when_type=>'NEVER'
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(63434351116030658)
,p_name=>'P260_INTERESTED_PARTY_ID'
,p_item_sequence=>30
,p_item_plug_id=>wwv_flow_api.id(63433904888030657)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(63434703197030658)
,p_name=>'P260_IPI_TERRITORY'
,p_item_sequence=>10
,p_item_plug_id=>wwv_flow_api.id(63433904888030657)
,p_prompt=>'IPI Territory'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_colspan=>2
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'N'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(63435111534030658)
,p_name=>'P260_IPI_BIRTH_PLACE'
,p_item_sequence=>40
,p_item_plug_id=>wwv_flow_api.id(63433904888030657)
,p_prompt=>'IPI Birth Place'
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
 p_id=>wwv_flow_api.id(63435554333030658)
,p_name=>'P260_IPI_BIRTH_DATE'
,p_item_sequence=>20
,p_item_plug_id=>wwv_flow_api.id(63433904888030657)
,p_prompt=>'IPI Birth Date'
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
 p_id=>wwv_flow_api.id(63435934460030659)
,p_name=>'P260_IPI_BIRTH_STATE'
,p_item_sequence=>50
,p_item_plug_id=>wwv_flow_api.id(63433904888030657)
,p_prompt=>'IPI Birth State'
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
 p_id=>wwv_flow_api.id(63436311719030659)
,p_name=>'P260_IPI_DEATH_DATE'
,p_item_sequence=>60
,p_item_plug_id=>wwv_flow_api.id(63433904888030657)
,p_prompt=>'IPI Death Date'
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
 p_id=>wwv_flow_api.id(63436754890030659)
,p_name=>'P260_IPI_AMMEND_DATE'
,p_item_sequence=>70
,p_item_plug_id=>wwv_flow_api.id(63433904888030657)
,p_prompt=>'IPI Ammendment Date'
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
 p_id=>wwv_flow_api.id(63456406170030683)
,p_name=>'P260_SOCIETY'
,p_item_sequence=>630
,p_item_plug_id=>wwv_flow_api.id(63450024162030678)
,p_prompt=>'Society'
,p_display_as=>'NATIVE_TEXT_FIELD'
,p_cSize=>30
,p_cMaxlength=>4000
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_attribute_01=>'NONE'
,p_attribute_02=>'N'
,p_attribute_04=>'TEXT'
,p_attribute_05=>'BOTH'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(63456885248030683)
,p_name=>'P260_SOCIETY_FILTER'
,p_item_sequence=>720
,p_item_plug_id=>wwv_flow_api.id(63450024162030678)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(63474464025030696)
,p_name=>'P260_IPI_NAME'
,p_item_sequence=>30
,p_item_plug_id=>wwv_flow_api.id(63474031644030696)
,p_prompt=>'IPI Name'
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
 p_id=>wwv_flow_api.id(63475626220030697)
,p_name=>'P260_CURR_SOCIETY'
,p_item_sequence=>50
,p_item_plug_id=>wwv_flow_api.id(63474031644030696)
,p_prompt=>'Current Society'
,p_source=>'select aff_ip.get_current_IPI_society(:P260_IP_ID) from dual'
,p_source_type=>'QUERY'
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
 p_id=>wwv_flow_api.id(63476028429030697)
,p_name=>'P260_PAYMENT_FREQ1'
,p_item_sequence=>70
,p_item_plug_id=>wwv_flow_api.id(63474031644030696)
,p_prompt=>'Payment Freq'
,p_source=>'P260_Payment_Freq'
,p_source_type=>'ITEM'
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
 p_id=>wwv_flow_api.id(63476491795030698)
,p_name=>'P260_PAYMENT_METHOD'
,p_item_sequence=>80
,p_item_plug_id=>wwv_flow_api.id(63474031644030696)
,p_use_cache_before_default=>'NO'
,p_item_default=>wwv_flow_string.join(wwv_flow_t_varchar2(
'declare',
'',
'l_par VARCHAR2(100);',
'begin',
'      begin',
'         l_par := aff_ip.get_payment_method(:P260_IP_ID);',
'         ',
'      exception',
'          when others then',
'          l_par := null;',
'     end;',
' return l_par;',
' end;'))
,p_item_default_type=>'PLSQL_FUNCTION_BODY'
,p_prompt=>'Payment Method'
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
 p_id=>wwv_flow_api.id(63477242190030698)
,p_name=>'P260_NO_OF_SONGS'
,p_item_sequence=>180
,p_item_plug_id=>wwv_flow_api.id(63474031644030696)
,p_item_default=>wwv_flow_string.join(wwv_flow_t_varchar2(
'BEGIN',
'return spi_screen_utility.get_ip_song_count(:P260_ip_id);',
'END;'))
,p_item_default_type=>'PLSQL_FUNCTION_BODY'
,p_prompt=>'No of Songs Registered'
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
 p_id=>wwv_flow_api.id(63477602979030699)
,p_name=>'P260_AVAILABLE_BALANCE'
,p_item_sequence=>110
,p_item_plug_id=>wwv_flow_api.id(63474031644030696)
,p_item_default=>wwv_flow_string.join(wwv_flow_t_varchar2(
'declare',
'l_avail_bal VARCHAR2(20);',
'begin',
'           begin',
'                 select to_char(SUM(ad.money_paid +                   ad.money_recouped),''999,999,999.99'')',
'                 into l_avail_bal ',
'            from acmi_affiliates aa,acmi_detail ad',
'              where aa.assoc_ip_id = :P260_ip_id',
'              and aa.detail_id = ad.detail_id;',
'        exception ',
'               when others then',
'               l_avail_bal := ''0'';',
'          end;',
'return l_avail_bal;',
'end;'))
,p_item_default_type=>'PLSQL_FUNCTION_BODY'
,p_prompt=>'ACMI Balance'
,p_display_as=>'NATIVE_DISPLAY_ONLY'
,p_colspan=>3
,p_grid_label_column_span=>1
,p_field_template=>wwv_flow_api.id(51869870681240840)
,p_item_template_options=>'#DEFAULT#'
,p_help_text=>'ACMI Balance'
,p_attribute_01=>'N'
,p_attribute_02=>'VALUE'
,p_attribute_04=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(63478567288030700)
,p_name=>'P260_BAD_ADDRESS_INDICATOR'
,p_item_sequence=>130
,p_item_plug_id=>wwv_flow_api.id(63474031644030696)
,p_item_default=>wwv_flow_string.join(wwv_flow_t_varchar2(
'declare',
'v_cnt NUMBER := 0;',
'begin',
'            BEGIN          ',
'              select 1 ',
'                      into v_cnt',
'                       from aff_ip_addresses',
'                    where ip_id = :P260_IP_ID',
'                     AND (upper(status) =''BAD''',
'                   OR upper(status) =''B'');',
'        EXCEPTION',
'                 WHEN others THEN',
'                 v_cnt := 0;',
'        END;',
'IF v_cnt = 1 THEN ',
'return ''Y'';',
'else',
'  return ''N'';',
'end if;',
'end;'))
,p_item_default_type=>'PLSQL_FUNCTION_BODY'
,p_prompt=>'Bad Address Indicator'
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
 p_id=>wwv_flow_api.id(65211297999221610)
,p_name=>'P260_AFFILIATE_NO'
,p_item_sequence=>20
,p_item_plug_id=>wwv_flow_api.id(65211164049221609)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65211364031221611)
,p_name=>'P260_CALLING_PAGE_ID'
,p_item_sequence=>40
,p_item_plug_id=>wwv_flow_api.id(65211164049221609)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'Y'
);
wwv_flow_api.create_page_item(
 p_id=>wwv_flow_api.id(65211428154221612)
,p_name=>'P260_REQUEST'
,p_item_sequence=>60
,p_item_plug_id=>wwv_flow_api.id(65211164049221609)
,p_prompt=>'Request'
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
 p_id=>wwv_flow_api.id(67584455459106046)
,p_name=>'P260_HFA_NUMBER'
,p_item_sequence=>30
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_prompt=>'HFA#'
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
 p_id=>wwv_flow_api.id(1684581898060661521)
,p_name=>'P260_QUERY_CONSOLIDATED'
,p_item_sequence=>370
,p_item_plug_id=>wwv_flow_api.id(63393586162030628)
,p_display_as=>'NATIVE_HIDDEN'
,p_attribute_01=>'N'
);
wwv_flow_api.create_page_computation(
 p_id=>wwv_flow_api.id(26246488408751709)
,p_computation_sequence=>10
,p_computation_item=>'P260_SHOW_RESULT'
,p_computation_point=>'AFTER_BOX_BODY'
,p_computation_type=>'STATIC_ASSIGNMENT'
,p_computation=>'SHOW_RESULT'
);
wwv_flow_api.create_page_validation(
 p_id=>wwv_flow_api.id(51304480264501106)
,p_validation_name=>'CHK_PARTY_TYPE'
,p_validation_sequence=>10
,p_validation=>wwv_flow_string.join(wwv_flow_t_varchar2(
'--This validation is not to allow user to copy party information to test env when party is not LICENSEE',
'DECLARE',
'BEGIN',
'    --2 means LICENSEE',
'    IF :P260_IP_TYPE <> 2',
'    THEN',
'         RETURN ''Only LICENSEE can be copied to CTSTST environment !'';',
'    END IF;',
'',
'END;'))
,p_validation_type=>'FUNC_BODY_RETURNING_ERR_TEXT'
,p_when_button_pressed=>wwv_flow_api.id(51304293076501104)
,p_error_display_location=>'INLINE_WITH_FIELD_AND_NOTIFICATION'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(67581547136106017)
,p_name=>'REFREESH_BAND'
,p_event_sequence=>10
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(63460421216030686)
,p_bind_type=>'bind'
,p_bind_event_type=>'apexafterclosedialog'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(67581642339106018)
,p_event_id=>wwv_flow_api.id(67581547136106017)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(63460421216030686)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(20921394249266312)
,p_name=>'REFREESH_ACCOUNTS'
,p_event_sequence=>20
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(63381954351030619)
,p_bind_type=>'bind'
,p_bind_event_type=>'apexafterclosedialog'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(20921465516266313)
,p_event_id=>wwv_flow_api.id(20921394249266312)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(63381954351030619)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(7576465001132020)
,p_name=>'REFERESH_HIERARCHY'
,p_event_sequence=>30
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(2445906509167107)
,p_bind_type=>'bind'
,p_bind_event_type=>'apexafterclosedialog'
,p_display_when_type=>'NEVER'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(7576538224132021)
,p_event_id=>wwv_flow_api.id(7576465001132020)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(2445906509167107)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(12166992338077003)
,p_event_id=>wwv_flow_api.id(7576465001132020)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(19406575483337105)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(37384795798207401)
,p_name=>'REFERESH_HIERARCHY_INTERACTIVE'
,p_event_sequence=>40
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(37332744869575513)
,p_bind_type=>'bind'
,p_bind_event_type=>'apexafterclosedialog'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(38910995609902128)
,p_event_id=>wwv_flow_api.id(37384795798207401)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
':P260_QUERY_HIE:= ''Y'';',
':P260_IP_TAB :=7;'))
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(37384801020207402)
,p_event_id=>wwv_flow_api.id(37384795798207401)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(37332744869575513)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(37384928775207403)
,p_event_id=>wwv_flow_api.id(37384795798207401)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(37338287862580018)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(37795642051097441)
,p_event_id=>wwv_flow_api.id(37384795798207401)
,p_event_result=>'TRUE'
,p_action_sequence=>40
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(37793925872097424)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(42502474404432830)
,p_name=>'REFERESH_HIERARCHY2'
,p_event_sequence=>60
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(2445906509167107)
,p_bind_type=>'bind'
,p_bind_event_type=>'apexafterclosedialog'
,p_display_when_type=>'NEVER'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42502562753432831)
,p_event_id=>wwv_flow_api.id(42502474404432830)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_HIDE'
,p_affected_elements_type=>'BUTTON'
,p_affected_button_id=>wwv_flow_api.id(13586346594450124)
,p_attribute_01=>'N'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(37385028453207404)
,p_name=>'REFERESH_HIERARCHY2_INTERACTIVE'
,p_event_sequence=>70
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(37332744869575513)
,p_bind_type=>'bind'
,p_bind_event_type=>'apexafterclosedialog'
,p_display_when_type=>'FUNCTION_BODY'
,p_display_when_cond=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'	l_COUNT	NUMBER;',
'BEGIN ',
'',
'    SELECT COUNT(*) ',
'    INTO l_COUNT',
'    FROM aff_hierarchy ',
'    WHERE RELATE_ACCOUNT_NUMBER IS NULL ',
'    AND HIER_LEVEL=0 AND CHILD_PARTY_ID=:P260_IP_ID',
'    AND end_date IS NULL AND NOWED_AFF_HIER_ID IS NULL;',
'',
'    IF :P260_HFA_NUMBER LIKE ''P%'' AND l_Count = 0 THEN',
'        RETURN FALSE;',
'    ELSIF :P260_HFA_NUMBER LIKE ''P%'' AND l_count > 0 THEN	',
'        RETURN TRUE;',
'    ELSE',
'        RETURN TRUE;',
'    END IF;',
'    ',
'END;	'))
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(4375442395140804)
,p_event_id=>wwv_flow_api.id(37385028453207404)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_HIDE'
,p_affected_elements_type=>'BUTTON'
,p_affected_button_id=>wwv_flow_api.id(37333096277575516)
,p_attribute_01=>'N'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(67581764686106019)
,p_name=>'REFRESH_NAME'
,p_event_sequence=>80
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(23950324549820744)
,p_bind_type=>'bind'
,p_bind_event_type=>'apexafterclosedialog'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(67581877802106020)
,p_event_id=>wwv_flow_api.id(67581764686106019)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(23950464724820745)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(14394864965004944)
,p_name=>'REFRESH_ADDRESS'
,p_event_sequence=>90
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(23950324549820744)
,p_bind_type=>'bind'
,p_bind_event_type=>'apexafterclosedialog'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(14394937131004945)
,p_event_id=>wwv_flow_api.id(14394864965004944)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(26273367519107401)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(67582220720106024)
,p_name=>'REFRESH_NOTES'
,p_event_sequence=>100
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(23950324549820744)
,p_bind_type=>'bind'
,p_bind_event_type=>'apexafterclosedialog'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(67582362900106025)
,p_event_id=>wwv_flow_api.id(67582220720106024)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(63407589302030638)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(22937891738627742)
,p_event_id=>wwv_flow_api.id(67582220720106024)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(26248445354751729)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(67802066365724541)
,p_name=>'REFERESH_CONTACTS'
,p_event_sequence=>110
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(23950324549820744)
,p_bind_type=>'bind'
,p_bind_event_type=>'apexafterclosedialog'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(67802153823724542)
,p_event_id=>wwv_flow_api.id(67802066365724541)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(26246719289751712)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(4537915807564114)
,p_name=>'P260_QUERY_HISTORY'
,p_event_sequence=>120
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'[href="#HIST"]'
,p_bind_type=>'one'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(4538035107564115)
,p_event_id=>wwv_flow_api.id(4537915807564114)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
':P260_QUERY_HISTORY := ''Y'';',
':P260_IP_TAB :=11;'))
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(4538268732564117)
,p_event_id=>wwv_flow_api.id(4537915807564114)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>'$s("P260_QUERY_HISTORY","Y");'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(6943043917121042)
,p_event_id=>wwv_flow_api.id(4537915807564114)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(63464898628030689)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(4538407064564119)
,p_name=>'P260_QUERY_SONG'
,p_event_sequence=>130
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'[href="#SONG"]'
,p_bind_type=>'one'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(4538537839564120)
,p_event_id=>wwv_flow_api.id(4538407064564119)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
':P260_QUERY_SONG := ''Y'';',
':P260_IP_TAB :=12;'))
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(4538632039564121)
,p_event_id=>wwv_flow_api.id(4538407064564119)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>'$s("P260_QUERY_SONG","Y");'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(6942934434121041)
,p_event_id=>wwv_flow_api.id(4538407064564119)
,p_event_result=>'TRUE'
,p_action_sequence=>40
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(63386355177030623)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(1684581930851661522)
,p_name=>'P260_QUERY_CONSOLIDATED_HOLDS'
,p_event_sequence=>140
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'[href="#CONSHOLD"]'
,p_bind_type=>'one'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(1684582084059661523)
,p_event_id=>wwv_flow_api.id(1684581930851661522)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
':P260_QUERY_CONSOLIDATED := ''Y'';',
':P260_IP_TAB :=13;'))
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(1684582156683661524)
,p_event_id=>wwv_flow_api.id(1684581930851661522)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>'$s("P260_QUERY_CONSOLIDATED","Y");'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(1684582225891661525)
,p_event_id=>wwv_flow_api.id(1684581930851661522)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(34294360991208905)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(1684582308067661526)
,p_event_id=>wwv_flow_api.id(1684581930851661522)
,p_event_result=>'TRUE'
,p_action_sequence=>40
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(34040902188522802)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(1684582495929661527)
,p_event_id=>wwv_flow_api.id(1684581930851661522)
,p_event_result=>'TRUE'
,p_action_sequence=>50
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(34042375590522816)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(39608789380660245)
,p_name=>'P260_QUERY_ACCOUNT'
,p_event_sequence=>150
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'[href="#ACCOUNTS"]'
,p_bind_type=>'one'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(39608846051660246)
,p_event_id=>wwv_flow_api.id(39608789380660245)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
':P260_QUERY_ACCOUNT:= ''Y'';',
'--:P260_IP_TAB :=2;'))
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(39608911888660247)
,p_event_id=>wwv_flow_api.id(39608789380660245)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>'$s("P260_QUERY_ACCOUNT","Y");'
,p_stop_execution_on_error=>'Y'
);
end;
/
begin
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42342433693324332)
,p_event_id=>wwv_flow_api.id(39608789380660245)
,p_event_result=>'TRUE'
,p_action_sequence=>40
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(63480178688030701)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42342572546324333)
,p_event_id=>wwv_flow_api.id(39608789380660245)
,p_event_result=>'TRUE'
,p_action_sequence=>50
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(63490129285030709)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(39609168113660249)
,p_name=>'P260_QUERY_BANDS'
,p_event_sequence=>160
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'[href="#BAND"]'
,p_bind_type=>'one'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(39609295613660250)
,p_event_id=>wwv_flow_api.id(39609168113660249)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
':P260_QUERY_BANDS:= ''Y'';',
':P260_IP_TAB :=3;'))
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42339376681324301)
,p_event_id=>wwv_flow_api.id(39609168113660249)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>'$s("P260_QUERY_BANDS","Y");'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42339400015324302)
,p_event_id=>wwv_flow_api.id(39609168113660249)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(63460421216030686)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(42339575952324303)
,p_name=>'P260_QUERY_NOTES'
,p_event_sequence=>170
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'[href="#NOTE"]'
,p_bind_type=>'one'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42339611283324304)
,p_event_id=>wwv_flow_api.id(42339575952324303)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
':P260_QUERY_NOTES:= ''Y'';',
':P260_IP_TAB :=4;'))
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42339730999324305)
,p_event_id=>wwv_flow_api.id(42339575952324303)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>'$s("P260_QUERY_NOTES","Y");'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42339889956324306)
,p_event_id=>wwv_flow_api.id(42339575952324303)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(63407589302030638)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(42339923264324307)
,p_name=>'P260_QUERY_ROYAL'
,p_event_sequence=>180
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'[href="#ROYL"]'
,p_bind_type=>'one'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42340023291324308)
,p_event_id=>wwv_flow_api.id(42339923264324307)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
':P260_QUERY_ROYAL:= ''Y'';',
':P260_IP_TAB :=5;'))
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42340162105324309)
,p_event_id=>wwv_flow_api.id(42339923264324307)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>'$s("P260_QUERY_ROYAL","Y");'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42340206893324310)
,p_event_id=>wwv_flow_api.id(42339923264324307)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(20922090990266319)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42342628405324334)
,p_event_id=>wwv_flow_api.id(42339923264324307)
,p_event_result=>'TRUE'
,p_action_sequence=>40
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(69228743323028224)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42342740151324335)
,p_event_id=>wwv_flow_api.id(42339923264324307)
,p_event_result=>'TRUE'
,p_action_sequence=>50
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(69228625468028223)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(37792753873097412)
,p_event_id=>wwv_flow_api.id(42339923264324307)
,p_event_result=>'TRUE'
,p_action_sequence=>60
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(37791860935097403)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(42340354999324311)
,p_name=>'P260_QUERY_IPIHED'
,p_event_sequence=>190
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'[href="#HIPH"]'
,p_bind_type=>'one'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42340497099324312)
,p_event_id=>wwv_flow_api.id(42340354999324311)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
':P260_QUERY_IPIHED:= ''Y'';',
':P260_IP_TAB :=6;'))
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42340564148324313)
,p_event_id=>wwv_flow_api.id(42340354999324311)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>'$s("P260_QUERY_IPIHED","Y");'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42502210276432828)
,p_event_id=>wwv_flow_api.id(42340354999324311)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(63437118105030659)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42340637415324314)
,p_event_id=>wwv_flow_api.id(42340354999324311)
,p_event_result=>'TRUE'
,p_action_sequence=>40
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(63440345180030662)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(42340792966324315)
,p_name=>'P260_QUERY_HIE'
,p_event_sequence=>200
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'[href="#HIAG"]'
,p_bind_type=>'one'
,p_bind_event_type=>'click'
,p_display_when_type=>'NEVER'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42340818375324316)
,p_event_id=>wwv_flow_api.id(42340792966324315)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>':P260_QUERY_HIE:= ''Y'';'
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42340996241324317)
,p_event_id=>wwv_flow_api.id(42340792966324315)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>'$s("P260_QUERY_HIE","Y");'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42502110673432827)
,p_event_id=>wwv_flow_api.id(42340792966324315)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(2445906509167107)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42341059717324318)
,p_event_id=>wwv_flow_api.id(42340792966324315)
,p_event_result=>'TRUE'
,p_action_sequence=>40
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(33806569131587639)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42342833042324336)
,p_event_id=>wwv_flow_api.id(42340792966324315)
,p_event_result=>'TRUE'
,p_action_sequence=>50
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(19406575483337105)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(37337623960580012)
,p_name=>'P260_QUERY_HIE_Interactive'
,p_event_sequence=>210
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'[href="#HIAGH"]'
,p_bind_type=>'one'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(37337797190580013)
,p_event_id=>wwv_flow_api.id(37337623960580012)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
':P260_QUERY_HIE:= ''Y'';',
':P260_IP_TAB :=7;'))
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(37337879387580014)
,p_event_id=>wwv_flow_api.id(37337623960580012)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
'$s("P260_QUERY_HIE","Y");',
''))
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(37337962899580015)
,p_event_id=>wwv_flow_api.id(37337623960580012)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(37332744869575513)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(37338047377580016)
,p_event_id=>wwv_flow_api.id(37337623960580012)
,p_event_result=>'TRUE'
,p_action_sequence=>40
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(37339724360580033)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(37338149451580017)
,p_event_id=>wwv_flow_api.id(37337623960580012)
,p_event_result=>'TRUE'
,p_action_sequence=>50
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(37338287862580018)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(37795553316097440)
,p_event_id=>wwv_flow_api.id(37337623960580012)
,p_event_result=>'TRUE'
,p_action_sequence=>60
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(37793925872097424)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(42341201061324320)
,p_name=>'P260_QUERY_COLL'
,p_event_sequence=>220
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'[href="#CLAG"]'
,p_bind_type=>'bind'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42341350502324321)
,p_event_id=>wwv_flow_api.id(42341201061324320)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
':P260_QUERY_COLL:= ''Y'';',
':P260_IP_TAB :=8;',
''))
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42341421280324322)
,p_event_id=>wwv_flow_api.id(42341201061324320)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>'$s("P260_QUERY_COLL","Y");'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42341503221324323)
,p_event_id=>wwv_flow_api.id(42341201061324320)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(6149592920415126)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42342969543324337)
,p_event_id=>wwv_flow_api.id(42341201061324320)
,p_event_result=>'TRUE'
,p_action_sequence=>40
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(29620144102493525)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(38889673227446426)
,p_event_id=>wwv_flow_api.id(42341201061324320)
,p_event_result=>'TRUE'
,p_action_sequence=>50
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(37796228300097447)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(42341640619324324)
,p_name=>'P260_QUERY_AFF'
,p_event_sequence=>230
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'[href="#AFAG"]'
,p_bind_type=>'one'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42341780474324325)
,p_event_id=>wwv_flow_api.id(42341640619324324)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
':P260_QUERY_AFF:= ''Y'';',
':P260_IP_TAB :=9;'))
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42341837003324326)
,p_event_id=>wwv_flow_api.id(42341640619324324)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>'$s("P260_QUERY_AFF","Y");'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42341934639324327)
,p_event_id=>wwv_flow_api.id(42341640619324324)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(63498510122030715)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42343088969324338)
,p_event_id=>wwv_flow_api.id(42341640619324324)
,p_event_result=>'TRUE'
,p_action_sequence=>40
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(11420285810058749)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(38908525970902104)
,p_event_id=>wwv_flow_api.id(42341640619324324)
,p_event_result=>'TRUE'
,p_action_sequence=>50
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(38889816560446428)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(2457182830447933)
,p_event_id=>wwv_flow_api.id(42341640619324324)
,p_event_result=>'TRUE'
,p_action_sequence=>60
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(2208531419471449)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(42342071910324328)
,p_name=>'P260_QUERY_ACMI'
,p_event_sequence=>240
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'[href="#ACMI"]'
,p_bind_type=>'one'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42342199195324329)
,p_event_id=>wwv_flow_api.id(42342071910324328)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
':P260_QUERY_ACMI:= ''Y'';',
':P260_IP_TAB:=10;'))
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42342231625324330)
,p_event_id=>wwv_flow_api.id(42342071910324328)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>'$s("P260_QUERY_ACMI","Y");'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(42342355120324331)
,p_event_id=>wwv_flow_api.id(42342071910324328)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(63504112702030720)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(6938994653121001)
,p_name=>'P260_QUERY_SONG_1'
,p_event_sequence=>250
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(63386355177030623)
,p_bind_type=>'bind'
,p_bind_event_type=>'mousedown'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(6939037937121002)
,p_event_id=>wwv_flow_api.id(6938994653121001)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>':P260_QUERY_SONG := ''Y'';'
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(6939136117121003)
,p_event_id=>wwv_flow_api.id(6938994653121001)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>'$s("P260_QUERY_SONG","Y");'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(28316810044272604)
,p_name=>'REFRESH_NOTES_1'
,p_event_sequence=>260
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(63407589302030638)
,p_bind_type=>'bind'
,p_bind_event_type=>'apexafterclosedialog'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(28316956146272605)
,p_event_id=>wwv_flow_api.id(28316810044272604)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(26248445354751729)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(28317030615272606)
,p_event_id=>wwv_flow_api.id(28316810044272604)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(63407589302030638)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(22291233120322405)
,p_name=>'REFRESH_IP_HEADER_DISPLAY'
,p_event_sequence=>270
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(63498510122030715)
,p_bind_type=>'bind'
,p_bind_event_type=>'apexafterclosedialog'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(22291342114322406)
,p_event_id=>wwv_flow_api.id(22291233120322405)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_SET_VALUE'
,p_affected_elements_type=>'ITEM'
,p_affected_elements=>'P260_STATUS'
,p_attribute_01=>'SQL_STATEMENT'
,p_attribute_03=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT lkup.description ',
'  FROM aff_interested_parties aip, aff_lookups lkup',
' WHERE aip.ip_id         = :P260_IP_ID',
'   AND lkup.lookup_code  = aip.status_code_id',
'   AND lkup.lookup_type  = ''MC_ACCOUNT_STATUS''',
'   AND lkup.enabled_flag = ''Y'';'))
,p_attribute_07=>'P260_IP_ID'
,p_attribute_08=>'Y'
,p_attribute_09=>'N'
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(22291462914322407)
,p_event_id=>wwv_flow_api.id(22291233120322405)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'ITEM'
,p_affected_elements=>'P260_STATUS'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(22292791340322420)
,p_name=>'REFRESH_COLLECTION_PG_250'
,p_event_sequence=>280
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(63498510122030715)
,p_bind_type=>'bind'
,p_bind_event_type=>'apexafterclosedialog'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(22292936991322422)
,p_event_id=>wwv_flow_api.id(22292791340322420)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
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
'',
'--Retrieving lastest derived status to update in collection of page 250',
'SELECT NVL2(ra.aff_category_hfa, ra.aff_category||''/''||ra.aff_category_hfa, ra.aff_category) ',
'INTO v_derived_status',
'  FROM rep_affiliates ra',
' WHERE ra.ip_id = :P260_IP_ID;  ',
'',
'',
'--Retrieving sequence number or PKey of apex collection.',
'SELECT seq_id ',
'       INTO v_seq_id ',
'  FROM apex_collections  ',
' WHERE collection_name = ''IP_INQUIRY_COLLECTION_PG250'' ',
'   AND C001 = :P260_IP_ID;',
'',
'--Updating collection of page 250',
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
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(22291595512322408)
,p_name=>' REFRESH_HIERARCHY_HISTORY'
,p_event_sequence=>290
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(2445906509167107)
,p_bind_type=>'bind'
,p_bind_event_type=>'apexafterclosedialog'
,p_display_when_type=>'NEVER'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(22291708282322410)
,p_event_id=>wwv_flow_api.id(22291595512322408)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(33806569131587639)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(37385292245207406)
,p_name=>' REFRESH_HIERARCHY_HISTORY_INTERACTIVE'
,p_event_sequence=>300
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(37332744869575513)
,p_bind_type=>'bind'
,p_bind_event_type=>'apexafterclosedialog'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(37385346002207407)
,p_event_id=>wwv_flow_api.id(37385292245207406)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(37339724360580033)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(22291857226322411)
,p_name=>'REFRESH_VIEW_USED_HIERARCHY'
,p_event_sequence=>310
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(2445906509167107)
,p_bind_type=>'bind'
,p_bind_event_type=>'apexafterclosedialog'
,p_display_when_type=>'NEVER'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(22291986549322412)
,p_event_id=>wwv_flow_api.id(22291857226322411)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(19406575483337105)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(37385445843207408)
,p_name=>'REFRESH_VIEW_USED_HIERARCHY_INTERACTIVE'
,p_event_sequence=>320
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(37332744869575513)
,p_bind_type=>'bind'
,p_bind_event_type=>'apexafterclosedialog'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(37385528466207409)
,p_event_id=>wwv_flow_api.id(37385445843207408)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(37338287862580018)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(28318171118272617)
,p_name=>'VIEW_DOCUMENTS '
,p_event_sequence=>330
,p_triggering_element_type=>'BUTTON'
,p_triggering_button_id=>wwv_flow_api.id(28318094651272616)
,p_bind_type=>'bind'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(28318291432272618)
,p_event_id=>wwv_flow_api.id(28318171118272617)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
'String.prototype.replaceAll = String.prototype.replaceAll || function(string, replaced) {',
'  return this.replace(new RegExp(string, ''g''), replaced);',
'};',
'',
'var appItemVal = ''&APP_OPENTXT_IP_URL.'';',
'//window.open(''http://aotcstapw.sesac.com/livelink/livelink.exe?func=search&where1="Attr_441684_2":"'' + $v("P260_IP_NUMBER") + ''"'', ''_blank'');',
'//window.open(appItemVal + ''":"'' + $v("P260_IP_NUMBER") + ''"'', ''_blank'');',
'appItemVal = appItemVal.replaceAll(''ipVal'', $v("P260_IP_NUMBER"));',
'window.open(appItemVal);'))
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(37795971311097444)
,p_name=>'REFRESH_RECORD_LOG_INTERACTIVE'
,p_event_sequence=>340
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(37332744869575513)
,p_bind_type=>'bind'
,p_bind_event_type=>'apexafterclosedialog'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(37796000501097445)
,p_event_id=>wwv_flow_api.id(37795971311097444)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(37793925872097424)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(33528970295128226)
,p_name=>'Refresh Collection Agreements'
,p_event_sequence=>350
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(6149592920415126)
,p_bind_type=>'bind'
,p_bind_event_type=>'apexafterclosedialog'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(33529073073128227)
,p_event_id=>wwv_flow_api.id(33528970295128226)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(6149592920415126)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(38889736123446427)
,p_event_id=>wwv_flow_api.id(33528970295128226)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(37796228300097447)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(32121053445480327)
,p_name=>'Refresh_Membership_Agreements'
,p_event_sequence=>360
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(63498510122030715)
,p_bind_type=>'bind'
,p_bind_event_type=>'apexafterclosedialog'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(32121171644480328)
,p_event_id=>wwv_flow_api.id(32121053445480327)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(63498510122030715)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(43292229331079907)
,p_event_id=>wwv_flow_api.id(32121053445480327)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT NVL2(ra.aff_category_hfa, ra.aff_category||''/''||ra.aff_category_hfa, ra.aff_category) ',
'into :P260_DERIVED_STATUS',
'       FROM rep_affiliates ra',
'       WHERE ra.ip_id = :P260_IP_ID;'))
,p_attribute_03=>'P260_DERIVED_STATUS'
,p_attribute_04=>'N'
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(43292324247079908)
,p_event_id=>wwv_flow_api.id(32121053445480327)
,p_event_result=>'TRUE'
,p_action_sequence=>40
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'ITEM'
,p_affected_elements=>'P260_DERIVED_STATUS'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(38908498117902103)
,p_event_id=>wwv_flow_api.id(32121053445480327)
,p_event_result=>'TRUE'
,p_action_sequence=>50
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(38889816560446428)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(20922599085266324)
,p_name=>'Refresh_royalties_threshhold'
,p_event_sequence=>370
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(20922090990266319)
,p_bind_type=>'bind'
,p_bind_event_type=>'apexafterclosedialog'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(20922633074266325)
,p_event_id=>wwv_flow_api.id(20922599085266324)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_REFRESH'
,p_affected_elements_type=>'REGION'
,p_affected_region_id=>wwv_flow_api.id(20922090990266319)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(23441722499268123)
,p_name=>'CheckNull'
,p_event_sequence=>380
,p_triggering_element_type=>'ITEM'
,p_triggering_element=>'P260_TAX_ID'
,p_condition_element=>'P260_TAX_ID'
,p_triggering_condition_type=>'NULL'
,p_bind_type=>'bind'
,p_bind_event_type=>'change'
);
end;
/
begin
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(23441856211268124)
,p_event_id=>wwv_flow_api.id(23441722499268123)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
'var myFunction = function() {',
'    $(''#P260_TAX_ID_LABEL'').css(''color'',''red'');',
'    $(''#P260_TAX_ID_LABEL'').css(''font-weight'',''bold'');',
'    $(''#P260_DISPLAY_TAX_ID_LABEL'').css(''color'',''red'');',
'    $(''#P260_DISPLAY_TAX_ID_LABEL'').css(''font-weight'',''bold'');',
'    ',
'    setTimeout(function(){',
'    $(''#P260_TAX_ID_LABEL'').css(''color'',''rgb(77, 77, 77)'');',
'    $(''#P260_TAX_ID_LABEL'').css(''font-weight'',''bold'');',
'    $(''#P260_DISPLAY_TAX_ID_LABEL'').css(''color'',''rgb(77, 77, 77)'');',
'    $(''#P260_DISPLAY_TAX_ID_LABEL'').css(''font-weight'',''bold'');',
'    }, 500);',
'    ',
'',
'};',
'',
'var timer =  setInterval(myFunction, 1000);'))
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(23441968892268125)
,p_event_id=>wwv_flow_api.id(23441722499268123)
,p_event_result=>'FALSE'
,p_action_sequence=>10
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
'$(''#P260_TAX_ID_LABEL'').css(''color'',''rgb(77, 77, 77)'');',
'$(''#P260_TAX_ID_LABEL'').css(''font-weight'',''normal'');',
'$(''#P260_DISPLAY_TAX_ID_LABEL'').css(''color'',''rgb(77, 77, 77)'');',
'$(''#P260_DISPLAY_TAX_ID_LABEL'').css(''font-weight'',''normal'');'))
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(23442042848268126)
,p_name=>'CheckNull2'
,p_event_sequence=>390
,p_triggering_element_type=>'ITEM'
,p_triggering_element=>'P260_DISPLAY_TAX_ID'
,p_condition_element=>'P260_DISPLAY_TAX_ID'
,p_triggering_condition_type=>'NULL'
,p_bind_type=>'bind'
,p_bind_event_type=>'change'
,p_display_when_type=>'NEVER'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(23442105111268127)
,p_event_id=>wwv_flow_api.id(23442042848268126)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_SHOW'
,p_affected_elements_type=>'JAVASCRIPT_EXPRESSION'
,p_affected_elements=>wwv_flow_string.join(wwv_flow_t_varchar2(
'$(''#P260_DISPLAY_TAX_ID_LABEL'').css(''color'',''red'');',
'$(''#P260_DISPLAY_TAX_ID_LABEL'').css(''font-weight'',''bold'');'))
,p_attribute_01=>'N'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(23442219000268128)
,p_event_id=>wwv_flow_api.id(23442042848268126)
,p_event_result=>'FALSE'
,p_action_sequence=>10
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_HIDE'
,p_affected_elements_type=>'JAVASCRIPT_EXPRESSION'
,p_affected_elements=>wwv_flow_string.join(wwv_flow_t_varchar2(
'$(''#P260_DISPLAY_TAX_ID_LABEL'').css(''color'',''rgb(77, 77, 77)'');',
'$(''#P260_DISPLAY_TAX_ID_LABEL'').css(''font-weight'',''normal'');'))
,p_attribute_01=>'N'
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(43295515532079940)
,p_name=>'Check_INACTIVE_STATUS'
,p_event_sequence=>400
,p_bind_type=>'bind'
,p_bind_event_type=>'ready'
,p_display_when_type=>'NEVER'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(43295616717079941)
,p_event_id=>wwv_flow_api.id(43295515532079940)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_DISABLE'
,p_affected_elements_type=>'BUTTON'
,p_affected_button_id=>wwv_flow_api.id(2445622172167104)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(43295763478079942)
,p_event_id=>wwv_flow_api.id(43295515532079940)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_DISABLE'
,p_affected_elements_type=>'BUTTON'
,p_affected_button_id=>wwv_flow_api.id(13586346594450124)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(37386295107207416)
,p_name=>'Check_INACTIVE_STATUS_INTERACTIVE'
,p_event_sequence=>410
,p_bind_type=>'bind'
,p_bind_event_type=>'ready'
,p_display_when_type=>'VAL_OF_ITEM_IN_COND_EQ_COND2'
,p_display_when_cond=>'P260_STATUS'
,p_display_when_cond2=>'INACTIVE'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(37386383157207417)
,p_event_id=>wwv_flow_api.id(37386295107207416)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_DISABLE'
,p_affected_elements_type=>'BUTTON'
,p_affected_button_id=>wwv_flow_api.id(37332910113575515)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(37386479057207418)
,p_event_id=>wwv_flow_api.id(37386295107207416)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_DISABLE'
,p_affected_elements_type=>'BUTTON'
,p_affected_button_id=>wwv_flow_api.id(37333096277575516)
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(56531470074292318)
,p_name=>'SHOW_MSG'
,p_event_sequence=>420
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(2445906509167107)
,p_bind_type=>'bind'
,p_bind_event_type=>'apexafterclosedialog'
,p_display_when_type=>'NEVER'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(56531579245292319)
,p_event_id=>wwv_flow_api.id(56531470074292318)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_SET_VALUE'
,p_affected_elements_type=>'ITEM'
,p_affected_elements=>'P260_CLOSED_RETURN_VAL'
,p_attribute_01=>'DIALOG_RETURN_ITEM'
,p_attribute_09=>'N'
,p_attribute_10=>'P12_SEND_TO_P260'
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(56531791927292321)
,p_event_id=>wwv_flow_api.id(56531470074292318)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>'null;'
,p_attribute_02=>'P260_CLOSED_RETURN_VAL'
,p_attribute_03=>'P260_CLOSED_RETURN_VAL'
,p_attribute_04=>'N'
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(56531639652292320)
,p_event_id=>wwv_flow_api.id(56531470074292318)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
'var itemVal = apex.item(''P260_CLOSED_RETURN_VAL'').getValue();',
'var pMessage = ''SPI Nowing is submitted. Scheduled to run between 19:00 to 5:00'';',
'apex.message.clearErrors();',
'if (itemVal != '''') ',
'    {',
'        itemVal = parseInt(itemVal);',
'        if ( itemVal == 1 )',
'            apex.message.showPageSuccess( pMessage );',
'    }',
''))
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(37385866642207412)
,p_name=>'SHOW_MSG_INTERACTIVE'
,p_event_sequence=>430
,p_triggering_element_type=>'REGION'
,p_triggering_region_id=>wwv_flow_api.id(37332744869575513)
,p_bind_type=>'bind'
,p_bind_event_type=>'apexafterclosedialog'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(37385900215207413)
,p_event_id=>wwv_flow_api.id(37385866642207412)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_SET_VALUE'
,p_affected_elements_type=>'ITEM'
,p_affected_elements=>'P260_CLOSED_RETURN_VAL'
,p_attribute_01=>'DIALOG_RETURN_ITEM'
,p_attribute_09=>'N'
,p_attribute_10=>'P12_SEND_TO_P260'
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(37386099983207414)
,p_event_id=>wwv_flow_api.id(37385866642207412)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>'null;'
,p_attribute_02=>'P260_CLOSED_RETURN_VAL'
,p_attribute_03=>'P260_CLOSED_RETURN_VAL'
,p_attribute_04=>'N'
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(37386149699207415)
,p_event_id=>wwv_flow_api.id(37385866642207412)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
'var itemVal = apex.item(''P260_CLOSED_RETURN_VAL'').getValue();',
'var pMessage = ''SPI Nowing is submitted. Scheduled to run between 19:00 to 5:00'';',
'apex.message.clearErrors();',
'if (itemVal != '''') ',
'    {',
'        itemVal = parseInt(itemVal);',
'        if ( itemVal == 1 )',
'            apex.message.showPageSuccess( pMessage );',
'    }',
''))
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(33766927809980205)
,p_name=>'OnDeleteClick'
,p_event_sequence=>440
,p_triggering_element_type=>'JQUERY_SELECTOR'
,p_triggering_element=>'.deleteAccButton'
,p_bind_type=>'bind'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(33767139113980207)
,p_event_id=>wwv_flow_api.id(33766927809980205)
,p_event_result=>'TRUE'
,p_action_sequence=>20
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_SET_VALUE'
,p_affected_elements_type=>'ITEM'
,p_affected_elements=>'P260_CONTACT_PNT_ID'
,p_attribute_01=>'JAVASCRIPT_EXPRESSION'
,p_attribute_05=>'$(this.triggeringElement).attr(''contact-point-id'')'
,p_attribute_09=>'N'
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(33767232735980208)
,p_event_id=>wwv_flow_api.id(33766927809980205)
,p_event_result=>'TRUE'
,p_action_sequence=>30
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>'null;'
,p_attribute_02=>'P260_CONTACT_PNT_ID'
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(39455342560647232)
,p_name=>'OnClick'
,p_event_sequence=>450
,p_triggering_element_type=>'ITEM'
,p_triggering_element=>'P260_DISPLAY_TAX_ID2'
,p_bind_type=>'bind'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(39455450891647233)
,p_event_id=>wwv_flow_api.id(39455342560647232)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
'select tax_id into :P265_DISPLAY_TAX_ID from aff_interested_parties where ip_number = :P260_IP_NUMBER;',
''))
,p_attribute_02=>'P265_DISPLAY_TAX_ID'
,p_attribute_03=>'P265_DISPLAY_TAX_ID'
,p_attribute_04=>'N'
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(40027070067812604)
,p_name=>'OnClickTaxID'
,p_event_sequence=>460
,p_triggering_element_type=>'ITEM'
,p_triggering_element=>'P260_DISPLAY_TAX_ID'
,p_bind_type=>'bind'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(40027102293812605)
,p_event_id=>wwv_flow_api.id(40027070067812604)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'Y'
,p_action=>'NATIVE_EXECUTE_PLSQL_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
'BEGIN',
'select tax_id into :P265_DISPLAY_TAX_ID from aff_interested_parties where ip_number = :P260_IP_NUMBER;',
'EXCEPTION WHEN no_data_found THEN',
'null;',
'end;'))
,p_attribute_02=>'P265_DISPLAY_TAX_ID'
,p_attribute_03=>'P265_DISPLAY_TAX_ID'
,p_attribute_04=>'N'
,p_stop_execution_on_error=>'Y'
,p_wait_for_result=>'Y'
);
wwv_flow_api.create_page_da_event(
 p_id=>wwv_flow_api.id(28187159022944534)
,p_name=>'POPUP_Perf_SONG_CATALOG'
,p_event_sequence=>470
,p_triggering_element_type=>'BUTTON'
,p_triggering_button_id=>wwv_flow_api.id(28187018058944533)
,p_bind_type=>'bind'
,p_bind_event_type=>'click'
);
wwv_flow_api.create_page_da_action(
 p_id=>wwv_flow_api.id(28187222487944535)
,p_event_id=>wwv_flow_api.id(28187159022944534)
,p_event_result=>'TRUE'
,p_action_sequence=>10
,p_execute_on_page_init=>'N'
,p_action=>'NATIVE_JAVASCRIPT_CODE'
,p_attribute_01=>wwv_flow_string.join(wwv_flow_t_varchar2(
'/*apex.server.process("DYNAMIC_URL_PROCESSING",',
'	{x01:$v("")}, // pass an item for passing a parameter',
'		{dataType: ''text'',',
'                    success: function(data)',
'			{',
'				var vUrl  = data;      ',
'				eval(vUrl);',
'			}    ',
'		}',
'	);',
'*/',
'javascript:window.open(''f?p=&APP_ID.:63:&APP_SESSION.:RPT:::P63_IP_NUMBER:&P260_AFFILIATE_NO.'',''_blank'');',
''))
,p_stop_execution_on_error=>'Y'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(63525293712030735)
,p_process_sequence=>20
,p_process_point=>'AFTER_HEADER'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'Populate IPI Data'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'begin',
'SELECT FOUNDATION_BIRTH_PLACE,',
'  FOUNDATION_BIRTH_STATE ,',
'  (foundation_birth_year',
'  ||''-''',
'  ||foundation_birth_month',
'  ||''-''',
'  ||foundation_birth_day) birth ,',
'  (dissolution_death_year',
'  ||''-''',
'  ||dissolution_death_month',
'  ||''-''',
'  ||dissolution_death_day) death ,',
'  TO_CHAR(to_date(TO_CHAR(AMMENDMENT_DATE,''DD-MON-YYYY'')',
'  ||'' ''',
'  ||AMMENDMENT_TIME,''DD-MON-YYYY HH24MISS''),''MM/DD/YYYY HH24:MI:SS'') ammend_datetime ,',
'  trim(itnt.TISAN) territory,',
'  iip.interested_party_id',
'INTO :P260_IPI_BIRTH_PLACE,',
'  :P260_IPI_BIRTH_STATE,',
'  :P260_IPI_BIRTH_DATE,',
'  :P260_IPI_DEATH_DATE ,',
'  :P260_IPI_ammend_date ,',
'  :P260_IPI_territory,',
'  :P260_interested_party_id',
'FROM IPI_INTERESTED_PARTY iip,',
'  IPI_TERRITORY_NAMES_TISN itnt',
'WHERE itnt.territory_names_tisn_id(+) = iip.territory_names_tisn_id',
'AND iip.base_number                   = NVL(:P260_IPI_BASE_NUMBER,0) ;',
'exception when OTHERS then ',
'null;',
'end;'))
,p_process_when=>'P260_IPI_BASE_NUMBER'
,p_process_when_type=>'ITEM_IS_NOT_NULL'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(4537872702564113)
,p_process_sequence=>30
,p_process_point=>'AFTER_HEADER'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'POPULATE_FLAGS'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'begin',
'',
'  :P260_QUERY_BANDS := ''N'';',
'  :P260_QUERY_NOTES := ''N'';',
'  :P260_QUERY_ROYAL := ''N'';',
'  :P260_QUERY_IPIHED := ''N'';',
'  :P260_QUERY_HIE := ''N'';',
'  :P260_QUERY_SONG := ''N'';',
'  :P260_QUERY_COLL := ''N'';',
'  :P260_QUERY_AFF := ''N'';',
'  :P260_QUERY_ACMI := ''N'';',
'  :P260_QUERY_HISTORY := ''N'';',
'  :P260_QUERY_CONSOLIDATED := ''N'';',
'',
'/*proc_logs(''before :P260_QUERY_SONG '' || :P260_QUERY_SONG);',
'proc_logs(''before :P260_QUERY_HIE '' || :P260_QUERY_HIE);',
'proc_logs(''before :P260_QUERY_COLL '' || :P260_QUERY_COLL);',
'proc_logs(''before :P260_IP_TAB '' || :P260_IP_TAB);*/',
'',
'if :P260_QUERY_ACCOUNT != ''Y'' or :P260_QUERY_ACCOUNT is null then ',
'  :P260_QUERY_ACCOUNT := ''N'';',
'end if;',
'',
'if :P260_IP_TAB=3 then',
'  :P260_QUERY_BANDS := ''Y'';',
'end if;',
'',
'if :P260_IP_TAB=4 then',
'  :P260_QUERY_NOTES := ''Y'';',
'end if;',
'',
'if :P260_IP_TAB=5 then',
'  :P260_QUERY_ROYAL := ''Y'';',
'end if;',
'',
'if :P260_IP_TAB=6 then',
'  :P260_QUERY_IPIHED := ''Y'';',
'end if;',
'',
'if :P260_IP_TAB=7 then',
'  :P260_QUERY_HIE := ''Y'';',
'end if;',
'',
'',
'if :P260_IP_TAB=8 then',
'  :P260_QUERY_COLL := ''Y'';',
'end if;',
'',
'if :P260_IP_TAB=9 then',
'  :P260_QUERY_AFF := ''Y'';',
'end if;',
'',
'if :P260_IP_TAB=10 then',
'  :P260_QUERY_ACMI := ''Y'';',
'end if;',
'',
'--Proc_debug(''P260_IP_TAB'' || :P260_IP_TAB);',
'--Proc_debug(''P260_QUERY_HISTORY'' || :P260_QUERY_HISTORY);',
'',
'if :P260_IP_TAB = 11 then',
' :P260_QUERY_HISTORY := ''Y'';',
'end if; ',
'',
'--Proc_debug(''P260_IP_TAB'' || :P260_IP_TAB);',
'--Proc_debug(''P260_QUERY_SONG'' || :P260_QUERY_SONG);',
'',
'if :P260_IP_TAB=12 then',
' :P260_QUERY_SONG := ''Y'';',
'end if; ',
'',
'if :P260_IP_TAB=13 then',
' :P260_QUERY_CONSOLIDATED := ''Y'';',
'end if; ',
'',
'/*proc_logs(''after :P260_QUERY_SONG '' || :P260_QUERY_SONG);',
'proc_logs(''after :P260_QUERY_HIE '' || :P260_QUERY_HIE);',
'proc_logs(''after :P260_QUERY_COLL '' || :P260_QUERY_COLL);',
'proc_logs(''after :P260_IP_TAB '' || :P260_IP_TAB);*/',
'',
'',
'end;'))
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(19716247876749046)
,p_process_sequence=>40
,p_process_point=>'AFTER_HEADER'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'HFA Managed Agreements MSG'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'l_count NUMBER;',
'BEGIN',
'',
'SELECT COUNT(1)',
'    INTO l_count ',
'    FROM db2_publisher_action_stage_v db2pasv',
'    WHERE db2pasv.pub_acct_no = :P260_HFA_NUMBER;',
'    ',
'    IF l_count = 0 THEN',
'        BEGIN ',
'            SELECT ''Refer to Hierarchy Agreements TAB for details''  ',
'            INTO :P260_HFA_MAN_AGR_MSG',
'            FROM aff_hierarchy',
'            WHERE child_party_id = :P260_ip_id',
'            AND child_party_id <> parent_party_id',
'            AND end_date IS NULL ',
'            AND nowed_aff_hier_id IS NULL',
'            AND hier_level = 0',
'            AND rownum = 1;',
'            ',
'        EXCEPTION WHEN OTHERS THEN',
'            :P260_HFA_MAN_AGR_MSG := NULL;',
'',
'        END;',
'',
'    END IF;',
'',
'END;'))
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(34043118790522824)
,p_process_sequence=>50
,p_process_point=>'AFTER_HEADER'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'Populate_party_hold'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'SELECT 	PAY_HOLD_REASON ',
'INTO    :P260_PARTY_HOLD',
'FROM  	AFF_INTERESTED_PARTIES ',
'WHERE 	IP_ID= :P260_IP_ID;'))
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(63524898633030735)
,p_process_sequence=>10
,p_process_point=>'AFTER_SUBMIT'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'Reset Sorting'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'BEGIN',
'      APEX_UTIL.REMOVE_SORT_PREFERENCES(:APP_USER);',
'END;'))
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
,p_process_when_button_id=>wwv_flow_api.id(6381720925453592)
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(63523691821030734)
,p_process_sequence=>20
,p_process_point=>'AFTER_SUBMIT'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'Reset_hidden_items'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'begin',
':p260_Inactive_flag := :p260_Inactive_address;',
'end;'))
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(63524006112030734)
,p_process_sequence=>30
,p_process_point=>'AFTER_SUBMIT'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'Reset_Society_Filter'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'begin',
':P260_Society_Filter := :P260_Society  ;',
'end;'))
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(2448509563167133)
,p_process_sequence=>40
,p_process_point=>'AFTER_SUBMIT'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'onClick Delete from Hierarchy'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'',
'BEGIN',
'  ',
'  FOR i in 1..APEX_APPLICATION.G_F01.COUNT ',
'  LOOP',
'  ',
'    DELETE FROM aff_hierarchy',
'    WHERE  aff_hier_id = APEX_APPLICATION.G_F01(i);',
'  ',
'  END LOOP;',
'  ',
'END;'))
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
,p_process_when_type=>'NEVER'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(60167034844808437)
,p_process_sequence=>50
,p_process_point=>'AFTER_SUBMIT'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'Delete_Unlink_Contact_Point'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'Delete from AFF_IP_CONTACT_POINTS	',
'where ip_id=  :P260_ip_id',
'and ip_contact_id is NULL',
'and IP_CONTACT_POINT_ID = :P260_CONTACT_PNT_ID;',
''))
,p_process_error_message=>'ERROR:PROCESS:Remove Contact Point:#SQLERRM#'
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
,p_process_when=>'DELETE_ONE'
,p_process_when_type=>'REQUEST_EQUALS_CONDITION'
,p_process_success_message=>'Unlinked Contact Point removed successfully.'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(27137138330379635)
,p_process_sequence=>60
,p_process_point=>'AFTER_SUBMIT'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'Send EAPP'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'begin',
'',
'      update aff_interested_parties',
'      set password = ''Y'' ',
'       where ip_id = :P260_ip_id;',
'end;'))
,p_process_error_message=>'Send EAPP flag update is failed.'
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
,p_process_when_button_id=>wwv_flow_api.id(27136663064364108)
,p_process_success_message=>'Send EAPP flag is updated successfully.'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(28661290655759542)
,p_process_sequence=>70
,p_process_point=>'AFTER_SUBMIT'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'Link Contact Points'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'BEGIN',
'',
'',
'    FOR i in 1..APEX_APPLICATION.G_F01.COUNT LOOP',
'',
'        UPDATE AFF_IP_CONTACT_POINTS SET ip_contact_id = APEX_APPLICATION.G_F01(i)',
'        WHERE ip_contact_point_id = APEX_APPLICATION.G_F02(i);',
'        ',
'        sf_outbound_api.send_contact_details(:p260_ip_id, APEX_APPLICATION.G_F01(i));',
'',
'    END LOOP;',
'    ',
'        sf_outbound_api.send_account_details(:p260_ip_id);--SPI-3847',
'',
'END;',
''))
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
,p_process_when_button_id=>wwv_flow_api.id(28661121377759541)
,p_process_success_message=>'Contact points linked successfully.'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(51304300941501105)
,p_process_sequence=>80
,p_process_point=>'AFTER_SUBMIT'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'COPY_TO_CTSTST'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
' ',
' v_instance  VARCHAR2(10);',
'',
'BEGIN',
'    --Getting environment name to make this functioanlity conditional.',
'    SELECT instance_name INTO v_instance',
'     FROM V$INSTANCE;',
'',
'    --Do when affiliate is Licensee and application is connected to CTSPRD.',
'    --On Production, the variable v_instance must be checking as v_instance=''CTSPRD'' and on DEV this should check as v_instance=''CTSDEV''.',
'    IF :P260_IP_TYPE = 2 AND v_instance IN(''CTSDEV'',''CTSPRD'')',
'    THEN',
'        copy_licensee_to_ctstst(:P260_HFA_NUMBER,:P260_IP_ID);',
'    END IF; ',
'    --Closing of "Do when affiliate is Licensee and application is connected to CTSPRD."',
'',
'END;'))
,p_process_error_message=>'#SQLERRM#'
,p_error_display_location=>'INLINE_IN_NOTIFICATION'
,p_process_when_button_id=>wwv_flow_api.id(51304293076501104)
,p_process_success_message=>'Licensee copied to CTSTST successfully.'
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(63524490468030734)
,p_process_sequence=>10
,p_process_point=>'BEFORE_HEADER'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'Populate Data'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'DECLARE',
'    l_ip_id            NUMBER := 0;',
'    l_name             VARCHAR2 (100) := NULL;',
'    l_email            VARCHAR2 (500);',
'    l_derived_status   VARCHAR2 (100);',
'BEGIN',
'    /*Block added by Saurav on 16-FEB-2018*/',
'    BEGIN',
'        SELECT NVL2 (reaff.aff_category_hfa,',
'                     reaff.aff_category || ''/'' || reaff.aff_category_hfa,',
'                     reaff.aff_category)',
'          INTO l_derived_status',
'          FROM rep_affiliates reaff',
'         WHERE ip_id = :P260_IP_ID;',
'    EXCEPTION',
'        WHEN NO_DATA_FOUND',
'        THEN',
'            NULL;',
'    END;',
'',
'    SELECT DECODE (ipi_type,',
'                   ''L'', ''Legal Entity/Publisher'',',
'                   ''N'', ''Natural Person/Writer'')',
'               aff_IPI_type,',
'           full_name              Primary_Name,',
'           IP_Number,',
'           IP_TYPE_ID,',
'           HFA_NUMBER,',
'           Sex,',
'           IPI_Base_Number        IPI_Base_Number,',
'           Name_Number            IPI_NAME_NUMBER,',
'           Foundation_birth_date  Birth_Date,',
'           dissolution_death_date Death_date,',
'           --tax_id,',
'           l_derived_status       Derived_Status,',
'           affiliate_rep          Affiliate_Rep,',
'            /* Vikas Yadav 20182604 (JIRA SPI - 488)',
'           NVL ( (SELECT ''Y''',
'                    FROM aff_membership_agreements ma, aff_right_types rt',
'                   WHERE     ip_id = ip.ip_id',
'                         AND society_id = 72',
'                         AND RT.RIGHT_TYPE_ID = ma.right_type_id',
'                         AND valid_to_date > SYSDATE',
'                         AND ma.right_type_id IN (25,',
'                                                  26,',
'                                                  52,',
'                                                  53,',
'                                                  54)',
'                         AND ROWNUM < 2),',
'                ''N'')',
'               Perf_right_flag,',
'           NVL (',
'               (SELECT ''Y''',
'                  FROM aff_membership_agreements ma, aff_right_types rt',
'                 WHERE     ip_id = ip.ip_id',
'                       AND society_id = 72',
'                       AND RT.RIGHT_TYPE_ID = ma.right_type_id',
'                       AND valid_to_date > SYSDATE',
'                       AND ma.right_type_id IN (51)',
'                       AND ROWNUM < 2),',
'               ''N'')',
'               Sync_right_flag,',
'           NVL (',
'               (SELECT ''Y''',
'                  FROM aff_membership_agreements ma, aff_right_types rt',
'                 WHERE     ip_id = ip.ip_id',
'                       AND society_id = 72',
'                       AND RT.RIGHT_TYPE_ID = ma.right_type_id',
'                       AND valid_to_date > SYSDATE',
'                       AND ma.right_type_id IN (21, 22)',
'                       AND ROWNUM < 2),',
'               ''N'')',
'               Mech_right_flag,',
'           ''N''                    Grand_right_flag*/',
'           --aff_ip.Get_Search_Tax_ID(ip.ap_supplier_id) search_tax_id,',
'           --aff_ip.Get_Display_Tax_ID(ip.ap_supplier_id) display_tax_id,',
'            (SELECT tisan',
'               FROM aff_territories',
'              WHERE     territory_id = ip.RESIDENCY_TERRITORY_ID',
'                    AND tisan_valid_to_date > SYSDATE + 1)',
'               aff_residence_terr,',
'           ip.FOREIGN_RESIDENCE,',
'           (SELECT tisan',
'              FROM aff_territories',
'             WHERE     territory_id = ip.CITIZENSHIP_TERRITORY_ID',
'                   AND tisan_valid_to_date > SYSDATE + 1)',
'               aff_citizen_terr,',
'           (SELECT description',
'              FROM pay_hold_types',
'             WHERE hold_type_id = ip.pay_hold_id)',
'               pay_hold_id,',
'           ip.pay_hold_reason,',
'           ip.CURRENT_PAY_FREQ_CODE,',
'           ip.ROYALTIES_THRESHOLD,',
'           (SELECT code || ''	-	'' || language preferred_lang',
'              FROM AFF_LANGUAGE_CODES',
'             WHERE language_id = ip.PREFERRED_LANGUAGE_ID)',
'               preferred_lang,',
'           ip.DART_ROYALTIES_COLLECTED,',
'           ip.PAPER_STATEMENTS_ONLY,',
'           /*TO_CHAR (',
'               xxsec_hz_affiliate_util.supplier_balance@apps (',
'                   ip.ap_supplier_id),',
'               ''fm9999999.90'')',
'               ap_open_bal,*/',
'           ip.orig_effective_date,',
'           NVL (ip.prospect_flag, ''N''),',
'           (SELECT aip.ip_number || '' - '' || ain.full_name',
'              FROM aff_interested_parties aip, aff_ip_names ain',
'             WHERE     aip.ip_id=ain.IP_id and aip.ip_number = ip.parent_ip_number',
'                   AND aip.status_code_id = ip.status_code_id',
'                   AND ain.name_type_id = 6',
'                   AND ROWNUM < 2)',
'               parent_ip,',
'               --added for status code MCORDTS',
'           DECODE (ip.ipi_registered_flag,  ''Y'', ''Yes'',  ''N'', ''No'',  ''No''),',
'           (SELECT  description',
'                FROM    aff_lookups',
'                WHERE   lookup_type = ''MC_ACCOUNT_STATUS''',
'                    AND   lookup_code = ip.status_code_id',
'                    AND   enabled_flag = ''Y''',
'           ) status,',
'           ip.W8_signature_date,',
'           ip.W9_received_flag,',
'           ip.sfdc_account_id',
'      INTO :P260_AFF_IPI_TYPE,',
'           :P260_PRIMARY_NAME,',
'           :P260_IP_NUMBER,',
'           :P260_IP_TYPE,',
'           :P260_HFA_NUMBER,',
'           :P260_SEX,',
'           :P260_IPI_BASE_NUMBER,',
'           :P260_IPI_NAME_NUMBER,',
'           :P260_BIRTH_DATE,',
'           :P260_DEATH_DATE,',
'           --:P260_TAX_ID,',
'           :P260_DERIVED_STATUS,',
'           :P260_AFFILIATE_REP,',
'--           :P260_PERF_RIGHT_FLAG,',
'--           :P260_SYNC_RIGHT_FLAG,',
'--           :P260_MECH_RIGHT_FLAG,',
'--           :P260_GRAND_RIGHT_FLAG,',
'           --:P260_TAX_ID,',
'           --:P260_DISPLAY_TAX_ID,',
'           :P260_aff_residence_terr,',
'           :P260_foreign_residence,',
'           :P260_aff_citizen_terr,',
'           :P260_PAY_HOLD_ID,',
'           :P260_PAY_HOLD_REASON,',
'           :P260_PAYMENT_FREQ,',
'           :P260_ROY_TH_AMOUNT,',
'           :P260_PREFERRED_LANG,',
'           :P260_DART_ROYALTIES,',
'           :P260_PAPER_STMT_ONLY,',
'           --:P260_AP_OPEN_BALANCE,',
'           :P260_ORIG_EFF_DATE,',
'           :P260_prospect_flag,',
'           :P260_parent_ip,',
'           :P260_IPI_REGISTERED,',
'           :P260_STATUS,',
'           :P260_W8_SIGNATURE_DATE,',
'           :P260_W9_RECEIVED_FLAG,',
'           :P260_SFDC_ACCOUNT_ID',
'      FROM aff_interested_parties ip, aff_ip_names ain, aff_name_types nt',
'     WHERE     ain.ip_id = ip.ip_id',
'           AND nt.name_Type_id = ain.name_type_id',
'           AND NVL (nt.name_type, ''PA'') = ''PA''',
'           AND ip.ip_id = :P260_IP_ID;',
'    ',
'    BEGIN',
'        SELECT DECODE (',
'                   NVL (renew_allowed_flag, ''N''),',
'                   ''Y'',   ADD_MONTHS (',
'                              TRUNC (',
'                                  ADD_MONTHS (valid_to_date,',
'                                              renewal_term_months),',
'                                  ''q''),',
'                              3)',
'                        - 1,',
'                   valid_to_date)',
'          INTO :P260_RENEWAL_DATE',
'          FROM AFF_MEMBERSHIP_AGREEMENTS a',
'         WHERE     ip_id = :P260_IP_ID',
'               AND valid_to_date > SYSDATE',
'               AND society_id = 72',
'               AND renewal_term_months IS NOT NULL',
'               AND ROWNUM < 2;',
'    EXCEPTION',
'        WHEN OTHERS',
'        THEN',
'            :P260_RENEWAL_DATE := NULL;',
'    END;',
'',
'    BEGIN',
'        SELECT valid_to_date',
'          INTO :P260_CONTRACT_END_DATE',
'          FROM AFF_MEMBERSHIP_AGREEMENTS',
'         WHERE ip_id = :P260_IP_ID AND society_id = 72 AND ROWNUM = 1;',
'    EXCEPTION',
'        WHEN OTHERS',
'        THEN',
'            :P260_CONTRACT_END_DATE := NULL;',
'    END;',
'',
'    BEGIN',
'        IF :P260_AFF_IPI_TYPE = ''N''',
'        THEN',
'            BEGIN',
'                SELECT ip_id',
'                  INTO l_ip_id',
'                  FROM aff_interested_parties',
'                 WHERE     group_number = :P260_Group_number',
'                       AND ipi_type = ''L''',
'                       AND ROWNUM = 1;',
'            EXCEPTION',
'                WHEN OTHERS',
'                THEN',
'                    l_ip_id := 0;',
'            END;',
'        ELSIF :P260_AFF_IPI_TYPE = ''L''',
'        THEN',
'            BEGIN',
'                SELECT ip_id',
'                  INTO l_ip_id',
'                  FROM aff_interested_parties',
'                 WHERE     group_number = :P260_Group_number',
'                       AND ipi_type = ''N''',
'                       AND ROWNUM = 1;',
'            EXCEPTION',
'                WHEN OTHERS',
'                THEN',
'                    l_ip_id := 0;',
'            END;',
'        END IF;',
'',
'        IF NVL (l_ip_id, 0) <> 0',
'        THEN',
'            BEGIN',
'                SELECT name',
'                  INTO l_name',
'                  FROM aff_ip_names',
'                 WHERE ip_id = l_ip_id AND name_type_id = 6;',
'',
'                :P260_IPI_NAME := l_name;',
'            EXCEPTION',
'                WHEN OTHERS',
'                THEN',
'                    l_name := NULL;',
'            END;',
'        END IF;',
'    END;',
'',
'    /*    FOR email_id in (SELECT email_address',
'                        FROM aff_ip_contact_points',
'                        WHERE ip_id = :P260_IP_ID',
'                        AND UPPER(status) = ''ACTIVE''',
'                        AND UPPER(display_flag) = ''Y''',
'                        AND email_address IS NOT NULL)',
'        LOOP',
'            l_email := l_email || '','' || email_id.email_address;',
'            l_email := LTRIM(l_email, '','');',
'        END LOOP; */',
'',
'    BEGIN',
'        --SPI-3563',
'        select email_address into :P260_EMAIL',
'        from ',
'        (',
'        select email_address ',
'        from aff_ip_contact_points aicp, aff_ip_contact_relationship air',
'        where aicp.ip_contact_id = air.ip_contact_id',
'        and cont_point_type_id = 2',
'        and aicp.status = ''ACTIVE''',
'        and air.status = ''A''',
'        and air.ip_id = :P260_IP_ID',
'        and email_address is not null',
'        and aicp.cont_point_type_id = 2',
'        and air.cont_type_id = 5',
'        order by aicp.creation_date desc',
'        )--SPI-3865',
'        where rownum=1;',
'    EXCEPTION',
'        WHEN NO_DATA_FOUND',
'        THEN',
'            NULL;',
'    END;',
'',
'',
'    BEGIN',
'        --SPI-3563',
'        select url ',
'        into :P260_WEB',
'        from aff_ip_contact_points aicp, aff_ip_contact_relationship air',
'        where aicp.ip_contact_id = air.ip_contact_id',
'        and cont_point_type_id = 2',
'        and aicp.status = ''ACTIVE''',
'        and air.status = ''A''',
'        and air.ip_id = :P260_IP_ID',
'        and url is not null',
'        and aicp.cont_point_type_id = 3',
'        and air.cont_type_id = 5',
'        and rownum = 1',
'        order by aicp.creation_date;        ',
'    EXCEPTION',
'        WHEN NO_DATA_FOUND',
'        THEN',
'            NULL;',
'    END;',
'',
' --Getting environment name to make this functioanlity conditional.',
'    SELECT instance_name INTO :p260_instance_name',
'     FROM V$INSTANCE;',
'',
'END;',
'',
'',
''))
,p_process_when=>'P260_IP_ID'
,p_process_when_type=>'ITEM_IS_NOT_NULL'
);
end;
/
begin
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(63525648503030736)
,p_process_sequence=>40
,p_process_point=>'BEFORE_HEADER'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'Populate 	Party Rank in "IP Header Display" Section'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'BEGIN',
'  SELECT EARN_RANK',
'  INTO :P260_AFF_EARNINGS_RANK',
'  FROM AFF_EARNINGS_RANK',
'  WHERE AFFILIATE_NUMBER=:P260_AFFILIATE_NO;',
'EXCEPTION',
'WHEN no_data_found THEN',
'  :P260_AFF_EARNINGS_RANK:=NULL;',
'END;'))
);
wwv_flow_api.create_page_process(
 p_id=>wwv_flow_api.id(33245068021442645)
,p_process_sequence=>10
,p_process_point=>'ON_DEMAND'
,p_process_type=>'NATIVE_PLSQL'
,p_process_name=>'DYNAMIC_URL_PROCESSING'
,p_process_sql_clob=>wwv_flow_string.join(wwv_flow_t_varchar2(
'declare',
'',
'	v_return_url varchar2(500) := null;',
'	v_url_text varchar2(500) := null;',
'	v_param_val number;',
'',
'begin',
'',
'	--v_param_val := APEX_APPLICATION.G_X01;',
'	v_url_text := ''f?p=''||:APP_ID||'':63:''||:APP_SESSION||''::::P63_COLLECTOR_ID,P63_RIGHT_TYPE,P63_IP_NUMBER:''||:P260_IP_ID||'',1,''||:P260_IP_NUMBER;',
'    v_return_url := apex_util.prepare_url(v_url_text);',
'',
'    HTP.PRN(v_return_url);',
'end;'))
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
