*&---------------------------------------------------------------------*
*& Report  Z_UTIL_NW_EXPORT_DATA_TO_CSV
*&
*&---------------------------------------------------------------------*
*&
*&
*&---------------------------------------------------------------------*

report z_util_nw_export_data_to_csv.

*#  This program can be used to read table data and export it as CSV
*   files. The data can either be read using Open SQL or via RFC using
*   a custom function (if available).
*#  The parameters for the table read and file export need to be
*   defined in the setup section. It is possible to read the data in
*   packages by providing multiple where claused. For example you could
*   read and export by Fiscal Year.
*#  To use packages you need to define one or more SQL WHERE clauses
*   and a corresponding name, for example if you wanted to package by
*   Fiscal Years for 2019-2020 you would create an where clause as
*   follows `2019=FISCYEAR eq '2019';2020=FISCYEAR eq '2020';`.

types:
  begin of ty_wa_param,
    p_rdest                        type rfcdest,
    p_tabnm                        type tabname,
    p_where                        type string,
    p_psize                        type i,
    p_packs                        type i,
  end of ty_wa_param.

types:
  begin of ty_wa_sql,
    select                         type string,
    from                           type string,
    where                          type string,
    orderby                        type string,
  end of ty_wa_sql.

types:
  begin of ty_wa_file,
    dirnm                          type string,
    sysnm                          type c length 6,
    prfix                          type c length 20,
    prtnm                          type c length 6,
    numbr                          type c length 3,
  end of ty_wa_file.

types
  ty_v_csv                         type c length 5000.

data:
  lwa_param                        type ty_wa_param,
  lwa_sql                          type ty_wa_sql,
  lwa_file                         type ty_wa_file,
  lv_where                         type	string,
  lt_dv_where                      type stringtab,
  lwa_dd03l                        type dd03l,
  lt_dd03l                         type table of dd03l,
  lv_xml                           type string,
  lo_csv_converter                 type ref to cl_rsda_csv_converter,
  lv_csv_header                    type ty_v_csv,
  lv_csv_line                      type ty_v_csv,
  lt_csv                           type table of ty_v_csv.

data:
  lv_cursor                        type cursor,
  lv_index                         type i,
  lv_dset                          type string.

data:
  lo_typedescr                     type ref to cl_abap_typedescr,
  lo_structdescr                   type ref to cl_abap_structdescr,
  lwa_component                    type abap_componentdescr,
  lt_component                     type abap_component_tab.

data:
  lr_data                          type ref to data,
  lr_wa_data                       type ref to data,
  lr_t_data                        type ref to data.

field-symbols:
  <lwa_data>                       type any,
  <lt_data>                        type standard table.

data:
  lo_root                          type ref to cx_root.

*#  Setup. Define control parameters.

lwa_param-p_rdest = ''.
lwa_param-p_psize = 50000.
lwa_param-p_packs = 100.

lwa_param-p_tabnm = '/BI0/PMATERIAL'.

"Define one or more SQL WHERE clause and the name for the file.
lwa_param-p_where = 'ALL='.

"Example of multiple packages below.
*lwa_param-p_where =
*  |2018=AEDAT >= '20180101' and AEDAT <= '20181231';| &&
*  |2019=AEDAT >= '20190101' and AEDAT <= '20191231';| &&
*  |2020=AEDAT >= '20200101' and AEDAT <= '20201231';|.

lwa_file-dirnm = |/usr/sap/trans/Temp/DEV/DRA/|.
lwa_file-sysnm = |DBI|.
lwa_file-prfix = |{ lwa_param-p_tabnm }|.

replace all occurrences of '/' in lwa_file-prfix with '_'.

*#  Step 1. Get the DDIC definition for the source table.

clear lwa_sql.

lwa_sql-select = |FIELDNAME POSITION INTTYPE INTLEN LENG DECIMALS|.
lwa_sql-from   = |DD03L|.
lwa_sql-where  =
  |TABNAME = '{ lwa_param-p_tabnm }' and | &&
  |AS4LOCAL = 'A' and | &&
  |( COMPTYPE = 'E' OR COMPTYPE = '' )|.
lwa_sql-orderby = 'POSITION'.

if lwa_param-p_rdest is not initial.

  "Initialise data retrieval.
  call function 'ZXUN_BWU_IXML_INIT'
    destination lwa_param-p_rdest
    exporting
      iv_select   = lwa_sql-select
      iv_from     = lwa_sql-from
      iv_where    = lwa_sql-where
      iv_orderby  = lwa_sql-orderby
      iv_packsize = lwa_param-p_psize
      iv_packages = lwa_param-p_packs
    exceptions
      setup_error = 1
      others      = 2.

  do.

    clear lv_xml.

    "Retrieve data in XML format.
    call function 'ZXUN_BWU_IXML_DATA'
      destination lwa_param-p_rdest
      importing
        ev_xml       = lv_xml
      exceptions
        no_more_data = 1
        others       = 2.
    if sy-subrc <> 0.
      exit.
    endif.

    "Convert the XML string to an internal table.
    call transformation id
      source xml lv_xml
      result tab = lt_dd03l.

  enddo.

else.

  select (lwa_sql-select)
    into corresponding fields of table lt_dd03l
    from (lwa_sql-from)
    where (lwa_sql-where)
    order by (lwa_sql-orderby).

endif.

*#  Step 2. Create an internal table for the data.

"Define internal table using components.
if lt_dd03l is not initial.

  "Process each field in the table definition.
  loop at lt_dd03l into lwa_dd03l.

    "Only DDIC elements and ABAP built-in types are supported.
    if not ( lwa_dd03l-comptype = 'E' or lwa_dd03l-comptype = ' ' ).
      continue.
    endif.

    "The following built-in data types are not supported.
    if
      lwa_dd03l-inttype = ' ' or
      lwa_dd03l-inttype = 'a' or
      lwa_dd03l-inttype = 'e'.
      continue.
    endif.

    "Create a reference data object for each component.
    try.

        case lwa_dd03l-inttype.
          when 'g'.
            create data lr_data type string.
          when 'y'.
            create data lr_data type xstring.
          when 'I' or 'X' or 's'.
            create data lr_data type i.
          when 'D' or 'T' or 'F'.
            create data lr_data type (lwa_dd03l-inttype).
          when 'P'.
            create data lr_data type p
                                length lwa_dd03l-intlen
                                decimals lwa_dd03l-decimals.
          when others.
            create data lr_data type (lwa_dd03l-inttype)
                                length lwa_dd03l-intlen.
        endcase.

      catch cx_sy_create_data_error into lo_root.
        lo_root->get_longtext( ).
    endtry.

    "Create a run-time type description for the component.
    call method cl_abap_typedescr=>describe_by_data_ref
      exporting
        p_data_ref  = lr_data
      receiving
        p_descr_ref = lo_typedescr.

    "Add the component definition to the component list.
    lwa_component-name = lwa_dd03l-fieldname.
    lwa_component-type ?= lo_typedescr.
    append lwa_component to lt_component.

  endloop.

  "Create a structure definition based on the field definitions.
  try.

      call method cl_abap_structdescr=>create
        exporting
          p_components = lt_component
        receiving
          p_result     = lo_structdescr.

    catch cx_sy_struct_creation .
  endtry.

  "Create a work area based on the structure definition.
  create data lr_wa_data type handle lo_structdescr.
  assign lr_wa_data->* to <lwa_data>.

  "Create an internal table data object based on the work area.
  create data lr_t_data like table of <lwa_data>.
  assign lr_t_data->* to <lt_data>.

endif.

*#  Step 3. Read and export data.

clear lwa_sql.

"Create instance of class to convert from a work area to CSV.
call method cl_rsda_csv_converter=>create
  receiving
    r_r_conv = lo_csv_converter.

"Define projection clause.
loop at lt_dd03l into lwa_dd03l.
  lwa_sql-select = |{ lwa_sql-select }{ lwa_dd03l-fieldname } |.
endloop.

"Set source table.
lwa_sql-from  = |{ lwa_param-p_tabnm }|.

"Set the header line.
loop at lt_dd03l into lwa_dd03l.
  lv_csv_header = |{ lv_csv_header }{ lwa_dd03l-fieldname },|.
endloop.

"Set where clause.
split lwa_param-p_where at ';' into table lt_dv_where.

"Loop through partitions.
loop at lt_dv_where into lv_where.

  "Split string into Partition name and SQL where clause.
  split lv_where at '=' into lwa_file-prtnm lwa_sql-where.

  if lwa_param-p_rdest is not initial.

    "Initialise data retrieval.
    call function 'ZXUN_BWU_IXML_INIT'
      destination lwa_param-p_rdest
      exporting
        iv_select   = lwa_sql-select
        iv_from     = lwa_sql-from
        iv_where    = lwa_sql-where
        iv_orderby  = lwa_sql-orderby
        iv_packsize = lwa_param-p_psize
        iv_packages = lwa_param-p_packs
      exceptions
        setup_error = 1
        others      = 2.

  else.

    open cursor with hold lv_cursor for
      select (lwa_sql-select)
       from (lwa_sql-from)
       where (lwa_sql-where)
       order by (lwa_sql-orderby).

  endif.

  "Process in packages.
  do.

    "Store index.
    lv_index = sy-index.

    if lwa_param-p_rdest is not initial.

      clear lv_xml.

      "Retrieve data in XML format.
      call function 'ZXUN_BWU_IXML_DATA'
        destination lwa_param-p_rdest
        importing
          ev_xml       = lv_xml
        exceptions
          no_more_data = 1
          others       = 2.
      if sy-subrc <> 0.
        exit.
      endif.

      "Convert the XML string to an internal table.
      call transformation id
        source xml lv_xml
        result tab = <lt_data>.

    else.

      fetch next cursor lv_cursor
        into corresponding fields of table <lt_data>
        package size lwa_param-p_psize.
      if sy-subrc <> 0.
        close cursor lv_cursor.
        exit.
      endif.

    endif.

    clear lt_csv.

    "Add the header line.
    append lv_csv_header to lt_csv.

    "Add the data lines.
    loop at <lt_data> assigning <lwa_data>.

      "Conevrt work area to CSV.
      call method lo_csv_converter->structure_to_csv
        exporting
          i_s_data = <lwa_data>
        importing
          e_data   = lv_csv_line.

      append lv_csv_line to lt_csv.

    endloop.

    "Derive file number.
    lwa_file-numbr = |{ sy-index width = 3 align = right pad = '0' }|.

    "Derive file name.
    lv_dset =
      |{ lwa_file-dirnm }| &&
      |{ lwa_file-sysnm }_| &&
      |{ lwa_file-prfix }_| &&
      |{ lwa_file-prtnm }_| &&
      |{ lwa_file-numbr }| &&
      |.csv|.

    "Open file.
    open dataset lv_dset for output in text mode encoding default.
    if sy-subrc <> 0.
      exit.
    endif.

    "Process each line.
    loop at lt_csv into lv_csv_line.

      "Write to file.
      transfer lv_csv_line to lv_dset.

    endloop.

    "Close file.
    close dataset lv_dset.

  enddo. "Process in packages.

endloop. "Loop through partitions.
