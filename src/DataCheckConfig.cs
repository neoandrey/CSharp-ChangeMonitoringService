
using  System;
using  System.Collections;
using  System.Collections.Generic;
using  System.Text;

namespace MonitoringUtilities
{

    public class DataCheckConfig{

        public string       to_address  { set; get;}
        public string       from_address { set; get;}
        public string       bcc_address  { set; get;}
        public string       cc_address  { set; get;}
        public string       smtp_server  { set; get;}
        public int          smtp_port   { set; get;}
        public string       sender     { set; get;}
        public string       sender_password  { set; get;}
        public bool         is_ssl_enabled  { set; get;}
        public int          threads  { set; get;}
        public int          batch_size  { set; get;}
        public ArrayList    tables_to_monitor  { set; get;}
        public ArrayList    tables_column_map  { set; get;}
        public ArrayList    tables_ref_column_map  { set; get;}
        public string       destination_server  { set; get;}
        public string       destination_database  { set; get;}
        public string       destination_port  { set; get;}
        public string       destination_server_type { set; get;}
        public string       source_database  { set; get;}
        public string       source_port    { set; get;}
        public string       source_server  { set; get;}
        public string       source_server_type { set; get;}
        public ArrayList    change_monitoring_modes { set; get;}
        public bool         send_email_notification { set; get;}
        public bool         send_notification { set; get;}
        public string       log_file { set; get;}
        public char         column_value_separator { set; get;}
        public char         field_separator { set; get;}
        public char         record_separator {set;get;}
        public ArrayList    sql_destination_table_map{set;get;}
        public ArrayList    mongo_destination_collection_map{set;get;}
        public ArrayList    mysql_destination_table_map{set;get;}
        public int          mongo_connection_port{set; get;}
        public string       alternate_row_colour{set; get;}
        public string       email_font_family{set; get;}
        public string       email_font_size{set; get;}
        public string       color{set; get;}
        public string       border_color{set; get;}

        
    }
}