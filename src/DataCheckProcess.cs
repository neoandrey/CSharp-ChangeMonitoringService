using System; 
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Microsoft.SqlServer.Server;
using System.Data.SqlTypes;
using System.Data.SqlClient;
using System.IO;
using System.IO.Compression;
using System.Diagnostics;
using System.Linq;
using System.Data.OleDb;
using System.Configuration;
using System.Threading;
using System.Runtime.InteropServices;
using System.Data;
using System.Globalization;
using System.Net.Mail;
using System.Data.SQLite;
using System.Data.DataSetExtensions;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using MongoDB.Driver.Linq;
using MySql.Data.MySqlClient;
using System.Threading.Tasks;

namespace MonitoringUtilities{

                public class  DataCheckProcess{

                        public SQLiteConnection liteConnect;
                        public string    getCurrentTableStructureFromServerScript   = "SELECT COLUMN_NAME,ORDINAL_POSITION = CONVERT(VARCHAR(20),ORDINAL_POSITION), COLUMN_DEFAULT = CONVERT(VARCHAR(255),  COLUMN_DEFAULT),IS_NULLABLE = CONVERT(VARCHAR(255), IS_NULLABLE ), DATA_TYPE,CHARACTER_MAXIMUM_LENGTH =  CONVERT(VARCHAR(255), CHARACTER_MAXIMUM_LENGTH), TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'TABLE_NAME_VAL'";
                        public string    getCurrentColumnValuesFromServerScript     = "SELECT reference_column_name monitored_column_list  FROM CURRENT_TABLE_NAME WITH (NOLOCK)"; 
                       
                        public string    createTableSnapShotScript                  = "CREATE TABLE current_table_name( reference_column_name monitored_column_list_val);";
                        public string    createCurrentTableHistoryScript            = "CREATE TABLE table_snapshot_history(ind_num INTEGER PRIMARY KEY, current_table_name TEXT, monitored_table_name TEXT, monitored_column_list TEXT, reference_column TEXT, time_of_change TEXT)";
                        public string    getLastSnapshotTableScript                 = "SELECT current_table_name FROM table_snapshot_history WHERE  ind_num = (SELECT MAX(ind_num) FROM  table_snapshot_history WHERE monitored_table_name = 'table_name_val')";
                        public string    addSnapshotTableHistoryDataScript          = "INSERT INTO table_snapshot_history(current_table_name,monitored_table_name,monitored_column_list,reference_column, time_of_change) VALUES ('current_table_name_val','monitored_table_name_val','monitored_column_list_val', 'reference_column_val', 'time_of_change_val')";
                        public string    addSnapShotDataToTableScript               = "INSERT INTO  current_table_name ( reference_column_name monitored_column_names_val) VALUES( reference_column_name_val monitored_column_values_string) ";
                        public string    getDataFromLastSnapShotScript              = "SELECT reference_column_name_val monitored_column_list_val FROM current_table_name_val";
    
                        public string    createSchemaTableScript                    = "CREATE TABLE  schema_table (ind_num INTEGER PRIMARY KEY, table_name TEXT,  column_name TEXT, ordinal_position TEXT,  column_default  TEXT, is_nullable  TEXT,  data_type TEXT, character_maximum_length TEXT)";
                        public string    createTableSchemaChangeHistoryScript       = "CREATE TABLE  schema_table_change_history(ind_num INTEGER PRIMARY KEY, table_name TEXT, change_time TEXT, change_type TEXT, affected_columns TEXT, change_description TEXT)";
                        public string    createColumnChangeHistoryScript            = "CREATE TABLE  column_table_change_history(ind_num INTEGER PRIMARY KEY, table_name TEXT ,reference_field_value TEXT,  column_value_map TEXT, change_time  TEXT)";
                        public string    getCurrentValuesForColumnScript            = "SELECT reference_field_value,current_value FROM current_column_value_store WHERE column_name = 'COLUMN_NAME_VAL' AND  TABLE_NAME= 'TABLE_NAME_VAL' ";
                        public string    checkIfColumnExistsInStore                 = "SELECT ind_num FROM   current_column_value_store WHERE table_name = 'TABLE_NAME_VAL' AND column_name = 'COLUMN_NAME_VAL' AND reference_field_value = 'reference_field_value_val'";
                        public string    checkIfRecordExistsInSchemaScript          = "SELECT ind_num FROM   schema_table  WHERE table_name = 'TABLE_NAME_VAL'  AND  column_name = 'COLUMN_NAME_VAL'";
                        public string    checkIfTableExistsInSchemaScript           = "SELECT ind_num FROM   schema_table  WHERE table_name = 'TABLE_NAME_VAL'";
                        public string    addTablechemaInformationScript             = "INSERT INTO schema_table (table_name,column_name,ordinal_position,column_default,is_nullable,data_type,character_maximum_length ) VALUES('table_name_val','column_name_val','ordinal_position_val','column_default_val','is_nullable_val','data_type_val','character_maximum_length_val')";
                        public string    addColumnChangeHistoryScript               = "INSERT INTO column_table_change_history( table_name ,reference_field_value,  column_value_map, change_time ) VALUES( 'table_name_val' ,'reference_field_value_val',  'column_value_map_val', 'change_time_val' )";
                        public string    addSchemaChangeHistoryScript               = "INSERT INTO schema_table_change_history( table_name,change_time,change_type,affected_columns,change_description) VALUES( 'table_name_val','change_time_val','change_type_val','affected_columns_val','change_description_val')";
                        public string    getTableSchemaFromStoreScript              = "SELECT TABLE_NAME, COLUMN_NAME,ORDINAL_POSITION,COLUMN_DEFAULT,IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM  schema_table WHERE  table_name = 'TABLE_NAME_VAL'";
                        public string    removeSchemaInformationScript              = "DELETE FROM  schema_table WHERE  table_name = 'TABLE_NAME_VAL'";
                        public string    getDifferentRecordsScript                  = "SELECT  * FROM CURRENT_TABLE_NAME_VAL WITH (NOLOCK)  WHERE  COLUMN_VALUE_LIST ;";
                        public string    sourceServerConnectionString               = "";
                        public string    destinationServerConnectionString          = "";
                        public string    liteConnectionString                       = "";
                        public string    mongoDestConString                         = "";
                        public string    mysqlDestConString                         = "";
                        public DataTable tableExistsInfo                            = new DataTable();  
                        public string    fectLastChangeScritpt                      = "SELECT * FROM column_table_change_history ORDER BY  ind_num DESC LIMIT  no_of_changes_val";
                        public string    checkIfTableExistsScript                   = "SELECT   '1' AS table_exists_flag  FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME =  'table_name_val'";
                        public string    getSQLTableCreateScript                    = "select   table_create = 'create table  dest_table_name_val (' union all select  table_create =   column_name +'  '+data_type+ case when   character_maximum_length is not null  then '(' +convert(varchar(30),character_maximum_length)+') ' else  '' end +case when is_nullable='no' then ' not null ' else  '' end+',' from information_schema.columns where table_name = 'source_table_name_val'";
                        public bool      hasDataChanged                             = false;
                        public int       numberOfChanges                            = 0;
                        public StringBuilder emailBody                              = new StringBuilder();
	                	public StringBuilder emailError                             = new StringBuilder();
                        public string todaysDate                                    = DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss tt");
                        public DataTable schemaDifferencesTab                       = new DataTable();
                        public DataTable columnDifferencesTab                       = new DataTable();

                        public DataCheckProcess(){

                               try{
                                DataUtilLibrary.log( "=================================================Starting New Data Change Monitoring Session at "+DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")+"=================================================");
                                initLiteConnectionString();
                                checkComparisonDatabase();
                                initSourceServerConnectionString();
                                if(DataUtilLibrary.destinationServerType.ToLower()=="mssql")         initDestinationServerConnectionString();
                                else if  (DataUtilLibrary.destinationServerType.ToLower()=="mongo")  initMongoDestinationString();
                                else if  (DataUtilLibrary.destinationServerType.ToLower()=="mysql")  initMySqlDestinationString();
                                
                                
                                ArrayList  monitoredColumnList  = new ArrayList();
                
                                DataUtilLibrary.writeToLog("Number of tables to monitor: "+DataUtilLibrary.tablesToMonitor.Count);
                                
                                if(DataUtilLibrary.tablesToMonitor.Count>0){

                                     DataTable tableExistsInfo     =  new   DataTable();
                                     DataTable columnExistsInfo    =  new   DataTable();
                   
                                    int runs = 0;
                                    foreach(string tableName in DataUtilLibrary.tablesToMonitor ){
                                        ++runs;
                                        DataUtilLibrary.writeToLog(runs.ToString()+" run for "+tableName);
                                        if (DataUtilLibrary.changeMonitoringModes.Contains("schema")){
                                           
                                              DataUtilLibrary.writeToLog("Checking for schema changes");

                                              tableExistsInfo        =  getDataFromStore(checkIfTableExistsInSchemaScript.Replace("TABLE_NAME_VAL",tableName));

						                      bool tableExists       =   false;
                                              if(tableExistsInfo.Rows.Count>0){

                                                     tableExists     = !string.IsNullOrEmpty(tableExistsInfo.Rows[0]["ind_num"].ToString());

                                              }
                                           
                                             if(tableExists){
                                              
                                               inspectTableSchema(tableName);
                                             

                                             }else{

                                                updateTableSchemaInformation(tableName," New Schema Information Added" ,new DataTable());

                                             }


                                            
                                        }
                                          
                                          if(DataUtilLibrary.changeMonitoringModes.Contains("update")){

                                             DataUtilLibrary.writeToLog("Checking for DML changes");
                                            DataUtilLibrary.writeToLog("Checking for DML changes");
                                            Dictionary<string, string> tabColMap = DataUtilLibrary.tableColMap;
                                            DataTable  lastSnapShotTableInfo     = new DataTable();
                                            DataTable  columnStoredValueInfo     = new DataTable();
                                            DataTable  columnServerValueInfo     = new DataTable();
                                            DataTable  serverTableInfo           = new DataTable();

                                          //  bool columnExists                    = false;
                                              string  refCol                       = "";
                                              string monitoredColumnStr            = "";
                                              string monitoredColsQueryStr         = "";
                                              string lastSnapshotTable             = "";
                                              ArrayList addedColumns                 = new ArrayList();
                                          //  string monitorTableName              = "";
                                              string affectedRows                  = "";
                                         //   List<string> keyList = new List<string>(tabColMap.Keys);
                                         //   foreach(string tabName in  keyList ){
                                              monitoredColsQueryStr         = "";
                                              monitoredColumnList           = new ArrayList();
                                              monitoredColumnStr            = "";
                                              monitoredColsQueryStr         = "";
                                              lastSnapshotTable             = "";
                                             
                                              refCol                        = DataUtilLibrary.tableRefColMap[tableName].ToString();
                                              monitoredColumnStr            = tabColMap[tableName].ToString();
                                              monitoredColumnStr            = monitoredColumnStr.EndsWith(",")?monitoredColumnStr.Substring(0,monitoredColumnStr.Length-1):monitoredColumnStr;
                                              monitoredColumnList.AddRange(monitoredColumnStr.Split(','));
                                              int counter = 0; 

                                              foreach(string col in monitoredColumnList){

                                                  ++counter;
                                        
                                                    if(counter>1){ 
                                                        monitoredColsQueryStr+=","+col.Trim()+" = CONVERT(VARCHAR(MAX), "+col.Trim()+")";  
                                                    }
                                                    else {

                                                        monitoredColsQueryStr+=col.Trim()+" = CONVERT(VARCHAR(MAX), "+col.Trim()+")";
                                                    }
                                   
                                                  
                                              }
                                              addedColumns= new ArrayList();
                                              
                                            if(monitoredColumnStr.ToLower().Split(',').Contains(refCol.Trim().ToLower())){
                                                          

                                              serverTableInfo                    = getDataFromServer(sourceServerConnectionString,getCurrentColumnValuesFromServerScript
                                                                                                    .Replace("reference_column_name","")
                                                                                                    .Replace("monitored_column_list",monitoredColsQueryStr)
                                                                                                    .Replace("CURRENT_TABLE_NAME",tableName)
                                                                                                    );
                                            }else{

                                                  serverTableInfo                    = getDataFromServer(sourceServerConnectionString,getCurrentColumnValuesFromServerScript
                                                                                                    .Replace("reference_column_name",refCol+"=CONVERT("+refCol+" ,VARCHAR(MAX)),")
                                                                                                    .Replace("monitored_column_list",monitoredColsQueryStr)
                                                                                                    .Replace("CURRENT_TABLE_NAME",tableName)
                                                                                                    );


                                            }

                                              lastSnapShotTableInfo              = getDataFromStore(getLastSnapshotTableScript
                                                                                                    .Replace("table_name_val", tableName)
                                                                                                    );
                                            if(lastSnapShotTableInfo.Rows.Count>0){
                                              lastSnapshotTable = lastSnapShotTableInfo.Rows[0]["current_table_name"].ToString();
                                             }

                                              if(string.IsNullOrEmpty(lastSnapshotTable)){
                  
                                                        updateSnapshotTableInformation( monitoredColumnStr,  tableName,    refCol, serverTableInfo );

                                              }else {

                                                   
                                                     DataTable latestTableSnapshotData          = new DataTable();
                                                     if(monitoredColumnStr.ToLower().Split(',').Contains(refCol.Trim().ToLower())){
                                                                latestTableSnapshotData          = getDataFromStore(getDataFromLastSnapShotScript.Replace("reference_column_name_val", "")
                                                                                                 .Replace("monitored_column_list_val",monitoredColumnStr)
                                                                                                 .Replace("current_table_name_val",lastSnapshotTable)
                                                                );


                                                                }else{
                                                                latestTableSnapshotData          = getDataFromStore(getDataFromLastSnapShotScript.Replace("reference_column_name_val", refCol+",")
                                                                                                                                                .Replace("monitored_column_list_val",monitoredColumnStr)
                                                                                                                                                .Replace("current_table_name_val",lastSnapshotTable)
                                                            
                                                                                                                            );
                                                            }
                                                         DataTable differencesTable             =  getDifferentRecords(serverTableInfo, latestTableSnapshotData);  
                                                         
                                                        StringBuilder queryBuilderStr  =   new StringBuilder();
                                                        string  diffRecords   = "";
                                                      
                                                         if (differencesTable.Rows.Count != 0)  {
                                                               hasDataChanged        = true;
                                                               numberOfChanges      += differencesTable.Rows.Count;
                                                                    foreach (DataRow row in differencesTable.Rows) {
                                                                            queryBuilderStr =  new StringBuilder();
                                                                            foreach(DataColumn diffCol in differencesTable.Columns){
                                                                                            queryBuilderStr.Append(" ").Append(diffCol.ToString()).Append("=\'").Append(row[diffCol.ToString().Trim()]).Append("\' ").Append(" AND");
                                                                                //  affectedRows+ =diffCol.ToString()+":"+row[diffCol.ToString().Trim()]+"; ";

                                                                            }
                                                                            diffRecords = queryBuilderStr.ToString().Substring(0, queryBuilderStr.ToString().Length-3 );
                                                                            DataTable diffTable   = getDataFromServer(sourceServerConnectionString, getDifferentRecordsScript.Replace("CURRENT_TABLE_NAME_VAL",tableName)
                                                                                                                                                                             .Replace("COLUMN_VALUE_LIST",diffRecords) );
                                                                             
                                                                             affectedRows = "";
                                                                             foreach (DataRow diffRow in diffTable.Rows) {
                                                                           
                                                                                            foreach(DataColumn diffCol2 in diffTable.Columns){
                                                                                                        affectedRows+=diffCol2.ToString()+DataUtilLibrary.columnSeparator+diffRow[diffCol2.ToString().Trim()]+DataUtilLibrary.fieldSeparator+"";
                                                                                            }
                                                                             }
                                                                             affectedRows   = affectedRows.Length >0? affectedRows.Substring(0,affectedRows.Length -1)+DataUtilLibrary.recordSeparator:affectedRows;
                                                                                    
                                                                             executeOnDataStore( addColumnChangeHistoryScript.Replace("table_name_val",tableName)
                                                                                                                            .Replace("reference_field_value_val", refCol)
                                                                                                                            .Replace("column_value_map_val",affectedRows)
                                                                                                                            .Replace("change_time_val",DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss" ))
                                                                                );

                                                                    }
                                                     
                                                         updateSnapshotTableInformation( monitoredColumnStr,  tableName,    refCol, serverTableInfo );
                                                       
                                                    }else{

                                                       
                                                        DataUtilLibrary.writeToLog("No changes recorded for "+tableName+" at "+ DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss" ));

                                                    }
                                                  

                                                  
                                              }




                                       //}

                                    }
                                    }
                                
                                            if(!hasDataChanged){
                                                 if(DataUtilLibrary.destinationServerType.ToLower()=="mssql") {
                                                      exportChangesToSQL(10);
                                                 }
                                                 else if (DataUtilLibrary.destinationServerType.ToLower()=="mongo") {
                                                 Task.WaitAll(Task.Run(() => exportChangesToMongo(10)));
                                                 }else if (DataUtilLibrary.destinationServerType.ToLower()=="mysql") {
                                                     exportChangesToMysql(10);
                                                 }
                                            
                                             if(DataUtilLibrary.sendNotificationMail){

                                                        sendNotification();
                     
                                                }
                                            }
                                }else{

                                    DataUtilLibrary.writeToLog("There are no tables to monitor. Please add tables with the \'tables_to_monitor\' property in the configuration file.");
                                
                                }
                          DataUtilLibrary.closeLogFile();
                         }catch(Exception e){
                        
                                    Console.WriteLine(e.Message);
                                    Console.WriteLine(e.StackTrace);
                                    DataUtilLibrary.writeToLog(e.Message);
                                    DataUtilLibrary.writeToLog(e.StackTrace);
                                    emailError.AppendLine("<div style=\"color:red\">" + e.Message); 
                                    emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace); 
                         }
                    }
                    

                public void exportChangesToSQL(int numOfChanges){
                      
                        DataTable changedRecordsTable         =  getDataFromStore(fectLastChangeScritpt.Replace("no_of_changes_val",numOfChanges.ToString()));    
                        string  tabName                       =  "";                                         
                        string  refVal                        =  "";
                        string  columnValueMap                =  "";
                        bool    isFirstRecord                 =  true;

                        foreach (DataRow changedRow           in changedRecordsTable.Rows) {
                
                        tabName                               =  changedRow["table_name"].ToString(); 
                        refVal                                =  changedRow["reference_field_value"].ToString();
                        columnValueMap                        =  changedRow["column_value_map"].ToString(); 


                        string[] records                      =  columnValueMap.Split(DataUtilLibrary.recordSeparator);
                        StringBuilder  changedRecordsBuilder  =  new StringBuilder();
                        DataTable changeTable                 =  new DataTable(DataUtilLibrary.sqlDestinationTableMap[tabName]); 

                        int i                                 =  1;
                        string dataType                       =  "";

                        string  record = records[0].ToString();
                            
                        if(!string.IsNullOrEmpty(record)){

                            string[] fieldDetails             =  record.Split(DataUtilLibrary.fieldSeparator);
                            DataRow changeRow                 =  changeTable.NewRow();  

                            foreach(string  field in  fieldDetails){

                                string[] fieldInfo     = field.Split(DataUtilLibrary.columnSeparator);
                                
                                if (fieldInfo[0].GetType() == typeof(int)){
                                    dataType    =  "int";
                                }else if (fieldInfo[0].GetType() == typeof(String)){
                                    dataType    =  "String";
                                }else if (fieldInfo[0].GetType() == typeof(Double)){
                                    dataType    =  "Double";
                                }
                                 if(isFirstRecord){
                                     
                                    if (dataType    ==   "int") columnDifferencesTab.Columns.Add(fieldInfo[0], typeof(int));  
                                   else if(dataType=="Double")  columnDifferencesTab.Columns.Add(fieldInfo[0], typeof(Double));
                                   else  columnDifferencesTab.Columns.Add(fieldInfo[0], typeof(String));
                                   isFirstRecord = false;

                                 }
                                if(i==1){

                                   if (dataType    ==   "int")       changeTable.Columns.Add(fieldInfo[0], typeof(int));  
                                   else if(dataType=="Double")  changeTable.Columns.Add(fieldInfo[0], typeof(Double));
                                   else  changeTable.Columns.Add(fieldInfo[0], typeof(String));
                                  
                                }   
                               if(fieldDetails.Length >0) changeRow[fieldInfo[0]]  = fieldInfo[1];

                            }
                                changeTable.Rows.Add(changeRow);  
                                columnDifferencesTab.Rows.Add(changedRow);
                            }
                            ++i;

                        if(!string.IsNullOrEmpty(DataUtilLibrary.sqlDestinationTableMap[tabName])){
                            
                                DataTable  targetTabExistsInfo       = getDataFromServer(destinationServerConnectionString, checkIfTableExistsScript.Replace("table_name_val",DataUtilLibrary.sqlDestinationTableMap[tabName]));
                                if(targetTabExistsInfo.Rows.Count    == 0  || targetTabExistsInfo.Rows[0]["table_exists_flag"].ToString()!="1"){
                                DataTable      tableCreateInfo       = getDataFromServer(sourceServerConnectionString ,getSQLTableCreateScript.Replace("dest_table_name_val",
                                                                                    DataUtilLibrary.sqlDestinationTableMap[tabName]).Replace("source_table_name_val",tabName)
                                                                   ); 

                                StringBuilder tableCreateScriptBldr                =  new StringBuilder();
                                string tableCreateScript                           =  "";

                                foreach(DataRow dRow in tableCreateInfo.Rows){

                                    tableCreateScriptBldr.Append(dRow["table_create"].ToString().Trim());
                                   

                                }
                                    tableCreateScript     =  tableCreateScriptBldr.ToString().Substring(0,  tableCreateScriptBldr.ToString().Length -1)+");";
                                    executeOnServer(destinationServerConnectionString, tableCreateScript);
                                }

                                try{
                            

                                     DataUtilLibrary.writeToLog("Running bulk insert");
                               
                                using (SqlConnection bulkCopyConnection =  new SqlConnection(destinationServerConnectionString)){
                                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(bulkCopyConnection,
                                    SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.KeepNulls,null)){ 
                                    bulkCopyConnection.Open();
                                    bulkCopy.BulkCopyTimeout = 0;
                                    bulkCopy.BatchSize = DataUtilLibrary.defaultBatchSize;
                                    bulkCopy.DestinationTableName =  DataUtilLibrary.sqlDestinationTableMap[tabName];
                                    bulkCopy.WriteToServer(changeTable);
                              

                                }
                                }
                              
                                }catch(Exception e){
                                    
                                    Console.WriteLine(e.Message);
                                    Console.WriteLine(e.StackTrace);
                                    DataUtilLibrary.writeToLog(e.Message);
                                    DataUtilLibrary.writeToLog(e.StackTrace);
                                    emailError.AppendLine("<div style=\"color:red\">" + e.Message); 
                                    emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);

                                }

                        }else{

                            DataUtilLibrary.writeToLog(DataUtilLibrary.sqlDestinationTableMap[tabName]+" has not been specified in the configuration");
                        }
                        }
                }


                public void inspectTableSchema(string tableName){


                        try{
                          
                           tableExistsInfo                 =  getDataFromStore(checkIfTableExistsInSchemaScript.Replace("TABLE_NAME_VAL",tableName));
						   bool tableExists                =  false;

                            if (tableExistsInfo.Rows.Count >  0 ) {

                                tableExists                =  !string.IsNullOrEmpty(tableExistsInfo.Rows[0]["ind_num"].ToString());
                            } 
                           DataTable storedSchemaTable     =  getDataFromStore(getTableSchemaFromStoreScript.Replace("TABLE_NAME_VAL",tableName));
                           DataTable serverSchemaTable     =  getDataFromServer(sourceServerConnectionString, getCurrentTableStructureFromServerScript.Replace("TABLE_NAME_VAL",tableName));
                           DataTable differencesTable      =  getDifferentRecords(storedSchemaTable, serverSchemaTable);  

                            if (differencesTable.Rows.Count != 0)  {
                                

                                    schemaDifferencesTab = differencesTable;
                                    updateTableSchemaInformation(tableName,"Row differences recorded",differencesTable);

                            }else{

                                Console.WriteLine("No schema changes recorded for "+tableName+" at "+ DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss" ));
                                DataUtilLibrary.writeToLog("No schema changes recorded for "+tableName+" at "+ DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss" ));

                            }
                                
                        }catch(Exception e){
                        
                            Console.WriteLine(e.Message);
                            Console.WriteLine(e.StackTrace);
                            DataUtilLibrary.writeToLog(e.Message);
                            DataUtilLibrary.writeToLog(e.StackTrace);
                            emailError.AppendLine("<div style=\"color:red\">" + e.Message); 
                            emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);
                         }
                }

                public void initSourceServerConnectionString(){

                    try {

                         sourceServerConnectionString   =  "Network Library=DBMSSOCN;Data Source=" + DataUtilLibrary.sourceConnectionProps.getSourceServer() + ","+DataUtilLibrary.sourcePort+";database=" + DataUtilLibrary.sourceConnectionProps.getSourceDatabase()+ ";User id=" + DataUtilLibrary.sourceConnectionProps.getSourceUser()+ ";Password=" + DataUtilLibrary.sourceConnectionProps.getSourcePassword() + ";Connection Timeout=0;Pooling=false;";     
                    
                    }catch (Exception  e){
                            Console.WriteLine("Source connection properties have not been initialized");
                            DataUtilLibrary.writeToLog("Source connection properties have not been initialized");
                            Console.WriteLine(e.Message);
                            Console.WriteLine(e.StackTrace);
                            DataUtilLibrary.writeToLog(e.Message);
                            DataUtilLibrary.writeToLog(e.StackTrace);
                            emailError.AppendLine("<div style=\"color:red\">Source connection properties have not been initialized. Error:  " + e.Message); 
                            emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);

                    }
                }
        
                public void initDestinationServerConnectionString(){
                      try {
                       destinationServerConnectionString   =  "Network Library=DBMSSOCN;Data Source=" +  DataUtilLibrary.destinationConnectionProps.getSourceServer() + ","+DataUtilLibrary.destinationPort+";database=" + DataUtilLibrary.destinationConnectionProps.getSourceDatabase()+ ";User id=" +  DataUtilLibrary.destinationConnectionProps.getSourceUser()+ ";Password=" + DataUtilLibrary.destinationConnectionProps.getSourcePassword() + ";Connection Timeout=0;Pooling=false;";     
                      }catch (Exception  e){
                            Console.WriteLine("Destination connection properties have not been initialized");
                            DataUtilLibrary.writeToLog("Destination connection properties have not been initialized");
                            Console.WriteLine(e.Message);
                            Console.WriteLine(e.StackTrace);
                            DataUtilLibrary.writeToLog(e.Message);
                            DataUtilLibrary.writeToLog(e.StackTrace);
                            emailError.AppendLine("<div style=\"color:red\">Destination connection properties have not been initialized. Error:  " + e.Message); 
                            emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);

                    }
                }
                
                public void  initLiteConnectionString(){

                        liteConnectionString =  "Data Source=.\\db\\comparison_database.sqlite;Version=3;";

                }
            
                 public void checkComparisonDatabase() {

                        if (!File.Exists(".\\db\\comparison_database.sqlite")){

                                    SQLiteConnection.CreateFile(".\\db\\comparison_database.sqlite");
                        }
                        if(!checkIfTableExists("schema_table")){

                            createTable(createSchemaTableScript);
                        }
                        if(!checkIfTableExists("schema_table_change_history")){

                            createTable(createTableSchemaChangeHistoryScript);
                        }
                            if(!checkIfTableExists("column_table_change_history")){

                            createTable(createColumnChangeHistoryScript);
                        }

                        if(!checkIfTableExists("table_snapshot_history")){

                            createTable(createCurrentTableHistoryScript);
                        }
                 }

                 public bool checkIfTableExists(string  tableName){ 
                        try{

                               using  (SQLiteConnection  liteConnect = new SQLiteConnection(liteConnectionString)){
                                    liteConnect.Open();
                                    string sql = "SELECT 1 FROM sqlite_master WHERE type='table' AND name='"+tableName+"';";
                                    SQLiteCommand command = new SQLiteCommand(sql, liteConnect);
                                    Object result = command.ExecuteScalar();
                                    command.Dispose();
                                    if(result.ToString() == "1"){

                                    return true;
                                }
                               }
                        } catch(Exception e){
                                Console.WriteLine(e.Message);
                                Console.WriteLine(e.StackTrace);
                                emailError.AppendLine("<div style=\"color:red\">E " + e.Message); 
                                emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);
                        }
                        return false;
                        }

                        public  bool createTable(String createScript){
                           
                        try{
                               using  ( SQLiteConnection  liteConnect = new SQLiteConnection(liteConnectionString)){
                                        liteConnect.Open();
                                        string sql =createScript;
                                        
                                        SQLiteCommand command = new SQLiteCommand(sql, liteConnect);
                                        if(command.ExecuteNonQuery()>=0) return true;
                                       // command.Dispose();

                               }
                         } catch(Exception e){
                                Console.WriteLine(e.Message);
                                Console.WriteLine(e.StackTrace);
                                emailError.AppendLine("<div style=\"color:red\">" + e.Message); 
                                emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);
                        }
                        return false;
                        }

                    public  void executeOnDataStore(string sqlScript){

                        try{
                            using  (SQLiteConnection  liteConnect = new SQLiteConnection(liteConnectionString)){
                                    
                                    DataUtilLibrary.writeToLog("Running: "+sqlScript);
                                    liteConnect.Open();
                                    SQLiteCommand command = new SQLiteCommand(sqlScript, liteConnect);
                                    command.CommandTimeout = -1;
                                    command.ExecuteNonQuery();

                                    DataUtilLibrary.writeToLog("Query complete");
                                    command.Dispose();
                                }
                                    
                                    
                        } catch(Exception e){
                        
                            Console.WriteLine(e.Message);
                            Console.WriteLine(e.StackTrace);
                            DataUtilLibrary.writeToLog(e.Message);
                            DataUtilLibrary.writeToLog(e.StackTrace);
                            emailError.AppendLine("<div style=\"color:red\">  " + e.Message); 
                            emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);
                            
                        }

                        
                    }
       
       	 public void  executeOnServer( string targetConnectionString, string script){
			 
					try{
			      		using (SqlConnection serverConnection =  new SqlConnection(targetConnectionString)){
						 SqlCommand cmd = new SqlCommand(script, serverConnection);
						 DataUtilLibrary.writeToLog("Executing script: "+script);
						 cmd.CommandTimeout =0;
						 serverConnection.Open();
					     cmd.ExecuteNonQuery();
				}
				 }catch(Exception e){
					 Console.WriteLine("Error while running script: " + e.Message);
				     Console.WriteLine(e.StackTrace);
				     DataUtilLibrary.writeToLog("Error while running script: " + e.Message);
                     DataUtilLibrary.writeToLog(e.StackTrace);
                     emailError.AppendLine("<div style=\"color:red\">Error while running script:  " + e.Message); 
                     emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);
				 }
		 }

    public void  updateTableSchemaInformation(string tableName, string description, DataTable schemeDiffTab){
          
           executeOnDataStore(removeSchemaInformationScript.Replace("TABLE_NAME_VAL", tableName));
           DataTable currentTableSchema    = getDataFromServer(sourceServerConnectionString, getCurrentTableStructureFromServerScript.Replace("TABLE_NAME_VAL", tableName));

           string affectedRows             =  "";
           ArrayList  diffColList          = new ArrayList();
    
           foreach (DataColumn coln in  schemeDiffTab.Columns){

                    diffColList.Add(coln.ToString());
           }
           foreach (DataRow row in schemeDiffTab.Rows) {
               
               foreach(string diffCol in diffColList){

                   affectedRows+=diffCol+DataUtilLibrary.columnSeparator+row[diffCol].ToString()+DataUtilLibrary.fieldSeparator;

               }
               affectedRows = affectedRows.Length>0? affectedRows.Substring(0,affectedRows.Length-1)+DataUtilLibrary.recordSeparator:affectedRows;

           }
           foreach (DataRow row in currentTableSchema.Rows) {

                   executeOnDataStore(addTablechemaInformationScript.Replace("table_name_val", row["table_name"].ToString())
                                                                    .Replace("column_name_val", row["column_name"].ToString())
                                                                    .Replace("ordinal_position_val", row["ordinal_position"].ToString())
                                                                    .Replace("column_default_val",row["column_default"].ToString())
                                                                    .Replace("is_nullable_val", row["is_nullable"].ToString())
                                                                    .Replace("data_type_val", row["data_type"].ToString())
                                                                    .Replace("character_maximum_length_val", row["character_maximum_length"].ToString())
                  
                   );

           }
//'table_name_val','change_time_val','change_type_val','affected_column_val','change_description_val'
             executeOnDataStore(addSchemaChangeHistoryScript.Replace("table_name_val", tableName)
                                                            .Replace("change_time_val", DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss" ))
                                                            .Replace("change_type_val", "ALTER/CREATE" )
                                                            .Replace("affected_columns_val", affectedRows)
                                                            .Replace("change_description_val",description)
                                                     
                  );
        
    }
                      
  public   System.Data.DataTable  getDataFromStore(string theScript){
  	         System.Data.DataTable  dt = new DataTable();
  
  	    try{
  		    using (SQLiteConnection  liteConnect = new SQLiteConnection(liteConnectionString)){
  		    	liteConnect.Open();
  			    SQLiteCommand cmd = new SQLiteCommand(theScript, liteConnect);
  			    DataUtilLibrary.writeToLog("Executing script: "+theScript);
 			    cmd.CommandTimeout =0;
  			    SQLiteDataReader  reader = cmd.ExecuteReader();
  			    dt.Load(reader);	
  	            cmd.Dispose();
  		  }
  	 } catch(Exception e){
			Console.WriteLine(e.Message);
			Console.WriteLine(e.StackTrace);
			DataUtilLibrary.writeToLog(e.Message);
			DataUtilLibrary.writeToLog(e.StackTrace);
            emailError.AppendLine("<div style=\"color:red\">" + e.Message); 
            emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);
  	  }
         return dt;
  }

  public   System.Data.DataTable  getDataFromServer(string connectionString, string theScript){
	         System.Data.DataTable  dt = new DataTable();

	    try{
		    using (SqlConnection serverConnection =  new SqlConnection(connectionString)){
                    serverConnection.Open();
                    SqlCommand cmd = new SqlCommand(theScript, serverConnection);
                    cmd.CommandTimeout =0;
                    DataUtilLibrary.writeToLog("Executing script: "+theScript);
			        SqlDataReader  reader = cmd.ExecuteReader();
			        dt.Load(reader);	
	                cmd.Dispose();
		  }
	 } catch(Exception e){

		  Console.WriteLine(e.Message);
		  Console.WriteLine(e.StackTrace);
		  DataUtilLibrary.writeToLog(e.Message);
		  DataUtilLibrary.writeToLog(e.StackTrace);
          emailError.AppendLine("<div style=\"color:red\">" + e.Message); 
          emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);

	  }
       return dt;
  }

    public void   updateSnapshotTableInformation(string monitoredColumnStr, string tableName,  string  refCol,DataTable serverTableInfo ){

                string monTabListStr            = monitoredColumnStr.Replace(","," TEXT ,")+" TEXT";
                string  monitoredTableName      = tableName+"_"+DateTime.Now.ToString("yyyyMMdd_hhmmss" );
                ArrayList  monitoredColumnList  = new ArrayList();
                monitoredColumnList .AddRange(monitoredColumnStr.Split(','));
               
                if(monitoredColumnStr.ToLower().Split(',').Contains(refCol.Trim().ToLower())){

                     executeOnDataStore(createTableSnapShotScript 
                                    .Replace("current_table_name",monitoredTableName)  
                                    .Replace("reference_column_name","")
                                    .Replace("monitored_column_list_val",monTabListStr)
                                    );
                }else{
                         executeOnDataStore(createTableSnapShotScript 
                                    .Replace("current_table_name",monitoredTableName)  
                                    .Replace("reference_column_name",refCol+" TEXT,")
                                    .Replace("monitored_column_list_val",monTabListStr)
                                    );
                }
               

                string queryString = "";
                string monitoredColumnNames ="";
                string monitoredColumnValues ="";

                foreach(DataRow record in serverTableInfo.Rows){

                        queryString            = addSnapShotDataToTableScript;
                        monitoredColumnNames   = "";
                        monitoredColumnValues  = "";

                        foreach(DataColumn columnName in serverTableInfo.Columns){
                                
                                monitoredColumnNames  += columnName.ToString().Trim()+",";
                                monitoredColumnValues += "'"+record[columnName.ToString().Trim()].ToString()+"',";

                        }
                        if(monitoredColumnNames.Length>0){

                            monitoredColumnNames = monitoredColumnNames.Substring(0,monitoredColumnNames.Length-1);

                        }

                        if(monitoredColumnValues.Length>0){

                            monitoredColumnValues = monitoredColumnValues.Substring(0,monitoredColumnValues.Length-1);

                        }
                        if(monitoredColumnStr.ToLower().Split(',').Contains(refCol.Trim().ToLower())){
                                                    executeOnDataStore(queryString.Replace("reference_column_name_val", "")
                                                                                  .Replace("reference_column_name","")
                                                                                .Replace("monitored_column_names_val",monitoredColumnNames)
                                                                                .Replace("monitored_column_values_string",monitoredColumnValues)
                                                                                .Replace("current_table_name",monitoredTableName)
                                                    
                                                    );
                        }else{
                                               executeOnDataStore(queryString.Replace("reference_column_name_val","'"+ record[refCol.Trim()]+"',")
                                                                                .Replace("reference_column_name",refCol)
                                                                                .Replace("monitored_column_names_val",monitoredColumnNames)
                                                                                .Replace("monitored_column_values_string",monitoredColumnValues)
                                                                                 .Replace("current_table_name",monitoredTableName)
                                                    
                                                    );
                            
                        }

                }
                
                    executeOnDataStore(addSnapshotTableHistoryDataScript.Replace("current_table_name_val", monitoredTableName)
                                                                        .Replace("monitored_table_name_val",tableName )
                                                                        .Replace("monitored_column_list_val",monitoredColumnStr)
                                                                        .Replace("reference_column_val",refCol)
                                                                        .Replace("time_of_change_val",DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss" ))
                                    ); 

    }
	 public void  executeScriptOnServer( string connectionString, string script){
			 
					try{
			      		using (SqlConnection serverConnection =  new SqlConnection(connectionString)){
						 SqlCommand cmd = new SqlCommand(script, serverConnection);
                         DataUtilLibrary.writeToLog("Executing script: "+script);

						 cmd.CommandTimeout =0;
						 serverConnection.Open();
					     cmd.ExecuteNonQuery();
				}
				 }catch(Exception e){

					 Console.WriteLine("Error while running script: " + e.Message);
				     Console.WriteLine(e.StackTrace);
					 DataUtilLibrary.writeToLog("Error while running script: " + e.Message);
                     DataUtilLibrary.writeToLog(e.StackTrace);
                     emailError.AppendLine("<div style=\"color:red\">Error while running script" + e.Message); 
                     emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);
					 
				 }
		 }

  
  public DataTable getDifferentRecords(DataTable FirstDataTable, DataTable SecondDataTable)   {   
     //Create Empty Table   
     DataTable ResultDataTable = new DataTable("ResultDataTable");   

     //use a Dataset to make use of a DataRelation object   
     using (DataSet ds = new DataSet())   
     {   
         //Add tables   
         ds.Tables.AddRange(new DataTable[] { FirstDataTable.Copy(), SecondDataTable.Copy() });   

         //Get Columns for DataRelation   
         DataColumn[] firstColumns = new DataColumn[ds.Tables[0].Columns.Count];   
         for (int i = 0; i < firstColumns.Length; i++)   
         {   
            firstColumns[i] = ds.Tables[0].Columns[i];   
         }   

         DataColumn[] secondColumns = new DataColumn[ds.Tables[1].Columns.Count];   
         for (int i = 0; i < secondColumns.Length; i++)   
         {   
             secondColumns[i] = ds.Tables[1].Columns[i];   
         }   

         //Create DataRelation   
         DataRelation r1 = new DataRelation(string.Empty, firstColumns, secondColumns, false);   
         ds.Relations.Add(r1);   

         DataRelation r2 = new DataRelation(string.Empty, secondColumns, firstColumns, false);   
         ds.Relations.Add(r2);   

         //Create columns for return table   
         for (int i = 0; i < FirstDataTable.Columns.Count; i++)   
         {   
             ResultDataTable.Columns.Add(FirstDataTable.Columns[i].ColumnName, FirstDataTable.Columns[i].DataType);   
         }   

         //If FirstDataTable Row not in SecondDataTable, Add to ResultDataTable.   
         ResultDataTable.BeginLoadData();   
         foreach (DataRow parentrow in ds.Tables[0].Rows)   
         {   
             DataRow[] childrows = parentrow.GetChildRows(r1);   
             if (childrows == null || childrows.Length == 0)   
                 ResultDataTable.LoadDataRow(parentrow.ItemArray, true);   
         }   

         //If SecondDataTable Row not in FirstDataTable, Add to ResultDataTable.   
         foreach (DataRow parentrow in ds.Tables[1].Rows)   
         {   
             DataRow[] childrows = parentrow.GetChildRows(r2);   
             if (childrows == null || childrows.Length == 0)   
                 ResultDataTable.LoadDataRow(parentrow.ItemArray, true);   
         }   
         ResultDataTable.EndLoadData();   
     }   

     return ResultDataTable;   
 }  

  public void initMongoDestinationString(){

            mongoDestConString = "mongodb://"+ DataUtilLibrary.destinationConnectionProps.getSourceUser()+":"+ DataUtilLibrary.destinationConnectionProps.getSourcePassword() + "@"+DataUtilLibrary.destinationConnectionProps.getSourceServer()+":"+DataUtilLibrary.mongoConnectionPort+"/admin";
    
  }
  
  public void initMySqlDestinationString(){

         mysqlDestConString = "SERVER=" + DataUtilLibrary.destinationConnectionProps.getSourceServer() + ";" + "DATABASE=" + DataUtilLibrary.destinationConnectionProps.getSourceDatabase() + ";" + "UID=" +  DataUtilLibrary.destinationConnectionProps.getSourceUser() + ";" + "PASSWORD=" + DataUtilLibrary.destinationConnectionProps.getSourcePassword() + ";";    
  }
 public async Task  exportChangesToMongo(int numOfChanges){
                      
                        DataTable changedRecordsTable             =  getDataFromStore(fectLastChangeScritpt.Replace("no_of_changes_val",numOfChanges.ToString()));    
                        string  tabName                           =  "";                                         
                        string  refVal                            =  "";
                        string  columnValueMap                    =  "";
                        int i                                     =  1;

	                    DataTable changeTable                     =  new DataTable(); 
                        
                        foreach (DataRow changedRow               in       changedRecordsTable.Rows) {
             
                
							tabName                               =  changedRow["table_name"].ToString(); 
							refVal                                =  changedRow["reference_field_value"].ToString();
							columnValueMap                        =  changedRow["column_value_map"].ToString(); 
							string[] records                      =  columnValueMap.Split(DataUtilLibrary.recordSeparator);
							StringBuilder  changedRecordsBuilder  =  new StringBuilder();
						
							
							string dataType                       =  "";
                            string  record 						  =  records[0].ToString();
                            
							if(!string.IsNullOrEmpty(record)){

								string[] fieldDetails             =  record.Split(DataUtilLibrary.fieldSeparator);
								DataRow changeRow                 =  changeTable.NewRow();  

								foreach(string  field in  fieldDetails){
 
									string[] fieldInfo     = field.Split(DataUtilLibrary.columnSeparator);
									
									if (fieldInfo[0].GetType() == typeof(int)){
										dataType    =  "int";
									}else if (fieldInfo[0].GetType() == typeof(String)){
										dataType    =  "String";
									}else if (fieldInfo[0].GetType() == typeof(Double)){
										dataType    =  "Double";
									}
                               
                                if(i==1){

                                   if (dataType    ==   "int")       changeTable.Columns.Add(fieldInfo[0], typeof(int));  
                                   else if(dataType=="Double")  changeTable.Columns.Add(fieldInfo[0], typeof(Double));
                                   else  changeTable.Columns.Add(fieldInfo[0], typeof(String));
                                       
                                }   
                               if(fieldDetails.Length >0) changeRow[fieldInfo[0]]  = fieldInfo[1];

                            }
                             ++i;
                                changeTable.Rows.Add(changeRow);  
                            }
                           
                        }

                        columnDifferencesTab = changeTable;
                     
                        if(!string.IsNullOrEmpty(DataUtilLibrary.sqlDestinationTableMap[tabName])){
                            
                              

                                try{

										var client              = new MongoClient(mongoDestConString);
										var database            = client.GetDatabase(DataUtilLibrary.destinationConnectionProps.getSourceDatabase());
                                        var collection          = database.GetCollection<BsonDocument>(DataUtilLibrary.sqlDestinationTableMap[tabName]);
                                        List<BsonDocument> batch = new List<BsonDocument>();

                                        foreach (DataRow dr in changeTable.Rows)
                                        {
                                            var dictionary = dr.Table.Columns.Cast<DataColumn>().ToDictionary(col => col.ColumnName, col => dr[col.ColumnName]);
                                            batch.Add(new BsonDocument(dictionary));
                                        }
                    
                                           await  collection.InsertManyAsync(batch.AsEnumerable());
                                        
                                }catch(Exception e){
                                    
                                    Console.WriteLine(e.Message);
                                    Console.WriteLine(e.StackTrace);
                                    DataUtilLibrary.writeToLog(e.Message);
                                    DataUtilLibrary.writeToLog(e.StackTrace); 
                                    emailError.AppendLine("<div style=\"color:red\">" + e.Message); 
                                    emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);   

                                }

                        }else{

                            DataUtilLibrary.writeToLog(DataUtilLibrary.sqlDestinationTableMap[tabName]+" has not been specified in the configuration");
                        }
                        
                }

                public void exportChangesToMysql(int numOfChanges){

                        DataTable changedRecordsTable         =  getDataFromStore(fectLastChangeScritpt.Replace("no_of_changes_val",numOfChanges.ToString()));    
                        string  tabName                       =  "";                                         
                        string  refVal                        =  "";
                        string  columnValueMap                =  "";
                        bool    isFirstRecord                 =  true;

                        foreach (DataRow changedRow           in changedRecordsTable.Rows) {
                
                        tabName                               =  changedRow["table_name"].ToString(); 
                        refVal                                =  changedRow["reference_field_value"].ToString();
                        columnValueMap                        =  changedRow["column_value_map"].ToString(); 


                        string[] records                      =  columnValueMap.Split(DataUtilLibrary.recordSeparator);
                        StringBuilder  changedRecordsBuilder  =  new StringBuilder();
                        DataTable changeTable                 =  new DataTable(DataUtilLibrary.sqlDestinationTableMap[tabName]); 

                        int i                                 =  1;
                        string dataType                       =  "";

                        string  record = records[0].ToString();
                            
                        if(!string.IsNullOrEmpty(record)){

                            string[] fieldDetails             =  record.Split(DataUtilLibrary.fieldSeparator);
                            DataRow changeRow                 =  changeTable.NewRow();  

                            foreach(string  field in  fieldDetails){

                                string[] fieldInfo     = field.Split(DataUtilLibrary.columnSeparator);
                                
                                if (fieldInfo[0].GetType() == typeof(int)){
                                    dataType    =  "int";
                                }else if (fieldInfo[0].GetType() == typeof(String)){
                                    dataType    =  "String";
                                }else if (fieldInfo[0].GetType() == typeof(Double)){
                                    dataType    =  "Double";
                                }
                                if(isFirstRecord){

                                    if (dataType    ==   "int") columnDifferencesTab.Columns.Add(fieldInfo[0], typeof(int));  
                                    else if(dataType=="Double")  columnDifferencesTab.Columns.Add(fieldInfo[0], typeof(Double));
                                    else  columnDifferencesTab.Columns.Add(fieldInfo[0], typeof(String));
                                    isFirstRecord = false;

                                }
                                if(i==1){

                                   if (dataType    ==   "int")       changeTable.Columns.Add(fieldInfo[0], typeof(int));  
                                   else if(dataType=="Double")  changeTable.Columns.Add(fieldInfo[0], typeof(Double));
                                   else  changeTable.Columns.Add(fieldInfo[0], typeof(String));
                                       
                                }   
                               if(fieldDetails.Length >0) changeRow[fieldInfo[0]]  = fieldInfo[1];

                            }
                                changeTable.Rows.Add(changeRow);  
                                columnDifferencesTab.Rows.Add(changedRow);
                            }
                            ++i;

                        if(!string.IsNullOrEmpty(DataUtilLibrary.sqlDestinationTableMap[tabName])){
                            
                                DataTable  targetTabExistsInfo       = getDataMysqlFromServer(mysqlDestConString, checkIfTableExistsScript.Replace("table_name_val",DataUtilLibrary.sqlDestinationTableMap[tabName]));
                                if(targetTabExistsInfo.Rows.Count    == 0  || targetTabExistsInfo.Rows[0]["table_exists_flag"].ToString()!="1"){
                                DataTable      tableCreateInfo       = getDataFromServer(sourceServerConnectionString ,getSQLTableCreateScript.Replace("dest_table_name_val",DataUtilLibrary.sqlDestinationTableMap[tabName]).Replace("source_table_name_val",tabName)
                                                                   ); 

                                StringBuilder tableCreateScriptBldr                =  new StringBuilder();
                                string tableCreateScript                           =  "";

                                foreach(DataRow dRow in tableCreateInfo.Rows){

                                    tableCreateScriptBldr.Append(dRow["table_create"].ToString().Trim());
                                   

                                }
                                    tableCreateScript     =  tableCreateScriptBldr.ToString().Substring(0,  tableCreateScriptBldr.ToString().Length -1)+");";
                                    executeScriptOnMySqlServer(mysqlDestConString, tableCreateScript);
                                }

                                try{
                                    StringBuilder  insertScriptBldr = new StringBuilder();
                                    insertScriptBldr.Append("INSERT INTO `"+DataUtilLibrary.sqlDestinationTableMap[tabName]+"` (");

                                    foreach(DataColumn col in changeTable.Columns){
                                   
                                          insertScriptBldr.Append(col.ToString()).Append(",");

                                    }
                                    
                                     insertScriptBldr.Remove(insertScriptBldr.Length-1,1);
                                     insertScriptBldr.Append(") VALUES (");
                                    
                                    foreach(DataRow dRows in  changeTable.Rows){
                                         int intCol = 0;
                                        foreach(DataColumn col in changeTable.Columns){                                       
                                        if (int.TryParse(dRows[col].ToString(), out intCol)){

                                              insertScriptBldr.Append(intCol.ToString()).Append(",");
                                        }else {
                                             insertScriptBldr.Append("\'").Append(dRows[col].ToString()).Append("\',");
                                        }
                                        }

                                    }
                                     insertScriptBldr.Remove(insertScriptBldr.Length-1, 1);
                                     insertScriptBldr.Append(")");
                                     string  insertScript  = insertScriptBldr.ToString();
                                     executeScriptOnMySqlServer(mysqlDestConString,insertScript);
                              
                                }catch(Exception e){
                                    
                                    Console.WriteLine(e.Message);
                                    Console.WriteLine(e.StackTrace);
                                    DataUtilLibrary.writeToLog(e.Message);
                                    DataUtilLibrary.writeToLog(e.StackTrace);    
                                    emailError.AppendLine("<div style=\"color:red\">" + e.Message); 
                                    emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);   


                                }

                        }else{

                            DataUtilLibrary.writeToLog(DataUtilLibrary.sqlDestinationTableMap[tabName]+" has not been specified in the configuration");
                        }
                        }
                }

				
				
					 public void  executeScriptOnMySqlServer( string connectionString, string script){
			 
					try{
			      		using (MySqlConnection serverConnection =  new MySqlConnection(connectionString)){
						 
					         serverConnection.Open();
							 MySqlCommand  cmd = new MySqlCommand(script, serverConnection);
							
							 DataUtilLibrary.writeToLog("Executing script: "+script);

							 cmd.CommandTimeout =0;
							 cmd.ExecuteNonQuery();
						
				}
				 }catch(Exception e){

					 Console.WriteLine("Error while running script: " + e.Message);
				     Console.WriteLine(e.StackTrace);
					 DataUtilLibrary.writeToLog("Error while running script: " + e.Message);
					 DataUtilLibrary.writeToLog(e.StackTrace);
                     emailError.AppendLine("<div style=\"color:red\">Error while running script: " + e.Message); 
                     emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);   

					 
				 }
		 }
		 
		 
  public   System.Data.DataTable  getDataMysqlFromServer(string connectionString, string theScript){
	         System.Data.DataTable  dt = new DataTable();

	    try{
		   using (MySqlConnection serverConnection =  new MySqlConnection(connectionString)){
				    serverConnection.Open ();   
                    MySqlCommand  cmd = new MySqlCommand(theScript, serverConnection);
                    cmd.CommandTimeout =0;
                    DataUtilLibrary.writeToLog("Executing script: "+theScript);
			        MySqlDataReader  reader = cmd.ExecuteReader();
			        dt.Load(reader);	
	                cmd.Dispose();
		   }
	 } catch(Exception e){

		  Console.WriteLine(e.Message);
		  Console.WriteLine(e.StackTrace);
		  DataUtilLibrary.writeToLog(e.Message);
		  DataUtilLibrary.writeToLog(e.StackTrace);
          emailError.AppendLine("<div style=\"color:red\">" + e.Message); 
          emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace); 

	  }
       return dt;
  }

    public void sendNotification(){
				DataUtilLibrary.writeToLog("Sending Notification... ");
                emailBody.AppendLine("<style type=\"text/css\">");
				emailBody.AppendLine("<div style=\"color:black\">Hi.</div>");
				emailBody.AppendLine("<div style=\"color:black\">\n</div>");
				emailBody.AppendLine("<div style=\"color:black\">Trust this meets you well</div>");
				emailBody.AppendLine("<div style=\"color:black\">\n</div>");
				emailBody.AppendLine("<div style=\"color:black\">Please see summary of changes on the "+ DataUtilLibrary.sourceConnectionProps.getSourceDatabase() +" of the "+ DataUtilLibrary.sourceConnectionProps.getSourceServer()+" below: </div>");
				emailBody.AppendLine("<div style=\"color:black\">\n</div>");
                MailMessage message = new MailMessage();

				foreach (var address in DataUtilLibrary.toAddress.Split(new [] {","}, StringSplitOptions.RemoveEmptyEntries)){
						if(!string.IsNullOrEmpty(address) &&  address.Contains("@") &&  address.EndsWith(".com")){
									message.To.Add(address);   	
						}
				}
			    foreach (var address in DataUtilLibrary.ccAddress.Split(new [] {","}, StringSplitOptions.RemoveEmptyEntries)){
					if(!string.IsNullOrEmpty(address)  &&  address.Contains("@") &&  address.EndsWith(".com")){
					message.CC.Add(address);   	
					}
				}
				foreach (var address in DataUtilLibrary.bccAddress.Split(new [] {","}, StringSplitOptions.RemoveEmptyEntries)){
							if(!string.IsNullOrEmpty(address) &&  address.Contains("@") &&  address.EndsWith(".com")){
										message.Bcc.Add(address);   	
							}
				}


				message.From = new MailAddress(DataUtilLibrary.fromAddress);				
				message.Subject = "Summary of Changes to the "+ DataUtilLibrary.sourceConnectionProps.getSourceDatabase() +" database of the "+ DataUtilLibrary.sourceConnectionProps.getSourceServer()+" server at "+todaysDate;
				message.IsBodyHtml = true;
		
				emailBody.AppendLine("<style type=\"text/css\">");
                emailBody.AppendLine("html * {");
                emailBody.AppendLine("	font-family:"+DataUtilLibrary.emailFontFamily);
                emailBody.AppendLine("	font-size:"+DataUtilLibrary.emailFontSize);
                emailBody.AppendLine("	color:"+DataUtilLibrary.color);
                emailBody.AppendLine("	border-color:"+DataUtilLibrary.borderColor);

                emailBody.AppendLine("}");
                emailBody.AppendLine("</style>");

				emailBody.AppendLine("table.gridtable {");
				emailBody.AppendLine("	font-family: arial,times new roman,verdana,sans-serif;");
				emailBody.AppendLine("	font-size:11px;");
				emailBody.AppendLine("	color:#333333;");
				emailBody.AppendLine("	border-width: 1px;");
				emailBody.AppendLine("	border-color: gray;");
				emailBody.AppendLine("	border-collapse: collapse;");
				emailBody.AppendLine("}");
				emailBody.AppendLine("table.gridtable th {");
				emailBody.AppendLine("	border-width: 1px;");
				emailBody.AppendLine("	padding: 8px;");
				emailBody.AppendLine("	border-style: solid;");
				emailBody.AppendLine("	border-color: gray;");
				emailBody.AppendLine("	background-color: #dedede;");
				emailBody.AppendLine("}");
				emailBody.AppendLine("table.gridtable td {");
				emailBody.AppendLine("	border-width: 1px;");
				emailBody.AppendLine("	padding: 8px;");
				emailBody.AppendLine("	border-style: solid;");
				emailBody.AppendLine("	border-color: gray;");
				emailBody.AppendLine("}");
				emailBody.AppendLine("</style>");
				emailBody.AppendLine("<div>\n</div>");
				emailBody.AppendLine("<div><hr/></div>");
				emailBody.AppendLine("<div justify=\"center\"><table class=\"gridtable\">");
				emailBody.AppendLine("<thead>");
				emailBody.AppendLine("<caption style=\"color:gray\" justify=\"left\">Service Parameter Details</caption>");
				emailBody.AppendLine("<th>No.</th><th>Parameter</th><th>Value</th>");
				emailBody.AppendLine("</thead>");
				emailBody.AppendLine("<tbody>");

                emailBody.AppendLine("<tr style=\"background-color:#ffffff\"><td>1. </td><td>Source Server</td><td>"+ DataUtilLibrary.sourceConnectionProps.getSourceServer()+"</td></tr>");
                emailBody.AppendLine("<tr style=\"background-color:"+DataUtilLibrary.alternateRowColour+"\"><td>2. </td><td>Destination Server</td><td>"+ DataUtilLibrary.destinationConnectionProps.getSourceServer()+"</td></tr>");
                emailBody.AppendLine("<tr style=\"background-color:#ffffff\"><td>3. </td><td>Source Database</td><td>"+ DataUtilLibrary.sourceConnectionProps.getSourceDatabase()+"</td></tr>");
                emailBody.AppendLine("<tr style=\"background-color:"+DataUtilLibrary.alternateRowColour+"\"><td>4. </td><td>Destination Database</td><td>"+DataUtilLibrary.destinationConnectionProps.getSourceDatabase()+"</td></tr>");
                emailBody.AppendLine("<tr style=\"background-color:#ffffff\"><td>5. </td><td>Monitored Tables</td><td>"+DataUtilLibrary.tablesToMonitor.ToString()+"</td></tr>");
                emailBody.AppendLine("<tr style=\"background-color:"+DataUtilLibrary.alternateRowColour+"\"><td>6. </td><td>Destination Server Type</td><td>"+DataUtilLibrary.destinationServerType+"</td></tr>");
                emailBody.AppendLine("</tbody>");
                emailBody.AppendLine("</table></div>");

                if(schemaDifferencesTab.Rows.Count>0){
                        emailBody.AppendLine("<div>\n</div>");
                        emailBody.AppendLine("<div><hr/></div>");
                        emailBody.AppendLine("<div justify=\"center\"><table class=\"gridtable\">");
                        emailBody.AppendLine("<thead>");
                        emailBody.AppendLine("<caption style=\"color:gray\" justify=\"left\">Schema Change Summary</caption>");
                        foreach(DataColumn col in schemaDifferencesTab.Columns){
                                emailBody.AppendLine("<th>"+col.ToString()+"</th>");
                        }
                        emailBody.AppendLine("</thead>");
                        emailBody.AppendLine("<tbody>");

                                    int k= 0;
                                    foreach (DataRow row in schemaDifferencesTab.Rows) {
                                        if(k%2!=0){
                                          
                                            emailBody.AppendLine("<tr style=\"background-color:#ffffff\">");
                                            } else{
                                            emailBody.AppendLine("<tr style=\"background-color:"+DataUtilLibrary.alternateRowColour+"\">");
                                          }
                                  
                                        foreach(DataColumn col in schemaDifferencesTab.Columns){
                                        emailBody.AppendLine("<td>"+row[col].ToString()+"</td>");
                                        }
                                        emailBody.AppendLine("</tr>");
                                    ++k;
                                    }
                            
                        emailBody.AppendLine("</tbody>");
                        emailBody.AppendLine("</table></div>");
                }
                emailBody.AppendLine("<div>\n</div>");
                emailBody.AppendLine("<div><hr/></div>");
                emailBody.AppendLine("<div justify=\"center\"><table class=\"gridtable\">");
                emailBody.AppendLine("<thead>");
                emailBody.AppendLine("<caption style=\"color:gray\" justify=\"left\">Data Change Summary</caption>");
                foreach(DataColumn col in columnDifferencesTab.Columns){
                     emailBody.AppendLine("<th>"+col.ToString()+"</th>");
                }
                emailBody.AppendLine("</thead>");
                emailBody.AppendLine("<tbody>");

                int v= 0;
                foreach (DataRow row in columnDifferencesTab.Rows) {
                if(v%2!=0){
            
                      emailBody.AppendLine("<tr style=\"background-color:#ffffff\">");
                } else{
                      emailBody.AppendLine("<tr style=\"background-color:"+DataUtilLibrary.alternateRowColour+"\">");
                }
                foreach(DataColumn col in columnDifferencesTab.Columns){
                     emailBody.AppendLine("<td>"+row[col].ToString()+"</td>");
                }
                     emailBody.AppendLine("</tr>");
                ++v;
                }
                
                emailBody.AppendLine("</tbody>");
                emailBody.AppendLine("</table></div>");
                if(!string.IsNullOrEmpty(emailError.ToString())){
                    emailBody.AppendLine("<div>\n</div>");
                    emailBody.AppendLine("<div>\n</div>");
                    emailBody.AppendLine("<div align=\"center\"><strong> Error List </strong></div>");
                    emailBody.AppendLine("<div>\n</div>");
                    emailBody.AppendLine("<div>\n</div>");
                    emailBody.AppendLine(emailError.ToString());
                }
			    emailBody.AppendLine("<div>\n</div>");
				emailBody.AppendLine("<div>\n</div>");
				emailBody.AppendLine("<div>\n</div>");
				emailBody.AppendLine("<div>\n</div>");
			    emailBody.AppendLine("Thank you.");
	            message.Body = emailBody.ToString();

                SmtpClient smtpClient = new SmtpClient();
                smtpClient.UseDefaultCredentials = true;
                smtpClient.Host =DataUtilLibrary.smtpServer;
                smtpClient.Port = Int32.Parse(DataUtilLibrary.smtpPort.ToString());
                smtpClient.EnableSsl = DataUtilLibrary.isSSLEnabled;
                smtpClient.Credentials = new System.Net.NetworkCredential(DataUtilLibrary.sender, DataUtilLibrary.senderPassword);
                smtpClient.Send(message);
			

                }
  public static void Main(string[] args){
      new   DataUtilLibrary();
      new  DataCheckProcess();

  }
                            
                        }

                }