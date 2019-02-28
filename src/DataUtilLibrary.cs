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
using System.Threading;
using System.Data;
using System.Globalization;
using System.Net.Mail;
using Newtonsoft.Json.Linq;

namespace MonitoringUtilities{

            public class   DataUtilLibrary{

                public  static string sourceServer                                       = "";
                public  static string sourceDatabase                                     = "";
                public  static string sourcePort                                         = "";
                public  static string sourceServertype                                   = "";

                public  static string destinationServer                                  = "";
                public  static string destinationDatabase                                = "";
                public  static string destinationPort                                    = "";
                public  static string destinationServerType                              = "";

                public  static Dictionary<string,string> tableColMap                     = new Dictionary<string,string>();
                public  static Dictionary<string,string>tableRefColMap                   = new Dictionary<string,string>();
                public  static System.IO.StreamWriter                    fs;
                public  static int serverRunInterval                                     = 3; //3 secs (to be multiplied by 1000)

                public  static string logFile                                            =  Directory.GetCurrentDirectory()+"\\log\\datachangecheck.log";
                public  static string configFileName                                     =  Directory.GetCurrentDirectory()+"\\conf\\datachangecheck.json";
                public  static int  defaultBatchSize                                     =  1000;

                public  static string fromAddress                                        =  "";
                public  static string toAddress                                          =  "";
                public  static string bccAddress                                         =  "";
                public  static string ccAddress                                          =  "";
                public  static string smtpServer                                         =  "";
                public  static int smtpPort                                              =  1433;
                public  static string sender                                             =  "";
                public  static string senderPassword                                     =  "";
                public  static bool isSSLEnabled                                         =  false;
                public  static DataCheckConfig configProperties                          = new DataCheckConfig() ;
                public  static ConnectionProperty sourceConnectionProps;
                public  static ConnectionProperty destinationConnectionProps;
                public  static ArrayList tablesToMonitor                                 =  new ArrayList();
                public  static ArrayList changeMonitoringModes                           =  new ArrayList();
                public  static bool sendNotificationMail                                 =  true;
                public  static  int noOfThreads                                          = 1;
                public  static string sourceServerType                                   = "";
                public  static char columnSeparator;                       
                public  static char fieldSeparator;                       
                public  static char recordSeparator;                      
                public  static  Dictionary<string,string>  sqlDestinationTableMap        = new  Dictionary<string,string>();
                public  static  Dictionary<string,string>  mongoDestinationCollectionMap = new  Dictionary<string,string>();
                public  static  Dictionary<string,string>  mysqlDestinationTableMap      = new  Dictionary<string,string>();
                public  static  string    mongoConnectionPort                              = "";
                public  static  string    alternateRowColour                             = ""; 
                public  static  string    emailFontFamily                                = ""; 
                public  static  string    emailFontSize                                  = ""; 
                public  static  string    color                                          = ""; 
                public  static  string    borderColor                                    = ""; 
                               
                public  DataUtilLibrary(){ 

                        readConfigFile(configFileName);
                        logFile = String.IsNullOrEmpty(logFile)?Directory.GetCurrentDirectory()+"\\log\\datachangecheck.log":logFile;

                        if (!File.Exists(logFile))  {

                                fs = File.CreateText(logFile);

                        }else{

                                fs = File.AppendText(logFile);

                        } 
                    if (!String.IsNullOrEmpty(sourceServer) &&  !String.IsNullOrEmpty(sourceDatabase)){

                          sourceConnectionProps      = new ConnectionProperty( sourceServer, sourceDatabase );

                    } else {

                        Console.WriteLine("Source connection details are not complete");
                        writeToLog("Source connection details are not complete");

                    }
                     if (!String.IsNullOrEmpty(destinationServer) &&  !String.IsNullOrEmpty(destinationDatabase)) {

                        destinationConnectionProps = new ConnectionProperty( destinationServer, destinationDatabase );

                     } else {

                                Console.WriteLine("Destination connection details are not complete");
                                writeToLog("Destination connection details are not complete");
                     }

                       
            }

          public  static  void readConfigFile(string configFileName){

                try{
                    string  propertyString             =  File.ReadAllText(configFileName);
                    configProperties                   =  Newtonsoft.Json.JsonConvert.DeserializeObject<DataCheckConfig>(propertyString);              
                    string jsonString                  =  Newtonsoft.Json.JsonConvert.SerializeObject(configProperties);
                    sourceServer                       =  configProperties.source_server;
                    sourceDatabase                     =  configProperties.source_database;
                    sourcePort                         =  configProperties.source_port;
                    destinationServer                  =  configProperties.destination_server;
                    destinationDatabase                =  configProperties.destination_database;
                    destinationPort                    =  configProperties.destination_port;
                    toAddress                          =  configProperties.to_address;
                    fromAddress                        =  configProperties.from_address;
                    ccAddress                          =  configProperties.cc_address;
                    bccAddress                         =  configProperties.bcc_address;
                    smtpServer                         =  configProperties.smtp_server;
                    smtpPort                           =  configProperties.smtp_port;
                    sender                             =  configProperties.sender;
                    senderPassword                     =  configProperties.sender_password;
                    isSSLEnabled                       =  configProperties.is_ssl_enabled;
                    defaultBatchSize                   =  configProperties.batch_size;
                    noOfThreads                        =  configProperties.threads; 
                    tableColMap                        =  readJSONMap(configProperties.tables_column_map); //  readJSONMap(configProperties.tables_column_map.ToDictionary(x => x.ToString(), x => x.ToString()));
                    tableRefColMap                     =  readJSONMap(configProperties.tables_ref_column_map);// readJSONMap(configProperties.tables_ref_column_map.ToDictionary(x => x.ToString(), x => x.ToString()));
                    logFile                            =  configProperties.log_file;
                    sendNotificationMail               =  configProperties.send_notification;
                    sourceServerType                   =  configProperties.source_server_type;
                    destinationServerType              =  configProperties.destination_server_type;
                    tablesToMonitor                    =  configProperties.tables_to_monitor;
                    changeMonitoringModes              =  configProperties.change_monitoring_modes;
                    columnSeparator                    =  configProperties.column_value_separator;
                    fieldSeparator                     =  configProperties.field_separator;
                    recordSeparator                    =  configProperties.record_separator;
                    sqlDestinationTableMap             =  readJSONMap(configProperties.sql_destination_table_map);// readJSONMap(configProperties.sql_destination_table_map.ToDictionary(x => x.ToString(), x => x.ToString())); 
                    mongoDestinationCollectionMap      =  readJSONMap(configProperties.mongo_destination_collection_map);// readJSONMap(configProperties.mongo_destination_collection_map.ToDictionary(x => x.ToString(), x => x.ToString()));
                    mysqlDestinationTableMap           =  readJSONMap(configProperties.mysql_destination_table_map);// readJSONMap(configProperties.mysql_destination_table_map.ToDictionary(x => x.ToString(), x => x.ToString()));
                    mongoConnectionPort                =  configProperties.mongo_connection_port.ToString();  
                    alternateRowColour                 =  configProperties.alternate_row_colour;
                    emailFontFamily                    =  configProperties.email_font_family;
                    emailFontSize                      =  configProperties.email_font_size;
                    color                              =  configProperties.color;
                    borderColor                        =  configProperties.border_color;
                    sendNotificationMail               =  configProperties.send_email_notification;
                }catch(Exception e){
                    
                    Console.WriteLine("Error reading configuration file: "+e.Message);
                    Console.WriteLine(e.StackTrace);
                    writeToLog("Error reading configuration file: "+e.Message);
                    writeToLog(e.StackTrace);

                }
            
            }
            public static void  writeToLog(string logMessage){

                 fs.WriteLine(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")+"=>"+logMessage);
                 fs.Flush();

            }
          /*   public static Dictionary<string,string>readJSONMap(Dictionary<string,string>rawMap){
                    Dictionary<string, string> tempDico = new  Dictionary<string, string>();
                    string tempVal  ="";
                    foreach(string keyVal in rawMap.Keys){

                             //   tempVal = keyVal.Replace("{","").Replace("}","").Replace("\"","").Trim();
                                tempDico.Add(tempVal.Split(':')[0],tempVal.Split(':')[1]);
                                                                
                    }
                  return tempDico;
            }*/
            public static Dictionary<string,string>readJSONMap(ArrayList rawMap){

                    Dictionary<string, string> tempDico = new  Dictionary<string, string>();
                    string tempVal  ="";
                    if(rawMap!=null)
                    foreach(var keyVal in rawMap){
                                
                                   tempVal = keyVal.ToString();
                                   if(!string.IsNullOrEmpty(tempVal)){
                                        tempVal = tempVal.Replace("{","").Replace("}","").Replace("\"","").Trim();
                                        Console.WriteLine("tempVal: "+tempVal);
                                     if(tempVal.Split(':').Count() ==2)tempDico.Add(tempVal.Split(':')[0].Trim(),tempVal.Split(':')[1].Trim());  
                                   }  

                    }
                  return tempDico;
            }
            public static  void log(string logMessage){
                    
                    fs.WriteLine(logMessage);
                    fs.Flush();
            }
            public static void closeLogFile(){
                   fs.Close();
            }


   }
}