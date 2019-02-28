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

namespace MonitoringUtilities{

            public class  DataCopyProcess {
                
              internal  DataServer                          sourceServer;
              internal  DataServer                          destinationServer;                               
              internal  DataRecordTable                     mainTable;
              internal  bool                                fileExists;
              internal  bool                                fileGroupExists;
              internal  bool                                tableExists;
              internal  bool                                isTransactionTable;
              internal  DataCopySchedule                    schedule;


              public DataCopyProcess(){

              }
            
              public DataCopyProcess(DataRecordTable mainTable, DataCopyDefinition tableCopyDef){

                   try{ 

                      sourceServer                   = initializeServer(mainTable.getName(), mainTable.getSourceServer());
                      destinationServer              = initializeServer(mainTable.getName(), mainTable.destinationServer());
                      schedule                       = tableCopyDef.getCopySchedule();

                   }catch(Exception e){

                         Console.WriteLine("Error openning connection to "+this.getServerName()+" : " + e.Message);
                         Console.WriteLine(e.StackTrace);

                   }
              }
              
              public string  getSourceServer(){

                 return  this.sourceServer;

              }
              public string  getSourceDatabase(){

                return  this.sourceDatabase;

              }
             public string  getSourcePort(){

                return  this.sourcePort;

              }

              public string  getDestinationServer(){
                return  this.destinationServer;
              }
              public string  getDestinationDatabase(){
                return  this.destinationDatabase;
              }
             public string  getDestinationPort(){
                return  this.destinationPort;
              }

              public void setDestinationConnectionString( ){
			            	 destinationConnectionString  = @"Network Library=DBMSSOCN;Data Source="+ destinationServer+","+destinationPort+";database="+destinationDatabase+";Trusted_Connection=True;Connection Timeout=0;Pooling=false;Packet Size=16384;"; 
	      	 }
 
             public string getDestinationConnectionString(){
                return  this.destinationConnectionString; 
             }

             public bool initSourceConnectionString(){
                    bool canConnect =false;
                    try
                        {

                        sourceConnectionString = @"Network Library=DBMSSOCN;Data Source="+ sourceServer+","+sourcePort+";database="+sourceDatabase+";User id=officeadmin;Password=AdminOfficer123;Connection Timeout=0;Pooling=false;Packet Size=16384;";
                        SqlConnection serverConnection =  new SqlConnection(sourceConnectionString);
                        serverConnection.Open();
                        canConnect =true;
                        serverConnection.Close();
                        }catch (Exception e){
                                DataUtilLibrary.writeToLog("Error connecting to server: " + e.Message);
                                DataUtilLibrary.writeToLog(e.StackTrace); 
                            }
                        return canConnect;
                    }
            
                 public bool initDestinationConnectionString(){
                    bool canConnect =false;
                    try
                        {

                        destinationConnectionString  = @"Network Library=DBMSSOCN;Data Source="+ destinationServer+","+destinationPort+";database="+destinationDatabase+";Trusted_Connection=True;Connection Timeout=0;Pooling=false;Packet Size=16384;"; 

                        SqlConnection serverConnection =  new SqlConnection(destinationConnectionString);
                        serverConnection.Open();
                        canConnect =true;
                        serverConnection.Close();
                        }catch (Exception e){
	                        DataUtilLibrary.writeToLog("Error while running script: " + e.Message);
				                  DataUtilLibrary.writeToLog(e.StackTrace);
                       }
                        return canConnect;
                    }
             
                    public System.Data.DataTable   getDataFromSourceDatabase (string script){
                            System.Data.DataTable dt = new DataTable();
                            try{

                                using (SqlConnection serverConnection =  new SqlConnection(sourceConnectionString)){
                                SqlCommand cmd = new SqlCommand(script, serverConnection);
                                DataUtilLibrary.writeToLog("Executing script: "+script+" on source database.");
                                cmd.CommandTimeout =0;
                                serverConnection.Open();
                                SqlDataReader  reader = cmd.ExecuteReader();
                                dt.Load(reader);	
                                cmd.Dispose();
                        }
                        }catch(Exception e){
                            DataUtilLibrary.writeToLog("Error while running script: " + e.Message);
                            DataUtilLibrary.writeToLog(e.StackTrace);
                        }
                        return dt;
                }

                 public System.Data.DataTable   getDataFromDestinationDatabase (string script){

                            System.Data.DataTable dt = new DataTable();
                            try{

                                using (SqlConnection serverConnection =  new SqlConnection(destinationConnectionString)){
                                SqlCommand cmd = new SqlCommand(script, serverConnection);
                                DataUtilLibrary.writeToLog("Executing script: "+script+" on destination database.");
                                cmd.CommandTimeout =0;
                                serverConnection.Open();
                                SqlDataReader  reader = cmd.ExecuteReader();
                                dt.Load(reader);	
                                cmd.Dispose();

                        }
                        }catch(Exception e){
                            DataUtilLibrary.writeToLog("Error while running script: " + e.Message);
                            DataUtilLibrary.writeToLog(e.StackTrace);
                        }
                        return dt;
                }

                public void  executeScriptOnSourceServer( string script){
			 
                try{
                        using (SqlConnection serverConnection =  new SqlConnection(sourceConnectionString)){
                        SqlCommand cmd = new SqlCommand(script, serverConnection);
                        Console.WriteLine("Executing script: "+script);
                        DataUtilLibrary.writeToLog("Executing script: "+script+" on source database.");
                        cmd.CommandTimeout =0;
                        serverConnection.Open();
                        cmd.ExecuteNonQuery();
              }
				 }catch(Exception e){
					  DataUtilLibrary.writeToLog("Error while running script: " + e.Message);
				      DataUtilLibrary.writeToLog(e.StackTrace);
				 }
		 }
            public void  executeScriptOnDestinationServer( string script){
			 
					try{
			      		using (SqlConnection serverConnection =  new SqlConnection(destination)){
						 SqlCommand cmd = new SqlCommand(script, serverConnection);
						 Console.WriteLine("Executing script: "+script);
                          DataUtilLibrary.writeToLog("Executing script: "+script+" on source database.");
						 cmd.CommandTimeout =0;
						 serverConnection.Open();
					     cmd.ExecuteNonQuery();
				}
				 }catch(Exception e){
					  DataUtilLibrary.writeToLog("Error while running script: " + e.Message);
				      DataUtilLibrary.writeToLog(e.StackTrace);
				 }
		 }
                public void  setSourceServer(string server){
                this.sourceServer = server;
                }
                public void setSourceDatabase(string database){
                this.sourceDatabase;
                }
                public void  setSourcePort(string port){
                  this.sourcePort = port;
                }

                public void  setDestinationServer(string server){
                     this.destinationServer  = server;
                }
                public void  setDestinationDatabase(string database ){
                  this.destinationDatabase =  database;
                }
                public void  setDestinationPort(string  port){
                    this.destinationPort   =  port;
                }

                public ArrayList getSourceTableColumns(string database, string tableName){
                     ArrayList columns =  new ArratList();
                     string  query ="SELECT COLUMN_NAME FROM  "+database+".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = \'"+tableName+"\'";
                     System.Data.DataTable  tableColumns = executeScriptOnSourceServer(query);
                     foreach (DataRow row in tableColumns.Rows) {
                            columns.Add(row['COLUMN_NAME']);
                     }
                     return columns;
                }
                
                public ArrayList getDestinationTableColumns( string database, string tableName){
                     ArrayList columns =  new ArratList();
                     string  query ="SELECT COLUMN_NAME FROM  "+database+".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = \'"+tableName+"\'";
                     System.Data.DataTable  tableColumns = executeScriptOnDestinationServer(query);
                     foreach (DataRow row in tableColumns.Rows) {
                            columns.Add(row['COLUMN_NAME']);
                     }
                     return columns;
                }
                public string getCreateScript(string server, string tableName){

                }


   }
            
 }