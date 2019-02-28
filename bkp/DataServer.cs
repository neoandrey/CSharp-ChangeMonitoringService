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

            public   class  DataServer {
                 
                  public string serverName;
                  public string ipAddress;
                  public string userName;
                  public string password;
                  public string database;
                  public string port;
                  public string connectionString;
                  public SqlConnection connection;
                  public string serverType;

                  public   DataServer(){


                  }

                  public string  getServerName(){
                      return this.serverName;
                  }
                  public   string  getIPAddress(){
                      return this.ipAddress;
                  }
                  public   string getUserName (){

                     return this.userName;
                  
                  }
                  public string setServerType(string srvType){

                       this.serverType = srvType;

                  }

                  public   string getDatabase(){

                       return this.database;

                  }
                  public   string  getPort(){

                      return this.port;

                  }
                  public   string getConnectionStr() {

                      return this.connectionString;

                  }
                  public   SqlConnection getConnection(){

                      return this.connection;

                  }

                  public string getServerType(){

                  }
                                
                public void  setServerName(string name){

                    this.serverName = name;

                }
                public   void  setIPAddress(string address){

                    this.ipAddress =  address;

                }
                public   void setUserName (string user){

                    this.userName = user;

                }

                public   void setDatabase(string db){

                        this.database  = this.db;

                }
                public   string  setPort(int pt){

                    this.port  = pt;

                } 
                public   string setConnectionStr(string conStr) {

                    this.connectionString = conStr;

                }
                public   string setConnectionStr(string conStr) {

                    this.connectionString = conStr;

                }
                    public   void setConnectionStr( ){

                    this.connectionString   = @"Network Library=DBMSSOCN;Data Source="+ this.serverName+","+this.port+";database="+database+";User id="+userName+";Password="+password+";Connection Timeout=0;Pooling=false;Packet Size=16384;";
               
                }
                public   bool initializeConnection(){
                    try{
                        this.connection =  new SqlConnection(this.sourceConnectionString);
                        this.connection.Open();
                        return this.connection.State != ConnectionState.Closed;
                    }catch(Exception e){
                        Console.WriteLine("Error openning connection to "+this.getServerName()+" : " + e.Message);
                        Console.WriteLine(e.StackTrace);
                    }
                }
                
                public   bool closeConnection(){
                    try{
                        this.connection.Close();
                        return this.connection.State == ConnectionState.Closed;
                    }catch(Exception e){
                        Console.WriteLine("Error closing connection to "+this.getServerName()+" : " + e.Message);
                        e.PrintStackTrace();
                    }
                }
            
            
            }
            
            
            }