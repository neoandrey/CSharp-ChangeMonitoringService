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

namespace MonitoringUtilities{

                public class  DataCheckDefinition {

                    internal  DataServer             sourceServer;
                    internal  DataServer             targetServer;
                    internal  DataCheckSchedule      checkSchedule;
                    internal  DataRecordTable        tableName;
                    internal  ArrayList              columnsMonitored;
                    
                    public DataCheckDefinition(){

                    }

                    public DataServer getSourceServer(){
                        return this.sourceServer;
                    }

                    public DataServer getTargetServer(){

                        return this.targetServer;
                    }

                    public string getSourceDatabase(){
                        return this.sourceDatabase;
                    }
                    public string getTargetDatabase(){
                        return  this.targetDatabase;
                    }

                    public ArrayList getColumnsMonitored(){

                         return this.columnsMonitored;

                    }

                    public Dictionary getTableColumnNames(){
                        return this.tableColumnNames;
                    }
                    
                    public Dictionary getTableColumnTypes(){
                        return this.tableColumnTypes;
                    }

                    public Dictionary getTableColumnSizes(){
                        return this.tableColumnSizes;
                    }
                
                    public void setColumnsMonitored(ArrayList cols ){

                        this.columnsMonitored = cols;
                    }

                }
            
         }