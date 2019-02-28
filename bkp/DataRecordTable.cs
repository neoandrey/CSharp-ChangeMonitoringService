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

namespace MonitoringUtilities{

    public class DataRecordTable{
        
         public string      name;
         public ArrayList   tableColumns;
         public string      sourceServer;
         public string      destinationServer;
         public string      tableCreateScript;
         public ArrayList   columnsMonitored;
         public DataTable   recordDataTable;

        public DataRecordTable(){

        }
        
        public DataRecordTable(string tableName){

            setName(tableName);

        }
       
        public DataRecordTable(string tableName, ArrayList colsToMonitor){

            setName(tableName);
            setColumnsMonitored(colsToMonitor);

        }
        public string getName (){

              return this.name;

        }
       
        public string[] getColumns(){

            return this.tableColumns;

        }

         public string getSourceServer(){

           return this.sourceServer;

        }
         public string getDestinationServer(){

           return this.destinationServer;

        }

        public string getTableCreateScript(){

            return this.tableCreate;

        }
        public DataTable getRecordDataTable(){
            return this.recordDataTable;
        }

        public void setName (string tbName){

            this.name	= tbName;

        }
       
        public void setColumns(ArrayList cols ){

              this.tableColumns = cols;

        }
       
        public void setSourceServer(string  server){

            this.sourceServer =  server;

        }
         public void setDestinationServer(string  server){

            this.destinationServer = server;

        }
        public void setColumnsMonitored(ArrayList colsMoned){

            this.columnsMonitored = colsMoned;

        }
    }
} 