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

            public class  DataTransferService : ServiceBase{


             private Timer timer1  = null;
             this.ServiceName = "PostilionOfficeDataTransfer";
             this.CanStop = true;
             this.CanPauseAndContinue = true;
             this.AutoLog = true;
            
              public DataTransferService(){
                  new PostilionOfficeUtilLibrary();
              }

                
            protected override void OnStart(string[]  args){

                timer1 = new Timer();
                this.timer1.Interval = serverRunInterval;
                this.timer1.Elapsed += new System.Timers.ElapsedEventHandler(this.initService());
                timer1.Enabled  = true;

            }
            private void  initService (object sender,  ElapsedEventArgs e){
                 PostilionOfficeUtilLibrary.log("===========================Started Service "+ this.ServiceName+" at "+DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")+"==============================");
            }

            protected override void OnStop(){
                 timer1.Enabled = false;
                 PostilionOfficeUtilLibrary.log("===========================Stopped service "+ this.ServiceName+" at "+DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")+"==============================");

            }

 		
        public static void Main(){
            System.ServiceProcess.ServiceBase.Run(new DataTransferService());
        }


     }
}