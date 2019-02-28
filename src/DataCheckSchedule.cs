
/**
scheduleName:
scheduleStatus:SCHEDULE_ENABLED, SCHEDULE_DISABLED;

scheduleType: ONE_TIME_SCHEDULE, RECURRING_SCHEDULE 
scheduleFrequnecy: MONTHLY_FREQUENCY, WEEKLY_FREQUENCY,DAILY_FREQUENCY
		   MONTHLY_FREQUENCY:  monthyFrequencyDay,  monthyFrequencyRate;
	           DAILY_FREQUENCY: dailyFrequencyOnceTime, dailyFrequencyTimePart  (DAILY_FREQUENCY_HOURS,DAILY_FREQUENCY_MINS, DAILY_FREQUENCY_SECS) ,
				    dailyFrequenctyStartTime,  dailyFrequencyEndTime
scheduleDuration:  scheduleDurationStartDate, scheduleDurationEndDate, SCHEDURATION_END_DATE;
scheduleDescription:
 */


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

                public class  DataCheckSchedule {



                    internal const int  ONE_TIME_SCHEDULE        = 1;
                    internal const int  RECURRING_SCHEDULE       = 2;
                    internal const int  SCHEDULE_DISABLED        = 0;
                    internal const int  SCHEDULE_ENABLED         = 1;                   
                    internal int        DAILY_FREQUENCY          = 1;
                    internal int        WEEKLY_FREQUENCY         = 2;
                    internal int        MONTHLY_FREQUENCY        = 3;
                    internal int        DAILY_FREQUENCY_HOURS    = 1;
                    internal int        DAILY_FREQUENCY_MINS     = 2;
                    internal int        DAILY_FREQUENCY_SECS     = 3;

                    internal string     scheduleName;
                    internal int        scheduleStatus;
                    internal int        scheduleType;
                    internal int        scheduleFrequnecy;
                    internal int        monthyFrequencyDay;
                    internal int        monthyFrequencyRate;
                    internal DateTime   dailyFrequencyOneTimeDate;
                    internal int        dailyFrequencyTimePart;
                    internal DateTime   dailyFrequencyStartTime;
                    internal DateTime   dailyFrequencyEndTime;
                    internal DateTime   scheduleDurationStartDate;
                    internal DateTime   scheduleDurationEndDate;
                    internal string     scheduleDescription;
                    internal bool       isRunning = false;

                    public DataCheckSchedule(){


                    }
                     public string  getScheduleName(){
                         return this.scheduleName;
                     }
                     public int  getScheduleStatus(){
                        return this.scheduleStatus;
                     }
                     public int getScheduleType(){
                         return this.scheduleType;
                     }
                     public int getScheduleFrequency(){
                         return this.scheduleFrequnecy;
                     }
                     public int  getMonthyFrequencyDay(){
                        return this.monthyFrequencyDay;
                     }
                     public  int  getMonthyFrequencyRate(){
                         return this.monthyFrequencyRate;
                     }
                     public DateTime getDailyFrequencyOneTimeDate(){
                         return this.dailyFrequencyOneTimeDate;
                     }
                     public int getDailyFrequencyTimePart(){
                        return  this.dailyFrequencyTimePart;
                     }
                     public DateTime getDailyFrequencyStartTime(){
                         return  this.dailyFrequencyStartTime;
                     }
                     public DateTime getDailyFrequencyEndTime(){
                         return  this.dailyFrequencyEndTime;
                     }
                     public DateTime getScheduleDurationStartDate(){
                        return this.scheduleDurationStartDate;
                     }
                    public DateTime getScheduleDurationEndDate(){
                        return this.scheduleDurationEndDate;
                    }
                    public string getScheduleDescription(){
                        return this.scheduleDescription;
                    }

                    public  bool getRunningStatus(){
                        return this.isRunning;
                    }

                     public void  setScheduleName(string name){
                          this.scheduleName =  name;
                     }
                     public void  setScheduleStatus(int  status){
                          this.scheduleStatus = status;
                     }
                     public void setScheduleType(int  type){
                          this.scheduleType = type;
                     }
                     public void setScheduleFrequency(int frequency){
                          this.scheduleFrequnecy = frequency;
                     }
                     public void  setMonthyFrequencyDay(int day){
                         this.monthyFrequencyDay =  day;
                     }
                     public  void  setMonthyFrequencyRate(int rate){
                          this.monthyFrequencyRate = rate;
                     }
                     public void setDailyFrequencyOneTimeDate(DateTime dDate){
                         this.dailyFrequencyOneTimeDate = dDate;
                     }
                     public void setDailyFrequencyTimePart(int timePart){
                         this.dailyFrequencyTimePart  = timePart;
                     }
                     public void setDailyFrequencyStartTime(DateTime fStartTime){
                           this.dailyFrequencyStartTime = fStartTime;
                     }
                     public void  setDailyFrequencyEndTime(DateTime endTime){
                           this.dailyFrequencyEndTime  =  endTime;
                     }
                     public void  setScheduleDurationStartDate(DateTime startDate){
                         this.scheduleDurationStartDate  =  startDate;
                     }
                    public void  setScheduleDurationEndDate(DateTime endDate){
                          this.scheduleDurationEndDate  =  endDate;
                    }
                    public void setScheduleDescription(string description){
                         this.scheduleDescription  = description;
                    }

                    public void setRunningStatus(bool running){
                        this.isRunning = running;
                    }
                
                }
                
    }