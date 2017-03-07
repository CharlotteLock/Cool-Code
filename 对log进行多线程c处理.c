//Scriptname:log_analyse
//Author:charlotte
//date:2016/03/17
//Purpose: sleve log and connect save on database
/*defina head file*/
#include "stdio.h"
#include "stdlib.h"
#include "string.h"    /*use function about string*/
#include "mysql.h"    /*use mysql DB*/
#include "pthread.h"   /*use multi threads*/
#include "sys/ipc.h"   /*call message queue*/
#include "sys/time.h"   /*call sleep*/
#include "regex.h"
#include "memory.h"
#define MAX_LINE 1024
/*define variable*/
int sign=0,sign_exit=1;   /*sign to mark no.2error,sign_exit to mark thread1 exit*/
char log_path[MAX_LINE]="record.log";    
char record_day_time[MAX_LINE];
char function_name[MAX_LINE];
int sid;
char description[MAX_LINE];
const char mysqlServer[20] = "127.0.0.1";
const char mysqlUser[20] = "root";
const char mysqlPasswd[20] = "123456";
const char mysqlDBName[20] = "operation_monitor";
int lines=0;   /*order to check source maybe occur error*/
int msgid;    /*set global variable to avoid lose*/
pthread_t thread[2];   /*define thread variable,two threads*/
pthread_mutex_t mut;   /*define mutex locks*/
typedef struct mymsg /*define msg struct */
{
 char record_day_time[MAX_LINE];
 char function_name[MAX_LINE];
 char description[MAX_LINE];
}MSG;
/*performance function*/
int check_error( char buf[])
{
       char errbuf[1024];
        regex_t reg;
        int err,pmatch_size=10;
        regmatch_t pmatch[pmatch_size];       //Regular Expression ERROR
        if ( regcomp( &reg, "(.*)ERROR(.*)",REG_EXTENDED|REG_ICASE) < 0)   //compile RE
        {
                regerror( err,&reg, errbuf, sizeof( errbuf));
                printf ( "err:%s\n", errbuf);
        }
        err = regexec( &reg, buf, pmatch_size, pmatch, 0);  // RE object string
        if ( err == REG_NOMATCH)
        {
                return 0;
        }
        else if ( err)
        {
                regerror( err, &reg, errbuf, sizeof(errbuf));
                printf ( "err:%s\n",errbuf);
        }
        else
        {
                printf ("match ok.\n");
                return 1;
        }
}
int save_error(char buf[])
{
        char record_day_time_mid[1024];
        char errbuf[1024];
        regex_t reg;
        int err,pmatch_size=10,i,mid_size;
        regmatch_t pmatch[pmatch_size];    // RE ERROR and save data that need
        if ( regcomp( &reg, "\\[(.*)-(.*)-(.*)[ ](.*):(.*):(.*):(.*)\\](.*) ERROR(.*)",REG_EXTENDED|REG_ICASE) < 0)
        {
                regerror( err,&reg, errbuf, sizeof( errbuf));
                printf ( "err:%s\n", errbuf);
        }
        err = regexec( &reg, buf, pmatch_size, pmatch, 0);
        if ( err == REG_NOMATCH)
        {
                return 0;
        }
        else if ( err)
        {
                regerror( err, &reg, errbuf, sizeof(errbuf));
                printf ( "err:%s\n",errbuf);
        }
        for ( i=0; i<pmatch_size && pmatch[i].rm_so != -1; i++)  // read buf by point_match
        {
                int se_len = pmatch[i].rm_eo - pmatch[i].rm_so;
                if( se_len && i >=1 && i <=6)
                {
                        memset( record_day_time_mid,'\0',sizeof(record_day_time_mid));
                        memcpy( record_day_time_mid, buf+pmatch[i].rm_so, se_len);
                        mid_size = sizeof(record_day_time_mid);
                        strncat( record_day_time, record_day_time_mid, mid_size);
                }
                else if( se_len && i == 8)
                {
                        memset( function_name, '\0', sizeof( function_name));
                        memcpy( function_name, buf+pmatch[i].rm_so, se_len);
                }
        }
}
void save_error2(char buf[])
{
        char errbuf[1024];
        regex_t reg;
        int err,pmatch_size=10,i,buf_size;
        regmatch_t pmatch[pmatch_size];   // RE ERROR next part 
        if ( regcomp( &reg, "^\\[(.*)\\](.*)",REG_EXTENDED|REG_ICASE) < 0)  // end by next record
        {
                regerror( err,&reg, errbuf, sizeof( errbuf));
                printf ( "err:%s\n", errbuf);
        }
        err = regexec( &reg, buf, pmatch_size, pmatch, 0);
        if ( err == REG_NOMATCH)
        {
                buf_size = strlen( buf);
                strncat( description, buf, buf_size);
        }
        else if ( err)
        {
                regerror( err, &reg, errbuf, sizeof(errbuf));
                printf ( "err:%s\n",errbuf);
        }
        else
        {
               sign = 0;      // set sign ,a full record
        }
}
int analyse_log(char buf[])
{
 int length,sign_send=1;
 MSG *new_msg;
 new_msg=(MSG *)malloc(sizeof(MSG));
 if ( sign == 0)     /*judge sign,half deal or full deal*/
 {
  if ( check_error(buf) == 0) /* search error ,not return*/
   ;
  else                       /* process error line*/
  {
   save_error(buf);
   sign=1;
  }
 }
 else if ( sign == 1)  /* half deal,process ip line*/
 {
  save_error2(buf);  /* get complete data*/
  if ( sign == 1)
   return 0;
  strcpy(new_msg->record_day_time,record_day_time);
  strcpy(new_msg->function_name,function_name);
  strcpy(new_msg->description,description);
  length = sizeof(MSG);
  while(sign_send)         /*set sign,until send ok*/
  {
   pthread_mutex_lock(&mut);      /* mutex cpu locks*/
   if(msgsnd(msgid,new_msg,length,IPC_NOWAIT)==0)
    {sign_send=0;pthread_mutex_unlock(&mut);}
   else pthread_mutex_unlock(&mut);      /* unlock cpu */
  }
  printf("send ok.\n");        /*initialize data before send ok*/
  memset(function_name,'\0',sizeof(function_name));
  memset(record_day_time,'\0',sizeof(record_day_time));
  memset(description,'\0',sizeof(description));
  analyse_log(buf);
 }
}
void read_log()        /*read log and store the required data*/
{
 char buf[MAX_LINE];
 FILE *fp;
 int len,i;
 if ((fp = fopen(log_path,"r")) == NULL) /* continue before valid log*/
 {
  perror("fail to read");
  exit(1);
 }
 i=1;
 while(fgets(buf,MAX_LINE,fp) != NULL ) /* process log line by line*/
 {
  lines++;
  len = strlen(buf);
  buf[len-1] = '\0';
  i++;
  analyse_log(buf);   /* call function to do second step :analyse*/
 }
 pthread_mutex_lock(&mut);      /* mutex cpu locks*/
 sign_exit=0;
 pthread_mutex_unlock(&mut);
 printf("read_thread exit,%d\n",sign_exit);
}
void *read_log_analyse()   /*define threads function1 to read log and get data*/
{
 read_log();
}
void insert_into_DB()
{
 int length;           /*use to count msg size*/
 MSG *new_msg;          /*use to save received msg*/
 new_msg=(MSG *)malloc(sizeof(MSG));
 length = sizeof(MSG);
 while(1)           /*dead circulation to receive msg and insert into DB*/
 {
  pthread_mutex_lock(&mut);      /* mutex cpu locks*/
  if(msgrcv(msgid,new_msg,length,0,IPC_NOWAIT)==-1) /*MQid,msg vessel,msg type(no require),set to avoid MQ block;success return no negative num*/
  { 
   pthread_mutex_unlock(&mut);     /*unlock MQ*/
   if(sign_exit==0)break;      /*if no message and the sign show no continue send msg,exit*/
   printf("no message,wait a monment.\n");  /*if no message,continue,wait*/
   sleep(1);
  }
  else 
  {
   pthread_mutex_unlock(&mut);     /*unlock MQ*/
   printf("rcv msg successful.\n");
   char ibuf[1024];
   MYSQL mysql,*sock;
   mysql_init(&mysql);       /*initialize mysql handle*/
   if ( !(sock=mysql_real_connect( &mysql,"127.0.0.1","root","123456","operation_monitor",3306,NULL,0))) /*return sock*/
   {/*connect DB*/
    fprintf(stderr,"Couldn't connect to engine!\n%s\n\n",mysql_error(&mysql));
    perror(" connect mysql error!\n");
    exit(1);
   }/*save inserts statement into ibuf*/
   sprintf( ibuf,"insert into error_log(id,function_name,description,record_time) values('','%s','%s','%s')",new_msg->function_name,new_msg->description,new_msg->record_day_time);
   if ( mysql_query(sock,ibuf))  /*operate executive*/
   {
    printf("insert data error!i No.%d\n",lines);
   }
   else
   {
    printf("insert ok.\n");
   }
   mysql_close(&mysql);
   mysql_close(sock);
  }
 }
 printf("insert_thread exit.\n");
}
void *insert_data_DB()   /*define threads function2 to insert data into DB*/
{
 insert_into_DB();
}
void thread_create()
{
 pthread_create(&thread[0],NULL,read_log_analyse,NULL); /*create thread,but maybe threads no start,attention incomming local parameters lose*/
 pthread_create(&thread[1],NULL,insert_data_DB,NULL);
}
void thread_wait()
{
 pthread_join(thread[0],NULL);  /*wait thread finish*/
 pthread_join(thread[1],NULL);
}
int get_key()
{
 int key;
 key=ftok(".",'s');
 return key;
}
/*function main*/
int main()
{
 msgid=msgget(0,IPC_CREAT|0644);  /*get a new MQ*/
 if(msgid<0)
 {
  perror("msgget error.");
  exit(1);
 }
 printf("get msg success\n");
 pthread_mutex_init(&mut,NULL);     /*initialize mutex locks variable mut*/
 thread_create();     /*call function to create thread*/
 thread_wait();      /*wait thread run end*/
 return 0;
}