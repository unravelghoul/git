#ifndef SplitApp_H_INCLUDED_20120418
#define SplitApp_H_INCLUDED_20120418
#include <sys/time.h>
#include <string>
#include <algorithm>
#include "appfrm/Application.h"
#include "base/Directory.h"
#include "log4cpp/Logger.h"
#include "base/FileOperate.h"
#include "SplitConfig.h"
#include "SplitErr.h"

extern ErrorMessages *const theErrorMessages;
class SplitApp: public Application
{
public:
    SplitApp();
    ~SplitApp();
public:
    bool processopt(int optopt, const char *optarg);
    bool initialization();
    bool loopProcess();
    bool recovery();
    bool beforeLoop();
    bool end();
    bool BackupFile(string sSrcFileName, string sDestFileName); //备份文件
    bool ProcessFile(const string &sSrcFileName);  //处理文件
    
    bool ProcessKafka();  //处理kafka消息 liujq
    
    int getFileName(); //获取文件
    bool commit();
    bool redoCommit();//add by xingq
    bool isValidPath(const char *r_path);
    bool checkConfig();
    string numToStr(const long &r_num);
    //void onSignal(const int sig);

    SplitConfig m_config;   //配置信息对象
    string m_filename;     //读取的原始文件名

    char *m_fieldValue;//字段缓冲区
    //add by yueyq  for TFS:362748   split 根据地市编码再根据用户后四位分通道
    char *m_cityValue;//地市字段缓冲区

//liujq kafka
	bool m_isHaveErrData;  //是否有错误数据
	vector<long> m_errDataOffset;  //读取或写入错误消息的offset
	map<long,string> m_rowidall;//offset与json头对应关系


	
    //end
private:
    FileOperate fileOper;//sdfs文件操作工具
    FileOperate fileOpertKafkaInput;//kafka文件操作对象 liujq
    FileOperate fileOperKafkaOutput;//kafka文件操作对象 liujq
    char *m_sCdrBuf; //存储一条话单纪录
    int m_totalCdr; //输入文件记录数
    vector<string> m_CdrVec;//缓存一个文件的话单

    Directory *m_dir;

    int m_fileNum;//读取的文件个数
    string m_outFileName;//输出文件名

    map<string , FileOperate * > m_outmap;
    map<string, int> tmpFileNameMap; //<生文件名,行数>
    map<string, int> srcFullFileMap; //<原始文件名,行数>

    map<string , FileOperate * > m_outmapKafka;
    map<string, int> m_outFullFileMapKafka;//<生文件名,行数>	

    //add by xingq start redo文件单独处理,获取配置
    map<string , FileOperate * > m_redoCdrOutmap;
    map<string, int> m_tmpRedoFileNameMap; //<redo生成文件名,行数>
    string m_outRedoFileName;
    bool m_resetRedoFileName;
    //add by xingq end

    int m_fsortmethod;//文件排序方式
    string m_sCdrFileName;//带路径的文件名
    bool m_LogIsOpen;//日志打开标志
    bool m_LogRedoIsOpen;//redo日志打开标志 add by xingq
    int m_splitPos;//对后几位取余分发
    time_t m_dealTime;//处理时间
    char m_beginTime[14+1];  //处理时间YYYYMMDDHHMISS liujq kafka
    time_t m_redoDealTime;//redo处理时间 add by xingq
    time_t m_startTime;//处理时间
    bool resetFileName;//是否重置文件名
    int m_timeout;//超时秒数
    int m_redoTimeout;//redo文件超时多少秒输出 add by xingq
    bool m_redoOpen;//是否打开redo文件单独处理功能 add by xingq
    int m_totalTimeout;//总处理超时时长
    bool outFlag;//是否提交
    bool result;//是否提交成功

    map<string, int> m_seqMap; //序列号
    map<string, int> m_seqRedoMap; //redo序列号 add by xingq

    //alter for x86 add deal duration 20151204 by zhangyw
    struct timeval startDealTime, endDealTime;
    int duration;//处理时长
#ifdef _DEBUG_
    int sgetsNum;
    int sputsNum;
#endif
};

#endif
