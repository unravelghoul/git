#ifndef _SPLIT_CONFIG_H_20150707_
#define _SPLIT_CONFIG_H_20150707_

#include <string>
#include <map>
#include <vector>
#include <list>
#include <occi.h>

#include "base/SysParam.h"
#include "base/StringUtil.h"
#include "config-all.h"
#include "dbi/dbi.h"
#include "SplitErr.h"
#include "appfrm/ErrorHandle.h"
#include "AbstractFieldSelector.h"
#include "BillingConstDef.h"//liujq kafka


using namespace std;
using namespace oracle::occi;

extern ErrorMessages *const theErrorMessages;

class CustChannel     //客户通道类,存储表的一些信息
{
public:
    int m_beginPartition;
    int m_endPartition;
    int m_dbNo;
    string m_channelNo;
    string m_provCode;
    //add by yueyq  for TFS:362748   split 根据地市编码再根据用户后四位分通道
    string m_cityCode;
    //end
    friend ostream &operator<<(ostream &r_os, const CustChannel &r_channel);
    friend bool operator<(const CustChannel &r_left, const CustChannel &r_right);
};

//配置文件类
class SplitConfig
{
public:
    SplitConfig();

    ~SplitConfig();

public:
    //连接数据库信息
    string m_dbCode;
    string m_dbServName;
    string m_dbUserName;
    string m_dbPassword;

    char m_separatorSign;    //文件分隔符
    int  m_split_pos;//以最后N位分通道
    string m_inputPath;      //输入文件目录
    string m_backupPath;     //备份文件目录
    string m_errPath;        //错误文件目录
    string m_tmpPath;        //临时文件目录
    string m_prov_code;      //省份

    int m_maxFileNum;	//处理多少文件输出一次
    int m_maxFileLine;	//分发文件多少行时拆分输出
    int m_timeout;//超时时间
    int m_totalTimeout;//总处理超时时间
    int m_redoTimeout;//redo文件超时时间 add by xingq
    int m_redoOpen;//是否开启redo单独处理功能 add by xingq

    size_t  m_maxFidLen;
    string m_fidSelectRule;
    AbstractFieldSelector *m_fidSelect;
    list<AbstractFieldSelector *> m_selectors;

    //add by yueyq  for TFS:362748   split 根据地市编码再根据用户后四位分通道
    string m_openCity; //地市开关
    string m_citySelectRule;
    AbstractFieldSelector *m_citySelect;
    //end

    string m_prefix;

    map<string, string> m_outChanInfo; //存储配置文件中通道信息map<通道号,分发地址>

    //存储分发通道表TD_SPLIT_CHANNEL_DEF信息
    vector<CustChannel> m_custChannel;



    //add by liuq kakfa
    struct KafkaProperty
    {
        string m_propertycpType;                 //P-生产者,C-消费者
        string m_propertycpScope;                //G-全局,T-自身
        string m_propertycpName;                 //属性名称
        string m_propertycpValue;                //属性值
    };

    int m_fsMode;//#读取文件模式 0:本地文件系统 1:SDFS 2:KAFKA 3:先KAFKA再SDFS
    int m_maxCdrOnceRead;                        //KAFKA模式,一次读取的最大话单数
    int m_maxCdrOnceWrite;                       //KAFKA模式,生产者open参数
    int m_jsonBulkSize;                          //KAFKA模式,每个json包包含的最大话单数
    int m_fschangeInterval;                      //getfile_mode为3时文件系统切换时间间隔
    //vector<KafkaProperty> m_kafkaParamVec;     //kafka分组参数
    vector<string> m_kafkaParamVec;              //kafka分组参数

    vector<string> m_mdsPathvector;              //正常回收话单消息输出目录解析容器
    string m_inputPathMds;                       //input_path后面配置的MDS
    //string m_outputPathMds;                      //output_path后面配置的MDS
    string m_inputPathErrMds;                    //err_path后面配置的MDS
    //add by liuq kakfa


    //获取客户通道信息,并将信息存入 m_custChannel中
    bool GetAllChannel() ;

    //从数据库td_b_channel_param表中获取通道的文件读取方式
    bool getFsMode(int r_channelNo);//liujq kafka

    //读取配置文件中信息
    bool GetSystemConfigInfo(SysParam *pSysParam, string sProcName, int iChannelNo);

    //通过客户ID取得相对应的通道号
    bool getChanNoByInfo(const int &sCustId, string &sInfoPaths);

    //add by yueyq  for TFS:362748   split 根据地市编码再根据用户后四位分通道
    //通过客户ID和地市取得相对应的通道号
    bool getChanNoByInfo(const int &sCustId, string &city_code, string &channelNo);
    //end

    AbstractFieldSelector *generateSelect(char *r_strRule, const char r_delimiter, const size_t r_maxLen);

    //标准化目录
    void standardPath(string &r_path);

    //liujq kafka 校验kafka topic and partition应用目录中配置的MDS
	bool isPath(const string &r_pathName);
};

#endif
