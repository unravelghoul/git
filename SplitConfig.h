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

class CustChannel     //�ͻ�ͨ����,�洢���һЩ��Ϣ
{
public:
    int m_beginPartition;
    int m_endPartition;
    int m_dbNo;
    string m_channelNo;
    string m_provCode;
    //add by yueyq  for TFS:362748   split ���ݵ��б����ٸ����û�����λ��ͨ��
    string m_cityCode;
    //end
    friend ostream &operator<<(ostream &r_os, const CustChannel &r_channel);
    friend bool operator<(const CustChannel &r_left, const CustChannel &r_right);
};

//�����ļ���
class SplitConfig
{
public:
    SplitConfig();

    ~SplitConfig();

public:
    //�������ݿ���Ϣ
    string m_dbCode;
    string m_dbServName;
    string m_dbUserName;
    string m_dbPassword;

    char m_separatorSign;    //�ļ��ָ���
    int  m_split_pos;//�����Nλ��ͨ��
    string m_inputPath;      //�����ļ�Ŀ¼
    string m_backupPath;     //�����ļ�Ŀ¼
    string m_errPath;        //�����ļ�Ŀ¼
    string m_tmpPath;        //��ʱ�ļ�Ŀ¼
    string m_prov_code;      //ʡ��

    int m_maxFileNum;	//��������ļ����һ��
    int m_maxFileLine;	//�ַ��ļ�������ʱ������
    int m_timeout;//��ʱʱ��
    int m_totalTimeout;//�ܴ���ʱʱ��
    int m_redoTimeout;//redo�ļ���ʱʱ�� add by xingq
    int m_redoOpen;//�Ƿ���redo���������� add by xingq

    size_t  m_maxFidLen;
    string m_fidSelectRule;
    AbstractFieldSelector *m_fidSelect;
    list<AbstractFieldSelector *> m_selectors;

    //add by yueyq  for TFS:362748   split ���ݵ��б����ٸ����û�����λ��ͨ��
    string m_openCity; //���п���
    string m_citySelectRule;
    AbstractFieldSelector *m_citySelect;
    //end

    string m_prefix;

    map<string, string> m_outChanInfo; //�洢�����ļ���ͨ����Ϣmap<ͨ����,�ַ���ַ>

    //�洢�ַ�ͨ����TD_SPLIT_CHANNEL_DEF��Ϣ
    vector<CustChannel> m_custChannel;



    //add by liuq kakfa
    struct KafkaProperty
    {
        string m_propertycpType;                 //P-������,C-������
        string m_propertycpScope;                //G-ȫ��,T-����
        string m_propertycpName;                 //��������
        string m_propertycpValue;                //����ֵ
    };

    int m_fsMode;//#��ȡ�ļ�ģʽ 0:�����ļ�ϵͳ 1:SDFS 2:KAFKA 3:��KAFKA��SDFS
    int m_maxCdrOnceRead;                        //KAFKAģʽ,һ�ζ�ȡ����󻰵���
    int m_maxCdrOnceWrite;                       //KAFKAģʽ,������open����
    int m_jsonBulkSize;                          //KAFKAģʽ,ÿ��json����������󻰵���
    int m_fschangeInterval;                      //getfile_modeΪ3ʱ�ļ�ϵͳ�л�ʱ����
    //vector<KafkaProperty> m_kafkaParamVec;     //kafka�������
    vector<string> m_kafkaParamVec;              //kafka�������

    vector<string> m_mdsPathvector;              //�������ջ�����Ϣ���Ŀ¼��������
    string m_inputPathMds;                       //input_path�������õ�MDS
    //string m_outputPathMds;                      //output_path�������õ�MDS
    string m_inputPathErrMds;                    //err_path�������õ�MDS
    //add by liuq kakfa


    //��ȡ�ͻ�ͨ����Ϣ,������Ϣ���� m_custChannel��
    bool GetAllChannel() ;

    //�����ݿ�td_b_channel_param���л�ȡͨ�����ļ���ȡ��ʽ
    bool getFsMode(int r_channelNo);//liujq kafka

    //��ȡ�����ļ�����Ϣ
    bool GetSystemConfigInfo(SysParam *pSysParam, string sProcName, int iChannelNo);

    //ͨ���ͻ�IDȡ�����Ӧ��ͨ����
    bool getChanNoByInfo(const int &sCustId, string &sInfoPaths);

    //add by yueyq  for TFS:362748   split ���ݵ��б����ٸ����û�����λ��ͨ��
    //ͨ���ͻ�ID�͵���ȡ�����Ӧ��ͨ����
    bool getChanNoByInfo(const int &sCustId, string &city_code, string &channelNo);
    //end

    AbstractFieldSelector *generateSelect(char *r_strRule, const char r_delimiter, const size_t r_maxLen);

    //��׼��Ŀ¼
    void standardPath(string &r_path);

    //liujq kafka У��kafka topic and partitionӦ��Ŀ¼�����õ�MDS
	bool isPath(const string &r_pathName);
};

#endif
