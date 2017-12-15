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
    bool BackupFile(string sSrcFileName, string sDestFileName); //�����ļ�
    bool ProcessFile(const string &sSrcFileName);  //�����ļ�
    
    bool ProcessKafka();  //����kafka��Ϣ liujq
    
    int getFileName(); //��ȡ�ļ�
    bool commit();
    bool redoCommit();//add by xingq
    bool isValidPath(const char *r_path);
    bool checkConfig();
    string numToStr(const long &r_num);
    //void onSignal(const int sig);

    SplitConfig m_config;   //������Ϣ����
    string m_filename;     //��ȡ��ԭʼ�ļ���

    char *m_fieldValue;//�ֶλ�����
    //add by yueyq  for TFS:362748   split ���ݵ��б����ٸ����û�����λ��ͨ��
    char *m_cityValue;//�����ֶλ�����

//liujq kafka
	bool m_isHaveErrData;  //�Ƿ��д�������
	vector<long> m_errDataOffset;  //��ȡ��д�������Ϣ��offset
	map<long,string> m_rowidall;//offset��jsonͷ��Ӧ��ϵ


	
    //end
private:
    FileOperate fileOper;//sdfs�ļ���������
    FileOperate fileOpertKafkaInput;//kafka�ļ��������� liujq
    FileOperate fileOperKafkaOutput;//kafka�ļ��������� liujq
    char *m_sCdrBuf; //�洢һ��������¼
    int m_totalCdr; //�����ļ���¼��
    vector<string> m_CdrVec;//����һ���ļ��Ļ���

    Directory *m_dir;

    int m_fileNum;//��ȡ���ļ�����
    string m_outFileName;//����ļ���

    map<string , FileOperate * > m_outmap;
    map<string, int> tmpFileNameMap; //<���ļ���,����>
    map<string, int> srcFullFileMap; //<ԭʼ�ļ���,����>

    map<string , FileOperate * > m_outmapKafka;
    map<string, int> m_outFullFileMapKafka;//<���ļ���,����>	

    //add by xingq start redo�ļ���������,��ȡ����
    map<string , FileOperate * > m_redoCdrOutmap;
    map<string, int> m_tmpRedoFileNameMap; //<redo�����ļ���,����>
    string m_outRedoFileName;
    bool m_resetRedoFileName;
    //add by xingq end

    int m_fsortmethod;//�ļ�����ʽ
    string m_sCdrFileName;//��·�����ļ���
    bool m_LogIsOpen;//��־�򿪱�־
    bool m_LogRedoIsOpen;//redo��־�򿪱�־ add by xingq
    int m_splitPos;//�Ժ�λȡ��ַ�
    time_t m_dealTime;//����ʱ��
    char m_beginTime[14+1];  //����ʱ��YYYYMMDDHHMISS liujq kafka
    time_t m_redoDealTime;//redo����ʱ�� add by xingq
    time_t m_startTime;//����ʱ��
    bool resetFileName;//�Ƿ������ļ���
    int m_timeout;//��ʱ����
    int m_redoTimeout;//redo�ļ���ʱ��������� add by xingq
    bool m_redoOpen;//�Ƿ��redo�ļ����������� add by xingq
    int m_totalTimeout;//�ܴ���ʱʱ��
    bool outFlag;//�Ƿ��ύ
    bool result;//�Ƿ��ύ�ɹ�

    map<string, int> m_seqMap; //���к�
    map<string, int> m_seqRedoMap; //redo���к� add by xingq

    //alter for x86 add deal duration 20151204 by zhangyw
    struct timeval startDealTime, endDealTime;
    int duration;//����ʱ��
#ifdef _DEBUG_
    int sgetsNum;
    int sputsNum;
#endif
};

#endif
