#include "SplitApp.h"
#include "SplitConfig.h"


SplitApp g_application;
Application *const theApp = &g_application;

SplitApp::SplitApp()
{
    m_fileNum = 0;
    m_LogIsOpen = false;
    m_LogRedoIsOpen = false;
    resetFileName = true;
    m_resetRedoFileName = true;
    m_fieldValue = new char[MAX_FIELD_LENGTH];
    //add by yueyq  for TFS:362748   split 根据地市编码再根据用户后四位分通道
    m_cityValue = new char[MAX_FIELD_LENGTH];
    //end
    m_sCdrBuf = new char[MAX_CDR_LENGTH + 1];
    result = true;
}

SplitApp:: ~SplitApp()
{
    m_seqMap.clear();
    m_seqRedoMap.clear();
    delete m_dir;
    delete m_fieldValue;
    delete m_sCdrBuf;
    //add by yueyq  for TFS:362748   split 根据地市编码再根据用户后四位分通道
    delete m_cityValue;
    m_cityValue = NULL;
    //end
    m_fieldValue = NULL;
    m_sCdrBuf = NULL;
}

bool SplitApp::initialization()
{
    setoptstr("c:");

    if (!Application::initialization())
    {
        return false;
    }

    //初始化数据
    m_totalCdr = 0;

    if(!m_config.GetSystemConfigInfo(m_theSysParam, m_name, m_channelNo))
    {
        theErrorMessages->insert(EAPPFRM_SYSVAL, "read config error...");
        return false;
    }
    m_timeout = m_config.m_timeout;
    //add by xingq start redo单独处理开关
    if(m_config.m_redoOpen == 1)
    {
        m_redoOpen = true;
        m_redoTimeout = m_config.m_redoTimeout;
    }
    else
    {
        m_redoOpen = false;
    }
    //add by xingq end
    m_totalTimeout = m_config.m_totalTimeout;

		//liujq kafka 从数据库读取配置 td_b_channel_param,td_b_mds_config_group
		if (false == m_config.getFsMode(m_channelNo))
		{
#ifdef _DEBUG_
			cout << __FILE__ << __LINE__ << " getChannelParamFromDb false!" << endl;
#endif
			return false;
		}


    //获取数据库通道信息,并将信息存入m_custChannel中
    m_config.GetAllChannel();


#ifndef _DEBUG_
    set_asdaemon(true);
    set_onlyone(true);
    set_runinbkg(true);
#endif
    return true;
}

bool SplitApp::beforeLoop()
{
    if(Application::beforeLoop() == false)
    {
        return false;
    }

#ifdef USE_SDFS
    string message = "init sdfs";
    if(!ServerInfo::getInstance()->initServer(message))
    {
#ifdef _DEBUG_
        cout << "sdfs server init error! error message:" << message << "file:" << __FILE__ << "line:" << __LINE__ << endl;
#endif
        message = "SDFS error!ErrMsg=[" + message + "]";
        theErrorMessages->insert(EAPPFRM_SYSVAL, message.c_str());
        return false;
    }
    ServerInfo::getInstance()->initAppInfo(m_name, m_channelNo);

    //进行dealRenameErrorFile处理
    if(!ServerInfo::getInstance()->dealRenameErrorFile(message))
    {
        cout << __FILE__ << __LINE__ << "dealRenameErrorFile false,检查bin目录下该通道rename日志，是否需要手动处理 file" << endl;
        theErrorMessages->insert(EAPPFRM_SYSVAL, "dealRenameErrorFile false,检查bin目录下该通道rename日志，是否需要手动处理 file");
        return false;
    }
    //进行dealRenameErrorFile处理
    if(!ServerInfo::getInstance()->dealCloseErrorFile(message))
    {
        cout << __FILE__ << __LINE__ << "dealCloseErrorFile false,检查bin目录下该通道close日志，是否需要手动处理，并验证bin/out/ 关闭失败缓存文件是否生成 file" << endl;
        theErrorMessages->insert(EAPPFRM_SYSVAL, "dealCloseErrorFile false,检查bin目录下该通道close日志，是否需要手动处理，并验证bin/out/ 关闭失败缓存文件是否生成 file");
        return false;
    }
    if(!fileOper.setSDFS(ServerInfo::getInstance()->m_pSdfs))
    {
#ifdef _DEBUG_
        cout << "sdfs operor init error!" << fileOper.getErrInfo() << endl;
#endif
        return false;
    }
#endif


    //初始化kafka、路径校验
    if(m_config.m_fsMode == GETFILE_FROM_KAFKA || m_config.m_fsMode == GETFILE_FROM_KAFKA_SDFS)
    {
        string t_msg = "";

        //正常输出kafka集群初始化
        if (KafkaInfo::getInstance()->init(m_config.m_mdsPathvector, t_msg, m_name, "", "", true) == -1)
        {
#ifdef _DEBUG_
            cout << __FILE__ << __LINE__ << t_msg << endl;
#endif
            m_theErrorMessages->insert(ERROR_KAFKA_INIT_ERROR, "t_normalCDR->init failed!" + t_msg);
            return false;
        }

        //kafka topic与partition校验
        //正常消息输出集群校验
        for(vector<string>::iterator iter = m_config.m_mdsPathvector.begin(); iter != m_config.m_mdsPathvector.end(); ++iter)
        {
            if(!m_config.isPath(*iter))
            {
                m_theErrorMessages->insert(ERROR_KAFKA_PATH_ERROR, "kafka目录:" + (*iter) + " 校验失败!");
                return false;
            }
        }
    }
    //add by liujq kafka end


    if(!checkConfig())
    {
        return false;
    }

    //liujq kafka
    if(m_config.m_fsMode == GETFILE_FROM_KAFKA || m_config.m_fsMode == GETFILE_FROM_KAFKA_SDFS)
    {
        fileOpertKafkaInput.setKAFKA(KafkaInfo::getInstance());
        for(vector<string>::iterator itr = m_config.m_kafkaParamVec.begin(); itr != m_config.m_kafkaParamVec.end(); itr = itr + 4)
        {
#ifdef _DEBUG_
            cout << "*itr = " << *itr << endl;
            cout << "*(itr+1) = " << *(itr + 1) << endl;
            cout << "*(itr+2) = " << *(itr + 2) << endl;
            cout << "*(itr+3) = " << *(itr + 3) << endl;
#endif
            fileOpertKafkaInput.setKafkaParam(*itr, *(itr + 1), *(itr + 2), *(itr + 3));
        }


        fileOpertKafkaInput.setJsonBulkSize(m_config.m_jsonBulkSize);

    }
    if(m_config.m_fsMode == GETFILE_FROM_SDFS)
    {
        //设置输入目录
        m_dir = new Directory();
        m_fsortmethod = Directory::SF_MODIFYTIME;
        m_dir->setPath(m_config.m_inputPath.c_str());
        m_dir->setFilter(m_config.m_prefix.c_str());
        m_dir->setMaxScan(1000);
    }

    //设置输入目录
    //m_dir = new Directory();
    //m_fsortmethod = Directory::SF_MODIFYTIME;
    //m_dir->setPath(m_config.m_inputPath.c_str());
    //m_dir->setFilter(m_config.m_prefix.c_str());
    //m_dir->setMaxScan(1000);

    //按后几位取余
    switch(m_config.m_split_pos)
    {
    case ONE:
    {
        m_splitPos = 10;
        break;
    }
    case TWO:
    {
        m_splitPos = 100;
        break;
    }
    case THREE:
    {
        m_splitPos = 1000;
        break;
    }
    case FOUR:
    {
        m_splitPos = 10000;
        break;
    }
    default:
    {
        m_splitPos = 10000;
        break;
    }
    }

    return true;
}

bool SplitApp::processopt(int optopt, const char *optarg)
{
    bool rc = true;
    switch (optopt)
    {
    case 'c':
    case ':':
    case '?':
    default:
        rc = Application::processopt(optopt, optarg);
    }
    return rc;
}

bool SplitApp::loopProcess()
{
    if(m_stop) return false;
    int result = 0;
    string t_msg;

    //add by liujq kakfa
    if(m_config.m_fsMode == GETFILE_FROM_KAFKA || m_config.m_fsMode == GETFILE_FROM_KAFKA_SDFS)
    {
        //如果是循环中读取的第一个文件 记录时间
        if(m_fileNum == 0) time(&m_startTime);
        time(&m_dealTime);
        strcpy(m_beginTime, (StringUtil::format(m_startTime  , "%Y%m%d%H%M%S")).c_str());//日志使用
        //StringUtil::format(m_dealTime,"%Y%m%d%H%M%S");//日志使用
        time(&m_redoDealTime);
        if(!m_LogIsOpen)
        {
            theLoggerProcessID = theLogger->pBegin();  //日志
            gettimeofday( &startDealTime, NULL);//alter for x86 add deal duration 20151204 by zhangyw
            m_LogIsOpen = true;
        }
        if(m_redoOpen == true)
        {
            if(!m_LogRedoIsOpen)
            {
                theLoggerProcessID = theLogger->pBegin();  //日志
                gettimeofday( &startDealTime, NULL);//alter for x86 add deal duration 20151204 by zhangyw
                m_LogRedoIsOpen = true;
            }
        }

        if(ProcessKafka() == false)
        {
            m_stop = true;
            return false;
        }
        //关闭日志
        if(m_LogIsOpen)
        {
            theLogger->pEnd(theLoggerProcessID);
            m_LogIsOpen = false;
        }

    }
    if(m_config.m_fsMode == GETFILE_FROM_SDFS)
    {
        while(m_stop == false && (result = getFileName()) > 0)
        {
            //如果是循环中读取的第一个文件 记录时间
            if(m_fileNum == 0) time(&m_startTime);
            time(&m_dealTime);
            time(&m_redoDealTime);
            if(!m_LogIsOpen)
            {
                theLoggerProcessID = theLogger->pBegin();  //日志
                gettimeofday( &startDealTime, NULL);//alter for x86 add deal duration 20151204 by zhangyw
                m_LogIsOpen = true;
            }
            if(m_redoOpen == true)
            {
                if(!m_LogRedoIsOpen)
                {
                    theLoggerProcessID = theLogger->pBegin();  //日志
                    gettimeofday( &startDealTime, NULL);//alter for x86 add deal duration 20151204 by zhangyw
                    m_LogRedoIsOpen = true;
                }
            }
            theLogger->info("[GET FILE]%s", m_sCdrFileName.c_str());
            //m_fileNum++;
            if(ProcessFile(m_filename) == false)
            {
                m_stop = true;
                if(m_fileNum != 0 )
                {
                    redoCommit();
                    commit();
                }
                result = false;
                return false;
            }
            //判断总处理时间是否超时
            if(time(NULL) - m_startTime >= m_totalTimeout)
            {
                outFlag = true;
            }
            //add by xingq start redo文件单独处理
            if(outFlag)
            {
                if(redoCommit() == false || commit() == false)
                {
                    m_stop = true;
                    return false;
                }
            }
            if(m_redoOpen == true)
            {
                if(time(NULL) - m_redoDealTime >= m_redoTimeout)
                {
#ifdef _DEBUG_
                    cout << __FILE__ << __LINE__ << "[fileNum]=" << m_fileNum << " [time(NULL)-m_redoDealTime]=" << time(NULL) - m_redoDealTime << endl;
#endif
                    if(redoCommit() == false)
                    {
                        m_stop = true;
                        return false;
                    }
                }
            }
            //add by xingq end redo文件单独处理
        }

        if(result == 0)
        {
#ifdef _DEBUG_
            cout << "等待文件..." << endl;
#endif
            // 等待超时后没有输入文件,将work文件输出
            //总处理时间超时后，将work文件输出
            if(m_fileNum != 0 && ((time(NULL) - m_dealTime >= m_timeout) || (time(NULL) - m_startTime >= m_totalTimeout)))
            {
                cout << __FILE__ << __LINE__ << "[fileNum]=" << m_fileNum << " [time(NULL)-m_dealTime]=" << time(NULL) - m_dealTime << endl;
                if( redoCommit() == false || commit() == false)
                {
                    m_stop = true;
                    return false;
                }
            }
            //add by xingq start redo文件单独处理
            if(m_redoOpen == true)
            {
                if(m_fileNum != 0 && (time(NULL) - m_redoDealTime >= m_redoTimeout))
                {
#ifdef _DEBUG_
                    cout << __FILE__ << __LINE__ << "[fileNum]=" << m_fileNum << " [time(NULL)-m_redoDealTime]=" << time(NULL) - m_redoDealTime << endl;
#endif
                    if(redoCommit() == false)
                    {
                        m_stop = true;
                        return false;
                    }
                }
            }
            //add by xingq end redo文件单独处理
            sleep(5);

            //取完文件再次扫描目录
#ifndef USE_SDFS
            m_dir->scanFiles((Directory::SortFlags)m_fsortmethod);
#else
            m_dir->scanFiles(ServerInfo::getInstance()->m_pSdfs, (Directory::SortFlags)m_fsortmethod);
#endif
        }
        else if(result < 0)
        {
            m_stop = true ;
            return false;
        }

    }


    return Application::loopProcess();
}

int SplitApp::getFileName()
{
    char fileName[256];
    int ret;

    ret = m_dir->getFile(fileName, sizeof(fileName));
    if (fileName[0] == '#' || fileName[0] == '%')
        return getFileName();
    m_filename = fileName;
    m_sCdrFileName = m_config.m_inputPath + m_filename;
    return ret;
}

//liujq kafka
bool SplitApp::ProcessKafka()
{
    m_totalCdr = 0;
    int trycount = 0;

    m_CdrVec.clear();

    string tempCdr;//临时存放话单
    int t_cdrLen;
    const char *t_pCdrEnd;
    int t_iResult = 0;
    string t_realOutPath;//输出目录
    string t_outfileName;//输出全文件名 MDS/业务_省份_....

    map<string , FileOperate *>::iterator it_out_kakfa;


    //time(&m_dealTime);
    //-----------------------------------读取消费消息--------------------------------------
    int t_ret1 = 0;
    //若为MDS系统，则需要将文件名转换为对应topic
    //open认为是扫描目录，则对此在此过程中对topic进行扫描，并对处理的文件进行文件名命名
    //待优化为1、扫描多个topic实现，2、文件名命名优化
    //打开文件失败
    if(m_config.m_maxCdrOnceRead <= 0)	//一次读取的最大话单数已设置,则按设置的数目读取,否则按照默认数目
    {
        t_ret1 = fileOpertKafkaInput.open(m_config.m_inputPathMds.c_str(), "r");
    }
    else
    {
        t_ret1 = fileOpertKafkaInput.open(m_config.m_inputPathMds.c_str(), "r", m_config.m_maxCdrOnceRead);
    }

    if(t_ret1 != 0)
    {
        char msg[1024];
        memset(msg, 0x00, 1024);
        sprintf(msg, "kafka open {%s} error!Error:%s", m_config.m_inputPathMds.c_str(), fileOpertKafkaInput.getErrInfo());
        theErrorMessages->insert(ERROR_KAFKA_OPEN_ERROR, msg);
        return false;
    }

    //消费过程遇到错误数据
    m_isHaveErrData = fileOpertKafkaInput.getErrdata(m_errDataOffset);


label1:

    //返回当前所有offset与json头对应关系map<long,string>
    if(fileOpertKafkaInput.rowidall(m_rowidall) < 0)
    {
#ifdef _DEBUG_
        cout << __FILE__ << __LINE__ << " rowidall failed!" << endl;
#endif
        char errmsg[1024];
        memset(errmsg, 0x00, 1024);
        sprintf(errmsg, "[%s:%d] %s get rowidall error!msg:%s", __FILE__, __LINE__, m_config.m_inputPathMds.c_str(), fileOpertKafkaInput.getErrInfo());
        theErrorMessages->insert(ERROR_KAFKA_ROWIDALL_ERROR, errmsg);

        return false;
    }
    if (m_rowidall.empty())//没有获取到消息
    {
        ++trycount;
        if (trycount < 6)
        {
            sleep(10 * trycount);
            goto label1;
        }
#ifdef _DEBUG_
        cout << m_config.m_inputPathMds << " 读取不到消息,应该为异常情况,请核查处理... " << " " << __FILE__ << __LINE__ << endl;
#endif
        theLogger->info("%s没有可消费的消息!程序退出", m_config.m_inputPathMds.c_str()); //获取消息
        return false;
    }

    //组装文件名,取第一个消息的消息头
    //fullname=/topic/partition/业务_省份_时间（年月日时分秒）_OFFSET(begin-end)
    char t_fileName[500];
    string t_sfileName;
    memset(t_fileName, 0x00, 500);
    string t_json = (m_rowidall.begin())->second;
    string t_cdrType = t_json;
    int iPos = t_json.find_first_of("_");
    if (iPos != t_json.npos && iPos > 0 && iPos < t_json.length() - 1)
    {
        t_cdrType = t_json.substr(0, iPos);
    }
    sprintf(t_fileName, "%s_%s_%s_%ld-%ld", t_cdrType.c_str(), m_config.m_prov_code.c_str(),
            m_beginTime,
            (m_rowidall.begin())->first,
            (m_rowidall.rbegin())->first);

    t_sfileName = m_config.m_inputPathMds + "/" + t_json;

    theLogger->info("[GET MSG] %s", t_sfileName.c_str()); //获取消息

    memset(m_sCdrBuf, 0, (MAX_CDR_LENGTH + 1)*sizeof(char));//初始化

    while(1)
    {
        if(fileOpertKafkaInput.gets(m_sCdrBuf, MAX_CDR_LENGTH) != NULL)
        {
            //cout << "m_sCdrBuf = " << m_sCdrBuf << endl;
            if(m_sCdrBuf[0] == 0)
            {
                continue;
            }

            ++m_totalCdr;
            t_cdrLen = strlen(m_sCdrBuf);

            //去掉最后换行符
            if(m_sCdrBuf[t_cdrLen - 1] == '\n')
            {
                m_sCdrBuf[t_cdrLen - 1] = '\0';
                t_cdrLen--;
            }
#ifdef _DEBUG_
            sgetsNum++;
#endif
            tempCdr = m_sCdrBuf;
            //cout<<"tempCdr = "<<m_sCdrBuf<<endl;
            m_CdrVec.push_back(tempCdr);
        }
        else
        {
            if(fileOpertKafkaInput.eof() == 0)
            {
                char errmsg[1024];
                memset(errmsg, 0x00, 1024);
                sprintf(errmsg, "[%s:%d] %s sget file error!msg:%s", __FILE__, __LINE__, m_config.m_inputPathMds.c_str(), fileOpertKafkaInput.getErrInfo());
                theErrorMessages->insert(ERROR_FILE_READ, errmsg);
                return false;
            }
            break;
        }

    }

    //打印日志
    // "type=in,call_duration=0,data=0,baseFee=0,FEE=0,offset_begin=%d,offset_end=%d",
    //(m_rowidall.begin())->first,(m_rowidall.rbegin())->first
    theLogger->pInput(theLoggerProcessID, t_fileName, m_totalCdr, "MsgNum=%d,offset_begin=%d,offset_end=%d", m_rowidall.size(), (m_rowidall.begin())->first, (m_rowidall.rbegin())->first);

    for(int i = 0; i < m_CdrVec.size(); i++)
    {
        string t_outputPathMds; //输出目录中,通道后面配置的MDS

        memset(m_sCdrBuf, 0, MAX_CDR_LENGTH + 1);
        strcpy(m_sCdrBuf, m_CdrVec[i].c_str());

        //cout<<"----------m_sCdrBuf1= "<<m_sCdrBuf<<endl;

        long custId;
        t_pCdrEnd = m_sCdrBuf + strlen(m_sCdrBuf);
        t_iResult = m_config.m_fidSelect->selectFieldValue(m_sCdrBuf, t_pCdrEnd, m_fieldValue, MAX_CDR_LENGTH);
        custId = atol(m_fieldValue);

        int t_realCust	= custId % m_splitPos; //以最后四位确定分发目录
#ifdef _DEBUG_
        cout << " custId:" << custId << " realCust:" << t_realCust << endl;
#endif
        //add by yueyq	for TFS:362748	 split 根据地市编码再根据用户后四位分通道
        string city_code;
        if(m_config.m_openCity == "Y")
        {
            t_pCdrEnd = m_sCdrBuf + strlen(m_sCdrBuf);
            t_iResult = m_config.m_citySelect->selectFieldValue(m_sCdrBuf, t_pCdrEnd, m_cityValue, MAX_CDR_LENGTH);
            city_code = m_cityValue;
        }
#ifdef _DEBUG_
        cout << " city_code:" << city_code << "   " << __FILE__ << __LINE__ << endl;
        cout << "m_config.m_openCity:" << m_config.m_openCity << "	" << __FILE__ << __LINE__ << endl;
#endif
        string t_paths;
        //根据客户ID和库号求通道号
        /**
        if(m_config.getChanNoByInfo(t_realCust,t_paths) == false)
        {
        	char errmsg[256];
        	memset(errmsg,0x00,256);
        	sprintf(errmsg,"[%s:%d]get outchannel error! cust_id=%d !",__FILE__,__LINE__,t_realCust);
        	theErrorMessages->insert(E_OUT_CHANNEL,errmsg);
        	return false;
        }
        **/
        if(city_code != "" && m_config.m_openCity == "Y")
        {
            if(m_config.getChanNoByInfo(t_realCust, city_code, t_paths) == false)
            {
                char errmsg[256];
                memset(errmsg, 0x00, 256);
                sprintf(errmsg, "[%s:%d]get outchannel error! cust_id=%d !	city_code=%s", __FILE__, __LINE__, t_realCust, city_code.c_str());
                theErrorMessages->insert(E_OUT_CHANNEL, errmsg);
                return false;
            }
        }
        else if ( m_config.m_openCity == "N")
        {
            if(m_config.getChanNoByInfo(t_realCust, t_paths) == false)
            {
                char errmsg[256];
                memset(errmsg, 0x00, 256);
                sprintf(errmsg, "[%s:%d]get outchannel error! cust_id=%d !", __FILE__, __LINE__, t_realCust);
                theErrorMessages->insert(E_OUT_CHANNEL, errmsg);
                return false;
            }
        }
        else
        {
            char errmsg[256];
            memset(errmsg, 0x00, 256);
            sprintf(errmsg, "话单有问题，请核查话单city_code!  city_code=%s", __FILE__, __LINE__, t_realCust, city_code.c_str());
            theErrorMessages->insert(E_OUT_CHANNEL, errmsg);
            return false;
        }
        //end

#ifdef _DEBUG_
        cout << __FILE__ << __LINE__ << " channel:" << t_paths << endl;
#endif
        //获取分发目录,并解析目录后面的MDS
        t_realOutPath = m_config.m_outChanInfo[t_paths];
        vector<string> t_pathsVec;
        t_pathsVec.clear();
        if (strlen(t_realOutPath.c_str()) > 0)
        {
            t_pathsVec = StringUtil::split(t_realOutPath, ";");
            if(t_pathsVec.size() == 2 && t_pathsVec[1].substr(0, 3) == "MDS")
            {
                string t_s = t_pathsVec[1];
                if(t_s.substr(t_s.length() - 1) == "/") //去掉最后的斜杠
                {
                    t_s = t_s.substr(0, t_s.length() - 1);
                }
                t_outputPathMds = t_s;
            }

        }
        //----------------------------------输出写消息--------------------------------------
        //fileOperKafkaOutput.setKAFKA(KafkaInfo::getInstance());
        int t_ret2;

        //拼文件名 输出的MDS/业务_省份_....
        t_outfileName = t_outputPathMds + "/" + t_fileName;


        map<string, int>::iterator t_itr = m_outFullFileMapKafka.find(t_outfileName);
        if (t_itr == m_outFullFileMapKafka.end())
        {
            m_outFullFileMapKafka[t_outfileName] = 1;
        }
        else
        {
            (t_itr->second)++;
        }

        it_out_kakfa =  m_outmapKafka.find(t_outputPathMds);
        if(it_out_kakfa == m_outmapKafka.end())
        {
            m_outmapKafka[t_outputPathMds] = new FileOperate();
            m_outmapKafka[t_outputPathMds]->setKAFKA(KafkaInfo::getInstance());

            if(m_config.m_maxCdrOnceRead <= 0)	//一次读取的最大话单数已设置,则按设置的数目读取,否则按照默认数目
            {
                t_ret2 = m_outmapKafka[t_outputPathMds]->open(t_outfileName.c_str(), "w");
            }
            else
            {
                t_ret2 = m_outmapKafka[t_outputPathMds]->open(t_outfileName.c_str(), "w", m_config.m_maxCdrOnceRead);
            }

            if(t_ret2 != 0)
            {
                char msg[1024];
                memset(msg, 0x00, 1024);
                sprintf(msg, "open file {%s} error!Error:%s", t_outputPathMds.c_str(), fileOperKafkaOutput.getErrInfo());
                theErrorMessages->insert(ERROR_FILE_OPEN, msg);
                return false;
            }
        }


        //cout<<"----------m_sCdrBuf2= "<<m_sCdrBuf<<endl;

        if(m_outmapKafka[t_outputPathMds]->puts(m_sCdrBuf) == -1)//写消息
        {
            char msg[512];
            memset(msg, 0x00, 512);
            sprintf(msg, "[%s:%d]write mds:%s error!!msg:%s", __FILE__, __LINE__, t_outputPathMds.c_str(), fileOperKafkaOutput.getErrInfo());
            theErrorMessages->insert(ERROR_WRITE_FILE, msg);
            return false;
        }
#ifdef _DEBUG_
        sputsNum++;
#endif
    }

    if (fileOpertKafkaInput.close() != 0)
    {
        theErrorMessages->insert(ERROR_FILE_CLOSE, "close input msg: " + m_config.m_inputPathMds + " error!!!");
        theErrorMessages->insert(ERROR_FILE_CLOSE, fileOpertKafkaInput.getErrInfo());
        //return false;
    }

    //保存offset 因为kafka模式下rename的参数实际是没用的,所以参数随便传
    if(fileOpertKafkaInput.rename(t_realOutPath.c_str(), t_realOutPath.c_str()) != 0 )
    {
        char errmsg[512];
        memset(errmsg, 0x00, 512);
        sprintf(errmsg, "[FILE:%s,LINE:%d] msg:%s seve offset error! %s", __FILE__, __LINE__, m_config.m_inputPathMds.c_str(), fileOpertKafkaInput.getErrInfo());
        theErrorMessages->insert(ERROR_KAFKA_SAVE_OFFSET_ERROR, errmsg);
        return false;
    }

    for(it_out_kakfa = m_outmapKafka.begin(); it_out_kakfa != m_outmapKafka.end(); ++it_out_kakfa)
    {
        if ((it_out_kakfa->second)->close() != 0)
        {
            theErrorMessages->insert(ERROR_FILE_CLOSE, "close output msg: " + m_config.m_inputPathMds + " error!!!");
            theErrorMessages->insert(ERROR_FILE_CLOSE, (it_out_kakfa->second)->getErrInfo());
        }
        delete it_out_kakfa->second;
        it_out_kakfa->second = NULL;
    }

    m_outmapKafka.clear();
    m_outmapKafka.swap(m_outmapKafka);

    for(map<string, int>::iterator t_itr2 = m_outFullFileMapKafka.begin(); t_itr2 != m_outFullFileMapKafka.end(); t_itr2++)
    {
        theLogger->pOutput(theLoggerProcessID, t_itr2->first, t_itr2->second);
    }

    //m_outFullFileMapKafka.clear();
    //m_outFullFileMapKafka.swap(m_outFullFileMapKafka);
    m_outFullFileMapKafka.erase(m_outFullFileMapKafka.begin(), m_outFullFileMapKafka.end());

    return true;
}
bool SplitApp::ProcessFile(const string &sSrcFileName)
{
    m_totalCdr = 0;
    int t_iResult = 0;
    const char *t_pCdrEnd;
    outFlag = false;//是否开始输出
    string m_sCdrFileNameOut, sCdrFileErrOut;
    m_CdrVec.clear();

    //处理文件
    //循环读取文件中每条话单进行处理
    int trycount = 0;
    map<string , FileOperate *>::iterator it_out;

label1:
    if(fileOper.open(m_sCdrFileName.c_str(), "r") < 0)
    {
#ifdef _DEBUG_
        cout << "sopen file:{" << m_sCdrFileName << "} failed!" << __FILE__ << __LINE__ << endl;
#endif
        theErrorMessages->insert(ERROR_FILE_OPEN, " open src file: " + m_sCdrFileName + " error!!");
        theErrorMessages->insert(ERROR_FILE_OPEN, fileOper.getErrInfo());
        return false;
    }
    int t_cdrLen, t_sputRet;
    //add by xingq start redo文件单独处理
    bool m_isRedoCdr = false;
    if(m_redoOpen == true)
    {
        if((sSrcFileName.find("redo") != string::npos)
                ||
                (sSrcFileName.find("REDO") != string::npos)
          )
        {
            m_isRedoCdr = true;
        }
    }
    //增加变量resetFileName,修复bug:第50个文件单独写入新文件.

    if(m_isRedoCdr == true)
    {
        if(m_resetRedoFileName)
        {
            m_resetRedoFileName = false;
            m_outRedoFileName = sSrcFileName;
        }
    }
    else
    {
        if(resetFileName)
        {
            resetFileName = false;
            m_outFileName = sSrcFileName;
        }
    }
    //add by xingq end redo文件单独处理
    string tempCdr;//临时存放话单

    while(1)
    {
        if(fileOper.gets(m_sCdrBuf, MAX_CDR_LENGTH) != NULL)
        {
            if(m_sCdrBuf[0] == 0)
            {
                continue;
            }

            ++m_totalCdr;
            t_cdrLen = strlen(m_sCdrBuf);

            //去掉最后换行符
            if(m_sCdrBuf[t_cdrLen - 1] == '\n')
            {
                m_sCdrBuf[t_cdrLen - 1] = '\0';
                t_cdrLen--;
            }
#ifdef _DEBUG_
            sgetsNum++;
#endif
            tempCdr = m_sCdrBuf;
            m_CdrVec.push_back(tempCdr);
        }
        else
        {
            if(fileOper.eof() == 0)
            {
                char errmsg[1024];
                memset(errmsg, 0x00, 1024);
                sprintf(errmsg, "[%s:%d]sget file error !file is:%s!%s", __FILE__, __LINE__, m_sCdrFileName.c_str(), fileOper.getErrInfo());
                theErrorMessages->insert(ERROR_FILE_READ, errmsg);
                return false;
            }
            break;
        }

    }
    for(int i = 0; i < m_CdrVec.size(); i++)
    {
        memset(m_sCdrBuf, 0, MAX_CDR_LENGTH + 1);
        strcpy(m_sCdrBuf, m_CdrVec[i].c_str());

        long custId;
        t_pCdrEnd = m_sCdrBuf + strlen(m_sCdrBuf);
        t_iResult = m_config.m_fidSelect->selectFieldValue(m_sCdrBuf, t_pCdrEnd, m_fieldValue, MAX_CDR_LENGTH);
        custId = atol(m_fieldValue);

        int t_realCust  = custId % m_splitPos; //以最后四位确定分发目录
#ifdef _DEBUG_
        //cout<<" custId:"<<custId <<" realCust:"<<t_realCust<<endl;
#endif

        //add by yueyq  for TFS:362748   split 根据地市编码再根据用户后四位分通道
        string city_code;
        if(m_config.m_openCity == "Y")
        {
            t_pCdrEnd = m_sCdrBuf + strlen(m_sCdrBuf);
            t_iResult = m_config.m_citySelect->selectFieldValue(m_sCdrBuf, t_pCdrEnd, m_cityValue, MAX_CDR_LENGTH);
            city_code = m_cityValue;
        }
#ifdef _DEBUG_
        cout << " city_code:" << city_code << "   " << __FILE__ << __LINE__ << endl;
        cout << "m_config.m_openCity:" << m_config.m_openCity << "  " << __FILE__ << __LINE__ << endl;
#endif
        string t_paths;
        //根据客户ID和库号求通道号
        /**
        if(m_config.getChanNoByInfo(t_realCust,t_paths) == false)
        {
            char errmsg[256];
            memset(errmsg,0x00,256);
            sprintf(errmsg,"[%s:%d]get outchannel error! cust_id=%d !",__FILE__,__LINE__,t_realCust);
            theErrorMessages->insert(E_OUT_CHANNEL,errmsg);
            return false;
        }
        **/
        if(city_code != "" && m_config.m_openCity == "Y")
        {
            if(m_config.getChanNoByInfo(t_realCust, city_code, t_paths) == false)
            {
                char errmsg[256];
                memset(errmsg, 0x00, 256);
                sprintf(errmsg, "[%s:%d]get outchannel error! cust_id=%d !  city_code=%s", __FILE__, __LINE__, t_realCust, city_code.c_str());
                theErrorMessages->insert(E_OUT_CHANNEL, errmsg);
                return false;
            }
        }
        else if ( m_config.m_openCity == "N")
        {
            if(m_config.getChanNoByInfo(t_realCust, t_paths) == false)
            {
                char errmsg[256];
                memset(errmsg, 0x00, 256);
                sprintf(errmsg, "[%s:%d]get outchannel error! cust_id=%d !", __FILE__, __LINE__, t_realCust);
                theErrorMessages->insert(E_OUT_CHANNEL, errmsg);
                return false;
            }
        }
        else
        {
            char errmsg[256];
            memset(errmsg, 0x00, 256);
            sprintf(errmsg, "话单有问题，请核查话单city_code!  city_code=%s", __FILE__, __LINE__, t_realCust, city_code.c_str());
            theErrorMessages->insert(E_OUT_CHANNEL, errmsg);
            return false;
        }
        //end
        string tmpCdrOutFile, tmpCdrOutFileFull;
        //输出到临时文件
        if((m_fileNum != 0 && m_fileNum % m_config.m_maxFileNum == 0))
        {
#ifdef _DEBUG_
            cout << __FILE__ << __LINE__ << " [fileNum]=" << m_fileNum << endl;
#endif
            //m_fileNum = 0;alter by zhangyw 0309
            outFlag = true;
            resetFileName = true;
            m_resetRedoFileName = true;
        }

        //文件名后追加分发通道号
label_setName:
        //add by xingq start redo文件单独处理
        if(m_isRedoCdr == true)
        {
            if(m_seqRedoMap[t_paths] == 0)
            {
                tmpCdrOutFile = m_outRedoFileName + "_" + (t_paths) ;
            }
            else
            {
                tmpCdrOutFile = m_outRedoFileName + "_" + (t_paths) + "." + numToStr(m_seqRedoMap[t_paths]);
            }
            tmpCdrOutFileFull = m_config.m_tmpPath + tmpCdrOutFile;

            map<string, int>::iterator t_itr = m_tmpRedoFileNameMap.find(tmpCdrOutFile);
            if (t_itr == m_tmpRedoFileNameMap.end())
            {
                m_tmpRedoFileNameMap[tmpCdrOutFile] = 1;
            }
            else
            {
                //增加分发的文件行数超过4万的判断
                if(t_itr->second >= m_config.m_maxFileLine)
                {
                    m_seqRedoMap[t_paths]++;
                    goto label_setName;
                }
                (t_itr->second)++;//add by xingq
            }
            it_out = m_redoCdrOutmap.find(tmpCdrOutFileFull);
            if(it_out == m_redoCdrOutmap.end())
            {
                m_redoCdrOutmap[tmpCdrOutFileFull] = new FileOperate();
#ifdef USE_SDFS
                m_redoCdrOutmap[tmpCdrOutFileFull]->setSDFS(ServerInfo::getInstance()->m_pSdfs);
                //初始化写失败话单缓存
                m_redoCdrOutmap[tmpCdrOutFileFull]->setOutPath(m_config.m_tmpPath, true);
#endif
                if(m_redoCdrOutmap[tmpCdrOutFileFull]->open(tmpCdrOutFileFull.c_str(), "w") < 0)
                {
                    theErrorMessages->insert(ERROR_FILE_OPEN, "open temp file: " + tmpCdrOutFileFull + " error!");
                    theErrorMessages->insert(ERROR_FILE_OPEN, m_redoCdrOutmap[tmpCdrOutFileFull]->getErrInfo());
#ifdef _DEBUG_
                    cout << tmpCdrOutFileFull.c_str() << " 文件打开失败" << __FILE__ << __LINE__ << endl;
#endif
                    return false;
                }
            }
            t_cdrLen = strlen(m_sCdrBuf);
            m_sCdrBuf[t_cdrLen] = '\n';
            t_sputRet = m_redoCdrOutmap[tmpCdrOutFileFull]->puts(m_sCdrBuf);
            if(t_sputRet == -1)
            {
                char msg[512];
                memset(msg, 0x00, 512);
                //add for test 20150909
                sprintf(msg, "test---------------sdfs error :%s-----%s", ServerInfo::getInstance()->m_pSdfs->errcode, ServerInfo::getInstance()->m_pSdfs->errstr);
                theErrorMessages->insert(ERROR_WRITE_FILE, msg);
                sprintf(msg, "[%s:%d]write temp file %s error!!%s", __FILE__, __LINE__, tmpCdrOutFileFull.c_str(), m_redoCdrOutmap[tmpCdrOutFileFull]->getErrInfo());
                theErrorMessages->insert(ERROR_WRITE_FILE, msg);
                return false;
            }
        }
        else
        {
            if(m_seqMap[t_paths] == 0)
            {
                tmpCdrOutFile = m_outFileName + "_" + (t_paths) ;
            }
            else
            {
                tmpCdrOutFile = m_outFileName + "_" + (t_paths) + "." + numToStr(m_seqMap[t_paths]);
            }
            tmpCdrOutFileFull = m_config.m_tmpPath + tmpCdrOutFile;

            map<string, int>::iterator t_itr = tmpFileNameMap.find(tmpCdrOutFile);
            if (t_itr == tmpFileNameMap.end())
            {
                tmpFileNameMap[tmpCdrOutFile] = 1;
            }
            else
            {
                //增加分发的文件行数超过4万的判断
                if(t_itr->second >= m_config.m_maxFileLine)
                {
                    m_seqMap[t_paths]++;
                    goto label_setName;
                }
                (t_itr->second)++;//add by xingq
            }
            it_out = m_outmap.find(tmpCdrOutFileFull);
            if(it_out == m_outmap.end())
            {
                m_outmap[tmpCdrOutFileFull] = new FileOperate();
#ifdef USE_SDFS
                m_outmap[tmpCdrOutFileFull]->setSDFS(ServerInfo::getInstance()->m_pSdfs);
                //初始化写失败话单缓存
                m_outmap[tmpCdrOutFileFull]->setOutPath(m_config.m_tmpPath, true);
#endif
                if(m_outmap[tmpCdrOutFileFull]->open(tmpCdrOutFileFull.c_str(), "w") < 0)
                {
                    theErrorMessages->insert(ERROR_FILE_OPEN, "open temp file: " + tmpCdrOutFileFull + " error!");
                    theErrorMessages->insert(ERROR_FILE_OPEN, m_outmap[tmpCdrOutFileFull]->getErrInfo());
#ifdef _DEBUG_
                    cout << tmpCdrOutFileFull.c_str() << " 文件打开失败" << __FILE__ << __LINE__ << endl;
#endif
                    return false;
                }
            }
            t_cdrLen = strlen(m_sCdrBuf);
            m_sCdrBuf[t_cdrLen] = '\n';
            t_sputRet = m_outmap[tmpCdrOutFileFull]->puts(m_sCdrBuf);
            if(t_sputRet == -1)
            {
                char msg[512];
                memset(msg, 0x00, 512);
                //add for test 20150909
                sprintf(msg, "test---------------sdfs error :%s-----%s", ServerInfo::getInstance()->m_pSdfs->errcode, ServerInfo::getInstance()->m_pSdfs->errstr);
                theErrorMessages->insert(ERROR_WRITE_FILE, msg);
                sprintf(msg, "[%s:%d]write temp file %s error!!%s", __FILE__, __LINE__, tmpCdrOutFileFull.c_str(), m_outmap[tmpCdrOutFileFull]->getErrInfo());
                theErrorMessages->insert(ERROR_WRITE_FILE, msg);
                return false;
            }
        }

#ifdef _DEBUG_
        sputsNum++;
#endif
    }
    m_isRedoCdr = false;
    //add by xingq end redo文件单独处理
    /*if (fileOper.close() != 0)
    {
        theErrorMessages->insert(ERROR_FILE_OPEN, "close src file: "+m_sCdrFileName+" error!!!");
        theErrorMessages->insert(ERROR_FILE_OPEN,fileOper.getErrInfo());
        return false;
    }
    t_pCdrEnd = NULL;*/

    //没取到记录
    bool isErrorCdr = false;
    if(m_totalCdr == 0)
    {
        ++trycount;
        if (trycount < 6)
        {
            sleep(10 * trycount);
            goto label1;
        }
        else
        {
            isErrorCdr = true;
            theErrorMessages->insert(ERROR_FILE_OPEN, "src file: " + m_sCdrFileName + "content is null!!!!!");
            theErrorMessages->insert(ERROR_FILE_OPEN, fileOper.getErrInfo());
#ifdef _DEBUG_
            //cout<<m_sCdrFileName<<" 文件读取不到记录,应该为异常情况,请核查处理... "<<" "<<__FILE__<<__LINE__<<endl;
#endif
            //处理空文件不退出alter by zhangyw 20151202
            //return false;
        }
    }
    if(!isErrorCdr)
    {
        ++m_fileNum;
#ifdef _DEBUG_
        cout << "m_fileNum = " << m_fileNum << endl;
#endif
    }

    srcFullFileMap[m_sCdrFileName] = m_totalCdr;

    if (fileOper.close() != 0)
    {
        theErrorMessages->insert(ERROR_FILE_OPEN, "close src file: " + m_sCdrFileName + " error!!!");
        theErrorMessages->insert(ERROR_FILE_OPEN, fileOper.getErrInfo());
        //return false;
    }
    t_pCdrEnd = NULL;

    //处理完毕，备份
    if(m_config.m_backupPath.length() > 0 || isErrorCdr)
    {
#ifdef _DEBUG_
        cout << "bakup file : " << m_sCdrFileName << endl;
#endif
        string sTmpBackupFile;
        if (isErrorCdr)
        {
            sTmpBackupFile = m_config.m_errPath + sSrcFileName;
        }
        else
        {
            sTmpBackupFile = m_config.m_backupPath + sSrcFileName;
        }
        //alter by zhangyw 0316
        if(fileOper.rename(m_sCdrFileName.c_str(), sTmpBackupFile.c_str()) != 0 )
        {
            char errmsg[512];
            memset(errmsg, 0x00, 512);
            sprintf(errmsg, "[FILE:%s,LINE:%d] rename[%s] to [%s] error! %s", __FILE__, __LINE__, m_sCdrFileName.c_str(), sTmpBackupFile.c_str(), fileOper.getErrInfo());
            theErrorMessages->insert(E_MOVE_FILE_ERR, errmsg);
            return false;
        }
    }
    //打印日志
    theLogger->pInput(theLoggerProcessID, m_sCdrFileName.c_str(), m_totalCdr);

    /***
    if(outFlag)
    {
        if(commit()==false)
        {
            return false;
        }
    }***/
    return true;
}


//备份文件
bool SplitApp::BackupFile(string sSrcFileName, string sDestFileName)
{
    if(fileOper.access(sDestFileName.c_str(), F_OK) == 0)
    {
        //备份文件存在先删除
        fileOper.remove(sDestFileName.c_str());
        theErrorMessages->insert(E_MOVE_FILE_ERR, "file [" + sDestFileName + "] is exist! remove already!");
    }
    if(fileOper.link(sSrcFileName.c_str(), sDestFileName.c_str()) != 0)
    {
        theErrorMessages->insert(E_MOVE_FILE_ERR, "bakup file [" + sSrcFileName + "] to [" + sDestFileName + "] error! msg:" + fileOper.getErrInfo());
        return false;
    }
    return true;
}

bool SplitApp::recovery()
{
    Application::recovery();
    return true;
}

bool SplitApp::end()
{
    if(result)
    {
        if(commit() == false)
        {
            return false;
        }
        //add by xingq start redo文件单独处理
        if(redoCommit() == false)
        {
            return false;
        }
        //add by xingq end redo文件单独处理
    }
    return Application::end();
}

bool SplitApp::commit()
{
    //提交是否成功
    //result = true;
    //重置输出文件名标志位
    resetFileName = true;
    //重置分发文件序号
    m_seqMap.clear();

    m_fileNum = 0;

    map<string , FileOperate * >::iterator it_out;
    for(it_out = m_outmap.begin(); it_out != m_outmap.end(); ++it_out)
    {
        //string localCdrFileName = it_out->first.substr(it_out->first.find_last_of('/')+1,it_out->first.length()-1);
        //cout<<"localCdrFileName:"<<localCdrFileName<<endl;
        if ((it_out->second)->close() != 0)
        {
            //关闭文件失败
            char errmsg[512];
            memset(errmsg, 0x00, 512);
            sprintf(errmsg, "[%s:%d]close [%s] error!%s", __FILE__, __LINE__, it_out->first.c_str(), (it_out->second)->getErrInfo());
            theErrorMessages->insert(ERROR_FILE_CLOSE, errmsg);
            string localCdrFileName = it_out->first.substr(it_out->first.find_last_of('/') + 1, it_out->first.length() - 1);
            (it_out->second)->writeCdrtoLocal(localCdrFileName, "");
            //记录文件rename信息,不再提交rename操作
            int loc1 = localCdrFileName.find_last_of('_');
            string localCdrFileName_temp2 = localCdrFileName.substr(loc1 + 1);
            int locdot = localCdrFileName_temp2.find_last_of('.');
            string final_name ;
            if(locdot == -1)
            {
                final_name = m_config.m_outChanInfo[localCdrFileName_temp2] + localCdrFileName;
            }
            else
            {
                final_name = m_config.m_outChanInfo[localCdrFileName_temp2.substr(0, locdot)] + localCdrFileName;
            }
            //记录close失败信息
            fileOper.closeErrorRecord(localCdrFileName, final_name);
            //tmpFileNameMap中删除该文件的提交记录
            tmpFileNameMap.erase(localCdrFileName);

            result = false;
        }
        delete it_out->second;
        it_out->second = NULL;
    }
    m_outmap.clear();
    m_outmap.swap(m_outmap);

    //分发文件
    string tmp2_outFile, out_realPath, out_realFullPath;
    string out_chan2, tmp_fileName, out_chanFinal;

    for(map<string, int>::iterator it2 = tmpFileNameMap.begin(); it2 != tmpFileNameMap.end(); it2++)
    {
        //临时文件名: 原文件_分发通道，如GPP*_101.1
        tmp_fileName = it2->first;
        tmp2_outFile = m_config.m_tmpPath + tmp_fileName;
        int loc = tmp_fileName.find_last_of('_');

        //取出分发通道101.1
        out_chan2 = tmp_fileName.substr(loc + 1);
        int locdot = out_chan2.find_last_of('.');

        //取出分发目录
        if(locdot == -1)
        {
            out_realPath = m_config.m_outChanInfo[out_chan2];
        }
        else
        {
            out_chanFinal = out_chan2.substr(0, locdot);
            out_realPath = m_config.m_outChanInfo[out_chanFinal];
#ifdef _DEBUG_
            cout << __FILE__ << __LINE__ << " 分发通道号中含有. :" << out_chan2 << ",分发目录:" << out_realPath << endl;
#endif
        }

        //最终文件名
        out_realFullPath = out_realPath + tmp_fileName;
        if(fileOper.access(tmp2_outFile.c_str(), F_OK) == 0)
        {
            int no = 0;
            //文件已存在,则追加序号
            while(true)
            {
                if(fileOper.access(out_realFullPath.c_str(), F_OK) == 0)
                {
                    out_realFullPath = out_realPath + tmp_fileName + "." + numToStr(no);
                    no++;
                }
                else break;
            }

            if(fileOper.rename(tmp2_outFile.c_str(), out_realFullPath.c_str()) != 0)
            {
                char errmsg[512];
                memset(errmsg, 0x00, 512);
                sprintf(errmsg, "[%s:%d]rename[%s] to [%s]error! %s", __FILE__, __LINE__, tmp2_outFile.c_str(), out_realFullPath.c_str(), fileOper.getErrInfo());
                theErrorMessages->insert(E_MOVE_FILE_ERR, errmsg);
                fileOper.renameErrorRecord(tmp2_outFile.c_str(), out_realFullPath.c_str());
                //return false;
                result = false;
            }
            //alter for x86 add deal duration 20151204 by zhangyw
            //theLogger->pOutput(theLoggerProcessID,out_realFullPath,it2->second);
            gettimeofday( &endDealTime, NULL);
            duration = 1000 * (endDealTime.tv_sec - startDealTime.tv_sec) + (endDealTime.tv_usec - startDealTime.tv_usec) / 1000;
            theLogger->pOutput(theLoggerProcessID, out_realFullPath, it2->second, "dealtime=%dms", duration);
        }
    }

    //清空map
    tmpFileNameMap.clear();
    srcFullFileMap.clear();

    //关闭日志
    if(m_LogIsOpen)
    {
        theLogger->pEnd(theLoggerProcessID);
        m_LogIsOpen = false;
    }
    return result;
}
//add by xingq redo文件单独处理
bool SplitApp::redoCommit()
{
    if(m_redoOpen != true)
    {
        return true;
    }
    //重置分发文件序号
    m_seqRedoMap.clear();
    m_resetRedoFileName = true;
    map<string , FileOperate * >::iterator it_out;
    for(it_out = m_redoCdrOutmap.begin(); it_out != m_redoCdrOutmap.end(); ++it_out)
    {
        if ((it_out->second)->close() != 0)
        {
            //关闭文件失败
            char errmsg[512];
            memset(errmsg, 0x00, 512);
            sprintf(errmsg, "[%s:%d]close [%s] error!%s", __FILE__, __LINE__, it_out->first.c_str(), (it_out->second)->getErrInfo());
            theErrorMessages->insert(ERROR_FILE_CLOSE, errmsg);
            string localCdrFileName = it_out->first.substr(it_out->first.find_last_of('/') + 1, it_out->first.length() - 1);
            (it_out->second)->writeCdrtoLocal(localCdrFileName, "");
            //记录文件rename信息,不再提交rename操作
            int loc1 = localCdrFileName.find_last_of('_');
            string localCdrFileName_temp2 = localCdrFileName.substr(loc1 + 1);
            int locdot = localCdrFileName_temp2.find_last_of('.');
            string final_name ;
            if(locdot == -1)
            {
                final_name = m_config.m_outChanInfo[localCdrFileName_temp2] + localCdrFileName;
            }
            else
            {
                final_name = m_config.m_outChanInfo[localCdrFileName_temp2.substr(0, locdot)] + localCdrFileName;
            }
            //记录close失败信息
            fileOper.closeErrorRecord(localCdrFileName, final_name);
            //tmpFileNameMap中删除该文件的提交记录
            m_tmpRedoFileNameMap.erase(localCdrFileName);

            result = false;
        }
        delete it_out->second;
        it_out->second = NULL;
    }
    m_redoCdrOutmap.clear();
    m_redoCdrOutmap.swap(m_redoCdrOutmap);

    //分发文件
    string tmp2_outFile, out_realPath, out_realFullPath;
    string out_chan2, tmp_fileName, out_chanFinal;

    for(map<string, int>::iterator it2 = m_tmpRedoFileNameMap.begin(); it2 != m_tmpRedoFileNameMap.end(); it2++)
    {
        //临时文件名: 原文件_分发通道，如GPP*_101.1
        tmp_fileName = it2->first;
        tmp2_outFile = m_config.m_tmpPath + tmp_fileName;
        int loc = tmp_fileName.find_last_of('_');

        //取出分发通道101.1
        out_chan2 = tmp_fileName.substr(loc + 1);
        int locdot = out_chan2.find_last_of('.');

        //取出分发目录
        if(locdot == -1)
        {
            out_realPath = m_config.m_outChanInfo[out_chan2];
        }
        else
        {
            out_chanFinal = out_chan2.substr(0, locdot);
            out_realPath = m_config.m_outChanInfo[out_chanFinal];
#ifdef _DEBUG_
            cout << __FILE__ << __LINE__ << " 分发通道号中含有. :" << out_chan2 << ",分发目录:" << out_realPath << endl;
#endif
        }

        //最终文件名
        out_realFullPath = out_realPath + tmp_fileName;
        if(fileOper.access(tmp2_outFile.c_str(), F_OK) == 0)
        {
            int no = 0;
            //文件已存在,则追加序号
            while(true)
            {
                if(fileOper.access(out_realFullPath.c_str(), F_OK) == 0)
                {
                    out_realFullPath = out_realPath + tmp_fileName + "." + numToStr(no);
                    no++;
                }
                else break;
            }

            if(fileOper.rename(tmp2_outFile.c_str(), out_realFullPath.c_str()) != 0)
            {
                char errmsg[512];
                memset(errmsg, 0x00, 512);
                sprintf(errmsg, "[%s:%d]rename[%s] to [%s]error! %s", __FILE__, __LINE__, tmp2_outFile.c_str(), out_realFullPath.c_str(), fileOper.getErrInfo());
                theErrorMessages->insert(E_MOVE_FILE_ERR, errmsg);
                fileOper.renameErrorRecord(tmp2_outFile.c_str(), out_realFullPath.c_str());
                //return false;
                result = false;
            }
            //alter for x86 add deal duration 20151204 by zhangyw
            //theLogger->pOutput(theLoggerProcessID,out_realFullPath,it2->second);
            gettimeofday( &endDealTime, NULL);
            duration = 1000 * (endDealTime.tv_sec - startDealTime.tv_sec) + (endDealTime.tv_usec - startDealTime.tv_usec) / 1000;
            theLogger->pOutput(theLoggerProcessID, out_realFullPath, it2->second, "dealtime=%dms", duration);
        }
    }

    //清空map
    m_tmpRedoFileNameMap.clear();

    if(m_LogRedoIsOpen)
    {
        theLogger->pEnd(theLoggerProcessID);
        m_LogRedoIsOpen = false;
    }
    return result;
}

bool SplitApp::checkConfig()
{
    vector<CustChannel>::iterator t_itr;
    map<string, string>::iterator t_itmap;
    vector<string> info_paths;
    vector<string>::iterator t_itrS;

    //判断目录是否存在
    if(isValidPath(m_config.m_inputPath.c_str()) == false)
    {
#ifdef _DEBUG_
        cout << " path:" << m_config.m_inputPath << " not exists!" << endl;
#endif
        m_theErrorMessages->insert(EAPPFRM_SYSVAL, "path:" + m_config.m_inputPath + " not exists!");
        return false;
    }
    if(isValidPath(m_config.m_backupPath.c_str()) == false)
    {
#ifdef _DEBUG_
        cout << " path:" << m_config.m_backupPath << " not exists!" << endl;
#endif
        m_theErrorMessages->insert(EAPPFRM_SYSVAL, "path:" + m_config.m_backupPath + " not exists!");
        return false;
    }
    if(isValidPath(m_config.m_errPath.c_str()) == false)
    {
#ifdef _DEBUG_
        cout << " path:" << m_config.m_errPath << " not exists!" << endl;
#endif
        m_theErrorMessages->insert(EAPPFRM_SYSVAL, "path:" + m_config.m_errPath + " not exists!");
        return false;
    }
    if(isValidPath(m_config.m_tmpPath.c_str()) == false)
    {
#ifdef _DEBUG_
        cout << " path:" << m_config.m_tmpPath << " not exists!" << endl;
#endif
        m_theErrorMessages->insert(EAPPFRM_SYSVAL, "path:" + m_config.m_tmpPath + " not exists!");
        return false;
    }
    //检测TD_B_INFO_SPLITCHANNEL表配置的分发路径在split.cfg中有没有配置
    int t_i, t_beginId, t_endId;
    t_i = 0;
    ::sort(m_config.m_custChannel.begin(), m_config.m_custChannel.end());
    for(t_itr = m_config.m_custChannel.begin(); t_itr != m_config.m_custChannel.end(); t_itr++)
    {
        t_beginId = t_itr->m_beginPartition;
        if(strlen(m_config.m_prov_code.c_str()) < 3)
        {
            if (t_i == 0)
            {
                if (t_beginId != 0 && t_beginId != -1)
                {
                    theErrorMessages->insert(EAPPFRM_SYSVAL, "表[TD_SPLIT_CHANNEL_DEF]配置错误:起始分区ID不等于0或-1!");
                    return false;
                }
            }
            else
            {
                //modify by yueyq  for TFS:362748   split 根据地市编码再根据用户后四位分通道
                /**
                if ((t_endId + 1) != t_beginId)
                {
                    theErrorMessages->insert(EAPPFRM_SYSVAL, "表[TD_SPLIT_CHANNEL_DEF]配置错误:起始分区ID和结束分区ID不连续!");
                    return false;
                }
                **/
            }
        }

        t_itmap = m_config.m_outChanInfo.find(t_itr->m_channelNo);
        if (t_itmap == m_config.m_outChanInfo.end())
        {
            theErrorMessages->insert(EAPPFRM_SYSVAL, "分发目录" + t_itr->m_channelNo + "在split.cfg中未配置!");
            return false;
        }
        else
        {
            //liujq kafka
            vector<string> t_pathsVec;
            t_pathsVec.clear();
            string t_outPath = t_itmap->second.c_str();

            t_pathsVec = StringUtil::split(t_outPath, ";");
            t_outPath = t_pathsVec[0];
            if(t_pathsVec.size() == 2 && t_pathsVec[1].substr(0, 3) == "MDS")
            {
                //如果是kafa,则校验分号后面的MDS
                if(m_config.m_fsMode == GETFILE_FROM_KAFKA || m_config.m_fsMode == GETFILE_FROM_KAFKA_SDFS)
                {
                    string t_s = t_pathsVec[1];
                    if(t_s.substr(t_s.length() - 1) == "/") //去掉最后的斜杠
                    {
                        t_s = t_s.substr(0, t_s.length() - 1);
                    }
                    if(!m_config.isPath(t_s))
                    {
                        m_theErrorMessages->insert(ERROR_KAFKA_PATH_ERROR, "kafka目录:" + t_s + " 校验失败!");
                        return false;
                    }
                }
            }

            //如果是sdfs,则校验分号前面的输出目录
            if(m_config.m_fsMode == GETFILE_FROM_SDFS)
            {
                if(!isValidPath(t_outPath.c_str()))
                {
                    m_theErrorMessages->insert(EAPPFRM_SYSVAL, "path:" + t_itmap->second + " not exists!");
                    return false;
                }
            }

            //初始化分发文件序号
            m_seqMap.insert(map<string, int>::value_type(t_itr->m_channelNo, 0));
            //add by xingq redo文件单独处理
            m_seqRedoMap.insert(map<string, int>::value_type(t_itr->m_channelNo, 0));
        }
        t_endId = t_itr->m_endPartition;
        t_i++;
    }
    return true;
}

bool SplitApp::isValidPath(const char *r_path)
{
    if (fileOper.openDir(r_path) != 0)
    {
#ifdef _DEBUG_
        cout << " 不存在的路径:" << r_path << endl;
#endif
        return false;
    }
    return true;
}

//数字型转换成字符串
string SplitApp::numToStr(const long &r_num)
{
    stringstream str;
    string result;
    str << r_num;
    str >> result;
    return result;
}
/* x86注释掉
void SplitApp::onSignal(const int sig)
{
    string t_errorMsg = strerror(errno);
    theErrorMessages->insert(sig,t_errorMsg);
    return;
}
*/













