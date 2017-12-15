#include "pass/DBPassOper.h"
#include "SplitApp.h"
#include "SplitConfig.h"

SplitConfig::SplitConfig()
{
	m_fsMode = 1;//liujq 默认是1-sdfs模式
}
SplitConfig:: ~SplitConfig()
{
    for(list<AbstractFieldSelector *>::iterator r_itr = m_selectors.begin();
            r_itr != m_selectors.end(); ++r_itr)
    {
        delete *r_itr;
    }
    m_selectors.clear();
    m_fidSelect = NULL;
}


//读取配置文件，获得配置信息
bool SplitConfig::GetSystemConfigInfo(SysParam *pSysParam, string sProcName, int iChannelNo)
{
    //assert(pSysParam != NULL);
    char channelNo[8];
    string root, section, name, value;
    /* t_common:公共配置; t_channel:通道配置(目录等);
     * t_channel_info:分发目录;
     */
    string t_common, t_channel, t_channel_info;
    map<string, string> t_nodes;
    map<string, string>::iterator t_nodeMap;

    root = SECTDELIM + sProcName;
    memset(channelNo, 0x00, 8);
    sprintf(channelNo, "%d", iChannelNo);

    //公共配置
    t_nodes.clear();
    t_common = root + SECTDELIM + "common";
    if (!pSysParam->getValue(t_common, t_nodes))
    {
        theErrorMessages->insert(CS_ERROR_INITIALIZE, "get common config param false! " + t_common);
        return false;
    }

    //连接数据库信息
    m_dbUserName = t_nodes["db_username"];
#ifdef _PSW_FROM_FILE_
    m_dbServName = t_nodes["db_servname"];
    m_dbPassword = t_nodes["db_password"];
#else
    m_dbCode = t_nodes["db_code"];
    string cDbCode = m_dbCode;
    DBPass dbpass;
    DBPassOper *dbPassOper = DBPassOper::getOper();
    if(!dbPassOper->GetDBPass(dbpass, cDbCode, m_dbUserName))
    {
        theErrorMessages->insert(CS_ERROR_PWD, "加密获取密码失败...");
#ifdef _DEBUG_
        cout << "config error:GetDBPass error" << " " << __FILE__ << __LINE__ << endl;
#endif
        return false;
    }
    m_dbPassword      = dbpass.DecPasswd;
    m_dbServName      = dbpass.ServerName;
    delete dbPassOper;
#endif

    //记录分隔符
    if (t_nodes["separator_sign"] == "")
        m_separatorSign = ',';
    else
        m_separatorSign = t_nodes["separator_sign"][0];

    //拆分字段
    char t_buffer[1024];
    memset(t_buffer, 0x00, sizeof(t_buffer));
    m_fidSelectRule = t_nodes["split_fid"];
    strcpy(t_buffer, m_fidSelectRule.c_str());
    m_fidSelect = generateSelect(t_buffer, m_separatorSign, m_maxFidLen);

    //add by yueyq  for TFS:362748   split 根据地市编码再根据用户后四位分通道
    m_openCity = t_nodes["open_city"];
#ifdef _DEBUG_
    cout << "m_openCity:" << m_openCity << "  " << __FILE__ << __LINE__ << endl;
#endif
    char t_cityBuffer[1024];
    if(m_openCity == "Y")
    {
        memset(t_cityBuffer, 0x00, sizeof(t_cityBuffer));
        m_citySelectRule = t_nodes["city_code"];
        strcpy(t_cityBuffer, m_citySelectRule.c_str());
        m_citySelect = generateSelect(t_cityBuffer, m_separatorSign, m_maxFidLen);
    }
    //end

    //拆分字段长度
    m_maxFidLen = atoi(t_nodes["maxKeyLength"].c_str());

    //按后几位拆分
    if (t_nodes["split_pos"] == "")
    {
        m_split_pos = FOUR;
    }
    else
    {
        m_split_pos = atoi(t_nodes["split_pos"].c_str());
    }

    //处理多少个文件输出一次
    if (t_nodes["max_filenum"] == "")
    {
        theErrorMessages->insert(EAPPFRM_SYSVAL, t_common + SECTDELIM + "max_filenum 未配置!");
        return false;
    }
    m_maxFileNum = atoi(t_nodes["max_filenum"].c_str());

    //分发文件多少行时拆分输出
    if (t_nodes["max_fileLine"] == "")
    {
        theErrorMessages->insert(EAPPFRM_SYSVAL, t_common + SECTDELIM + "max_fileLine 未配置!");
        return false;
    }
    m_maxFileLine = atoi(t_nodes["max_fileLine"].c_str());

    //多长时间没有输入文件则将处理的文件输出
    name = "timeout";
    if (t_nodes["timeout"] == "")
    {
        theErrorMessages->insert(EAPPFRM_SYSVAL, t_common + SECTDELIM + "timeout 未配置!");
        return false;
    }
    m_timeout = atoi(t_nodes["timeout"].c_str());

    //add by xingq start redo文件单独处理,获取配置
    m_redoOpen = 0;
    if (t_nodes["redo_open"] != "")
    {
        m_redoOpen = atoi(t_nodes["redo_open"].c_str());
        if(1 == m_redoOpen)
        {
            if (t_nodes["redo_timeout"] != "")
            {
                m_redoTimeout = atoi(t_nodes["redo_timeout"].c_str());
            }
            else
            {
                theErrorMessages->insert(EAPPFRM_SYSVAL, t_common + SECTDELIM + "redo_timeout 未配置!");
                return false;
            }
        }
    }
    //add by xingq end

    //处理达到总时长后则将处理的文件输出
    if (t_nodes["total_timeout"] == "")
    {
        theErrorMessages->insert(EAPPFRM_SYSVAL, t_common + SECTDELIM + "total_timeout 未配置!");
        return false;
    }
    m_totalTimeout = atoi(t_nodes["total_timeout"].c_str());

    //读取通道配置
    t_nodes.clear();
    t_channel = root + SECTDELIM + channelNo;
    pSysParam->setSectionPath(t_channel);
    if (!pSysParam->getValue(t_channel, t_nodes))
    {
        theErrorMessages->insert(EAPPFRM_SYSVAL, t_channel + SECTDELIM + "未配置!");
        return false;
    }

    //库号
    m_prov_code = t_nodes["province_code"];
    //处理文件前缀
    m_prefix = t_nodes["file_prefix"];
    if (m_prefix == "")
    {
        m_prefix = "*";
    }
    //输入目录
    m_inputPath = t_nodes["Input_path"];
    if (m_inputPath == "")
    {
        theErrorMessages->insert(EAPPFRM_SYSVAL, t_channel + SECTDELIM + "Input_path 未配置!");
        return false;
    }

	 //begin liujq kafka
    vector<string> t_pathsVec;
    t_pathsVec.clear();
    if (strlen(m_inputPath.c_str()) > 0)
    {
        t_pathsVec = StringUtil::split(m_inputPath, ";");
        m_inputPath = t_pathsVec[0];
        standardPath(m_inputPath);
        if(t_pathsVec.size() == 2) //kafka
        {
            m_mdsPathvector.push_back(t_pathsVec[1]);//保存进容器，kafka init时候传给fileoperate
            m_inputPathMds = t_pathsVec[1];//保存起来，open的时候拼文件头用
        }

    }
    //end liujq kafka

    //备份目录
    m_backupPath = t_nodes["Backup_path"];
    if (m_backupPath == "")
    {
        theErrorMessages->insert(EAPPFRM_SYSVAL, t_channel + SECTDELIM + "Backup_path 未配置!");
        return false;
    }
    standardPath(m_backupPath);

    //错误目录
    m_errPath = t_nodes["Err_path"];
    if (m_errPath == "")
    {
        theErrorMessages->insert(EAPPFRM_SYSVAL, t_channel + SECTDELIM + "Err_path 未配置!");
        return false;
    }
	 //begin liujq kafka
    t_pathsVec.clear();
    if (strlen(m_errPath.c_str()) > 0)
    {
        t_pathsVec = StringUtil::split(m_errPath, ";");
        m_errPath = t_pathsVec[0];
        standardPath(m_errPath);
        if(t_pathsVec.size() == 2) //kafka
        {
            m_mdsPathvector.push_back(t_pathsVec[1]);//保存进容器，kafka init时候传给fileoperate
            m_inputPathErrMds = t_pathsVec[1];//保存起来，open的时候拼文件头用
        }
    }
    //end liujq kafka


    //工作目录
    m_tmpPath = t_nodes["Tmp_path"];
    if (m_errPath == "")
    {
        theErrorMessages->insert(EAPPFRM_SYSVAL, t_channel + SECTDELIM + "Tmp_path 未配置!");
        return false;
    }
    standardPath(m_tmpPath);

    //--------------以下参数通道内配置覆盖公共配置-----begin-------------------
    /*
     * split_pos\split_fid\maxKeyLength\max_filenum\max_fileLine\timeout
     */
    //拆分字段
    memset(t_buffer, 0x00, sizeof(t_buffer));
    if (t_nodes["split_fid"] != "")
    {
        m_fidSelectRule = t_nodes["split_fid"];
        strcpy(t_buffer, m_fidSelectRule.c_str());
        m_fidSelect = generateSelect(t_buffer, m_separatorSign, m_maxFidLen);
    }

    //add by yueyq  for TFS:362748   split 根据地市编码再根据用户后四位分通道
    if(t_nodes["open_city"] != "")
    {
        m_openCity = t_nodes["open_city"];
    }
#ifdef _DEBUG_
    cout << "m_openCity:" << m_openCity << "  " << __FILE__ << __LINE__ << endl;
#endif
    if(m_openCity == "Y")
    {
        memset(t_cityBuffer, 0x00, sizeof(t_cityBuffer));
        if (t_nodes["city_code"] != "")
        {
            m_citySelectRule = t_nodes["city_code"];
            strcpy(t_cityBuffer, m_citySelectRule.c_str());
            m_citySelect = generateSelect(t_cityBuffer, m_separatorSign, m_maxFidLen);
        }
#ifdef _DEBUG_
        cout << "m_citySelectRule:" << m_citySelectRule << "  " << __FILE__ << ":" << __LINE__ << endl;
#endif
        if (m_citySelectRule == "")
        {
            char errmsg[256];
            memset(errmsg, 0x00, 256);
            sprintf(errmsg, "city_code未进行配置，请进行配置!", __FILE__, __LINE__);
            theErrorMessages->insert(EAPPFRM_SYSVAL, errmsg);
            return false;
        }
    }
    else if(m_openCity == "")
    {
        char errmsg[256];
        memset(errmsg, 0x00, 256);
        sprintf(errmsg, "open_city未进行配置，请进行配置!", __FILE__, __LINE__);
        theErrorMessages->insert(EAPPFRM_SYSVAL, errmsg);
        return false;
    }
    //end

    //拆分字段长度
    if (t_nodes["maxKeyLength"] != "")
    {
        m_maxFidLen = atoi(t_nodes["maxKeyLength"].c_str());
    }
    //按后几位拆分
    if (t_nodes["split_pos"] != "")
    {
        m_split_pos = atoi(t_nodes["split_pos"].c_str());
    }
    //处理多少个文件输出一次
    if (t_nodes["max_filenum"] != "")
    {
        m_maxFileNum = atoi(t_nodes["max_filenum"].c_str());
    }
    //分发文件多少行时拆分输出
    if (t_nodes["max_fileLine"] != "")
    {
        m_maxFileLine = atoi(t_nodes["max_fileLine"].c_str());
    }
    //多长时间没有输入文件则将处理的文件输出
    if (t_nodes["timeout"] != "")
    {
        m_timeout = atoi(t_nodes["timeout"].c_str());
    }

    //add by xingq start 多长时间没有输入文件则将处理的redo文件输出

    if (t_nodes["redo_open"] != "")
    {
        m_redoOpen = atoi(t_nodes["redo_open"].c_str());
        if(1 == m_redoOpen)
        {
            if (t_nodes["redo_timeout"] != "")
            {
                m_redoTimeout = atoi(t_nodes["redo_timeout"].c_str());
            }
            else
            {
                theErrorMessages->insert(EAPPFRM_SYSVAL, t_common + SECTDELIM + "redo_timeout 未配置!");
                return false;
            }
        }
    }
    //add by xingq end
    //达到总处理时长则将处理的文件输出
    if (t_nodes["total_timeout"] != "")
    {
        m_totalTimeout = atoi(t_nodes["total_timeout"].c_str());
    }
    //--------------以上参数通道内配置覆盖公共配置-----end-------------------

    //读取分发路径信息
    t_channel_info = t_channel + SECTDELIM + "channel_info";

    t_nodes.clear();
    if(pSysParam->getValue(t_channel_info, t_nodes) == false)
    {
        //通道内没有channel_info配置,则读取common处配置
        string t_channel_info_com = root + SECTDELIM + "common\\channel_info" +  SECTDELIM + m_prov_code;
        if(pSysParam->getValue(t_channel_info_com, t_nodes) == false)
        {
            char errmsg[256];
            memset(errmsg, 0x00, 256);
            sprintf(errmsg, "[%s:%d] 分发通道目录未配置![%s or %s 至少配置一处!", __FILE__, __LINE__, t_channel_info.c_str(), t_channel_info_com.c_str());
            theErrorMessages->insert(EAPPFRM_SYSVAL, errmsg);
            return false;
        }
    }
    string tmpChannel, tmpPath;
    for(t_nodeMap = t_nodes.begin(); t_nodeMap != t_nodes.end(); ++t_nodeMap)
    {
        tmpChannel = t_nodeMap->first;
        tmpPath = t_nodeMap->second;
        standardPath(tmpPath);
        m_outChanInfo.insert(map<string, string>::value_type(tmpChannel, tmpPath));
    }

    return true;
}

/*//读取配置文件，获得配置信息
bool SplitConfig::GetSystemConfigInfo(SysParam *pSysParam,string sProcName,int iChannelNo)
{
    //assert(pSysParam != NULL);
    char channelNo[8];
    string root, section, name, value;
    // t_common:公共配置; t_channel:通道配置(目录等);
    // t_channel_info:分发目录;

    string t_common,t_channel,t_channel_info;
    map<string,string> t_nodes;
    map<string,string>::iterator t_nodeMap;

    root = SECTDELIM + sProcName;
    memset(channelNo,0x00,8);
    sprintf(channelNo, "%d", iChannelNo);

    //公共配置
    t_common = root + SECTDELIM + "common";

    //连接数据库信息
    #ifdef _PSW_FROM_FILE_
    name = "db_servname";
    if (!pSysParam->getValue(t_common, name, value))
    {
      theErrorMessages->insert(EAPPFRM_SYSVAL, t_common + SECTDELIM + name);
      return false;
    }
    m_dbServName = value;

    name = "db_username";
    if (!pSysParam->getValue(t_common, name, value))
    {
      theErrorMessages->insert(EAPPFRM_SYSVAL, t_common + SECTDELIM + name);
      return false;
    }
    m_dbUserName = value;

    name = "db_password";
    if (!pSysParam->getValue(t_common, name, value))
    {
      theErrorMessages->insert(EAPPFRM_SYSVAL, t_common + SECTDELIM + name);
      return false;
    }
    m_dbPassword = value;

    #else
    name = "db_code";
    if (!pSysParam->getValue(t_common, name, value))
    {
      theErrorMessages->insert(EAPPFRM_SYSVAL, t_common + SECTDELIM + name);

      return false;
    }
    m_dbCode = value;
    name = "db_username";
    if (!pSysParam->getValue(t_common, name, value))
    {
      theErrorMessages->insert(EAPPFRM_SYSVAL, t_common + SECTDELIM + name);
      return false;
    }
    m_dbUserName = value;

  	string cDbCode              = m_dbCode;
  	DBPass dbpass;
  	DBPassOper* dbPassOper      = DBPassOper::getOper();

  	if( ! dbPassOper->GetDBPass(dbpass, cDbCode, m_dbUserName))
 	{
 	  	theErrorMessages->insert(CS_ERROR_PWD, "加密获取密码失败...");
   		#ifdef _DEBUG_
      	cout<<"config error:GetDBPass error" <<" "<<__FILE__<<__LINE__<<endl;
    	#endif
      	return false;
    }
    m_dbPassword      = dbpass.DecPasswd;
    m_dbServName      = dbpass.ServerName;
    delete dbPassOper;
    #endif

    //记录分隔符
    name = "separator_sign";
    if (!pSysParam->getValue(t_common, name, value))
    {
        m_separatorSign = ',';
    }
    else
    {
        m_separatorSign = value[0];
    }

    //拆分字段
    char t_buffer[1024];
    memset(t_buffer,0x00,sizeof(t_buffer));
    name = "split_fid";
    if (!pSysParam->getValue(t_common, name, value))
    {
      theErrorMessages->insert(EAPPFRM_SYSVAL, t_common + SECTDELIM + name);
      return false;
    }
    strcpy(t_buffer,value.c_str());
    m_fidSelectRule = t_buffer;
    m_fidSelect = generateSelect(t_buffer,m_separatorSign,m_maxFidLen);
    //拆分字段长度
    name = "maxKeyLength";
    if (!pSysParam->getValue(t_common, name, value))
    {
      theErrorMessages->insert(EAPPFRM_SYSVAL, t_common + SECTDELIM + name);
      return false;
    }
    m_maxFidLen = atoi(value.c_str());
    //按后几位拆分
    name = "split_pos";
    if (!pSysParam->getValue(t_common, name, value))
    {
        m_split_pos = FOUR;
    }
    else
    {
        m_split_pos = atoi(value.c_str());
    }

    //处理多少个文件输出一次
    name = "max_filenum";
    if (!pSysParam->getValue(t_common, name, value))
    {
        theErrorMessages->insert(EAPPFRM_SYSVAL, t_common + SECTDELIM + name);
        return false;
    }
    m_maxFileNum = atoi(value.c_str());

    //分发文件多少行时拆分输出
    name = "max_fileLine";
    if (!pSysParam->getValue(t_common, name, value))
    {
        theErrorMessages->insert(EAPPFRM_SYSVAL, t_common + SECTDELIM + name);
        return false;
    }
    m_maxFileLine = atoi(value.c_str());

    //多长时间没有输入文件则将处理的文件输出
    name = "timeout";
    if (!pSysParam->getValue(t_common, name, value))
    {
        theErrorMessages->insert(EAPPFRM_SYSVAL, t_common + SECTDELIM + name);
        return false;
    }
    m_timeout= atoi(value.c_str());

    //读取通道配置
    t_channel = root + SECTDELIM + channelNo;
    pSysParam->setSectionPath(t_channel);

    //库号
    name = "province_code";
    if (!pSysParam->getValue(t_channel, name, value))
    {
      theErrorMessages->insert(EAPPFRM_SYSVAL, t_channel + SECTDELIM + name);
      return false;
    }
    m_prov_code = value;

    //处理文件前缀
    name = "file_prefix";
    if (!pSysParam->getValue(t_channel, name, value))
    {
      theErrorMessages->insert(EAPPFRM_SYSVAL, t_channel + SECTDELIM + name);
      return false;
    }
    m_prefix = value;
    if(m_prefix=="")
    {
        m_prefix = "*";
    }

    //输入目录
    name = "Input_path";
    if (!pSysParam->getValue(t_channel, name, value))
    {
      theErrorMessages->insert(EAPPFRM_SYSVAL, t_channel + SECTDELIM + name);
      return false;
    }
    m_inputPath = value;
    standardPath(m_inputPath);

    //备份目录
    name = "Backup_path";
    if (!pSysParam->getValue(t_channel, name, value))
    {
      theErrorMessages->insert(EAPPFRM_SYSVAL, t_channel + SECTDELIM + name);
      return false;
    }
    m_backupPath = value;
    standardPath(m_backupPath);

    //错误目录
    name = "Err_path";
    if (!pSysParam->getValue(t_channel, name, value))
    {
      theErrorMessages->insert(EAPPFRM_SYSVAL, t_channel + SECTDELIM + name);
      return false;
    }
    m_errPath = value;

    //工作目录
    name = "Tmp_path";
    if (!pSysParam->getValue(t_channel, name, value))
    {
      theErrorMessages->insert(EAPPFRM_SYSVAL, t_channel + SECTDELIM + name);
      return false;
    }
    m_tmpPath = value;
    standardPath(m_tmpPath);

    //--------------以下参数通道内配置覆盖公共配置-----begin-------------------
    //split_pos\split_fid\maxKeyLength\max_filenum\max_fileLine\timeout
    //拆分字段
    memset(t_buffer,0x00,sizeof(t_buffer));
    name = "split_fid";
    if (pSysParam->getValue(t_channel, name, value))
    {
        if(value!="")
        {
            strcpy(t_buffer,value.c_str());
            m_fidSelectRule = t_buffer;
            m_fidSelect = generateSelect(t_buffer,m_separatorSign,m_maxFidLen);
        }
    }

    //拆分字段长度
    name = "maxKeyLength";
    if (pSysParam->getValue(t_channel, name, value))
    {
        if(value!="")
        {
            m_maxFidLen = atoi(value.c_str());
        }
    }

    //按后几位拆分
    name = "split_pos";
    if (pSysParam->getValue(t_channel, name, value))
    {
        if(value!="")
        {
            m_split_pos = atoi(value.c_str());
        }
    }

    //处理多少个文件输出一次
    name = "max_filenum";
    if (pSysParam->getValue(t_channel, name, value))
    {
        if(value!="")
        {
            m_maxFileNum = atoi(value.c_str());
        }
    }

    //分发文件多少行时拆分输出
    name = "max_fileLine";
    if (pSysParam->getValue(t_channel, name, value))
    {
        if(value!="")
        {
            m_maxFileLine = atoi(value.c_str());
        }
    }

    //多长时间没有输入文件则将处理的文件输出
    name = "timeout";
    if (pSysParam->getValue(t_channel, name, value))
    {
        if(value!="")
        {
            m_timeout = atoi(value.c_str());
        }
    }
    //--------------以上参数通道内配置覆盖公共配置-----end-------------------

    //读取分发路径信息
    t_channel_info = t_channel + SECTDELIM + "channel_info";

    t_nodes.clear();
    if(pSysParam->getValue(t_channel_info,t_nodes)==false)
    {
      //theErrorMessages->insert(EAPPFRM_SYSVAL, t_channel_info + SECTDELIM + "t_nodes");
      //return false;

      //通道内没有channel_info配置,则读取common处配置
      string t_channel_info_com = root + SECTDELIM + "common\\channel_info" +  SECTDELIM + m_prov_code;
      if(pSysParam->getValue(t_channel_info_com,t_nodes)==false)
      {
        char errmsg[256];
        memset(errmsg,0x00,256);
        sprintf(errmsg,"[%s:%d] 分发通道目录未配置![%s or %s 至少配置一处!",__FILE__,__LINE__,t_channel_info.c_str(),t_channel_info_com.c_str());
        theErrorMessages->insert(EAPPFRM_SYSVAL, errmsg);
        return false;
      }
    }
    string tmpChannel,tmpPath;
    for(t_nodeMap=t_nodes.begin();t_nodeMap!=t_nodes.end();++t_nodeMap)
    {
        tmpChannel = t_nodeMap->first;
        tmpPath = t_nodeMap->second;
        standardPath(tmpPath);
        m_outChanInfo.insert(map<string, string>::value_type(tmpChannel, tmpPath));
    }

    return true;
}
*/

bool SplitConfig::GetAllChannel()
{
    Environment  *m_env;//指向OCCI定义的数据库环境类的指针
    Connection   *m_conn;//数据库连接类的指针
    Statement    *m_stmt;//数据库SQL执行类的指针
    ResultSet    *m_rs;  //结果集指针

    char  tempchr[512] = "\0";
    string m_selectSql;
    //modify by yueyq  for TFS:362748   split 根据地市编码再根据用户后四位分通道


	if(m_fsMode == GETFILE_FROM_KAFKA || m_fsMode == GETFILE_FROM_KAFKA_SDFS)
	{
	
    sprintf(tempchr, " SELECT BEGIN_PARTITION_ID,END_PARTITION_ID,DB_NO,CHANNEL_NO,\
            PROVINCE_CODE,EPARCHY_CODE FROM TD_USER_CHANNEL_DCC \
            WHERE PROVINCE_CODE = %s ", m_prov_code.c_str());

	}
	if(m_fsMode == GETFILE_FROM_SDFS)
	{
    sprintf(tempchr, " SELECT BEGIN_PARTITION_ID,END_PARTITION_ID,DB_NO,CHANNEL_NO,\
            PROVINCE_CODE,CITY_CODE FROM TD_SPLIT_CHANNEL_DEF \
            WHERE PROVINCE_CODE = %s ", m_prov_code.c_str());
	}


    m_selectSql = tempchr;
#ifdef _DEBUG_
    cout << __FILE__ << __LINE__ << " SQL:" << m_selectSql << endl;
#endif

    m_env = Environment::createEnvironment();//创建一个环境变量
    m_conn = m_env->createConnection(m_dbUserName, m_dbPassword, m_dbServName); //创建一个数据库连接对象
    m_stmt = m_conn->createStatement();//创建一个Statement对象

    m_stmt->setSQL(m_selectSql); //设置SQL语句到Statement对象中
    int sTmpBegin, sTmpEnd, sTmpDbNo;
    //modify by yueyq  for TFS:362748   split 根据地市编码再根据用户后四位分通道
    string sTmpChanNo, sTmpProv, sTmpCity;
    m_custChannel.clear();
    CustChannel t_custChannel;
    try
    {
        m_rs = m_stmt->executeQuery();//执行SQL语句
        while (m_rs->next())
        {
            sTmpBegin = m_rs->getInt(1);
            sTmpEnd = m_rs->getInt(2);
            sTmpDbNo = m_rs->getInt(3);
            sTmpChanNo = m_rs->getString(4);
            sTmpProv = m_rs->getString(5);
            //add by yueyq  for TFS:362748   split 根据地市编码再根据用户后四位分通道
            sTmpCity = m_rs->getString(6);
            t_custChannel.m_cityCode = sTmpCity;
            //end
            t_custChannel.m_beginPartition = sTmpBegin;
            t_custChannel.m_endPartition = sTmpEnd;
            t_custChannel.m_dbNo = sTmpDbNo;
            t_custChannel.m_channelNo = sTmpChanNo;
            t_custChannel.m_provCode = sTmpProv;
            m_custChannel.push_back(t_custChannel);
        }
    }
    catch(SQLException ex)
    {
        char errmsg[256];
        memset(errmsg, 0x00, 256);
        sprintf(errmsg, "Error:%d,%s", ex.getErrorCode(), ex.getMessage().c_str());
        theErrorMessages->insert(CS_ERROR_DB_EXEC, errmsg);
        theErrorMessages->insert(CS_ERROR_DB_EXEC, "executeQuery SQL [" + m_selectSql + "] failed!");
        return false;
    }

    //释放资源
    m_conn->terminateStatement(m_stmt); //终止Statement对象
    m_env->terminateConnection(m_conn); //断开数据库连接
    Environment::terminateEnvironment(m_env); //终止环境变量
    m_rs = (ResultSet *)NULL;
    m_stmt = (Statement *)NULL;
    m_conn = (Connection *)NULL;
    m_env = (Environment *)NULL;
    return true;
}

/*
bool SplitConfig::GetAllChannel()
{
    StringVector  t_recordVector;
    StringVector  t_errorVector;
    char  tempchr[2000+1] = "\0";
    DbInterface dbInterface;
    string m_selectInfoSql;

    if (dbInterface.connect(m_dbUserName.c_str(),m_dbPassword.c_str(),m_dbServName.c_str()))
    {
        return false;
    }

    sprintf(tempchr," SELECT BEGIN_PARTITION_ID,END_PARTITION_ID,DB_NO,CHANNEL_NO,PROVINCE_CODE FROM TD_SPLIT_CHANNEL_DEF WHERE PROVINCE_CODE = %s ",m_prov_code.c_str());
    m_selectInfoSql = tempchr;
    #ifdef _DEBUG_
    cout<<"sql=["<<m_selectInfoSql<<"]"<<endl;
    #endif

    //绑定数据
    t_recordVector.clear();
    t_errorVector.clear();

    if (dbInterface.dataBind(t_recordVector,t_errorVector))
    {
        return false;
    }

    int total = 0;
    int success = 0;
    int error = 0;
    int recordCount = 0;
    int fields = 5;
    vector<string>::iterator    iter;

    do
    {
        if (dbInterface.executeSql(m_selectInfoSql.c_str(),total,success,error))
        {
            return false;
        }
        m_custChannel.clear();
        CustChannel t_custChannel;
        for (iter=t_recordVector.begin(); iter!=t_recordVector.end(); iter+=fields)
        {
            t_custChannel.m_beginPartition = atoi((*iter).c_str());
            t_custChannel.m_endPartition = atoi((*(iter+1)).c_str());
            t_custChannel.m_dbNo= atoi((*(iter+2)).c_str());
            t_custChannel.m_channelNo = StringUtil::trim(*(iter+3));
            t_custChannel.m_provCode = *(iter+4);
            m_custChannel.push_back(t_custChannel);
        }
    }
    while (total >= DEFAULTARRAYLINE);
    //断开和数据库的连接
    dbInterface.disconnect();
    return true;
}
*/


//liujq kafka
bool SplitConfig::getFsMode(int r_channelNo)
{
    Environment  *m_env;//指向OCCI定义的数据库环境类的指针
    Connection   *m_conn;//数据库连接类的指针
    Statement    *m_stmt;//数据库SQL执行类的指针
    ResultSet    *m_rs;  //结果集指针

    int t_mdsGroupId = 0;

    char  tempchr[512] = "\0";
    string m_selectSql;
    sprintf(tempchr, "select a.mode_type,a.MDS_GROUP_ID,a.MAXCDR_ONCE_READ,a.JSONBULK_SIZE,a.SWITCH_TIME,a.maxcdr_once_write,a.param_value_1  "
            "from  td_b_channel_param a  where  "
            " channel_no=%d "
            " and a.application_type=%d "
            " and a.use_tag='1' "
            " and trim(a.province_code)='%s'  ",
            r_channelNo, 3, m_prov_code.c_str());

    m_selectSql = tempchr;
#ifdef _DEBUG_
    cout << __FILE__ << __LINE__ << " SQL:" << m_selectSql << endl;
#endif

    m_env = Environment::createEnvironment();//创建一个环境变量
    m_conn = m_env->createConnection(m_dbUserName, m_dbPassword, m_dbServName); //创建一个数据库连接对象
    m_stmt = m_conn->createStatement();//创建一个Statement对象

    m_stmt->setSQL(m_selectSql); //设置SQL语句到Statement对象中
    try
    {
        m_rs = m_stmt->executeQuery();//执行SQL语句
        while (m_rs->next())
        {
            m_fsMode = m_rs->getInt(1);
            t_mdsGroupId = m_rs->getInt(2);
            m_maxCdrOnceRead = m_rs->getInt(3);
            m_jsonBulkSize = m_rs->getInt(4);
            m_fschangeInterval = m_rs->getInt(5);
			m_maxCdrOnceWrite = m_rs->getInt(6);
        }
    }
    catch(SQLException ex)
    {
        char errmsg[256];
        memset(errmsg, 0x00, 256);
        sprintf(errmsg, "Error:%d,%s", ex.getErrorCode(), ex.getMessage().c_str());
        theErrorMessages->insert(CS_ERROR_DB_EXEC, errmsg);
        theErrorMessages->insert(CS_ERROR_DB_EXEC, "executeQuery SQL [" + m_selectSql + "] failed!");
        return false;
    }

    sprintf(tempchr, "select trim(propertycp_type), trim(property_scope), trim(property_name), trim(property_value)  "
            " from td_b_mds_config_group  where "
            " group_id = %d "
            " and app_type = %d ",
            t_mdsGroupId, 3);

    m_selectSql = tempchr;
#ifdef _DEBUG_
    cout << __FILE__ << __LINE__ << " SQL:" << m_selectSql << endl;
#endif

    KafkaProperty t_kafkaProperty;
    m_kafkaParamVec.clear();

    m_stmt->setSQL(m_selectSql); //设置SQL语句到Statement对象中
    try
    {
        m_rs = m_stmt->executeQuery();//执行SQL语句
        while (m_rs->next())
        {
            //t_kafkaProperty.m_propertycpType = m_rs->getString(1);
            //t_kafkaProperty.m_propertycpScope = m_rs->getString(2);
            //t_kafkaProperty.m_propertycpName = m_rs->getString(3);
            //t_kafkaProperty.m_propertycpValue = m_rs->getString(4);
            m_kafkaParamVec.push_back(m_rs->getString(1));
            m_kafkaParamVec.push_back(m_rs->getString(2));
            m_kafkaParamVec.push_back(m_rs->getString(3));
            m_kafkaParamVec.push_back(m_rs->getString(4));

            //m_kafkaParamVec.push_back(t_kafkaProperty);
        }
    }
    catch(SQLException ex)
    {
        char errmsg[256];
        memset(errmsg, 0x00, 256);
        sprintf(errmsg, "Error:%d,%s", ex.getErrorCode(), ex.getMessage().c_str());
        theErrorMessages->insert(CS_ERROR_DB_EXEC, errmsg);
        theErrorMessages->insert(CS_ERROR_DB_EXEC, "executeQuery SQL [" + m_selectSql + "] failed!");
        return false;
    }

    //释放资源
    m_conn->terminateStatement(m_stmt); //终止Statement对象
    m_env->terminateConnection(m_conn); //断开数据库连接
    Environment::terminateEnvironment(m_env); //终止环境变量
    m_rs = (ResultSet *)NULL;
    m_stmt = (Statement *)NULL;
    m_conn = (Connection *)NULL;
    m_env = (Environment *)NULL;
    return true;
}


bool SplitConfig::getChanNoByInfo(const int &sCustId, string &channelNo)
{
    vector<CustChannel>::iterator itr;
    for(itr = m_custChannel.begin(); itr != m_custChannel.end(); ++itr)
    {
        if((*itr).m_beginPartition <= sCustId && (*itr).m_endPartition >= sCustId)
        {
            channelNo = (*itr).m_channelNo;
            return true;
        }
    }
    return false;
}

//add by yueyq  for TFS:362748   split 根据地市编码再根据用户后四位分通道
bool SplitConfig::getChanNoByInfo(const int &sCustId, string &city_code, string &channelNo)
{
    vector<CustChannel>::iterator itr;
    for(itr = m_custChannel.begin(); itr != m_custChannel.end(); ++itr)
    {
        if((*itr).m_beginPartition <= sCustId && (*itr).m_endPartition >= sCustId && ((*itr).m_cityCode == city_code || (*itr).m_cityCode == "*" ))
        {
            channelNo = (*itr).m_channelNo;
            return true;
        }
    }
    return false;
}
//end

AbstractFieldSelector *SplitConfig::generateSelect(char *r_strRule, const char r_delimiter, const size_t r_maxLen)
{
    int t_fieldIndex, t_offset, t_length;
    AbstractFieldSelector *t_anySelect = new AnySelector();
    t_anySelect->setMaxLength(r_maxLen);
    for(char *t_s = strtok(r_strRule, ")"); t_s != NULL; t_s = strtok(NULL, ")"))
    {
        if(sscanf(t_s, t_s == r_strRule ? "%d%*[ (]%d%*[ ,]%d" : "%*[, ]%d%*[ (]%d%*[ ,]%d", &t_fieldIndex, &t_offset, &t_length) != 3)
        {
#ifdef _DEBUG_
            cout << "DIVIDEFORMAT config info error!{" << r_strRule << "}" << endl;
#endif
            return false;
        }

#ifdef _DEBUG_
        //cout<<__FILE__<<__LINE__<<" t_fieldIndex="<<t_fieldIndex<<" t_offset="<<t_offset<<" t_length="<<t_length<<endl;
#endif

        AbstractFieldSelector *t_sel = new DelimiterSelector(r_delimiter, t_fieldIndex - 1);
        if(t_length != 0)
        {
            t_sel->appendSubSelector(new FixSelector(t_offset, t_length));
        }
        t_anySelect->appendSubSelector(t_sel);
        t_s = NULL;
    }
    m_selectors.push_back(t_anySelect);
    return t_anySelect;
}

//路径规整
void SplitConfig::standardPath(string &r_path)
{
    const char *r_Path = r_path.c_str();
    if(strlen(r_Path) > 0 && r_Path[strlen(r_Path) - 1] != '/')
    {
        r_path = r_path + "/";
    }
    r_Path = NULL;
}

//liujq kafka
bool SplitConfig::isPath(const string &r_pathName)
{
    FileOperate t_path;
    vector<string> t_pathVec;
    t_pathVec.clear();

    StringUtil::split(r_pathName.c_str(), ";", t_pathVec);

    if(t_pathVec.size() > 0)
    {
        if(strncmp(t_pathVec[0].c_str(), "MDS", 3) == 0)
        {
#ifdef _DEBUG_
            cout << __FILE__ << __LINE__ << " kafka路径校验:" << t_pathVec[0] << endl;
#endif
            if(t_path.setKAFKA(KafkaInfo::getInstance()) == false)
            {
#ifdef _DEBUG_
                cout << __FILE__ << __LINE__ << " setKAFKA false!" << endl;
#endif
                return false;
            }
        }
        else
        {
#ifdef USE_SDFS
            t_path.setSDFS(ServerInfo::getInstance()->m_pSdfs);
#endif
        }

        //if(t_path.state(t_pathVec[0].c_str())<0)
        //{
        //return false;
        //}
        if(t_path.ISDIR(t_pathVec[0].c_str()))
            return true;
        else
            return false;
    }
    else
    {
        return false;
    }

}



ostream &operator<<(ostream &r_os, const CustChannel &r_channel)
{
    r_os << "BEGIN_PARTITION_ID: " << r_channel.m_beginPartition
         << " END_PARTITION_ID: " << r_channel.m_endPartition
         << " DB_NO: " << r_channel.m_dbNo
         << " CHANNEL_NO: " << r_channel.m_channelNo
         << " PROVINCE_CODE: " << r_channel.m_provCode;
}

bool operator<(const CustChannel &r_left, const CustChannel &r_right)
{
    if (r_left.m_provCode < r_right.m_provCode)
        return true;
    else if (r_left.m_provCode > r_right.m_provCode)
        return false;
    else
        return r_left.m_beginPartition < r_right.m_beginPartition;
}

