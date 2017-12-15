#ifndef CUSTSPLITTERR_H
#define CUSTSPLITTERR_H

const int MAX_FIELD_LENGTH = 256;//һ���ֶεĳ���
const int MAX_CDR_LENGTH   = 1024*4;//һ��������󳤶�

//�ļ�������
const int CS_FILEERROR_BASE = 500; 
const int ERROR_FILENAME_NAMERULE = CS_FILEERROR_BASE + 1;
const int ERROR_FILENAME_LENGTH   = CS_FILEERROR_BASE + 2;
const int ERROR_FILE_OPEN         = CS_FILEERROR_BASE + 3;
const int ERROR_UNLINK_FILE       = CS_FILEERROR_BASE + 4; //ɾ���ļ�����
const int ERROR_WRITE_FILE        = CS_FILEERROR_BASE + 5; //д�ļ�����
const int ERROR_FILE_CLOSE         = CS_FILEERROR_BASE + 6;
const int ERROR_FILE_READ         = CS_FILEERROR_BASE + 7;//���ļ�ʧ��


//���ݿ���ʴ���
const int CS_ERROR_DB                   = 550;
const int CS_ERROR_DB_CON               =CS_ERROR_DB + 1;
const int CS_ERROR_DB_BIND              =CS_ERROR_DB + 2;
const int CS_ERROR_DB_EXEC              =CS_ERROR_DB + 3;
const int CS_GET_FULLVAL                =CS_ERROR_DB + 21;

//�������
const int CS_ERROR_BASE       = 600;
const int CS_ERROR_INITIALIZE           = CS_ERROR_BASE + 1;   // ��ʼ������
const int E_MOVE_FILE_ERR               = CS_ERROR_BASE + 3;   //���߻���ʧ��
const int E_OUT_CHANNEL_DEFINED         = CS_ERROR_BASE + 4;   //������û�ж������ͨ��·��
const int E_OUT_CHANNEL                 = CS_ERROR_BASE + 5;   //���ݿͻ�ID�Ϳ����ͨ���Ŵ���
const int CS_ERROR_PWD                  = CS_ERROR_BASE + 7;   //���ܻ�ȡ����ʧ��

//liujq kafka
const int ERROR_KAFKA_INIT_ERROR        = CS_ERROR_BASE + 8;   //kafka��ʼ������
const int ERROR_KAFKA_PATH_ERROR        = CS_ERROR_BASE + 9;   //kafkaĿ¼����
const int ERROR_KAFKA_SAVE_OFFSET_ERROR = CS_ERROR_BASE + 10;   //kafka����offset����
const int ERROR_KAFKA_OPEN_ERROR        = CS_ERROR_BASE + 11;   //kafka�򿪴���
const int ERROR_KAFKA_ROWIDALL_ERROR    = CS_ERROR_BASE + 12;   //kafka rowidall����




const int ONE = 1;
const int TWO = 2;
const int THREE = 3;
const int FOUR = 4;

#endif
