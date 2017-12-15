-- CREATE TABLE
CREATE TABLE TD_SPLIT_CHANNEL_DEF
(
  BEGIN_PARTITION_ID NUMBER(5) NOT NULL,
  END_PARTITION_ID   NUMBER(5) NOT NULL,
  DB_NO              NUMBER(3) ,
  CHANNEL_NO         VARCHAR2(10) NOT NULL,
  PROVINCE_CODE      VARCHAR2(8) NOT NULL,
  EPARCHY_NAME       VARCHAR2(100)
);
-- add comments to the columns 
comment on column td_split_channel_def.begin_partition_id
  is '��ʼ����';
comment on column td_split_channel_def.end_partition_id
  is '��������';
comment on column td_split_channel_def.db_no
  is '���';
comment on column td_split_channel_def.channel_no
  is '�ַ�ͨ��';
comment on column td_split_channel_def.province_code
  is 'ʡ�ݱ���';
comment on column td_split_channel_def.eparchy_name
  is '������';

-- Create/Recreate primary, unique and foreign key constraints 
alter table TD_SPLIT_CHANNEL_DEF
  add constraint PK_TD_SPLIT_CHANNEL_DEF primary key (CHANNEL_NO, PROVINCE_CODE);
  
-- Grant/Revoke object privileges 
--create or replace synonym td_split_channel_def for ucr_param.td_split_channel_def; 
grant select, insert, update, delete on td_split_channel_def to uop_param;