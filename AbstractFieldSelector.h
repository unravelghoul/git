#ifndef _SPLIT_ABSTRACTFIELDSELECTOR_H_20150707_
#define _SPLIT_ABSTRACTFIELDSELECTOR_H_20150707_
#include "base/PString.h"

class AbstractFieldSelector
{
  public:
    AbstractFieldSelector();
    virtual ~AbstractFieldSelector();  
    
    //����ָ�����򣬴ӻ����л�ȡֵ
    virtual int selectFieldValue(const char* r_sfirst, const char *r_slast,
                                 char *r_buffer, size_t r_size) const;
    virtual void doSelect(const char* r_sfirst, const char *r_slast,
                          const char*&r_tfirst, const char *&r_tlast)const = 0;
    void setMaxLength(size_t r_maxLength)
    {
      m_maxLength = r_maxLength;
    }
    virtual bool appendSubSelector(AbstractFieldSelector *r_selector);

  protected:
    int m_maxLength; //��󳤶�

  private:
    vector<AbstractFieldSelector*> m_subSelectors;
};

class FixSelector : public AbstractFieldSelector
{
  public:
    FixSelector(int r_sOffSet, int r_length);
    virtual void doSelect(const char* r_sfirst, const char *r_slast,
                          const char*&r_tfirst, const char *&r_tlast)const;

  private:
    int m_offSet; //��ʼƫ����
    int m_len;    //����

};

class DelimiterSelector : public AbstractFieldSelector
{
  public:
    DelimiterSelector(char r_delimiter, int r_offset);
    virtual void doSelect(const char* r_sfirst, const char *r_slast,
                          const char*&r_tfirst, const char *&r_tlast)const;

  private:
    char m_delimiter; //�ָ��
    int  m_offset;    //�ڼ����ֶ�(1.2.3....)

};

class AnySelector : public AbstractFieldSelector
{
  public:
    virtual void doSelect(const char* r_sfirst, const char *r_slast,
                          const char*&r_tfirst, const char *&r_tlast)const;
    

};


#endif /* ABSTRACTFIELDSELECTOR_H_HEADER_INCLUDED_B764183B */
