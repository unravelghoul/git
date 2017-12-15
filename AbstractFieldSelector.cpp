#include "AbstractFieldSelector.h"
#include <iostream>

AbstractFieldSelector::AbstractFieldSelector()
{
  m_maxLength   =0;
}
AbstractFieldSelector::~AbstractFieldSelector()
{
  for(vector<AbstractFieldSelector*>::iterator t_itr=m_subSelectors.begin();
      t_itr!= m_subSelectors.end();++t_itr)
  {
    delete *t_itr;
    *t_itr=NULL;
  }
  m_subSelectors.clear();
}


int AbstractFieldSelector::selectFieldValue(const char* r_sfirst,
                                            const char *r_slast,
                                                  char *r_buffer,
                                                  size_t r_size) const
{
  const char *t_tfirst,*t_tlast;
  doSelect(r_sfirst,r_slast,t_tfirst,t_tlast);
  size_t t_count;
  if(m_subSelectors.empty())
  {
    t_count = t_tlast - t_tfirst;

    if(t_count > r_size-1)
    {
      #ifdef _DEBUG_
        cout<<"r_buffer ÄÚ´æ²»×ã!"<<__FILE__<<__LINE__<<endl;
      #endif
      return -1;
    }

    strncpy(r_buffer,t_tfirst, t_count);
    r_buffer[t_count] = '\0';

    if(m_maxLength > 0 && t_count > m_maxLength)
    {
      #ifdef _DEBUG_
        cout<<"×Ö¶Î³¬³¤!"<<__FILE__<<__LINE__<<endl;
      #endif
      return -1;
    }
    t_tfirst = t_tlast = NULL;
    return t_count;
  }
  else
  {
    char  *t_lbuf = r_buffer;
    size_t t_lsize = r_size;

    for(size_t i=0; i<m_subSelectors.size(); ++i)
    {
      t_count = m_subSelectors[i]->selectFieldValue(t_tfirst,t_tlast,t_lbuf,t_lsize);
      t_lbuf  += t_count;
      t_lsize -= t_count;
    }

    if(m_maxLength > 0 && t_count > m_maxLength)
    {
      #ifdef _DEBUG_
        cout<<"×Ö¶Î³¬³¤!"<<__FILE__<<__LINE__<<endl;
      #endif
      return -1;
    }
    t_tfirst = t_tlast = t_lbuf = NULL;
    return (r_size-t_lsize);
  }  
}


bool AbstractFieldSelector::appendSubSelector(AbstractFieldSelector *r_selector)
{
  m_subSelectors.push_back(r_selector);
  return true;
}
  
FixSelector::FixSelector(int r_sOffSet, int r_length)
{
  m_offSet = r_sOffSet;
  m_len    = r_length;
}

void FixSelector::doSelect(const char*  r_sfirst, const char *r_slast,
                           const char* &r_tfirst, const char *&r_tlast) const
{
  //¿Õ×Ö¶Î
  if (r_sfirst == r_slast)
  {
  	r_tfirst = r_sfirst;
  	r_tlast  = r_slast;
  }
  else//·Ç¿Õ×Ö¶Î
  {
    r_tfirst = r_sfirst + m_offSet;
    r_tlast  = r_sfirst + m_offSet + m_len;
  }
  if(r_tlast > r_slast)
  {
    r_tlast = r_slast;
  }
}


DelimiterSelector::DelimiterSelector(char r_delimiter, int r_offset)
{
  m_delimiter = r_delimiter;
  m_offset    = r_offset;
}

void DelimiterSelector::doSelect(const char*  r_sfirst, const char *r_slast,
                                 const char* &r_tfirst, const char *&r_tlast) const
{
  int t_i;
  const char *t_p;

  r_tfirst = r_sfirst;
  r_tlast  = r_slast;

  for(t_i=0, t_p = r_sfirst; t_p != r_slast; ++t_p)
  {
    if(*t_p == m_delimiter)
    {
      ++t_i;

      if(t_i == m_offset)
      {
        r_tfirst = t_p+1;
      }else if(t_i > m_offset)
      {
        r_tlast = t_p;
        break;
      }
    }
  }

  if(t_i < m_offset)
  {
    #ifdef _DEBUG_
      cout<<"Ô´×Ö¶ÎµÄ×Ö¶ÎÊý²»¹»!"<<__FILE__<<__LINE__<<endl;
    #endif
    r_tlast = r_tfirst;
  }
  t_p = NULL;
}


void AnySelector::doSelect(const char*  r_sfirst, const char *r_slast,
                           const char* &r_tfirst, const char *&r_tlast) const
{
  r_tfirst = r_sfirst;
  r_tlast  = r_slast;
}
