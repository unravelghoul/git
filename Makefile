################################################################################
include $(BUDE_HOME)/stdmake/stdenv

################################################################################
SUBDIRS          = 
AFTER_SUMMIT     = 

################################################################################
SRCS             =  AbstractFieldSelector.cpp SplitApp.cpp SplitConfig.cpp 

SUBMIT_HDRS      = 

#生成目标
PROGRAM          = split
STATIC_LIBRARY   = 
SHARE_LIBRARY    = 

#提交相关变量
DEST    = $(WORK_HOME)/bin
DESTLIB = $(WORK_HOME)/lib
DESTINC = $(WORK_HOME)/include
DESTSRC =
DESTDOC = 

################################################################################
THE_INCLUDE      = -I./  \
                   -I$(WORK_HOME)/include \
                   -I$(DSC_PATH)/include \
                   -I$(SDFS_HOME)/include \
                   -I$(MDS_HOME)/include\
                   -I$(ZKCLIENT_HOME)/include

THE_LIBPATH      = -L./  \
                   -L$(WORK_HOME)/lib \
                   -L$(BUDE_HOME)/syscomp/lib \
                   -L${FRAMEWORK_HOME}/lib \
                   -L${ZKCLIENT_HOME}/lib \
                   -L$(SDFS_HOME)/lib \
                   -L$(DSC_PATH)/lib \
                   -L$(MDS_HOME)/lib 
                   
THE_LIB          = -lz -lrt -lapp -llog4cpp -locci -lpass -lframe -lzookeeper_mt -lsdfs -ldsc -lbase -lmds -libmc++ 
################################################################################
CXXFLAGS = ${THE_INCLUDE} $(SYSCOMP_INCLUDE) $(ORA_INCLUDE) -D_PSW_FROM_FILE_ -DUSE_SDFS  -g -fno-inline  #-DFP_DEBUG #-qmaxmem=-1
CCFLAGS  = ${THE_INCLUDE} $(SYSCOMP_INCLUDE) $(ORA_INCLUDE)
LDFLAGS  = ${THE_LIBPATH} $(SYSCOMP_LIBPATH) $(ORA_LIBPATH)
LDLIB    = ${THE_LIB}

################################################################################
include $(BUDE_HOME)/stdmake/stdmk
