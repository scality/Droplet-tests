
PREFIX = /usr/local

RM = rm -f

INCS = -I/usr/local/include/droplet-1.0 -I/usr/local/ssl/include -I../common

PLATFORM ?= $(shell uname -s)
ifeq ($(PLATFORM),SunOS)
EXTRA_LIBS = -lsocket -lnsl
endif

LIBS = -L/usr/local/lib -ldroplet -lm -lz -lcrypto -lpthread $(EXTRA_LIBS) -L../common -lcommon
DEPENDS = /usr/local/lib/libdroplet.a ../common/libcommon.a

DROPLETKEYTOOLSSDIR = ../../tools/key
DROPLETSRWSTOOLSDIR = ../../tools/srws

#OFLAGS = -02

CFLAGSPROD = $(INCS) $(OFLAGS) -Wall -Wunused -Werror -Wuninitialized -D_REENTRANT -fPIC
CFLAGSDEBUG = $(INCS) -g3 -ggdb3 -Wall -Wunused -Werror -D_REENTRANT -fPIC -DASDK_DEBUG
CFLAGSVALGRIND = $(INCS) $(OFLAGS) -fno-inline -Wall -Wunused -Werror -D_REENTRANT -fPIC

LDFLAGSPROD = $(LIBS)
LDFLAGSDEBUG = $(LIBS)
LDFLAGVALGRIND = $(LIBS)

# not used by prod builds
CFLAGS=$(CFLAGSDEBUG)
LDFLAGS=$(LDFLAGSDEBUG)

TESTS = \
dpl_test_id_CRD

DPL_TEST_CRD_OBJS = dpl_test_id_CRD.o

all: prod

prod: CFLAGS=$(CFLAGSPROD)
prod: LDFLAGS=$(LDFLAGSPROD)
prod: compile

prod32: CFLAGS=$(CFLAGSPROD) -m32
prod32: LDFLAGS=$(LDFLAGSPROD) -m32
prod32: compile

debug: CFLAGS=$(CFLAGSDEBUG)
debug: LDFLAGS=$(LDFLAGSDEBUG)
debug: compile

valgrind: CFLAGS=$(CFLAGSVALGRIND)
valgrind: LDFLAGS=$(LDFLAGSVALGRIND)
valgrind: compile

compile: $(TESTS)

dpl_test_id_CRD: $(DPL_TEST_CRD_OBJS) $(DEPENDS)
	$(CC) -o dpl_test_id_CRD $(DPL_TEST_CRD_OBJS) $(LDFLAGS)

install:

rpm:

clean:
	$(RM) *.o $(TESTS)
