
AR = ar cq

#OFLAGS = -O2

INCS = -I/usr/local/include/droplet-1.0
CFLAGS = $(INCS) $(OFLAGS) -g -Wall -Wunused -Werror -D_REENTRANT -fPIC

CC  = gcc $(CFLAGS)

LIBCOMMON=libcommon.a

OBJS = \
common.o \
path.o \
file.o

all: $(LIBCOMMON)

%.o: %.c
	$(CC) -o $@ -c $<

$(LIBCOMMON): $(OBJS)
	rm -f $(LIBCOMMON)
	$(AR) $(LIBCOMMON) $(OBJS)

clean:
	rm -f $(OBJS) $(LIBCOMMON)
