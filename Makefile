
DIRS = \
common \
id

include Make.vars

all prod prod32 debug install rpm clean fproto:
	@+$(RECURSE)

valgrind: debug
