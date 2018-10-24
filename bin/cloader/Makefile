PG_CONFIG = pg_config
INCLUDEDIR := $(shell $(PG_CONFIG) --includedir)
LIBDIR := $(shell $(PG_CONFIG) --libdir)

CFLAGS += -I$(INCLUDEDIR) # add server's include directory for libpq-fe.h
CFLAGS += -L$(LIBDIR)
CFLAGS += -Wl,-rpath=/$(LIBDIR)

all: cloader

cloader: cloader.c
	$(CC) $(CFLAGS) -o cloader cloader.c -lpq

clean:
	rm -rf cloader.o cloader
