CC=gcc
CFLAGS=-std=c99 -I.
DEPS = tables.h record_mgr.h dberror.h storage_mgr.h buffer_mgr_stat.h buffer_mgr.h frame_pool.h test_helper.h dt.h expr.h
OBJ = rm_serializer.o record_mgr.o dberror.o storage_mgr.o buffer_mgr_stat.o buffer_mgr.o frame_pool.o test_assign3_1.o expr.o test_expr.o

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

test_assign3_1: $(OBJ)
	gcc -o  $@ $^ $(CFLAGS)

clean:
	rm -f $(OBJ)
