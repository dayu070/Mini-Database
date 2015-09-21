CC=gcc
CFLAGS=-std=c99 -I.
DEPS = dberror.h storage_mgr.h buffer_mgr_stat.h buffer_mgr.h frame_pool.h test_helper.h dt.h
OBJ = dberror.o storage_mgr.o buffer_mgr_stat.o buffer_mgr.o frame_pool.o test_assign2_1.o

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

test_assign2_1: $(OBJ)
	gcc -o  $@ $^ $(CFLAGS)

clean:
	rm -f $(OBJ)