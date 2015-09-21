
Description:
We put all our implementations into the frame_pool.c which exposes some interfaces for buffer_manager
to invoke.

Solution:
We use link list queue and an array table to implement the FIFO and LRU strategy.
We always put the latest loaded page node in the front of the queue and earliest loaded
page in the rear of the queue. Thus, we always evict page in the rear of the queue. The 
difference between LRU and FIFO is that we will put the page node that is pinned to
the front of the queue when LRU strategy is working. The functionality of the array table 
is as an index for referring the page. Meanwhile, we replace the page node in the array table
based on page number storing in the node evicted from the queue. Also, we can easily 
convict it to a real hash tabel for the purpose of future extension when we find some appropriate hash function. 

extra feature:
We partly implement the thread safe feature:
1. The buffer manager will be initialized only when it has not been initialized before. We
achieve this via keeping the buffer manager's pointer as a global variable;
2. If some function invokes the initBufferPool function after it's been initialized, we only
duplicate the BM_BufferPool.

