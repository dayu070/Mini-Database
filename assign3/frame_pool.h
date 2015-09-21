//
//  strategy.h
//  buffer_manager
//
//  Created by qizhan on 2/21/14.
//  Copyright 2014 __MyCompanyName__. All rights reserved.
//

#ifndef buffer_manager_strategy_h
#define buffer_manager_strategy_h

#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"



typedef bool IsPageDirty;
#define PAGE_NOT_DIRTY false
#define PAGE_DIRTY true

typedef struct QNode
{
    struct QNode *previous, *next;
    PageNumber pageNum;
    char *data; // of which memory need to be allocated and freed
    int fixCount;
    IsPageDirty dirtyFlag;
} QNode;

// A Queue (A FIFO collection of Queue Nodes)
typedef struct _Queue
{
    unsigned count;  // Number of filled frames
    unsigned totalNumOfFrames; // total number of frames
    QNode *front, *rear;
    
    //Statistics
    int *frameContents;
    bool *dirtyFlags;
    int *fixCounts;
    int readIOCount;
    int writeIOCount;
    
} Queue;

// A table (Collection of pointers to Queue Nodes)
typedef struct _Hash
{
    int curPos;// maitian current position for the purpose of replacement
    int capacity; // how many pages can be there
    QNode* *array; // an array of queue nodes
} Hash;

// frame pool to hold the queue and hash table
typedef struct _framePool
{
    Queue *queue;
    Hash *hash;
    SM_FileHandle *fHandle;
    
} framePool;




RC initFramePool(BM_BufferPool *bp);
void destroyFramePool(BM_BufferPool *bp);

RC forcePool(BM_BufferPool *const bm);
RC _forcePage (BM_BufferPool *const bm, BM_PageHandle *const page);
RC referencePage(BM_BufferPool *bp, BM_PageHandle *ph, PageNumber pageNum);

RC setIsDirty(BM_BufferPool *bp, BM_PageHandle *ph, IsPageDirty ditryFlag);
RC decreaseFixCount(BM_BufferPool *const bm, BM_PageHandle *const page);

//statistics
PageNumber *_getFrameContents (BM_BufferPool *const bm);
bool *_getDirtyFlags (BM_BufferPool *const bm);
int *_getFixCounts (BM_BufferPool *const bm);
int getNumberOfReadIO (BM_BufferPool *const bm);
int getNumberOfWriteIO (BM_BufferPool *const bm);

#endif
