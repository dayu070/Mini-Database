//
//  buffer_mgr.c
//  525Assignment2
//
//  Created by QI ZHAN, QIANQI GUAN, JINGYU ZHU on 2/21/14.
//  Copyright (c) 2014 __MyCompanyName__. All rights reserved.
//

#include <stdio.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "frame_Pool.h"

BM_BufferPool * global_bufferPool = NULL;

void duplicateBufferPool(BM_BufferPool * des, BM_BufferPool * src)
{
    des->pageFile = src->pageFile;
    des->numPages = src->numPages;
    des->strategy = src->strategy;
    des->mgmtData = src->mgmtData;
}

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
                  const int numPages, ReplacementStrategy strategy, 
                  void *stratData)
{
    RC ret = RC_GENERAL_ERR;
    
    if (global_bufferPool == NULL) {

        bm->pageFile = pageFileName;
        bm->numPages = numPages;
        bm->strategy = strategy;
            
        //initializ strategy and initialize bm->memtData
        initFramePool(bm);
            
        global_bufferPool = bm;
        ret = RC_OK;

    }else{
        duplicateBufferPool(bm, global_bufferPool);
        ret = RC_OK;
    }    
    return ret; 
}
RC shutdownBufferPool(BM_BufferPool *const bm)
{
    forcePool( bm);
    destroyFramePool(bm);
    global_bufferPool = NULL;
    return RC_OK;
}
RC forceFlushPool(BM_BufferPool *const bm)
{
    return forcePool( bm);
}

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    return setIsDirty(bm, page, PAGE_DIRTY);
}
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    return decreaseFixCount(bm, page);
}
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    return _forcePage ( bm, page);
}
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
            const PageNumber pageNum)
{
    return referencePage(bm, page, pageNum);
}

// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm)
{
    return _getFrameContents(bm);
}
bool *getDirtyFlags (BM_BufferPool *const bm)
{
    
    return _getDirtyFlags(bm);
}
int *getFixCounts (BM_BufferPool *const bm)
{
    return _getFixCounts(bm);
}
int getNumReadIO (BM_BufferPool *const bm)
{
    return getNumberOfReadIO(bm);
}
int getNumWriteIO (BM_BufferPool *const bm)
{
    return getNumberOfWriteIO(bm);
}

int getTotalPageNum(BM_BufferPool *const bm)
{
    framePool *fp =  bm->mgmtData;
    //SM_FileHandle *fHandle = fp->fHandle;
    return fp->fHandle->totalNumPages;
}

