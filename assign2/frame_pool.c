//
//  525Assignment2
//
//  Created by QI ZHAN, QIANQI GUAN, JINGYU ZHU on 2/17/14.
//  Copyright (c) 2014 __MyCompanyName__. All rights reserved.
//

#include <stdio.h>
#include <stdlib.h>
#include "frame_pool.h"
#include "storage_mgr.h"


// A utility function to create a new Queue Node. The queue Node
// will store the given 'pageNumber'
QNode* newQNode( BM_PageHandle *pageHandle )
{
    // Allocate memory and assign 'pageNumber'
    QNode* temp = (QNode *)malloc( sizeof( QNode ) );
    
    // Initialize previous and next as NULL
    temp->previous = temp->next = NULL;
    
    //copy pageHandle's data to the node
    temp->pageNum = pageHandle->pageNum;
    temp->data = malloc( PAGE_SIZE );
    memset(temp->data, 0, PAGE_SIZE);
    pageHandle->data = temp->data;
    
    temp->fixCount = 0;
    temp->dirtyFlag = PAGE_NOT_DIRTY;
    
    return temp;
}

// A utility function to create an empty Queue.
// The queue can have at most 'totalNumOfFrames' nodes
Queue* createQueue( int totalNumOfFrames )
{
    Queue* queue = (Queue *)malloc( sizeof( Queue ) );
    
    // The queue is empty
    queue->count = 0;
    queue->front = queue->rear = NULL;
    
    // Number of frames that can be stored in memory
    queue->totalNumOfFrames = totalNumOfFrames;
    
    //initialize file handle
    //queue->fHandle = (SM_FileHandle*)malloc(sizeof(SM_FileHandle));
    
    //initialize statistics
    queue->readIOCount = 0;
    queue->writeIOCount = 0;
    
    queue->frameContents = (int*) malloc(totalNumOfFrames*sizeof(int));
    memset(queue->frameContents, 0, totalNumOfFrames*sizeof(int));
    
    queue->dirtyFlags = (bool*) malloc(totalNumOfFrames*sizeof(bool));
    memset(queue->dirtyFlags, 0, totalNumOfFrames*sizeof(bool));
    
    queue->fixCounts = (int*) malloc(totalNumOfFrames*sizeof(int));
    memset(queue->dirtyFlags, 0, totalNumOfFrames*sizeof(int));

    return queue;
}

void destroyQueue(Queue* q)
{
    QNode * tmp  =NULL;
    QNode * tmp2  =NULL;
    for ( tmp = q->front; tmp != NULL;  ) {
        
        tmp2 = tmp->next;
        free(tmp->data);
        free(tmp);
        tmp = tmp2;
    }
    
    //free(q->fHandle); 
    free(q->frameContents);
    free(q->fixCounts);
    free(q->dirtyFlags);
    free(q);
    
}

// A utility function to create an empty Hash of given capacity
Hash* createHash( int capacity )
{
    
    //int curPos;// maitian current position for the purpose of replacement
    // Allocate memory for hash
    Hash* hash = (Hash *) malloc( sizeof( Hash ) );
    hash->capacity = capacity;
    hash->curPos = 0;
    // Create an array of pointers for refering queue nodes
    hash->array = (QNode **) malloc( hash->capacity * sizeof( QNode* ) );
    
    // Initialize all hash entries as empty
    int i;
    for( i = 0; i < hash->capacity; ++i )
        hash->array[i] = NULL;
    
    return hash;
}

void destroyHash(Hash* hash)
{
    free(hash->array);
    free(hash);
    
}

//not used for now, but for the purpose of future enxtension
RC ensureHashCapacity( Hash* h, int capacity )
{
    RC ret = RC_GENERAL_ERR;
    //Hash *hash = h;
    int oldCapacity = h->capacity;
    if (h->capacity < capacity) {
        h->capacity = 2*capacity;
        
        // Create an array of pointers for refering queue nodes
        QNode ** QNodeArray = (QNode **) malloc( h->capacity * sizeof( QNode* ) );
        memset(QNodeArray, 0, h->capacity * sizeof( QNode* ));
        memcpy(QNodeArray, h->array, oldCapacity * sizeof( QNode* ));
        free(h->array);
        h->array = QNodeArray;
        ret = RC_OK;
    }else{
        ret = RC_OK;
    }
    
    return ret;
}

// A function to check if there is slot available in memory
int AreAllFramesFull( Queue* queue )
{
    return queue->count == queue->totalNumOfFrames;
}

// A utility function to check if queue is empty
int isQueueEmpty( Queue* queue )
{
    return queue->rear == NULL;
}

//now we traverse the array, but we can refer the page in future using hash function between the page number and node
int seekHashPos(Hash *hash, PageNumber pageNum)
{
    int pos = 0;
    bool found = false;
    for( ; pos < hash->capacity; pos++ ){
        QNode *q = hash->array[pos];
        if (q != NULL) {
            if (q->pageNum == pageNum) {
                found = true;
                break;
            }
        }

    }
    if (!found) {
        pos = NO_PAGE;
    }
    return pos;
}

//traverse the queue to find a node of which fixcount is not larger than 0
//if find this node, return the node, otherwise return NULL
QNode* findDequeNode(Queue* queue )
{
    QNode *dequeNode = NULL;
    QNode *tmp = NULL;
    
    for ( tmp = queue->rear; tmp != NULL;  ) {
        if (tmp->fixCount > 0) {
            tmp = tmp->previous;
        }else{
            dequeNode = tmp;
            break;}
    }
    return tmp;
}


void SetHashReplacePos(Hash *hash, PageNumber pageNum)
{
    int pos = seekHashPos(hash, pageNum);
    if (pos != NO_PAGE) {
        hash->curPos = pos;
        hash->array[ hash->curPos ] = NULL;
    }
}

// A utility function to delete a frame from queue
RC deQueue( Queue* queue , Hash *hash, BM_BufferPool *bp/*, BM_PageHandle *pageHandle*/)
{
    RC ret = RC_OK;
    
    //framePool *fp = bp->mgmtData;
    
   if( !isQueueEmpty( queue ) )
    {
        // If this is the only node in list, then change front
        if (queue->front == queue->rear)
            queue->front = NULL;
        
        
        //find the node that will be deleted, of which fixcount is not larger than 0
        QNode* temp = findDequeNode(queue);
        QNode* temp2 = NULL;
        
        if (temp != NULL) {

            if (queue->rear == temp){
                
                // if node in the rear, change queue's rear pointer and remove the  rear node
                queue->rear = queue->rear->previous;
                queue->rear->next = NULL;
                
            }else if(queue->front == temp){
                //if node in the front, change queue's front pointer and remove the front node
                queue->front = temp->next;
                queue->front->previous = NULL;
            }
            else{
                //if node in the middle of the queue
                temp2 = temp->previous;
                temp2->next = temp->next;
                temp->next->previous = temp2;
            }
            
            SetHashReplacePos(hash, temp->pageNum);
            
            //write dirty page back to disk
            if(temp->dirtyFlag == PAGE_DIRTY)
            {
                BM_PageHandle page;
                page.pageNum = temp->pageNum;
                page.data = temp->data;
                _forcePage (bp,  &page);
                    
            }
                
            free(temp->data);
            free( temp );

            // decrement the number of full frames by 1
            queue->count--;
        }
        else{//if no frame's fixcount is less than 0
                ret = RC_NO_AVAILABLE_FRAME;
            }
    }
    
    return ret;
}


// A function to add a page with given 'pageNumber' to both queue
// and hash
RC Enqueue( Queue* queue, Hash* hash, BM_BufferPool *bp, BM_PageHandle *pageHandle )
{
    RC ret = RC_GENERAL_ERR;
    bool increaseFlag = false;
    
    framePool *fp = bp->mgmtData;
    
    // If all frames are full, remove the page at the rear
    if ( AreAllFramesFull ( queue ) )
    {
        
        deQueue( queue , hash, bp/*, pageHandle*/);
    }else{
        increaseFlag = true;
    }
    
    // Create a new node with given page number,
    // And add the new node to the front of queue
    QNode* temp = newQNode( pageHandle );
    
    //read page from disk to memory
    ret = readBlock (pageHandle->pageNum, fp->fHandle, temp->data);
    queue->readIOCount++;
    
    if (ret != RC_OK) {
        int totalNumOfPages = pageHandle->pageNum + 1;
        ensureCapacity(totalNumOfPages, bp->mgmtData);
        ret = RC_OK;
    }

    
    temp->fixCount++;
    
    temp->next = queue->front;
    
    // If queue is empty, change both front and rear pointers
    if ( isQueueEmpty( queue ) )
        queue->rear = queue->front = temp;
    else  // Else change the front
    {
        queue->front->previous = temp;
        queue->front = temp;
    }
    
    // Add page entry to hash 
    hash->array[ hash->curPos ] = temp;
    if (increaseFlag) {
        hash->curPos++;
        increaseFlag = false;
    }
    
    pageHandle->data = temp->data;
    
    // increment number of full frames
    queue->count++;
    
    return ret;
}




/*==================================================================
 
 *              Strategy LRU                                        *
 *                                                                  *
 *                                                                  *
==================================================================*/

// 1. Frame is not there in memory, we bring it in memory and add to the front
//    of queue, which is the same as FIFO implementation
// 2. Frame is there in memory, we move the frame to front of queue, which is
//    different from FIFO
RC strategyLRU( BM_BufferPool *bp, BM_PageHandle *pageHandle )
{
    RC ret = RC_OK;
    
    
    framePool *fp = bp->mgmtData;
    Queue* queue = fp->queue;
    Hash* hash = fp->hash;
    
    int pos = seekHashPos(hash, pageHandle->pageNum);
    
    // the page is not in cache, bring it to the memory
    if ( pos == NO_PAGE )
        ret = Enqueue( queue, hash, bp, pageHandle );
    
    else
    {
        QNode* reqPage = hash->array[ pos ];
        
        // page is there and not at front, bring it to the front
        if (reqPage != queue->front)
        {
            
            // Unlink rquested page from its current location
            // in queue.
            reqPage->previous->next = reqPage->next;
            if (reqPage->next)
                reqPage->next->previous = reqPage->previous;
            
            // If the requested page is rear, then change rear
            // as this node will be moved to front
            if (reqPage == queue->rear)
            {
                queue->rear = reqPage->previous;
                queue->rear->next = NULL;
            }
            
            // Put the requested page before current front
            reqPage->next = queue->front;
            reqPage->previous = NULL;
            
            // Change previous of current front
            reqPage->next->previous = reqPage;
            
            // Change front to the requested page
            queue->front = reqPage;
            
            
        }
        
        //assign data to pageHandle
        pageHandle->data = reqPage->data;
        
        //increase the fixCount
        reqPage->fixCount++;

        
    }
    
        return ret;

}

/*==================================================================
 
 *              Strategy FIFO                                       *
 *                                                                  *
 *                                                                  *
 ==================================================================*/
//keep latest loaded page in the front of queue
//and the earliest page in the rear of the queue
//always evict the page in the rear of the queue

RC strategyFIFO( BM_BufferPool *bp, BM_PageHandle *pageHandle )
{
    RC ret = RC_OK;
    
    framePool *fp = bp->mgmtData;
    Queue* queue = fp->queue;
    Hash* hash = fp->hash;
    
    int pos = seekHashPos(hash, pageHandle->pageNum);
    
    
    // if the page is not in cache, bring it to the memory
    if ( pos == NO_PAGE )
        ret = Enqueue( queue, hash, bp, pageHandle );
    
    // if page is in the memory, return the memory address
    else{
        
        QNode* reqPage = hash->array[ pos ];
        //assign data to pageHandle
        pageHandle->data = reqPage->data;
        
        //increase the fixCount
        reqPage->fixCount++;
        
    }
    return ret;
}



RC initFramePool(BM_BufferPool *bm)
{
    RC ret = RC_GENERAL_ERR;
    
    framePool *fp = (framePool*)malloc(sizeof(framePool));
    
    // Let cache can hold capacity pages
    fp->queue = createQueue( bm->numPages );
    
    if (fp->queue != NULL) {
        // Let totalPageNum different pages can be requested
        fp->hash = createHash( bm->numPages );
        if (fp->queue != NULL) {
            ret = RC_OK;
        }
    }
    
    fp->fHandle = (SM_FileHandle*)malloc(sizeof(SM_FileHandle));
    ret = openPageFile (bm->pageFile, fp->fHandle);
    bm->mgmtData = fp;
    
    return ret;
    

}

void destroyFramePool(BM_BufferPool *bp)
{
    framePool *fp = bp->mgmtData;
    destroyQueue(fp->queue);
    destroyHash(fp->hash);
    closePageFile(fp->fHandle);
    free(fp->fHandle);
    free(fp);
}




RC referencePage( BM_BufferPool *bp, BM_PageHandle *ph, PageNumber pageNum )
{
    RC ret = RC_GENERAL_ERR;
    
    ph->pageNum = pageNum;
    
    switch (bp->strategy) {
        case RS_FIFO:
            ret = strategyFIFO(bp, ph);
            break;
        case RS_LRU:
            ret = strategyLRU( bp, ph);
            break;
            
        default:
            break;
    }
    return ret;
}



RC setIsDirty(BM_BufferPool *bp, BM_PageHandle *ph, IsPageDirty dirtyFlag)
{
    framePool *fp = bp->mgmtData;
    RC ret = RC_GENERAL_ERR;
    
    int pos = seekHashPos(fp->hash, ph->pageNum);
    if (pos != NO_PAGE) {
        QNode* reqPage = fp->hash->array[ pos ];
        
        reqPage->dirtyFlag = dirtyFlag;
   
        ret = RC_OK;
    }
    
    
    return ret;
}


RC _decreaseFixCount(Hash* hash, BM_BufferPool *const bm, BM_PageHandle *const page)
{
    RC ret = RC_GENERAL_ERR;

    int pos = seekHashPos(hash, page->pageNum);
    if (pos != NO_PAGE) {
        QNode* reqPage = hash->array[ pos ];
        
        reqPage->fixCount--;
        ret = RC_OK;
    }
    
    return ret;
}

RC decreaseFixCount(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    framePool *fp = bm->mgmtData;
    return _decreaseFixCount(fp->hash,  bm,  page);
}


//statistics
PageNumber *_getFrameContents (BM_BufferPool *const bm)
{
    framePool *fp = bm->mgmtData;
    Queue *q = fp->queue;
    Hash *hash = fp->hash;

    QNode *tmp = NULL;
    
    int pos = 0;
    for( ; pos < hash->capacity; pos++ ){
        tmp = hash->array[pos];
        if (tmp != NULL) {
            q->frameContents[pos] = tmp->pageNum;
        }else{
            q->frameContents[pos] = NO_PAGE;
        }
        
    }

    return q->frameContents;
    
}
bool *_getDirtyFlags (BM_BufferPool *const bm)
{
 
    framePool *fp = bm->mgmtData;
    Queue *q = fp->queue;
    Hash *hash = fp->hash;
    
    QNode *tmp = NULL;
    
    int pos = 0;
    for( ; pos < hash->capacity; pos++ ){
        tmp = hash->array[pos];
        if (tmp != NULL) {
            q->dirtyFlags[pos] = tmp->dirtyFlag;
        }else{
            q->dirtyFlags[pos] = PAGE_NOT_DIRTY;
        }
        
    }
    
    return q->dirtyFlags;
}
int *_getFixCounts (BM_BufferPool *const bm)
{
    
    framePool *fp = bm->mgmtData;
    Queue *q = fp->queue;
    Hash *hash = fp->hash;
    
    QNode *tmp = NULL;
    
    int pos = 0;
    for( ; pos < hash->capacity; pos++ ){
        tmp = hash->array[pos];
        if (tmp != NULL) {
            q->fixCounts[pos] = tmp->fixCount;
        }else{
            q->fixCounts[pos] = 0;
        }
        
    }
    
    return q->fixCounts;
    
}


int getNumberOfReadIO (BM_BufferPool *const bm)
{
    
    framePool *fp = bm->mgmtData;
    Queue *q = fp->queue;
    return q->readIOCount;
}

int getNumberOfWriteIO (BM_BufferPool *const bm)
{

    
    framePool *fp = bm->mgmtData;
    Queue *q = fp->queue;
    return q->writeIOCount;
}


//force all dirty pages with fixcount not larger than 0 to be writed back to disk
RC forcePool(BM_BufferPool *const bm)
{
    
    RC ret = RC_GENERAL_ERR;
    framePool *fp = bm->mgmtData;
    Queue *q = fp->queue;
    Hash *hash = fp->hash;
    
    QNode *tmp = NULL;
    
    int pos = 0;
    for( ; pos < hash->capacity; pos++ ){
        tmp = hash->array[pos];
        if (tmp != NULL) {
            if (tmp->dirtyFlag == PAGE_DIRTY && tmp->fixCount <= 0) {
                int numberOfPages = tmp->pageNum + 1;
                ensureCapacity (numberOfPages, fp->fHandle);
                ret = writeBlock(tmp->pageNum, fp->fHandle, tmp->data);
                q->writeIOCount++;
                tmp->dirtyFlag = PAGE_NOT_DIRTY;
                if (ret != RC_OK) break;
            }
        }        
    }
    
    return ret;

}

RC _forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    RC ret = RC_GENERAL_ERR;
    framePool *fp = bm->mgmtData;
    int numberOfPages = page->pageNum + 1;
    ret = ensureCapacity (numberOfPages, fp->fHandle);
    if (ret == RC_OK) {
        ret = writeBlock(page->pageNum, fp->fHandle, page->data);
        fp->queue->writeIOCount++;
    }
    return ret;
    
}
