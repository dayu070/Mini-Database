//
//  STORAGE_MGR.c
//  CS525
//
//  Created by QI ZHAN, QIANQI GUAN, JINGYU ZHU on 1/17/14.
//  Copyright (c) 2014 __MyCompanyName__. All rights reserved.
//

#include "dberror.h"
#include "storage_mgr.h"
#include <stdio.h>



void initStorageManager (void)
{

}

 RC createPageFile (char *fileName)
{
    
    RC ret = RC_GENERAL_ERR;
    
    FILE *fp = NULL;
    fp = fopen(fileName, "wb");
    
    if (fp) {
        size_t sz = sizeof(char)*PAGE_SIZE;
        char* buf = (char*)malloc(sz);
        if (buf == NULL) {fputs("Memory error",stderr); exit (2);}
        memset(buf, 0, sz);
        ret = fwrite (buf , sizeof(char), PAGE_SIZE, fp);
        if (ret != PAGE_SIZE) {fputs ("writing error",stderr); exit (4);}
        free(buf);
        fclose(fp);
        ret = RC_OK;
    }else{
        ret = RC_FILE_HANDLE_NOT_INIT;
    }
    
    return ret;
}

 RC openPageFile (char *fileName, SM_FileHandle *fHandle)
{
    RC ret =  RC_OK;
    long sz = 0;
    int numberOfPages = 0;
    FILE *fp = NULL;
    
    fp=fopen(fileName, "rb+");
    if (fp) {
        //get the number of page
        fseek(fp, 0L, SEEK_END);// seek to end of file
        sz = ftell(fp);// get current file pointer
        fseek(fp, 0L, SEEK_SET);// seek back to beginning of file
        numberOfPages = sz/PAGE_SIZE;
        
        //initialize the SM_FileHandle
        fHandle->fileName = fileName;
        fHandle->totalNumPages = numberOfPages;
        fHandle->curPagePos = 0;
        fHandle->mgmtInfo = fp;
    }else{
        ret = RC_FILE_NOT_FOUND;
    }
    
    return ret;
}

 RC closePageFile (SM_FileHandle *fHandle)
{
    return fclose(fHandle->mgmtInfo);
}

 RC destroyPageFile (char *fileName)
{
    RC ret = RC_OK;
    
    if(remove(fileName) == -1)
        ret = RC_FILE_NOT_FOUND;
    
    return ret;
}

/* reading blocks from disc */
 RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    RC ret = RC_GENERAL_ERR;
    if (pageNum < fHandle->totalNumPages && pageNum >= 0) {
        if (fHandle->mgmtInfo != NULL) {
            if(0 == fseek(fHandle->mgmtInfo, pageNum*PAGE_SIZE*sizeof(char), SEEK_SET)){
                ret = fread(memPage,sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);
                fHandle->curPagePos = pageNum;
                if (ret != PAGE_SIZE) {fputs ("Reading error",stderr); exit (3);}
                ret = RC_OK;
            }
        }else{
            ret = RC_FILE_HANDLE_NOT_INIT;
        }

    }else{
        ret = RC_READ_NON_EXISTING_PAGE;
    }

    return ret;
}
 int getBlockPos (SM_FileHandle *fHandle)
{
    return fHandle->curPagePos;
}

 RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    return readBlock(0, fHandle, memPage);
}

 RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    RC ret = RC_OK;
    int preBlockPos = fHandle->curPagePos - 1;
    if (preBlockPos >= 0) {
        ret = readBlock (preBlockPos, fHandle, memPage);
        //fHandle->curPagePos = preBlockPos;
    }else{
        ret = RC_READ_NON_EXISTING_PAGE;
    }
    return ret;
}

 RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    RC ret = RC_OK;
    ret = readBlock (fHandle->curPagePos, fHandle, memPage);
    return ret;
}

 RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    RC ret = RC_OK;
    int nextBlockPos = fHandle->curPagePos + 1;
    if (nextBlockPos < fHandle->totalNumPages) {
        ret = readBlock (nextBlockPos, fHandle, memPage);
        //fHandle->curPagePos = nextBlockPos;
    }else{
        ret = RC_READ_NON_EXISTING_PAGE;
    }
    
    return ret;
}

 RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    RC ret = RC_OK;
    int lastBlockPos = fHandle->totalNumPages - 1;
    ret = readBlock (lastBlockPos, fHandle, memPage);
    return ret;
}

/* writing blocks to a page file */
 RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    RC ret = RC_WRITE_FAILED;
    if (pageNum < fHandle->totalNumPages && pageNum >= 0){
        if (sizeof(memPage) <= PAGE_SIZE*sizeof(char)) {
            if (fHandle->mgmtInfo != NULL){
                if(0 == fseek(fHandle->mgmtInfo, pageNum*PAGE_SIZE*sizeof(char), SEEK_SET)){
                    ret = fwrite (memPage , sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);
                    fHandle->curPagePos = pageNum;
                    if (ret != PAGE_SIZE) {fputs ("writing error",stderr); exit (4);}
                    ret = RC_OK;
                }
            }else{
                ret = RC_FILE_HANDLE_NOT_INIT;
            }
        }
    }else{
        ret = RC_WRITE_NON_EXISTING_PAGE;
    }
    return ret;
}

 RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    RC ret = RC_WRITE_FAILED;
    int curBlockPos = fHandle->curPagePos;
    ret = writeBlock (curBlockPos, fHandle, memPage);
    return ret;
}

 RC appendEmptyBlock (SM_FileHandle *fHandle)
{
    RC ret = RC_WRITE_FAILED;
    size_t sz = sizeof(char)*PAGE_SIZE;
    char* buf = (char*)malloc(sz);
    if (buf == NULL) {fputs("Memory error",stderr); exit (2);}
    memset(buf, 0, sz);
    
    if (fHandle->mgmtInfo != NULL){
        if(0 == fseek(fHandle->mgmtInfo, 0, SEEK_END)){
            ret = fwrite (buf, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);
            if (ret != PAGE_SIZE) {fputs ("writing error",stderr); exit (4);}
            ret = RC_OK;
            
        }
    }else{
        ret = RC_FILE_HANDLE_NOT_INIT;
    }
    
    free(buf);
    return ret;
}

 RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
{
    RC ret = RC_WRITE_FAILED;
    
    if (numberOfPages > fHandle->totalNumPages) {
        int addPageNum = numberOfPages - fHandle->totalNumPages;
        for (int i = 0; i < addPageNum; i++) {
            ret = appendEmptyBlock(fHandle);
            if(ret == RC_OK) fHandle->totalNumPages ++;
        }
         
    }else{
        ret = RC_OK;
    }
    return ret;
}
