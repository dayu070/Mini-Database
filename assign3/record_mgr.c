//
//  record_mgr.c
//  record_manager
//
//  Created by qizhan on 3/14/14.
//  Copyright 2014 __MyCompanyName__. All rights reserved.
//




#include <stdio.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
//#include "rm_serializer.c"
// Bookkeeping for scans


// table and manager
 RC initRecordManager (void *mgmtData)
{
    RC ret = RC_OK;
    return ret;
}
 RC shutdownRecordManager ()
{
    RC ret = RC_OK;
    return ret;
}



char * serialSchema(const char *filename, Schema *schema)		//save schema to the page in the memory
{
	char *page;
	const char sep = '\n';		//EOF is the sign to indicate that the string is at the end
	int offset = 0, numattr, keysize, i, length;
	numattr = schema->numAttr;
	page = (char *)malloc(sizeof(char)*PAGE_SIZE);
    
    memset(page, 0, PAGE_SIZE);
    
	*((int *)(page+offset)) = schema->numAttr;		//copy numAttr into page
	offset +=sizeof(int);		//int occupies 4 bytes
	
	for(i=0; i<numattr; i++)		//copy attrNames into page, and each attrNames is separate by \0 in the page
	{
		length = strlen(schema->attrNames[i])+1;
		memcpy( page+offset, schema->attrNames[i], length );
		offset +=length;			//update the datacount
	}
    
	for(i=0; i<numattr; i++)		//copy dataTypes into page
	{
		*((DataType *)(page+offset)) = schema->dataTypes[i];
		offset +=sizeof(DataType);
	}
	
	for(i=0; i<numattr; i++)		//copy typeLength into page
	{
		*((int *)(page+offset)) = schema->typeLength[i];
		offset +=sizeof(int);
	}
    
	keysize = schema->keySize;
	
	*((int *)(page+offset)) = schema->keySize;		//copy keySize into page
	offset +=sizeof(int);
    
	for(i=0; i<keysize; i++)		//copy keyAttrs into page
	{
		*((int *)(page+offset)) = schema->keyAttrs[i];
		offset +=sizeof(int);
	}
	

    
	return page;
}

RC initSchema( const char *page, Schema * schema)
{

    RC ret = RC_OK;
	int i, offset = 0;

	schema->numAttr = *((int *)(page+offset));			//read the numAttr from the page and store it to the schema
	offset +=sizeof(int);
    
    
    schema->attrNames = (char **) malloc(sizeof(char*) * schema->numAttr);
	for(i=0; i<schema->numAttr; i++)			//read the attrNames from the page to the schema
	{
		char buffer[256] = "";
		int dataindex = 0;
		while( (buffer[dataindex] = *(page+offset)) != '\0')		//read i'th attrNames and store it into the buffer.
        {
            offset++;
            dataindex++;
        }
			
		//buffer[dataindex] = '\0';
		schema->attrNames[i] = (char *)malloc(dataindex*sizeof(char)+1);
   
		//schema->attrNames[i] = '\0';
		strcpy( schema->attrNames[i], buffer );
		offset += 1;
	}
	
	schema->dataTypes = (DataType *)malloc(sizeof(DataType)*schema->numAttr);
	for(i=0; i<schema->numAttr; i++)		//read the DataType from the page to the schema
	{
		schema->dataTypes[i] = *((DataType *)(page+offset));
		offset += sizeof(DataType);
	}
	
	schema->typeLength = (int *)malloc(sizeof(int)*schema->numAttr);
	for(i=0; i<schema->numAttr; i++)		//read the typeLength from the page to the schema
	{
		schema->typeLength[i] = *((int *)(page+offset));
		offset += sizeof(int);
	}
    
	schema->keySize = *((int *)(page+offset));		//read keySize from the page to the schema
	offset += sizeof(int);
    
	schema->keyAttrs = (int *)malloc(sizeof(int)*schema->keySize);
	for(i=0; i<schema->keySize; i++)			//read the keyAttrs from the page to the schema.
	{
		schema->keyAttrs[i] = *((int *)(page+offset));
		offset += sizeof(int);
	}
	
	return ret;
}


//RC freeSchema(Schema *schema)
//{
    
//}

//Creating a table should create the underlying page file and store information about the schema, free-space, ... and so on in the Table Information pages.
 RC createTable (char *name, Schema *schema)
{
    RC ret = RC_OK;
    //int numOfRecords;
   // numOfRecords = schema.

    
    
    SM_FileHandle fh;

    
	
	ret = createPageFile(name);		//create a page file to storage the table
    ret = openPageFile(name, &fh);
	//fd = (int*)fh.mgmtInfo;		//acquire the file descriptor of the file.
    
	ret = ensureCapacity(1,&fh);			//ensure the first page to store the schema.
    
    SM_PageHandle memPage = serialSchema(name,  schema);
    
    ret = writeBlock ( 0 , &fh ,  memPage);
    
	closePageFile(&fh);
	
    free(memPage);
    
	return ret;
}
 RC openTable (RM_TableData *rel, char *name)
{
    RC ret = RC_OK;
    
    
    /* read the table schema from the file, initialize the RM_TableData and the buffer manager */
	//int length = 0;
	Schema *schema = (Schema *)malloc(sizeof(Schema));
	
	
    SM_FileHandle fh;
    ret = openPageFile(name, &fh);
    
    
    char *memPage = (char *)malloc(sizeof(char)*PAGE_SIZE);
    
    ret = readBlock (0,  &fh,  memPage);
    
    initSchema( memPage, schema);
    
    closePageFile(&fh);
    
    
    if(schema == NULL)
	{
		printf("No table with name %s exist!\n", name);
		return FALSE;
	}
	int length = strlen(name);
	rel->name = (char *)malloc( sizeof(char)*(length+1) );		//allocated memory for rel->name
	rel->name[length] = '\0';
	strcpy( rel->name, name );				//copy name into the rel->name
	rel->schema = schema;				//rel->schema points to the schema which is read from the file 
    
	/* rel->mgmtData points to the buffer */
	BM_BufferPool *bm;
	bm = (BM_BufferPool *)malloc(sizeof(BM_BufferPool));		//allocate memory for bm
				//rel->mgmtData points to the buffer pool
	
	ret = initBufferPool(bm, name, 5, RS_FIFO, NULL);		//initial the buffer pool with 5 page frame, replacement strategy FIFO
	
    rel->mgmtData = bm;	
    
    free(memPage);
    
    return ret;
}
 RC closeTable (RM_TableData *rel)
{
    RC ret = RC_OK;
    free(rel->name);
    ret = freeSchema(rel->schema);
    ret = shutdownBufferPool(rel->mgmtData);
    return ret;
}
 RC deleteTable (char *name)
{
    RC ret = RC_OK;
    
    if(remove(name) == -1)
        ret = RC_FILE_NOT_FOUND;
    
    return ret;
}
int getNumTuples (RM_TableData *rel)
{
    RC ret = RC_OK;
    BM_BufferPool *bp = rel->mgmtData;
    int totalPages = getTotalPageNum(bp);
    int totalTuples = 0;
    if (totalPages == 1) {
        totalTuples = 0;
    } else {
        BM_PageHandle *h = MAKE_PAGE_HANDLE();
        for (int i = 1; i < totalPages; i++) {
            ret = pinPage(rel->mgmtData, h, i);
            BlockHeader bh;
            getHeader(h, &bh);
            totalTuples += bh.numberOfRecords;
        }
        free(h);
    }
    return totalTuples;
}


RC getHeader (BM_PageHandle *page, BlockHeader *bh)        //An operation to get the header of a page
{
    RC ret = RC_OK;
    int size = sizeof(BlockHeader);
    memcpy(bh, page->data, size);
    
    return ret;
}

RC setHeader (BM_PageHandle *page, BlockHeader *bh)
{
    RC ret = RC_OK;
    int size = sizeof(BlockHeader);
    memcpy(page->data, bh, size);
    return ret;
}



RC insertRecord (RM_TableData *rel, Record *record)
{
    RC ret = RC_OK;
    BM_BufferPool *bp = rel->mgmtData;
    int totalPages = getTotalPageNum(bp);
    int totalSlotNum = PAGE_SIZE/getRecordSize(rel->schema);
    int recordSize = getRecordSize(rel->schema);
    int recordHeaderSize = getRecordHeaderSize(rel->schema);
    
    
    BM_PageHandle *h = MAKE_PAGE_HANDLE();
    int temp1 = 1;
    while(temp1 < totalPages)
    {
        
        ret = pinPage(rel->mgmtData, h, temp1);       //Take out a page
        BlockHeader bh;
        getHeader (h, &bh);             //Get the header of the page
        if (bh.deletedSlot != -1)    //There is a deleted slot in the page.
        {
            int nextDeletedSlot = 0;
            record->id.slot = bh.deletedSlot;   //Set RID of the record
            record->id.page = temp1;
            char *dest = h->data + PAGE_SIZE - (bh.deletedSlot + 1) * recordSize + recordHeaderSize;
            memcpy(&nextDeletedSlot, dest, sizeof(int));
            memcpy(dest, record->data, recordSize - recordHeaderSize);    //Copy the record into memory
            
            dest = dest - recordHeaderSize;
            memset(dest, 0, recordHeaderSize);
            bh.deletedSlot = nextDeletedSlot;
            bh.numberOfRecords++;
            ret = setHeader(h, &bh);              // Update the header.
            
            ret = markDirty(rel->mgmtData, h);
            // forcePage(bp, h);
            ret = unpinPage(bp, h);
            break;
        }
        else if(bh.freeSpace < totalSlotNum) //No deleted slot but free slot
        {
            record->id.slot = bh.freeSpace;    //Set RID of the record
            record->id.page = temp1;
            char *dest = h->data + PAGE_SIZE - (bh.freeSpace + 1) * recordSize + recordHeaderSize;
            memcpy(dest, record->data, recordSize - recordHeaderSize);
            
            dest = dest - recordHeaderSize;
            memset(dest, 0, recordHeaderSize);
            
            bh.freeSpace++;
            bh.numberOfRecords++;
            ret = setHeader(h, &bh);                    // Update the header.
            
            ret = markDirty(rel->mgmtData, h);
            //forcePage(bp, h);
            ret = unpinPage(bp, h);
            break;
        }
        else   //The page is full
        {
            temp1++;
            ret = unpinPage(bp, h);
        }
        
    }   //End while
    if (temp1 >= totalPages)            //Create a new page
    {
        record->id.page = temp1;         //Set RID of the record
        record->id.slot = 0;
        ret = pinPage(rel->mgmtData, h, temp1);
        char *dest = h->data + PAGE_SIZE -  1 * recordSize + recordHeaderSize;
        memcpy(dest, record->data, recordSize - recordHeaderSize);
        
        dest -= recordHeaderSize;
        
        memset(dest, 0, recordHeaderSize);
        BlockHeader bh;
        bh.blockId = 0;  //TBD
        bh.fileId = 0;  //TBD
        bh.deletedSlot = -1;
        bh.freeSpace = 1;
        bh.numberOfRecords = 1;
        ret = setHeader(h, &bh);
        
        ret = markDirty(rel->mgmtData, h);
        
        forcePage(bp, h);
        
        ret = unpinPage(bp, h);
        
    }
    free(h);
    return ret;
}


RC deleteRecord (RM_TableData *rel, RID id)
{
    RC ret = RC_OK;
    BM_PageHandle *h = MAKE_PAGE_HANDLE();
    ret = pinPage(rel->mgmtData, h, id.page);
    BlockHeader bh;
    getHeader(h, &bh);
    bh.numberOfRecords--;            //Reduce the counter of the page;
    
    bool deleted = true;
    int alreadyDeleted = bh.deletedSlot;
    int recordSize = getRecordSize(rel->schema);
    
    char *dest = h->data + PAGE_SIZE - (id.slot + 1) * recordSize;
    memcpy(dest, &deleted, sizeof(bool));
    
    dest = h->data + PAGE_SIZE - (id.slot + 1) * recordSize + (rel->schema->numAttr + 1);
    memcpy(dest, &alreadyDeleted, sizeof(int));
    
    bh.deletedSlot = id.slot;
    
    setHeader(h, &bh);
    
    ret = markDirty(rel->mgmtData, h);
    
    if (ret == RC_OK) {
        ret = unpinPage(rel->mgmtData, h);
    }
    free(h);
    return ret;
}

RC updateRecord (RM_TableData *rel, Record *record)                 //UPDATE
{
    RC ret = RC_OK;
    BM_PageHandle *h = MAKE_PAGE_HANDLE();
    ret = pinPage(rel->mgmtData, h, record->id.page);
    
    if(ret == RC_OK)
    {
        int recordSize = getRecordSize(rel->schema);
        int recHeaderSize = getRecordHeaderSize(rel->schema);
        
        
        char *dest = h->data + PAGE_SIZE - (record->id.slot + 1) * recordSize + recHeaderSize;
        memcpy(dest, record->data, recordSize - recHeaderSize);
        ret = markDirty(rel->mgmtData, h);
        if (ret == RC_OK) {
            ret = unpinPage(rel->mgmtData, h);
        }
        
    }
    free(h);
    
    return ret;
}


RC getRecord (RM_TableData *rel, RID id, Record *record)
{
    RC ret = RC_OK;
    
    BM_PageHandle *page = MAKE_PAGE_HANDLE();
    pinPage(rel->mgmtData, page, id.page);
    
    int recSize = getRecordSize(rel->schema);
    int recHeaderSize = getRecordHeaderSize(rel->schema);
	char* src = page->data + PAGE_SIZE - recSize*(id.slot + 1)+recHeaderSize;
    
    memcpy(record->data, src, recSize - recHeaderSize);
	record->id.page = id.page;
	record->id.slot = id.slot;	
    
	unpinPage(rel->mgmtData, page);
    free(page);
    return ret;
}

/*
RC getRecord (RM_TableData *rel, RID id, Record *record)
{
    RC ret = RC_OK;
    
    BM_PageHandle *page = MAKE_PAGE_HANDLE();
    pinPage(rel->mgmtData, page, id.page);
    
    int recSize = getRecordSize(rel->schema);
    int recHeaderSize = getRecordHeaderSize(rel->schema);
	char* src = page->data + PAGE_SIZE - recSize*(id.slot + 1);
    bool deleted = FALSE;
    memcpy(&deleted, src, sizeof(bool));
    if (deleted == FALSE) {
        src += recHeaderSize;
        memcpy(record->data, src, recSize - recHeaderSize);
        record->id.page = id.page;
        record->id.slot = id.slot;
        
    }else
    {
        record->data = NULL;
        memcpy(record->data, <#const void *#>, <#unsigned long#>)
        
    }
    
    
	unpinPage(rel->mgmtData, page);
    free(page);
    return ret;
}
*/


// scans
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    RC ret = RC_OK;
    RM_CondAndRID *cr = (RM_CondAndRID *)malloc(sizeof(RM_CondAndRID));
    cr->cond = cond;
    cr->current.page = 1;
    cr->current.slot = 0;
    //cr->currentPageFreeSlotNum = 0;
    BM_PageHandle *bp = MAKE_PAGE_HANDLE();
    BM_BufferPool *bb = rel->mgmtData;
    pinPage(bb, bp, 1);
    BlockHeader bh;
    getHeader(bp, &bh);
    cr->currentPageFreeSlotNum = bh.freeSpace;
    unpinPage(bb, bp);
    scan->rel = rel;
    scan->mgmtData = cr;
    free(bp);
    return ret;
}

Value meetCondition(Record *record, Expr *cond, RM_TableData *rel)
{
    Value result;
    //MAKE_VALUE(result, DT_BOOL, FALSE);
    if (cond->expr.op->type == OP_BOOL_NOT) {
        if (cond->expr.op->args[0]->type == EXPR_OP) {
            Value temp;
            temp.dt = meetCondition(record, cond->expr.op->args[0], rel).dt;
            temp.v.boolV = meetCondition(record, cond->expr.op->args[0], rel).v.boolV;
            boolNot (&temp, &result);
        } else {
            boolNot (cond->expr.op->args[0]->expr.cons, &result);
        }
    }
    else if (cond->expr.op->type == OP_BOOL_AND)
    {
        Value left, right;
        if (cond->expr.op->args[0]->type == EXPR_OP) {
            left.dt = meetCondition(record, cond->expr.op->args[0], rel).dt;
            left.v.boolV = meetCondition(record, cond->expr.op->args[0], rel).v.boolV;
        }else
        {
            left.dt = cond->expr.op->args[0]->expr.cons->dt;
            left.v.boolV = cond->expr.op->args[0]->expr.cons->v.boolV;
        }
        if (cond->expr.op->args[1]->type == EXPR_OP) {
            right.dt = meetCondition(record, cond->expr.op->args[1], rel).dt;
            right.v.boolV = meetCondition(record, cond->expr.op->args[1], rel).v.boolV;
        }else
        {
            right.dt = cond->expr.op->args[1]->expr.cons->dt;
            right.v.boolV = cond->expr.op->args[1]->expr.cons->v.boolV;
        }
        boolAnd (&left, &right, &result);
    }
    else if (cond->expr.op->type == OP_BOOL_OR)
    {
        Value left, right;
        if (cond->expr.op->args[0]->type == EXPR_OP) {
            left.dt = meetCondition(record, cond->expr.op->args[0], rel).dt;
            left.v.boolV = meetCondition(record, cond->expr.op->args[0], rel).v.boolV;
        }else
        {
            left.dt = cond->expr.op->args[0]->expr.cons->dt;
            left.v.boolV = cond->expr.op->args[0]->expr.cons->v.boolV;
        }
        if (cond->expr.op->args[1]->type == EXPR_OP) {
            right.dt = meetCondition(record, cond->expr.op->args[1], rel).dt;
            right.v.boolV = meetCondition(record, cond->expr.op->args[1], rel).v.boolV;
        }else
        {
            right.dt = cond->expr.op->args[1]->expr.cons->dt;
            right.v.boolV = cond->expr.op->args[1]->expr.cons->v.boolV;
        }
        
        boolOr (&left, &right, &result);
    }
    else if (cond->expr.op->type == OP_COMP_EQUAL)
    {
        Value *left, *right;
        if (cond->expr.op->args[0]->type == EXPR_ATTRREF) {
            int attrNum = cond->expr.op->args[0]->expr.attrRef;
            getAttr(record, rel->schema, attrNum, &left);
        } else {
            left = cond->expr.op->args[0]->expr.cons;
        }
        
        if (cond->expr.op->args[1]->type == EXPR_ATTRREF) {
            int attrNum = cond->expr.op->args[1]->expr.attrRef;
            getAttr(record, rel->schema, attrNum, &right);
        } else {
            right = cond->expr.op->args[1]->expr.cons;
        }
        valueEquals(left, right, &result);
    }
    else if (cond->expr.op->type == OP_COMP_SMALLER)
    {
        Value *left, *right;
        if (cond->expr.op->args[0]->type == EXPR_ATTRREF) {
            int attrNum = cond->expr.op->args[0]->expr.attrRef;
            getAttr(record, rel->schema, attrNum, &left);
        } else {
            left = cond->expr.op->args[0]->expr.cons;
        }
        
        if (cond->expr.op->args[1]->type == EXPR_ATTRREF) {
            int attrNum = cond->expr.op->args[1]->expr.attrRef;
            getAttr(record, rel->schema, attrNum, &right);
        } else {
            right = cond->expr.op->args[1]->expr.cons;
        }
        valueSmaller(left, right, &result);
    }
    return result;
}

RC next (RM_ScanHandle *scan, Record *record)
{
    RC ret = RC_OK;
    int totalPageNum = getTotalPageNum(scan->rel->mgmtData);
    Record *rec;     //Temporarily store a record to check if it matches the condition
    ret = createRecord(&rec, scan->rel->schema);
  //  int totalSlotNum = PAGE_SIZE/getRecordSize(scan->rel->schema);
    
    RID id;
    RM_CondAndRID *cr = scan->mgmtData;
   // int currentPage = cr->current.page;
   // int currentSlot = cr->current.slot;
    int currentPageSlotNum = cr->currentPageFreeSlotNum;
    bool flag = FALSE;
    
    do {
        if(cr->current.slot >= currentPageSlotNum - 1)
        {
            cr->current.slot = 0;
            cr->current.page++;
            if (cr->current.page >= totalPageNum) {
                ret = RC_RM_NO_MORE_TUPLES;
                break;
            }
            else
            {   
                BM_PageHandle *bp = MAKE_PAGE_HANDLE();
                BM_BufferPool *bb = scan->rel->mgmtData;
                pinPage(bb, bp, cr->current.page);
                BlockHeader bh;
                getHeader(bp, &bh);
                cr->currentPageFreeSlotNum = bh.freeSpace;
                unpinPage(bb, bp);
                free(bp);
            }
                
        }else
            cr->current.slot++;
        
        id.page = cr->current.page;
        id.slot = cr->current.slot;
        getRecord(scan->rel, id, rec);
        
        flag = meetCondition(rec, cr->cond, scan->rel).v.boolV;
        if (flag == TRUE) {
           getRecord(scan->rel, id, record);
        }
        
    } while (flag == FALSE);
    
    /*for (int page = 0; page < totalPageNum; page++) {
     for (int slot = 0; slot < totalSlotNum; slot++) {
     id.page = page;
     id.slot = slot;
     getRecord(scan->rel,id, rec);
     if (rec != NULL) {
     Value *Val;
     int attrNum = 2;// get from expression condition
     getAttr(rec, scan->rel->schema, attrNum, &Val);
     Value a = *Val; // usage of Val for instance
     }
     }
     }*/
     freeRecord(rec);
    
    return ret;
}

RC closeScan (RM_ScanHandle *scan)
{
    RC ret = RC_OK;
    free(scan);
    return ret;
}






RC _attrOffset (Schema *schema, int attrNum, int *result)
{
    int offset = 0;
    int attrPos = 0;
    
    for(attrPos = 0; attrPos < attrNum; attrPos++)
        switch (schema->dataTypes[attrPos])
    {
        case DT_STRING:
            offset += schema->typeLength[attrPos];
            break;
        case DT_INT:
            offset += sizeof(int);
            break;
        case DT_FLOAT:
            offset += sizeof(float);
            break;
        case DT_BOOL:
            offset += sizeof(bool);
            break;
    }
    
    *result = offset;
    return RC_OK;
}

// dealing with schemas
int getRecordSize (Schema *schema)
{
    RC ret = RC_OK;

    int recordSize = 0;
    _attrOffset (schema, schema->numAttr, &recordSize);
    recordSize+=getRecordHeaderSize(schema);
    return recordSize;
}

int getRecordHeaderSize(Schema *schema)
{
    RC ret = RC_OK;
    int recordHeaderSize = sizeof(bool)+sizeof(bool )*schema->numAttr;
    return recordHeaderSize;
}

Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
    //RC ret = RC_OK;
    Schema *schema = (Schema *)malloc(sizeof(Schema));
    schema->numAttr = numAttr;
    schema->attrNames = attrNames;
    schema->dataTypes = dataTypes;
    schema->typeLength = typeLength;
    schema->keySize = keySize;
    schema->keyAttrs = keys;
    
    // Schema
    return schema;
}
RC freeSchema (Schema *schema)
{
    RC ret = RC_OK;
    int i = 0;
    for (i = 0; i<schema->numAttr; i ++)
    {
        free(schema->attrNames[i]);
    }
    free(schema->dataTypes);
    free(schema->typeLength);
    free(schema->keyAttrs);
    free(schema);
    return ret;
}



// dealing with records and attribute values
/////////////////////////////////////////////////
RC createRecord (Record **record, Schema *schema)
{
    RC ret = RC_OK;
    
    int recordSize = getRecordSize(schema);
    Record* rec = (Record *)malloc(sizeof(Record));
    rec->data = (char *)malloc(recordSize);
    memset(rec->data, 0 , recordSize);
    *record = rec;
    
    return ret;
}

RC freeRecord (Record *record)
{
    RC ret = RC_OK;
    //free(record->id);
    free(record->data);
    free(record);
    return ret;
}





RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
    RC ret = RC_OK;
    int offset = 0;
    _attrOffset(schema, attrNum, &offset);
    *value = malloc(sizeof(Value));
    switch (schema->dataTypes[attrNum]) {
        case DT_INT:
            (*value)->dt = DT_INT;
            memcpy(&((*value)->v.intV), record->data+offset, sizeof(int));
            break;
        case DT_BOOL:
            (*value)->dt = DT_BOOL;
     
            memcpy(&((*value)->v.boolV), record->data+offset, sizeof(bool));
            break;
        case DT_FLOAT:
            (*value)->dt = DT_FLOAT;

            memcpy(&((*value)->v.floatV), record->data+offset, sizeof(float));
            break;
        case DT_STRING:
            (*value)->dt = DT_STRING;
            (*value)->v.stringV = malloc(schema->typeLength[attrNum]);
            memcpy((*value)->v.stringV, record->data+offset, schema->typeLength[attrNum]);

            
            break;
        default:
            (*value)->dt = DT_INT;
            (*value)->v.intV = -1;
            break;
            
    }
    
    
    return ret;
}

RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
    RC ret = RC_OK;
    
    int offset = 0;
    //int i = 0;
    _attrOffset(schema, attrNum, &offset);
    switch (value->dt) {
        case DT_BOOL:
            memcpy(record->data+offset, &(value->v.boolV), sizeof(bool));
            break;
        case DT_FLOAT:
            memcpy(record->data+offset, &(value->v.floatV), sizeof(float));
            break;
        case DT_STRING:

            memcpy(record->data+offset, value->v.stringV, schema->typeLength[attrNum]);
            
            break;
        case DT_INT:
            memcpy(record->data+offset, &(value->v.intV), sizeof(int));

            break;
    }
    
    
    return ret;
}
