#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "unistd.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include "expr.h"

// Table Details Struct.
typedef struct RM_TableDetail
{
	int numOfTuples;
	int schemaSize;
} RM_TableDetail;

// Record Manager Struct.
typedef struct RecordManager
{
	BM_BufferPool *bufferPool;
	int *freePages;
} RecordManager;

// Scan Manager Struct.
typedef struct RM_ScanManager
{
	Record *currentRecord;
	int currentSlot;
	Expr *expr;
	int currentPage;
} RM_ScanManager;

int totalNumberOfPages;

//calls the Mark Dirty function from buffer pool
void markDirtyInfo(RM_TableData *rel, BM_PageHandle *page)
{
	markDirty(((RecordManager *)rel->mgmtData)->bufferPool, page);
}

//calls the Unpin function from buffer pool
void unpinPageInfo(RM_TableData *rel, BM_PageHandle *page)
{
	unpinPage(((RecordManager *)rel->mgmtData)->bufferPool, page);
}

//calls the Force page function from buffer pool
void forcePageInfo(RM_TableData *rel, BM_PageHandle *page)
{
	forcePage(((RecordManager *)rel->mgmtData)->bufferPool, page);
}

/*
Ramya Krishnan(rkrishnan1@hawk.iit.edu) - A20506653
1. This method calls dirty info, unpin page and force page functions
2. Inputs- void
3. returns - Returns void
*/
void ModifyPageDetails(RM_TableData *rel, BM_PageHandle *page)
{
	markDirtyInfo(rel, page);
	unpinPageInfo(rel, page);
	forcePageInfo(rel, page);
}

RC SetOffAttrValue(Schema *schema, int attrNum, int *result)
{
	int value = 0;
	int offValue = value;
	int pos = value;

	for (pos = 0; pos < attrNum; pos++)
	{
		if (schema->dataTypes[pos] == DT_INT)
		{
			offValue += sizeof(int);
		}
		else if (schema->dataTypes[pos] == DT_BOOL)
		{
			offValue += sizeof(bool);
		}
		else if (schema->dataTypes[pos] == DT_STRING)
		{
			offValue += schema->typeLength[pos];
		}
		else if (schema->dataTypes[pos] == DT_FLOAT)
		{
			offValue += sizeof(float);
		}
	}
	*result = offValue;
	return RC_OK;
}

/*
Ramya Krishnan(rkrishnan1@hawk.iit.edu) - A20506653
1. This method initializes the record manager
2. Inputs- void
3. returns - Returns RC code
*/
RC initRecordManager(void *mgmtData)
{
	printf("Initializing record manager completed!\n");
	return RC_OK;
}

/*
Ramya Krishnan(rkrishnan1@hawk.iit.edu) - A20506653
1. This method shuts the record manager
2. Inputs- void
3. returns - Returns RC code
*/
RC shutdownRecordManager()
{
	printf("Shutdown of record manager completed!\n");
	return RC_OK;
}

//Checks if the create and open page are returing correct RC Code
//if not returns file not found
RC checkIfFileExist(RC returnCreatePage, RC returnOpenPage)
{
	if (returnCreatePage != RC_OK || returnOpenPage != RC_OK)
	{
		return RC_FILE_NOT_FOUND;
	}
	return RC_OK;
}

RM_TableDetail *createTableDetailObject()
{
	RM_TableDetail *tableDetail = (RM_TableDetail *)malloc(sizeof(RM_TableDetail));
	return tableDetail;
}

RecordManager *createRecordManagerObject()
{
	RecordManager *recordManager = (RecordManager *)malloc(sizeof(RecordManager));
	return recordManager;
}

/*
Ramya Krishnan(rkrishnan1@hawk.iit.edu) - A20506653
1. This method is used to create table
2. Inputs- schema and name
3. returns - Returns RC code
*/
RC createTable(char *name, Schema *schema)
{
	printf("Create table is started\n");
	int value = 0;
	SM_FileHandle filehandle;
	char *info = serializeSchema(schema);

	RM_TableDetail *tableDetail = createTableDetailObject();

	RC returnCreatePage = createPageFile(name);
	RC returnOpenPage = openPageFile(name, &filehandle);
	checkIfFileExist(returnCreatePage, returnOpenPage);

	tableDetail->schemaSize = value;
	RC writeflag = writeBlock(value, &filehandle, info);
	return (writeflag == RC_OK) ? RC_OK : RC_WRITE_FAILED;
	printf("Create table is ended\n");
}

//Total page value is set from the read Header
char SetTotalPages(char *readHeader)
{
	char *totalPage;
	totalPage = readHeader;
	return atoi(totalPage);
}

//Opens a file and returns the object
FILE *fileReturn(char *name)
{
	FILE *file = fopen(name, "r+");
	return file;
}

//Calls the fgets method to get the file
void getFile(char *readHeader, FILE *file)
{
	fgets(readHeader, PAGE_SIZE, file);
}

//Calls init buffer pool function from buffer pool class
void callInitBufferPool(BM_BufferPool *const bufferPool, char *name)
{
	int six = 6;
	initBufferPool(bufferPool, name, six, RS_FIFO, NULL);
}

//Calls pin page function from pin page class
void callPinPage(BM_BufferPool *const bufferPool, BM_PageHandle *const page)
{
	int zero = 0;
	pinPage(bufferPool, page, 0);
}

/*
Ramya Krishnan(rkrishnan1@hawk.iit.edu) - A20506653
1. This method is used to Open already created table
2. Inputs- table data and name
3. returns - Returns RC code
*/
RC openTable(RM_TableData *rel, char *name)
{
	printf("Open table is started\n");
	RecordManager *recordManager = createRecordManagerObject();
	FILE *file = fileReturn(name);
	char *readHeader;
	readHeader = (char *)calloc(PAGE_SIZE, sizeof(char));
	getFile(readHeader, file);
	totalNumberOfPages = SetTotalPages(readHeader);
	recordManager->bufferPool = MAKE_POOL();

	BM_PageHandle *page = MAKE_PAGE_HANDLE();
	callInitBufferPool(recordManager->bufferPool, name);
	callPinPage(recordManager->bufferPool, page);
	recordManager->freePages = (int *)malloc(sizeof(int));
	recordManager->freePages[0] = totalNumberOfPages;
	rel->name = name;
	rel->schema = deserializeSchema(page->data);
	rel->mgmtData = recordManager;
	free(page);
	free(readHeader);
	printf("Open table is ended\n");
	return RC_OK;
}

//frees all the attributes once the table is closed
void freeAttr(RecordManager *recordManager, RM_TableData *rel)
{
	char **attrName = rel->schema->attrNames;
	DataType *dataType = rel->schema->dataTypes;
	int *keyAttrs = rel->schema->keyAttrs;
	int *typeLength = rel->schema->typeLength;
	free(recordManager);
	free(attrName);
	free(dataType);
	free(keyAttrs);
	free(typeLength);
	free(rel->schema);
}

/*
Ramya Krishnan(rkrishnan1@hawk.iit.edu) - A20506653
1. This method is used to close already created table
2. Inputs- table data
3. returns - Returns RC code
*/
RC closeTable(RM_TableData *rel)
{
	printf("close table is started\n");
	RecordManager *recordManager = createRecordManagerObject();
	recordManager = rel->mgmtData;
	shutdownBufferPool(recordManager->bufferPool);
	freeAttr(recordManager, rel);
	printf("close table is ended\n");
	return RC_OK;
}

/*
Ramya Krishnan(rkrishnan1@hawk.iit.edu) - A20506653
1. This method is used to delete exisitng table
2. Inputs- name of the tabble
3. returns - Returns RC code
*/
RC deleteTable(char *name)
{
	printf("delete table is started\n");
	RC destroyFlag = destroyPageFile(name);
	return destroyFlag != RC_OK ? RC_FILE_NOT_FOUND : RC_OK;
	printf("delete table is ended\n");
}

//created record object
Record *createRecordObject()
{
	Record *record = (Record *)malloc(sizeof(Record));
	return record;
}

/*
Ramya Krishnan(rkrishnan1@hawk.iit.edu) - A20506653
1. This method is used to get the tuples value
2. Inputs- name of the tabble
3. returns - Returns tuple value
*/
int getNumTuples(RM_TableData *rel)
{
	Record *record = createRecordObject();
	RID ridValue;
	RC flagGetRecord;
	int one = 1;
	int zero = 0;
	int total = zero;
	ridValue.page = one;
	ridValue.slot = zero;
	if (ridValue.page < totalNumberOfPages && ridValue.page > 0)
	{
		flagGetRecord = getRecord(rel, ridValue, record);
		if (flagGetRecord == RC_OK)
		{
			total += one;
			ridValue.page += one;
			ridValue.slot = zero;
		}
	}
	record = NULL;
	free(record);
	return total;
}

char *callSerializeRecord(Record *record, RM_TableData *rel)
{
	return serializeRecord(record, rel->schema);
}

//memory is set for data 
void memorySet(char *data)
{
	memset(data, '\0', strlen(data));
}

/*
Ramya Krishnan(rkrishnan1@hawk.iit.edu) - A20506653
1. This method is used to insert records into the table 
2. Inputs- table data and record object
3. returns - Returns RC value
*/
RC insertRecord(RM_TableData *rel, Record *record)
{
	printf("Insert Record is started\n");
	Record *record1 = (Record *)malloc(sizeof(Record));
	RID rid;
	int one = 1;
	int zero = 0;
	rid.page = one;
	rid.slot = zero;

	while (rid.page < totalNumberOfPages && rid.page > zero)
	{
		rid.page = rid.page + one;
		rid.slot = zero;
	}
	record1 = NULL;
	free(record1);
	((RecordManager *)rel->mgmtData)->freePages[0] = rid.page;
	BM_PageHandle *page = MAKE_PAGE_HANDLE();
	int freepage = ((RecordManager *)rel->mgmtData)->freePages[0];
	record->id.page = freepage;
	record->id.slot = zero;
	Schema *schema = rel->schema;
	char *serializedRecord = serializeRecord(record, schema);
	BM_BufferPool *bufferPool = ((RecordManager *)rel->mgmtData)->bufferPool;
	int freepage1 = ((RecordManager *)rel->mgmtData)->freePages[0];
	pinPage(bufferPool, page, freepage1);
	char *dt = page->data;
	memorySet(dt);
	sprintf(page->data, "%s", serializedRecord);
	ModifyPageDetails(rel, page);
	free(page);
	((RecordManager *)rel->mgmtData)->freePages[0] = ((RecordManager *)rel->mgmtData)->freePages[0] + one;
	totalNumberOfPages = totalNumberOfPages + one;
	printf("insert record is ended\n");
	return RC_OK;
}

//creates char object
char *CreateCharObject()
{
	return (char *)malloc(sizeof(char *));
}

//performs string copy and string concatination
void stringOperation(char *flag, char *deleteflag, char *dt)
{
	strcpy(flag, deleteflag);
	strcat(flag, dt);
}

/*
Ramya Krishnan(rkrishnan1@hawk.iit.edu) - A20506653
1. This method is used to delete records from exisiting table 
2. Inputs- table data and rid
3. returns - Returns RC value
*/
RC deleteRecord(RM_TableData *rel, RID id)
{
	char deleteFlag[3] = "DEL";
	char *flag = CreateCharObject();
	int zero = 0;
	if (id.page < zero || id.page > totalNumberOfPages)
		return RC_RM_NO_MORE_TUPLES;
	else
	{
		BM_PageHandle *page = MAKE_PAGE_HANDLE();
		int pg = id.page;
		BM_BufferPool *bufferPool = ((RecordManager *)rel->mgmtData)->bufferPool;
		pinPage(bufferPool, page, pg);
		stringOperation(flag, deleteFlag, page->data);
		page->pageNum = id.page;
		char *dt = page->data;
		memorySet(dt);
		sprintf(page->data, "%s", flag);
		ModifyPageDetails(rel, page);
		page = NULL;
		free(page);
		return RC_OK;
	}
}

/*
Ramya Krishnan(rkrishnan1@hawk.iit.edu) - A20506653
1. This method is used to updated records from exisiting table 
2. Inputs- table data and record object
3. returns - Returns RC value
*/
RC updateRecord(RM_TableData *rel, Record *record)
{
	printf("update record is started\n");
	printf("record updated: %s\n", record->data);
	int zero = 0;
	if (record->id.page > totalNumberOfPages && record->id.page <= zero)
		return RC_RM_NO_MORE_TUPLES;
	else
	{
		BM_PageHandle *page = MAKE_PAGE_HANDLE();
		int pageNumber;
		int slotNumber;
		slotNumber = record->id.slot;
		pageNumber = record->id.page;
		BM_BufferPool *bufferPool = ((RecordManager *)rel->mgmtData)->bufferPool;
		char *record_str = serializeRecord(record, rel->schema);
		int pg = record->id.page;
		pinPage(bufferPool, page, pg);
		char *dt = page->data;
		memorySet(dt);
		sprintf(page->data, "%s", record_str);
		ModifyPageDetails(rel, page);
		free(record_str);
		free(page);
		return RC_OK;
	}
	printf("update record is ended\n");
}

/*
Ramya Krishnan(rkrishnan1@hawk.iit.edu) - A20506653
1. This method is used to get the records from table with rid value
2. Inputs- tabble data, rid and record
3. returns - Returns RC value
*/
RC getRecord(RM_TableData *rel, RID id, Record *record)
{
	printf("get record is started\n");
	int zero = 0;
	int three = 3;
	char deleteFlag[3] = "DEL";
	if (id.page > totalNumberOfPages && id.page <= zero)
		return RC_RM_NO_MORE_TUPLES;
	else
	{
		record->id = id;
		BM_PageHandle *page = MAKE_PAGE_HANDLE();
		BM_BufferPool *bufferPool = ((RecordManager *)rel->mgmtData)->bufferPool;
		int pg = id.page;
		pinPage(bufferPool, page, pg);
		char *dt = page->data;
		char *record_data = (char *)malloc(sizeof(char) * strlen(dt));
		strcpy(record_data, dt);
		printf("%s get record data: \n", record_data);
		Schema *schema = rel->schema;
		Record *deSerializedRecord = deserializeRecord(record_data, schema);
		unpinPage(bufferPool, page);

		record->data = deSerializedRecord->data;
		if (strncmp(record_data, deleteFlag, three) != 0)
		{
			free(deSerializedRecord);
			free(page);
			printf("get record is ended\n");
			return RC_OK;
		}
		else
			return RC_RM_UPDATE_NOT_POSSIBLE_ON_DELETED_RECORD;
	}
}

RM_ScanManager *createScanManagerObject()
{
	return (RM_ScanManager *)malloc(sizeof(RM_ScanManager));
}

RM_ScanManager *AssignScanManager(RM_ScanManager *scanManager)
{
	RM_ScanManager *sm = scanManager;
	return sm;
}

RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
	int zero = 0;
	int one = 1;
	RM_ScanManager *scanManager = createScanManagerObject();
	scanManager->currentRecord = createRecordObject();
	scan->rel = rel;
	scanManager->currentSlot = zero;
	scanManager->currentPage = one;
	Expr *expr = cond;
	scanManager->expr = expr;
	scan->mgmtData = AssignScanManager(scanManager);
	return RC_OK;
}

int AssignCurrentPage(RM_ScanHandle *scan)
{
	int pg;
	pg = ((RM_ScanManager *)scan->mgmtData)->currentPage;
	return pg;
}

int AssignCurrentSlot(RM_ScanHandle *scan)
{
	int slot;
	slot = ((RM_ScanManager *)scan->mgmtData)->currentSlot;
	return slot;
}

char *AssignCurrentRecordData(RM_ScanHandle *scan)
{
	char *dt;
	dt = ((RM_ScanManager *)scan->mgmtData)->currentRecord->data;
	return dt;
}

RID AssignCurrentRecordId(RM_ScanHandle *scan)
{
	RID id;
	id = ((RM_ScanManager *)scan->mgmtData)->currentRecord->id;
	return id;
}

int AssignCurrentPageNext(RM_ScanHandle *scan)
{
	int one = 1;
	int page;
	page = ((RM_ScanManager *)scan->mgmtData)->currentPage + one;
	return page;
}


RC next(RM_ScanHandle *scan, Record *record)
{
	Value *result;
	RID rid;
	int one = 1;
	int zero = 0;

	rid.slot = AssignCurrentSlot(scan);
	rid.page = AssignCurrentPage(scan);
	Expr *expr = ((RM_ScanManager *)scan->mgmtData)->expr;

	if (expr == NULL)
	{
		while (rid.page < totalNumberOfPages && rid.page > zero)
		{

			Record *rd1 = ((RM_ScanManager *)scan->mgmtData)->currentRecord;
			RM_TableData *rmTD1 = scan->rel;
			getRecord(rmTD1, rid, rd1);
			record->id = AssignCurrentRecordId(scan);
			record->data = AssignCurrentRecordData(scan);
			((RM_ScanManager *)scan->mgmtData)->currentPage = AssignCurrentPageNext(scan);
			rid.slot = AssignCurrentSlot(scan);
			rid.page = AssignCurrentPage(scan);
			return RC_OK;
		}
	}
	else
	{

		while (rid.page < totalNumberOfPages && rid.page > zero)
		{
			Record *rd = ((RM_ScanManager *)scan->mgmtData)->currentRecord;
			RM_TableData *rmTD = scan->rel;
			getRecord(rmTD, rid, rd);
			evalExpr(rd, rmTD->schema, expr, &result);
			if (result->v.boolV && result->dt == DT_BOOL)
			{
				record->id = AssignCurrentRecordId(scan);
				record->data = AssignCurrentRecordData(scan);
				((RM_ScanManager *)scan->mgmtData)->currentPage = AssignCurrentPageNext(scan);
				return RC_OK;
			}
			else
			{
				((RM_ScanManager *)scan->mgmtData)->currentPage = AssignCurrentPageNext(scan);
				rid.slot = AssignCurrentSlot(scan);
				rid.page = AssignCurrentPage(scan);
			}
		}
	}
	((RM_ScanManager *)scan->mgmtData)->currentPage = one;
	return RC_RM_NO_MORE_TUPLES;
}

Record *AssignCurrentRecord(RM_ScanHandle *scan)
{
	Record *rm = ((RM_ScanManager *)scan->mgmtData)->currentRecord;
	return rm;
}

RM_ScanHandle *returnNullScan()
{
	return NULL;
}


RC closeScan(RM_ScanHandle *scan)
{
	scan = returnNullScan();
	free(scan);
	return RC_OK;
}

/*
Ramya Krishnan(rkrishnan1@hawk.iit.edu) - A20506653
1. This method is used to get the records size from schema
2. Inputs- schema data
3. returns - Returns record size as integer
*/
int getRecordSize(Schema *schema)
{
	int i;
	int size = 0;
	int attr = schema->numAttr;
	for (i = 0; i < attr; i++)
	{
		if (schema->dataTypes[i] == DT_INT)
		{
			size += sizeof(int);
		}
		else if (schema->dataTypes[i] == DT_FLOAT)
		{
			size += sizeof(float);
		}
		else if (schema->dataTypes[i] == DT_BOOL)
		{
			size += sizeof(bool);
		}
		else
		{
			size += schema->typeLength[i];
		}
	}
	return size;
}

//created schema object
Schema *createSchemaObject()
{
	Schema *schema = (Schema *)malloc(sizeof(Schema));
	return schema;
}

//Assigns datatype object
DataType *AssignDataTypeObject(DataType *dataTypes)
{
	DataType *dt = dataTypes;
	return dt;
}

/*
Ramya Krishnan(rkrishnan1@hawk.iit.edu) - A20506653
1. This method is used to create database schema
2. Inputs- schema related properties
3. returns - Returns newly created schema
*/
Schema *createSchema(int numAttr, char **attrNames,
					 DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
	Schema *schema = createSchemaObject();
	int na = numAttr;
	char **atn = attrNames;
	int *len = typeLength;
	int ks = keySize;
	int *k = keys;
	if(true) schema->attrNames = atn;
	if(true) schema->typeLength = len;
	if(true) schema->numAttr = na;
	if(true) schema->keyAttrs = k;
	if(true) schema->dataTypes = AssignDataTypeObject(dataTypes);
	if(true) schema->keySize = ks;
	return schema;
}

/*
Ramya Krishnan(rkrishnan1@hawk.iit.edu) - A20506653
1. This method is used to free database schema
2. Inputs- schema object
3. returns - Returns RC code once the schema is freed
*/
RC freeSchema(Schema *schema)
{
	while (schema != NULL)
	{
		free(schema);
		break;
	}
	return RC_OK;
}


RC createRecord(Record **rec, Schema *schema)
{
	printf("Create Record started\n");
	int zero = 0;
	size_t r = sizeof(Record);
	*rec = createRecordObject();
	int size = getRecordSize(schema);
	(*rec)->data = (char *)malloc(size);
	char *dt = (*rec)->data;
	memset(dt, zero, r);
	printf("Create Record ended\n");
	return RC_OK;
}


RC freeRecord(Record *record)
{
	printf("free record is started\n");
	if (record == NULL)
		return RC_NULL;
	else
	{
		record = NULL;
		free(record);
		printf("free record is ended\n");
		return RC_OK;
	}
}


RC getAttr(Record *record, Schema *schema, int attrNum, Value **val)
{
	int set;
	char *attr;
	char *buf;
	int len;
	char *dt = record->data;
	int one = 1;
	int mall;

	*val = (Value *)malloc(sizeof(Value));

	SetOffAttrValue(schema, attrNum, &set);
	attr = dt + set;

	(*val)->dt = schema->dataTypes[attrNum];

	switch (schema->dataTypes[attrNum])
	{
	case DT_INT:
		memcpy(&((*val)->v.intV), attr, sizeof(int));
		break;
	case DT_STRING:
		len = schema->typeLength[attrNum];
		mall = len + one;
		buf = (char *)malloc(mall);
		strncpy(buf, attr, len);
		buf[len] = '\0';
		(*val)->v.stringV = buf;
		break;
	case DT_FLOAT:
		memcpy(&((*val)->v.floatV), attr, sizeof(float));
		break;
	case DT_BOOL:
		memcpy(&((*val)->v.boolV), attr, sizeof(bool));
		break;
	default:
		return RC_RM_NO_DESERIALIZER_FOR_THIS_DATATYPE;
		break;
	}

	return RC_OK;
}

RC setAttr(Record *record, Schema *schema, int attrNum, Value *value)
{
	int offset;
	char *attrData;
	char *buf;
	int len;
	SetOffAttrValue(schema, attrNum, &offset);
	char *dt = record->data;
	attrData = dt + offset;

	switch (schema->dataTypes[attrNum])
	{
	case DT_INT:
		memcpy(attrData, &(value->v.intV), sizeof(int));
		break;
	case DT_STRING:
		len = schema->typeLength[attrNum];
		buf = (char *)malloc(len);
		buf = value->v.stringV;
		buf[len] = '\0';
		memcpy(attrData, buf, len);
		break;
	case DT_FLOAT:
		memcpy(attrData, &(value->v.floatV), sizeof(float));
		break;
	case DT_BOOL:
		memcpy(attrData, &(value->v.boolV), sizeof(bool));
		break;
	default:
		return RC_RM_NO_DESERIALIZER_FOR_THIS_DATATYPE;
		break;
	}
	return RC_OK;
}