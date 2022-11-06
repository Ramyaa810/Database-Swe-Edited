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

void markDirtyInfo(RM_TableData *rel, BM_PageHandle *page)
{
	markDirty(((RecordManager *)rel->mgmtData)->bufferPool, page);
}

void unpinPageInfo(RM_TableData *rel, BM_PageHandle *page)
{
	unpinPage(((RecordManager *)rel->mgmtData)->bufferPool, page);
}

void forcePageInfo(RM_TableData *rel, BM_PageHandle *page)
{
	forcePage(((RecordManager *)rel->mgmtData)->bufferPool, page);
}

/*
 * Function: updatePageInfo
 * ---------------------------
 * This method is used to update the page information by calling
 * makeDirty, unpinPage and forcePage functions in buffer manager.
 *
 * schema : Management structure to maintain schema details.
 * result : Used to store the offset.
 * attrNum : Number of attributes.
 *
 * returns : RC_OK if all steps in attributeOffSet are successful.
 *
 */

void updatePageInfo(RM_TableData *rel, BM_PageHandle *page)
{
	markDirtyInfo(rel, page);
	unpinPageInfo(rel, page);
	forcePageInfo(rel, page);
}

/*
 * Function: attrituteOffset
 * ---------------------------
 * This method calculates the offset associated with each attribute
 * by taking size of each attribute datatype.
 *
 * schema : Management structure to maintain schema details.
 * result : Used to store the offset.
 * attrNum : Number of attributes.
 *
 * returns : RC_OK if all steps in attributeOffSet are successful.
 *
 */

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
 * Function: initRecordManager
 * ---------------------------
 * This method initializes the record manager.
 *
 * returns : RC_OK as initialing is done and nothing is left to initialize.
 *
 */

RC initRecordManager(void *mgmtData)
{
	printf("Initializing record manager completed!\n");
	return RC_OK;
}

/*
 * Function: shutdownRecordManager
 * ---------------------------
 * This method shuts down the record manager.
 *
 * returns : RC_OK as memory is made free during allocation.
 *
 */
RC shutdownRecordManager()
{
	printf("Shutdown of record manager completed!\n");
	return RC_OK;
}

// RC returnCreatePageFlag(char *name)
// {
// 	RC returnCreatePage = createPageFile(name);
// 	return returnCreatePage;
// }

// RC returnOpenPageFlag(char *name, SM_FileHandle *filehandle)
// {
// 	RC returnOpenPage = openPageFile(name, &filehandle);
// 	return returnOpenPage;
// }

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
 * Function: createTable
 * ---------------------------
 * This function is used to Create a Table.
 * Create the underlying page file and store information about the schema, free-space, ...
 * and so on in the Table Information pages.
 *
 * name: Name of the relation/table.
 * schema: Schema of the table.
 *
 * returns : RC_FILE_NOT_FOUND if pagefile creation of opening fails.
 *					 RC_WRITE_FAILED if write operation for writing serialized data fails.
 * 					 RC_OK if all steps are executed and table is created.
 *
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

char SetTotalPages(char *readHeader)
{
	char *totalPage;
	totalPage = readHeader;
	return atoi(totalPage);
}

FILE *fileReturn(char *name)
{
	FILE *file = fopen(name, "r+");
	return file;
}

void getFile(char *readHeader, FILE *file)
{
	fgets(readHeader, PAGE_SIZE, file);
}

void callInitBufferPool(BM_BufferPool *const bufferPool, char *name)
{
	int six = 6;
	initBufferPool(bufferPool, name, six, RS_FIFO, NULL);
}

void callPinPage(BM_BufferPool *const bufferPool, BM_PageHandle *const page)
{
	int zero = 0;
	pinPage(bufferPool, page, 0);
}

/*
 * Function: openTable
 * ---------------------------
 * This function is used to Open a table which is already created with name. This should have a pageFile created.
 * For any operation to be performed, the table has to be opened first.
 *
 * name: Name of the relation/table.
 * rel: Management Structure for a Record Manager to handle one relation.
 *
 * returns : RC_OK if all steps are executed and table is opened.
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

void freeAttr(RecordManager *recordManager, RM_TableData *rel)
{
	char *attrName = rel->schema->attrNames;
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

RC deleteTable(char *name)
{
	printf("delete table is started\n");
	RC destroyFlag = destroyPageFile(name);
	return destroyFlag != RC_OK ? RC_FILE_NOT_FOUND : RC_OK;
	printf("delete table is ended\n");
}

Record *createRecordObject()
{
	Record *record = (Record *)malloc(sizeof(Record));
	return record;
}

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

char callSerializeRecord(Record *record, RM_TableData *rel)
{
	return serializeRecord(record, rel->schema);
}

void memorySet(char *data)
{
	memset(data, '\0', strlen(data));
}

/*
 * Function: insertRecord
 * ---------------------------
 * This function is used to insert a new record into the table.
 * When a new record is inserted the record manager should assign an
 * RID to this record and update the record parameter passed to insertRecord .
 *
 * rel: Management Structure for a Record Manager to handle one relation.
 * record: Management Structure for Record which has rid and data of a tuple.
 *
 * returns : RC_OK if destroy getRecord is successful.
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
	char *serializedRecord = callSerializeRecord(record, rel);
	//char *serializedRecord = serializeRecord(record, schema);
	BM_BufferPool *bufferPool = ((RecordManager *)rel->mgmtData)->bufferPool;
	int freepage1 = ((RecordManager *)rel->mgmtData)->freePages[0];
	pinPage(bufferPool, page, freepage1);
	memorySet(page->data);
	 //memset(page->data, '\0', strlen(page->data));
	sprintf(page->data, "%s", serializedRecord);
	updatePageInfo(rel, page);
	free(page);
	((RecordManager *)rel->mgmtData)->freePages[0] += one;
	totalNumberOfPages = totalNumberOfPages + one;
	printf("insert record is ended\n");
	return RC_OK;
}

/*
 * Function: deleteRecord
 * ---------------------------
 * This function is used to delete a record from the table.
 *
 * rel: Management Structure for a Record Manager to handle one relation.
 * id: rid to be deleted.
 *
 * returns : RC_OK if delete record is successful.
 *					 RC_RM_NO_MORE_TUPLES if no tuples are available to delete.
 */
RC deleteRecord(RM_TableData *rel, RID id)
{

	char deleteFlag[3] = "DEL";
	char *deletedflagstr = (char *)malloc(sizeof(char *));
	if (id.page > 0 && id.page <= totalNumberOfPages)
	{
		BM_PageHandle *page = MAKE_PAGE_HANDLE();
		/*		if(strncmp(page->data, "DEL", 3) == 0)
					return RC_RM_UPDATE_NOT_POSSIBLE_ON_DELETED_RECORD;*/
		pinPage(((RecordManager *)rel->mgmtData)->bufferPool, page, id.page);
		strcpy(deletedflagstr, deleteFlag);
		strcat(deletedflagstr, page->data);
		page->pageNum = id.page;
		memset(page->data, '\0', strlen(page->data));
		sprintf(page->data, "%s", deletedflagstr);
		printf("deleted record: %s\n", page->data);
		updatePageInfo(rel, page);
		page = NULL;
		free(page);
		return RC_OK;
	}
	else
	{
		return RC_RM_NO_MORE_TUPLES;
	}

	return RC_OK;
}

/*
 * Function: updateRecord
 * ---------------------------
 * This function is used to update a record in the table.
 *
 * rel: Management Structure for a Record Manager to handle one relation.
 * record: Management Structure for a Record to store rid and data of a tuple.
 *
 * returns : RC_OK if delete record is successful.
 *					 RC_RM_NO_MORE_TUPLES if no tuples are available to update.
 */

RC updateRecord(RM_TableData *rel, Record *record)
{
	printf("update record is started\n");
	printf("record to be updated: %s\n", record->data);
	// Check boundary conditions for tuple availability
	if (record->id.page <= 0 && record->id.page > totalNumberOfPages)
	{
		return RC_RM_NO_MORE_TUPLES;
	}
	else
	{
		BM_PageHandle *page = MAKE_PAGE_HANDLE();
		int pageNum, slotNum;
		pageNum = record->id.page;
		slotNum = record->id.slot;
		char *record_str = serializeRecord(record, rel->schema);
		pinPage(((RecordManager *)rel->mgmtData)->bufferPool, page, record->id.page);
		memset(page->data, '\0', strlen(page->data));
		sprintf(page->data, "%s", record_str);
		free(record_str);
		updatePageInfo(rel, page);
		free(page);
		return RC_OK;
	}
	printf("update record is ended\n");
	return RC_OK;
}

/*
 * Function: getRecord
 * ---------------------------
 * This function is used to get a record from the table using rid.
 *
 *
 * rel: Management Structure for a Record Manager to handle one relation.
 * rid: Record identifier.
 * record: Management Structure for a Record to store rid and data of a tuple.
 *
 * returns : RC_OK if delete record is successful.
 *					 RC_RM_NO_MORE_TUPLES if no tuples are available to update.
 *
 */

RC getRecord(RM_TableData *rel, RID id, Record *record)
{
	printf("get record is started\n");
	if (id.page <= 0 && id.page > totalNumberOfPages)
	{
		return RC_RM_NO_MORE_TUPLES;
	}
	else
	{
		BM_PageHandle *page = MAKE_PAGE_HANDLE();
		pinPage(((RecordManager *)rel->mgmtData)->bufferPool, page, id.page);
		char *record_data = (char *)malloc(sizeof(char) * strlen(page->data));
		strcpy(record_data, page->data);
		printf("%s record data: \n", record_data);

		record->id = id;
		Record *deSerializedRecord = deserializeRecord(record_data, rel->schema);
		unpinPage(((RecordManager *)rel->mgmtData)->bufferPool, page);
		record->data = deSerializedRecord->data;
		if (strncmp(record_data, "DEL", 3) == 0)
			return RC_RM_UPDATE_NOT_POSSIBLE_ON_DELETED_RECORD;
		free(deSerializedRecord);
		free(page);

		printf("get record is ended\n");
		return RC_OK;
	}
	printf("get record is ended\n");
	return RC_OK;
}

/*
SCANS
*/

/*
 * Function: startScan
 * ---------------------------
 * This function is used to start scanning the table using scan management structure.
 *
 *
 * rel: Management Structure for a Record Manager to handle one relation.
 * scan_mgmt: holds the scan management data
 *
 * returns : RC_OK if initializing scan is successful.
 *
 */

RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{

	// Initialize the Scan Management Struct
	RM_ScanManager *scan_mgmt = (RM_ScanManager *)malloc(sizeof(RM_ScanManager));

	scan_mgmt->currentRecord = (Record *)malloc(sizeof(Record));

	// using Scan Handle Structure & init its attributes
	scan->rel = rel;

	scan_mgmt->currentPage = 1;
	scan_mgmt->currentSlot = 0;
	scan_mgmt->expr = cond;

	// update and store the managememt data
	scan->mgmtData = scan_mgmt;

	return RC_OK;
}

/*
 * Function: next
 * ---------------------------
 * This function is used with the above function to perform the scan function
 *
 *
 * rid: Record identifier.
 * record: Management Structure for a Record to store rid and data of a tuple.
 *
 * returns : RC_OK if scan operation is successful.
 *			 RC_RM_NO_MORE_TUPLES if no tuples are available to scan.
 *
 */
RC next(RM_ScanHandle *scan, Record *record)
{
	Value *result;
	RID rid;

	rid.page = ((RM_ScanManager *)scan->mgmtData)->currentPage;
	rid.slot = ((RM_ScanManager *)scan->mgmtData)->currentSlot;

	if (((RM_ScanManager *)scan->mgmtData)->expr == NULL)
	{
		while (rid.page > 0 && rid.page < totalNumberOfPages)
		{
			getRecord(scan->rel, rid, ((RM_ScanManager *)scan->mgmtData)->currentRecord);

			record->data = ((RM_ScanManager *)scan->mgmtData)->currentRecord->data;
			record->id = ((RM_ScanManager *)scan->mgmtData)->currentRecord->id;
			((RM_ScanManager *)scan->mgmtData)->currentPage = ((RM_ScanManager *)scan->mgmtData)->currentPage + 1;

			rid.page = ((RM_ScanManager *)scan->mgmtData)->currentPage;
			rid.slot = ((RM_ScanManager *)scan->mgmtData)->currentSlot;

			return RC_OK;
		}
	}
	else
	{
		while (rid.page > 0 && rid.page < totalNumberOfPages)
		{
			getRecord(scan->rel, rid, ((RM_ScanManager *)scan->mgmtData)->currentRecord);

			evalExpr(((RM_ScanManager *)scan->mgmtData)->currentRecord, scan->rel->schema, ((RM_ScanManager *)scan->mgmtData)->expr, &result);

			if (result->dt == DT_BOOL && result->v.boolV)
			{
				record->data = ((RM_ScanManager *)scan->mgmtData)->currentRecord->data;
				record->id = ((RM_ScanManager *)scan->mgmtData)->currentRecord->id;
				((RM_ScanManager *)scan->mgmtData)->currentPage = ((RM_ScanManager *)scan->mgmtData)->currentPage + 1;

				return RC_OK;
			}
			else
			{
				((RM_ScanManager *)scan->mgmtData)->currentPage = ((RM_ScanManager *)scan->mgmtData)->currentPage + 1;
				rid.page = ((RM_ScanManager *)scan->mgmtData)->currentPage;
				rid.slot = ((RM_ScanManager *)scan->mgmtData)->currentSlot;
			}
		}
	}

	((RM_ScanManager *)scan->mgmtData)->currentPage = 1;

	return RC_RM_NO_MORE_TUPLES;
}

/*
 * Function: closeScan
 * ---------------------------
 * This function is used to clean all the resources used by the record manager
 *
 * scan_mgmt: holds the scan management data
 * record: Management Structure for a Record to store rid and data of a tuple.
 *
 * returns : RC_OK if closing the scan operation is successful.
 */

RC closeScan(RM_ScanHandle *scan)
{
	// Make all the allocations, NULL and free them

	((RM_ScanManager *)scan->mgmtData)->currentRecord = NULL;
	free(((RM_ScanManager *)scan->mgmtData)->currentRecord);

	scan->mgmtData = NULL;

	free(scan->mgmtData);

	scan = NULL;
	free(scan);

	return RC_OK;
}

/*
 * DEALING WITH SCHEMAS
 */

/*
 * Function: getRecordSize
 * ---------------------------
 * This function is used to get a record size for dealing with schemas
 *
 * numAttr: attribute count in the schema
 *
 * returns : recordSize with the size of the record.
 *
 */
int getRecordSize(Schema *schema)
{
	int i, recordSize;
	recordSize = 0;

	for (i = 0; i < schema->numAttr; i++)
	{
		switch (schema->dataTypes[i])
		{
		case DT_INT:
			recordSize += sizeof(int);
			break;

		case DT_FLOAT:
			recordSize += sizeof(float);
			break;

		case DT_BOOL:
			recordSize += sizeof(bool);
			break;

		default:
			recordSize += schema->typeLength[i];
			break;
		}
	}

	return recordSize;
}

/*
 * Function: createSchema
 * ---------------------------
 * This function is used to cretae a scehma and initialize its attributes
 *
 * numAttr: number of attributes in the schema
 * attrNames: names of the attributes of schema
 * dataTypes: datatype of every attribute
 * typeLength: size of the attributes
 * keySize: size of the schema keys
 * keyAttrs: attributes associated with the keys
 *
 * returns : the newly created schema
 *
 */

Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
	// allocate memory for Schema to be created
	Schema *newSchema = (Schema *)malloc(sizeof(Schema));

	// initialize all the attributes for the schema
	newSchema->numAttr = numAttr;
	newSchema->attrNames = attrNames;
	newSchema->dataTypes = dataTypes;
	newSchema->typeLength = typeLength;
	newSchema->keySize = keySize;
	newSchema->keyAttrs = keys;

	return newSchema;
}

/*
 * Function: freeSchema
 * ---------------------------
 * This function is free the created schema after the operations are performed.
 *
 * returns : RC_OK if free schema is successful.
 *
 */

RC freeSchema(Schema *schema)
{
	if (schema != NULL)
	{
		// Free the schema content
		free(schema);
		return RC_OK;
	}
}

/*
 * DEALING WITH RECORDS AND ATTRIBUTE VALUES
 */

/*
 * Function: createRecord
 * ---------------------------
 * This function is used to create a record for the table by allocating memory.
 *
 * record: Management Structure for a Record to store rid and data of a tuple.
 *
 * returns : RC_OK if creation of record is successful.
 *
 */

RC createRecord(Record **rec, Schema *schema)
{
	printf("Create Record started\n");
	// Allocating memory for record
	*rec = (Record *)malloc(sizeof(Record));
	// Allocating memory for data
	(*rec)->data = (char *)malloc(getRecordSize(schema));
	memset((*rec)->data, 0, sizeof(Record));

	printf("Create Record ended\n");
	return RC_OK;
}

/*
 * Function: freeRecord
 * ---------------------------
 * This function is used to free a record from the table and the data associated with it.
 *
 *
 * data: data stored in the record
 * record: Management Structure for a Record to store rid and data of a tuple.
 *
 * returns : RC_OK if freeing the record is successful.
 *
 */
RC freeRecord(Record *record)
{
	printf("free record is started\n");
	if (record != NULL)
	{
		// Data is freed
		record->data = NULL;
		free(record->data);

		// Complete record is freed
		record = NULL;
		free(record);
	}
	printf("free record is ended\n");
	return RC_OK;
}

/*
 * Function: getAttr
 * ---------------------------
 * This function is get the attribute associated with the schema.
 *
 * offset: offset value of the attribute
 * attrData: data corresponding to the attribute
 * value: attribute value derived from the offset
 *
 * returns : RC_OK if the attribute is fetched properly.
 * 			 RC_RM_NO_DESERIALIZER_FOR_THIS_DATATYPE if the datatype is not correct
 */

RC getAttr(Record *record, Schema *schema, int attrNum, Value **val)
{
	int offset;
	char *attrData;

	*val = (Value *)malloc(sizeof(Value));

	// calculate the offset, to get the attribute value from
	SetOffAttrValue(schema, attrNum, &offset);
	attrData = record->data + offset;

	(*val)->dt = schema->dataTypes[attrNum];

	if (schema->dataTypes[attrNum] == DT_INT)
		memcpy(&((*val)->v.intV), attrData, sizeof(int)); // get the attribute into value

	else if (schema->dataTypes[attrNum] == DT_STRING)
	{
		char *buf;
		int len = schema->typeLength[attrNum];
		buf = (char *)malloc(len + 1);
		strncpy(buf, attrData, len);
		buf[len] = '\0';
		(*val)->v.stringV = buf;
	}

	else if (schema->dataTypes[attrNum] == DT_FLOAT)
		memcpy(&((*val)->v.floatV), attrData, sizeof(float));

	else if (schema->dataTypes[attrNum] == DT_BOOL)
		memcpy(&((*val)->v.boolV), attrData, sizeof(bool));

	else
		return RC_RM_NO_DESERIALIZER_FOR_THIS_DATATYPE;

	return RC_OK;
}

/*
 * Function: setAttr
 * ---------------------------
 * This function is set the attributes inside a record.
 *
 * offset: offset value of the attribute
 * attrData: data corresponding to the attribute
 * value: attribute value derived from the offset
 *
 * returns : RC_OK if the attribute is fetched properly.
 * 			 RC_RM_NO_DESERIALIZER_FOR_THIS_DATATYPE if the datatype is not correct
 *
 */

RC setAttr(Record *record, Schema *schema, int attrNum, Value *value)
{
	int offset;
	char *attrData;

	// offset values
	SetOffAttrValue(schema, attrNum, &offset);
	attrData = record->data + offset;

	// Attributes datatype value

	if (schema->dataTypes[attrNum] == DT_INT)
	{
		memcpy(attrData, &(value->v.intV), sizeof(int));
	}

	else if (schema->dataTypes[attrNum] == DT_STRING)
	{
		char *buf;
		int len = schema->typeLength[attrNum];
		buf = (char *)malloc(len);
		buf = value->v.stringV;
		// end the string with '\0'
		buf[len] = '\0';
		memcpy(attrData, buf, len);
	}
	else if (schema->dataTypes[attrNum] == DT_FLOAT)
		memcpy(attrData, &(value->v.floatV), sizeof(float));

	else if (schema->dataTypes[attrNum] == DT_BOOL)
		memcpy(attrData, &(value->v.boolV), sizeof(bool));

	else
		return RC_RM_NO_DESERIALIZER_FOR_THIS_DATATYPE;

	return RC_OK;
}