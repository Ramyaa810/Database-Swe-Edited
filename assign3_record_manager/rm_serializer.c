#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "dberror.h"
#include "tables.h"
#include "record_mgr.h"

// dynamic string
typedef struct VarString
{
	char *buf;
	int size;
	int bufsize;
} VarString;

#define MAKE_VARSTRING(var)                           \
	do                                                \
	{                                                 \
		var = (VarString *)malloc(sizeof(VarString)); \
		var->size = 0;                                \
		var->bufsize = 100;                           \
		var->buf = malloc(100);                       \
	} while (0)

#define FREE_VARSTRING(var) \
	do                      \
	{                       \
		free(var->buf);     \
		free(var);          \
	} while (0)

#define GET_STRING(result, var)              \
	do                                       \
	{                                        \
		result = malloc((var->size) + 1);    \
		memcpy(result, var->buf, var->size); \
		result[var->size] = '\0';            \
	} while (0)

#define RETURN_STRING(var)          \
	do                              \
	{                               \
		char *resultStr;            \
		GET_STRING(resultStr, var); \
		FREE_VARSTRING(var);        \
		return resultStr;           \
	} while (0)

#define ENSURE_SIZE(var, newsize)                     \
	do                                                \
	{                                                 \
		if (var->bufsize < newsize)                   \
		{                                             \
			int newbufsize = var->bufsize;            \
			while ((newbufsize *= 2) < newsize)       \
				;                                     \
			var->buf = realloc(var->buf, newbufsize); \
		}                                             \
	} while (0)

#define APPEND_STRING(var, string)                            \
	do                                                        \
	{                                                         \
		ENSURE_SIZE(var, var->size + strlen(string));         \
		memcpy(var->buf + var->size, string, strlen(string)); \
		var->size += strlen(string);                          \
	} while (0)

#define APPEND(var, ...)           \
	do                             \
	{                              \
		char *tmp = malloc(10000); \
		sprintf(tmp, __VA_ARGS__); \
		APPEND_STRING(var, tmp);   \
		free(tmp);                 \
	} while (0)

// prototypes
static RC attrOffset(Schema *schema, int attrNum, int *result);

// Seeializing the schema
char *serializeTableInfo(RM_TableData *rel)
{
	VarString *result;
	MAKE_VARSTRING(result);

	APPEND(result, "TABLE <%s> with <%i> tuples:\n", rel->name, getNumTuples(rel));
	APPEND_STRING(result, serializeSchema(rel->schema));

	RETURN_STRING(result);
}

char *serializeTableContent(RM_TableData *rel)
{
	int i;
	VarString *result;
	RM_ScanHandle *sc = (RM_ScanHandle *)malloc(sizeof(RM_ScanHandle));
	Record *r = (Record *)malloc(sizeof(Record));
	MAKE_VARSTRING(result);

	for (i = 0; i < rel->schema->numAttr; i++)
		APPEND(result, "%s%s", (i != 0) ? ", " : "", rel->schema->attrNames[i]);

	startScan(rel, sc, NULL);

	while (next(sc, r) != RC_RM_NO_MORE_TUPLES)
	{
		APPEND_STRING(result, serializeRecord(r, rel->schema));
		APPEND_STRING(result, "\n");
	}
	closeScan(sc);

	RETURN_STRING(result);
}

char *serializeSchema(Schema *schema)
{
	int i;
	VarString *result;
	MAKE_VARSTRING(result);

	APPEND(result, "Schema with <%i> attributes (", schema->numAttr);

	for (i = 0; i < schema->numAttr; i++)
	{
		APPEND(result, "%s%s: ", (i != 0) ? ", " : "", schema->attrNames[i]);
		switch (schema->dataTypes[i])
		{
		case DT_INT:
			APPEND_STRING(result, "INT");
			break;
		case DT_FLOAT:
			APPEND_STRING(result, "FLOAT");
			break;
		case DT_STRING:
			APPEND(result, "STRING[%i]", schema->typeLength[i]);
			break;
		case DT_BOOL:
			APPEND_STRING(result, "BOOL");
			break;
		}
	}
	APPEND_STRING(result, ")");

	APPEND_STRING(result, " with keys: (");

	for (i = 0; i < schema->keySize; i++)
		APPEND(result, "%s%s", ((i != 0) ? ", " : ""), schema->attrNames[schema->keyAttrs[i]]);

	APPEND_STRING(result, ")\n");

	RETURN_STRING(result);
}

char *serializeRecord(Record *record, Schema *schema)
{
	VarString *result;
	MAKE_VARSTRING(result);

	int i;

	APPEND(result, "[%i-%i] (", record->id.page, record->id.slot);

	for (i = 0; i < schema->numAttr; i++)
	{
		APPEND_STRING(result, serializeAttr(record, schema, i));
		APPEND(result, "%s", (i == (schema->numAttr - 1)) ? "" : ",");
	}

	APPEND_STRING(result, ")");

	RETURN_STRING(result);
}

char *serializeAttr(Record *record, Schema *schema, int attrNum)
{
	int offset;
	char *attrData;
	VarString *result;
	MAKE_VARSTRING(result);

	attrOffset(schema, attrNum, &offset);
	attrData = record->data + offset;

	switch (schema->dataTypes[attrNum])
	{
	case DT_INT:
	{
		int val = 0;
		memcpy(&val, attrData, sizeof(int));
		APPEND(result, "%s:%i", schema->attrNames[attrNum], val);
	}
	break;
	case DT_STRING:
	{
		char *buf;
		int len = schema->typeLength[attrNum];
		buf = (char *)malloc(len + 1);
		strncpy(buf, attrData, len);
		buf[len] = '\0';

		APPEND(result, "%s:%s", schema->attrNames[attrNum], buf);
		free(buf);
	}
	break;
	case DT_FLOAT:
	{
		float val;
		memcpy(&val, attrData, sizeof(float));
		APPEND(result, "%s:%f", schema->attrNames[attrNum], val);
	}
	break;
	case DT_BOOL:
	{
		bool val;
		memcpy(&val, attrData, sizeof(bool));
		APPEND(result, "%s:%s", schema->attrNames[attrNum], val ? "TRUE" : "FALSE");
	}
	break;
	default:
		return "NO SERIALIZER FOR DATATYPE";
	}

	RETURN_STRING(result);
}

char *serializeValue(Value *val)
{
	VarString *result;
	MAKE_VARSTRING(result);

	switch (val->dt)
	{
	case DT_INT:
		APPEND(result, "%i", val->v.intV);
		break;
	case DT_FLOAT:
		APPEND(result, "%f", val->v.floatV);
		break;
	case DT_STRING:
		APPEND(result, "%s", val->v.stringV);
		break;
	case DT_BOOL:
		APPEND_STRING(result, ((val->v.boolV) ? "true" : "false"));
		break;
	}

	RETURN_STRING(result);
}

Value *stringToValue(char *val)
{
	Value *result = (Value *)malloc(sizeof(Value));

	switch (val[0])
	{
	case 'i':
		result->dt = DT_INT;
		result->v.intV = atoi(val + 1);
		break;
	case 'f':
		result->dt = DT_FLOAT;
		result->v.floatV = atof(val + 1);
		break;
	case 's':
		result->dt = DT_STRING;
		result->v.stringV = malloc(strlen(val));
		strcpy(result->v.stringV, val + 1);
		break;
	case 'b':
		result->dt = DT_BOOL;
		result->v.boolV = (val[1] == 't') ? TRUE : FALSE;
		break;
	default:
		result->dt = DT_INT;
		result->v.intV = -1;
		break;
	}

	return result;
}

RC attrOffset(Schema *schema, int attrNum, int *result)
{
	int offset = 0;
	int attrPos = 0;

	for (attrPos = 0; attrPos < attrNum; attrPos++)
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

char *createCharObject1()
{
	return (char *)malloc(sizeof(char *));
}

Record *createRecordObject1()
{
	return (Record *)malloc(sizeof(Record *));
}

Schema *createSchemaObject1()
{
	return (Schema *)malloc(sizeof(Schema));
}

bool compareAttrLast(int i, int AttrLast)
{
	return (i != AttrLast) ? true : false;
}

bool compareStrInt(char *end)
{
	return (strcmp(end, "INT") == 0) ? true : false;
}

bool compareStrFloat(char *end)
{
	return (strcmp(end, "FLOAT") == 0) ? true : false;
}

bool compareStrBool(char *end)
{
	return (strcmp(end, "BOOL") == 0) ? true : false;
}

bool compareStrString(char *end)
{
	return (strcmp(end, "STRING") == 0) ? true : false;
}

Schema *AssignToSchema(Schema *schema, int n, int index, DataType dt)
{
	schema->typeLength[index] = n;
	schema->dataTypes[index] = DT_STRING;
	return schema;
}

void callStringconcat(char *stringParam, char *splitchar, int i)
{
	sprintf(stringParam, "%d", i);
	strcat(splitchar, stringParam);
}

Schema *deserializeSchema(char *serializedSchemaData)
{
	VarString *result;

	MAKE_VARSTRING(result);

	int i;
	int j;
	int AttrNum;
	int AttrLast;
	int one = 1;
	int zero = 0;

	Schema *schema;
	schema = createSchemaObject1();

	char *start, *end, *splitchar;

	start = createCharObject1();
	end = createCharObject1();
	splitchar = createCharObject1();
	end = strtok(NULL, ">");
	start = strtok(serializedSchemaData, "<");

	AttrNum = strtol(end, &start, 10);

	schema->numAttr = AttrNum;

	schema->typeLength = (int *)malloc(sizeof(int) * AttrNum);
	schema->attrNames = (char **)malloc(sizeof(char *) * AttrNum);
	schema->dataTypes = (DataType *)malloc(sizeof(DataType) * AttrNum);

	end = strtok(NULL, "(");

	AttrLast = AttrNum - one;

	for (i = zero; i < AttrNum; i = i + 1)
	{
		end = strtok(NULL, ": ");

		schema->attrNames[i] = createCharObject1();

		strcpy(schema->attrNames[i], end);

		if (compareAttrLast(i, AttrLast))
		{
			end = strtok(NULL, ", ");
		}

		else
		{
			end = strtok(NULL, ") ");
		}
		if (compareStrInt(end))
		{
			schema->dataTypes[i] = DT_INT;
			schema->typeLength[i] = zero;
		}
		else if (compareStrFloat(end))
		{
			schema->dataTypes[i] = DT_FLOAT;
			schema->typeLength[i] = zero;
		}
		else if (compareStrBool(end))
		{
			schema->dataTypes[i] = DT_BOOL;
			schema->typeLength[i] = zero;
		}
		else
		{
			char *stringParam;
			strcpy(splitchar, end);
			stringParam = createCharObject1();
			callStringconcat(stringParam, splitchar, i);
			 sprintf(stringParam, "%d", i);
			 strcat(splitchar, stringParam);

			stringParam = NULL;
			free(stringParam);
		}
	}

	if ((end = strtok(NULL, "(")) != NULL)
	{

		char *key;
		char *keyAttr[AttrNum];
		int totalKeys = zero;

		end = strtok(NULL, ")");
		key = createCharObject1();
		key = strtok(end, ", ");

		while (key != NULL)
		{
			keyAttr[totalKeys] = createCharObject1();
			strcpy(keyAttr[totalKeys], key);
			totalKeys = totalKeys + 1;
			key = strtok(NULL, ", ");
		}

		key = NULL;
		free(key);

		schema->keyAttrs = (int *)malloc(sizeof(int) * totalKeys);
		schema->keySize = totalKeys;

		for (i = zero; i < totalKeys; i++)
		{
			for (j = zero; j < AttrNum; j++)
			{
				if (strcmp(keyAttr[i], schema->attrNames[j]) == zero)
				{
					schema->keyAttrs[i] = j;
					break;
				}
			}
		}
	}

	if (strlen(splitchar) != zero)
	{
		splitchar = strtok(splitchar, "[");
		if (compareStrString(splitchar))
		{
			int n;
			int index;
			splitchar = strtok(NULL, "]");
			n = atoi(splitchar);
			splitchar = strtok(NULL, "=");
			index = atoi(splitchar);
			schema = AssignToSchema(schema, n, index, DT_STRING);
			// schema->typeLength[index] = n;
			// schema->dataTypes[index] = DT_STRING;
		}
	}

	end = NULL;
	free(end);
	splitchar = NULL;
	free(splitchar);
	start = NULL;
	free(start);
	return schema;
}

Record *deserializeRecord(char *deserialize_record_str, Schema *schema)
{
	Value *value;
	Record *record;
	char *start;
	char *end;
	int one = 1;
	int i;
	int num;
	int attr;
	bool boolean;
	float pointer;

	attr = schema->numAttr - one;
	record = createRecordObject1();
	record->data = createCharObject1();

	start = strtok(deserialize_record_str, "(");

	for (i = 0; i < schema->numAttr; i++)
	{
		end = strtok(NULL, ":");

		if (i != attr)
		{
			end = strtok(NULL, ",");
		}
		else
		{
			end = strtok(NULL, ")");
		}
		switch (schema->dataTypes[i])
		{
		case DT_INT:
			num = strtol(end, &start, 10);
			MAKE_VALUE(value, DT_INT, num);
			setAttr(record, schema, i, value);
			free(value);
			break;
		case DT_FLOAT:
			pointer = strtof(end, NULL);
			MAKE_VALUE(value, DT_FLOAT, pointer);
			setAttr(record, schema, i, value);
			free(value);
			break;
		case DT_BOOL:
			if (end[0] == 't')
			{
				boolean = TRUE;
			}
			else
			{
				boolean = FALSE;
			}
			MAKE_VALUE(value, DT_BOOL, boolean);
			setAttr(record, schema, i, value);
			free(value);
			break;
		case DT_STRING:
			MAKE_STRING_VALUE(value, end);
			setAttr(record, schema, i, value);
			freeVal(value);
			break;
		default:
			printf("DataType is not available");
		}
	}

	return record;
}
