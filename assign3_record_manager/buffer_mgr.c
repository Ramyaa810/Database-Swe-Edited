#include "stdio.h"
#include "stdlib.h"
#include "string.h"

#include "buffer_mgr.h"
#include "storage_mgr.h"

//Creating structure to store buffer frame
typedef struct BufferFrame
{
	struct BufferFrame *prevFrame;
	int dirtyFlag;
	struct BufferFrame *nextFrame;
	int pageNumber;
	int count;
	char *data;
	int refBit;	
} BufferFrame;

//Creating structure to store buffer manager with buffer frame, number of read and write etc
typedef struct BufferManager
{
	BufferFrame *head, *start, *tail;
	int numRead;
	int numWrite;
	SM_FileHandle *smFileHandle;
	int count;
	void *strategyData;
} BufferManager;

/*1. This method is used to assign frame values such as previous frame, next frame, tail, head
2. Takes BufferManger and BufferFrame as the input
3. Assigns values to the frame and buffer manager
*/
void AssignBufferManagerValues(BufferManager *bufferManager, BufferFrame *frame)
{	//define head as start of buffer
	bufferManager->head = bufferManager->start;
	//case that head has a value
	if (bufferManager->head != NULL)
	{	//case frame has a value
		if (frame != NULL)
		{
			//assigns values to frame and manager
			bufferManager->tail->nextFrame = frame;
			frame->prevFrame = bufferManager->tail;
			bufferManager->tail = bufferManager->tail->nextFrame;
		}
	}
	//case head has no value
	else
	{
		if (frame != NULL)
		{
			bufferManager->head = frame;
			bufferManager->tail = bufferManager->head;
			bufferManager->start = bufferManager->tail;
		}
	}
	//iterate through
	bufferManager->tail->nextFrame = bufferManager->head;
	bufferManager->head->prevFrame = bufferManager->tail;
}

/*
Different approach: External method to be used within initbufferpool
1. This method creates the buffer frame from the buffer manager
2. dirty flag, count, pagenumber and frames will be initialized
*/
void createBufferFrame(BufferManager *bufferManager)
{
	int i = 0;
	int pn = -1;
	BufferFrame *frame = (BufferFrame *)malloc(sizeof(BufferFrame));
	char *calldata = calloc(PAGE_SIZE, sizeof(char *));
	frame->data = calldata;
	//counts num of dirty flags
	if (i == 0)
	{
		frame->dirtyFlag = i;
		frame->count = i;
	}
	if (pn == -1)
	{
		frame->pageNumber = pn;
	}
	AssignBufferManagerValues(bufferManager, frame);
}

/*
1. This method is used to empty the buffer pool and buffer manager
2. Null value is set to all the properties in buffer pool and manager
*/
void CleanBufferPool(BufferManager *bufferManager, BM_BufferPool *bufferPool)
{
	int NumberofPages;
	bufferPool->pageFile = NULL;
	bufferPool->mgmtData = NULL;
	bufferPool->numPages = NumberofPages;
	bufferManager->start = NULL;
	bufferManager->head = NULL;
	bufferManager->tail = NULL;
	free(bufferManager);
}

/*
1. This method is used to check if the given management data is valid
2. If Null,returns false. If not null, returns true
*/
bool CheckValidManagementData(BM_BufferPool *const bufferPool)
{
	return (bufferPool->mgmtData == NULL) ? false : true;
}

/*
1. This method is used to assign head from buffer manager to the frame
2. Takes bufferManager as input and returns the Buffer Frame
*/
BufferFrame *getunpinPageFrame(BufferManager *const bufferManager)
{
	BufferFrame *frame = bufferManager->head;
	return frame;
}

/*
1. This method is used to assign management data from buffer pool 
to the buffer manager
2. Takes buffer pool as input and returns the Buffer Manager
*/
BufferManager *getunpinPageManager(BM_BufferPool *const bm)
{
	BufferManager *bufferManager = bm->mgmtData;
	return bufferManager;
}

/*
1. This method does meory allocation for Buffer Manager and File Handle
2. Returns Buffer Manager as the output
*/
BufferManager *GetBufferManager()
{
	BufferManager *bufferManager = (BufferManager *)malloc(sizeof(BufferManager));
	bufferManager->smFileHandle = (SM_FileHandle *)malloc(sizeof(SM_FileHandle));
	return bufferManager;
}

/*
Jason Scott - A20436737
1. This method initiazatizes buffer pool
2. Opens a existing page with new frames
3. Initial data is stored within page and closed
*/
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData)
{
//Memory allocation to store the Buffer Pool Management Data
	BufferManager *bp_mgmt = (BufferManager*)malloc(sizeof(BufferManager));

	bp_mgmt->start = NULL;
	//Storage manager file handle
	SM_FileHandle fHandle;

	int i;

	//open the page file, whose pages are to be cached
	openPageFile((char*) pageFileName,&fHandle);

	//create the frames for buffer pool
	for(i=0;i<numPages;i++)
	{
		createBufferFrame(bp_mgmt);
	}

	//initialize the values and store it in management data
	bp_mgmt->tail = bp_mgmt->head;
	bp_mgmt->strategyData = stratData;
	bp_mgmt->count = 0;
	bp_mgmt->numRead = 0;
	bp_mgmt->numWrite = 0;
	bm->numPages = numPages;
	bm->pageFile = (char*) pageFileName;
	bm->strategy = strategy;
	bm->mgmtData = bp_mgmt;

	//close the page file
	closePageFile(&fHandle);

	return RC_OK;
	}

/*
Jason Scott - A20436737
1. This method opens and checks for dirty pages
2. All dirtypages with fix count zero are written to disk
3. Writes it then closes before returning to method above to be destroyed
*/
RC forceFlushPool(BM_BufferPool *const bm)
{
	BufferManager *bufferManager = getunpinPageManager(bm);
	BufferFrame *frame = getunpinPageFrame(bufferManager);

	bufferManager->smFileHandle = (SM_FileHandle *)malloc(sizeof(SM_FileHandle));
	RC openpageReturnCode = openPageFile((char *)(bm->pageFile), bufferManager->smFileHandle);
	//again, checking that pages exists since we are not creating one
	if (openpageReturnCode == RC_OK)
	{
		do
		{	//required case that all pages with fix count 0... then we check if they're dirty
			if (frame->count == 0)
			{
				//from checking, case that dirty pages exist
				if (frame->dirtyFlag != 0)
				{
					int pn = frame->pageNumber;
					SM_FileHandle *smhandle = bufferManager->smFileHandle;
					//case in which dirty page is written back to disk
					RC writeBlockReturnCode = writeBlock(pn, smhandle, frame->data);
					if (writeBlockReturnCode == RC_OK)
					{
						frame->dirtyFlag = 0;
						bufferManager->numWrite++;
					}
					//case we can't write back, we close the page and don't delete
					else
					{
						closePageFile(bufferManager->smFileHandle);
						return writeBlockReturnCode;
					}
				}
			}
			//iterate through the frame
			frame = frame->nextFrame;
		} while (frame != bufferManager->head);
	}
	else
		return openpageReturnCode;
	//close when completed
	closePageFile(bufferManager->smFileHandle);
	return RC_OK;
}

/*
Jason Scott - A20436737
1. This method destroyes buffer pool
2. Utilizes forceflush method to write all the dirtyPages back again, before destroying
3. It frees up the buffer pool's resources
*/
RC shutdownBufferPool(BM_BufferPool *const bm)
{
	int i;
	//checks it exists/init and is good to go
	if (!CheckValidManagementData(bm))
		return RC_BUFFER_POOL_NOT_INIT;
	//load the mgmt of buffer pool and gets head node 	
	BufferManager *bufferManager = bm->mgmtData;
	BufferFrame *frame = bufferManager->head;
	//calls upon forceflush method for dirty pages with fix count 0 to be written
	forceFlushPool(bm);
	//iterates through frames
	frame = frame->nextFrame;
	//frees all the page data in the frame
	for (i = 0; frame != bufferManager->head; i++)
	{
		//frees it then iterates through the frames
		free(frame->data);
		frame = frame->nextFrame;
	}
	free(frame);
	CleanBufferPool(bufferManager, bm);
	return RC_OK;
}

/* Darek Nowak A20497998 + Ramya Krishnan(rkrishnan1@hawk.iit.edu) - A20506653
// This function will pass trhough a page, check if it's in the buffer, and then mark it dirty so that it can't be removed until it's written back to disk.
//  1. Searches for given page
//  2. If found in buffer, marks as dirty
*/
RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page)
{
	int flag = 1;
	//check buffer pool exists
	if (!CheckValidManagementData(bm))
		return RC_BUFFER_POOL_NOT_INIT;

	BufferManager *bufferManager = bm->mgmtData;
	BufferFrame *frame = bufferManager->head;
	do
	{
		//case it exists and is dirty, mark dirty
		if (frame->pageNumber == page->pageNum)
		{
			frame->dirtyFlag = flag;
			return RC_OK;
		}
		//not dirty so iterates through
		else
			frame = frame->nextFrame;

	} while (frame != bufferManager->head);

	return RC_OK;
}

/* Darek Nowak A20497998 + Ramya Krishnan(rkrishnan1@hawk.iit.edu) - A20506653
// 1. Checks to see if page is in the buffer pool
// 2. If it is, decrement total count of frame used in buffer 
*/
RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
	BufferManager *bufferManager = getunpinPageManager(bm);
	BufferFrame *frame = getunpinPageFrame(bufferManager);
	int number = bufferManager->head->pageNumber;
	PageNumber pn = page->pageNum;
	if (number == pn)
	{
		bufferManager->head->count--;
		return RC_OK;
	}
	frame = frame->nextFrame;
	int i;
	//iterates through manager
	for (i = 0; frame != bufferManager->head; i++)
	{
		//decrement total count of frame used in buffer
		if (frame->pageNumber == page->pageNum)
		{
			frame->count--;
			return RC_OK;
		}
		//iterate through
		frame = frame->nextFrame;
	}

	return RC_OK;
}

/* Darek Nowak A20497998 + Ramya Krishnan(rkrishnan1@hawk.iit.edu) - A20506653
// 1. Checks if the page exists
// 2. If it does, it will check to see if it is dirty
// 3. If it is dirty, writes to disk and increments writeIO count
*/
RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
	BufferManager *bufferManager = getunpinPageManager(bm);
	BufferFrame *frame = getunpinPageFrame(bufferManager);

	bufferManager->smFileHandle = (SM_FileHandle *)malloc(sizeof(SM_FileHandle));
	RC openpageReturnCode = openPageFile((char *)(bm->pageFile), bufferManager->smFileHandle);
	//checks if existing based on return code
	if (openpageReturnCode == RC_OK)
	{
		do
		{	//dirty checking and page number within frame check
			if (frame->dirtyFlag == 1 && frame->pageNumber == page->pageNum)
			{
				RC writeBlockReturnCode = writeBlock(frame->pageNumber, bufferManager->smFileHandle, frame->data);
				//case we can write, then write to disk and continue
				if (writeBlockReturnCode == RC_OK)
				{
					frame->dirtyFlag = 0;
					bufferManager->numWrite++;
				}
				else
				{
					closePageFile(bufferManager->smFileHandle);
					return writeBlockReturnCode;
				}
			}
			frame = frame->nextFrame;
		} while (frame != bufferManager->head);
	}
	else
		return openpageReturnCode;
	closePageFile(bufferManager->smFileHandle);
	return RC_OK;
}

/*
1. This method gets the strategy, page number and required 
parameters to check if the page exists
2. Returns RC_OK if page exists
*/
RC CheckIfPageExists(int algo, const PageNumber pageNum, 
BufferManager *bufferManager, BM_PageHandle *const page)
{
	int value = 0;
	BufferFrame *frame = bufferManager->head;
	do
	{	//case the manager is not at the head and frame page number isnt the page number  
		if (frame->pageNumber != pageNum)
			//iterate through
			frame = frame->nextFrame;
		else
		{	//if not, then re-assign values
			page->pageNum = pageNum;
			page->data = frame->data;

			frame->pageNumber = pageNum;
			frame->count++;
			//required and mentioned LRU
			if (algo == RS_LRU)
			{
				bufferManager->tail = bufferManager->head->nextFrame;
				bufferManager->head = frame;
			}
			value = 1;
			return RC_OK;
		}
	} while (frame != bufferManager->head);
	//error display for non-existing
	if (value == 0)
		return RC_IM_KEY_NOT_FOUND;
	return RC_OK;
}

/*
1. This method is used to check if the buffer pool is empty*/
void CheckIfBufferPoolIsEmpty(const PageNumber pageNumber, 
BM_BufferPool *const bufferPool)
{
	BufferManager *bufferManager = getunpinPageManager(bufferPool);
	BufferFrame *frame = getunpinPageFrame(bufferManager);
	int np = bufferPool->numPages;
	int c = bufferManager->count;
	if (np > c)
	{
		frame = bufferManager->head;
		frame->pageNumber = pageNumber;
		BufferFrame *nxt = frame->nextFrame;
		BufferFrame *head = bufferManager->head;
		if (nxt != head)
		{
			bufferManager->head = nxt;
		}
		frame->count = frame->count + 1;
		bufferManager->count = bufferManager->count + 1;
	}
}

/*
This method is used to close the page file and return code
*/
RC CheckReturnCode(SM_FileHandle sm_fileHandle, RC returnCode)
{
	closePageFile(&sm_fileHandle);
	return returnCode;
}

/*
1.This method is used to pin the last recently used frame from the buffer frame
2. Returns RC_OK if the write block and read block are executed and succeeded
*/
RC LRU(SM_FileHandle sm_FileHandle, BM_PageHandle *const page, const PageNumber pageNumber,
	   BufferFrame *frame, BM_BufferPool *const bm, BufferManager *bufferManager)
{
	if (bm->numPages <= bufferManager->count)
	{
		frame = bufferManager->tail;
		do
		{
			if (frame->count != 0)
				frame = frame->nextFrame;
			else
			{
				if (frame->dirtyFlag != 0)
				{
					int pn = frame->pageNumber;
					ensureCapacity(pn, &sm_FileHandle);
					RC writeBlockReturnCode = writeBlock(pn, &sm_FileHandle, frame->data);
					if (writeBlockReturnCode == RC_OK)
						bufferManager->numWrite++;
					else
						return CheckReturnCode(sm_FileHandle, writeBlockReturnCode);
				}

				if (bufferManager->tail == bufferManager->head)
				{
					PageNumber pnu = pageNumber;
					frame = frame->nextFrame;
					frame->pageNumber = pnu;
					frame->count++;
					bufferManager->tail = frame;
					bufferManager->head = bufferManager->tail;
					bufferManager->tail = frame->prevFrame;
					break;
				}
				else
				{
					PageNumber pnu = pageNumber;
					frame->pageNumber = pnu;
					frame->count++;
					bufferManager->tail = frame->nextFrame;
					break;
				}
			}
		} while (frame != bufferManager->tail);
	}
	else
		CheckIfBufferPoolIsEmpty(pageNumber, bm);
	ensureCapacity((pageNumber + 1), &sm_FileHandle);
	RC readBlockReturnCode = readBlock(pageNumber, &sm_FileHandle, frame->data);
	if (readBlockReturnCode == RC_OK)
		bufferManager->numRead++;
	else
		return readBlockReturnCode;
	page->pageNum = pageNumber;
	page->data = frame->data;
	closePageFile(&sm_FileHandle);
	return RC_OK;
}

/*
1.This method is used to pin the FIFO frame from the buffer frame
2. Returns RC_OK if the write block and read block are executed and succeeded
*/
RC FIFO(SM_FileHandle sm_FileHandle, BM_PageHandle *const page, const PageNumber pageNumber,
		BufferFrame *bufferFrame, BM_BufferPool *const bm, BufferManager *mgmt)
{
	if (bm->numPages <= mgmt->count)
	{
		bufferFrame = mgmt->tail;
		do
		{
			if (bufferFrame->count != 0)
				bufferFrame = bufferFrame->nextFrame;
			else
			{
				if (bufferFrame->dirtyFlag != 0)
				{
					int pn = bufferFrame->pageNumber;
					ensureCapacity(pn, &sm_FileHandle);
					RC writeBlockReturnCode = writeBlock(pn, &sm_FileHandle, bufferFrame->data);
					if (writeBlockReturnCode == RC_OK)
						mgmt->numWrite++;
					else
						return CheckReturnCode(sm_FileHandle, writeBlockReturnCode);
				}

				mgmt->tail = bufferFrame->nextFrame;
				bufferFrame->pageNumber = pageNumber;
				mgmt->head = bufferFrame;
				bufferFrame->count++;
				break;
			}

		} while (bufferFrame != mgmt->head);
	}

	else
		CheckIfBufferPoolIsEmpty(pageNumber, bm);
	ensureCapacity((pageNumber + 1), &sm_FileHandle);
	RC readBlockReturnCode = readBlock(pageNumber, &sm_FileHandle, bufferFrame->data);
	if (readBlockReturnCode == RC_OK)
		mgmt->numRead++;
	else
		return CheckReturnCode(sm_FileHandle, readBlockReturnCode);
	page->pageNum = pageNumber;
	page->data = bufferFrame->data;
	closePageFile(&sm_FileHandle);
	return RC_OK;
}

/*
1.This method is used to check the replacement strategy 
2. Pins LRU, FIFO based on the replacement strategy algorithm
3. Returns RC_OK if the LRU and FIFO are executed and succeeded
*/
RC CheckReplacementStrategy(BM_PageHandle *const page, BufferManager *bufferManager, const PageNumber pageNum,
							ReplacementStrategy strategy, BufferFrame *frame, SM_FileHandle sm_FileHandle, BM_BufferPool *const bufferPool)
{
	RC IsPageExistsReturnCode;
	if (bufferPool->strategy == RS_LRU)
	{
		IsPageExistsReturnCode = CheckIfPageExists(RS_LRU, pageNum, bufferManager, page);
		if (IsPageExistsReturnCode == RC_OK)
			return RC_OK;
		else
			LRU(sm_FileHandle, page, pageNum, frame, bufferPool, bufferManager);
	}
	else if (bufferPool->strategy == RS_FIFO)
	{
		IsPageExistsReturnCode = CheckIfPageExists(RS_FIFO, pageNum, bufferManager, page);
		if (IsPageExistsReturnCode == RC_OK)
			return RC_OK;
		else
			FIFO(sm_FileHandle, page, pageNum, frame, bufferPool, bufferManager);
	}
	return RC_OK;
}

RC pinPageFIFO(BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum)
{
	SM_FileHandle fh;
	BufferManager *bp_mgmt = bm->mgmtData;
	BufferFrame *frame = bp_mgmt->head;

	openPageFile((char*) bm->pageFile,&fh);

	// if page is already present in the buffer pool
	do
	{
		//put the data onto the page and increment the fix count
		if(frame->pageNumber == pageNum)
		{
			page->pageNum = pageNum;
			page->data = frame->data;

			frame ->pageNumber = pageNum;
			frame->count++;
			return RC_OK;
		}
		frame = frame->nextFrame;
	}while(frame!= bp_mgmt->head);

	//if there are remaining frames in the buffer pool, i.e. bufferpool is not fully occupied
	//pin the pages in the empty spaces
	if(bp_mgmt->count < bm->numPages)
	{
		frame = bp_mgmt->head;
		frame->pageNumber = pageNum;

		//move the header to next empty space
		if(frame->nextFrame != bp_mgmt->head)
		{
			bp_mgmt->head = frame->nextFrame;
		}
		frame->count++;
		bp_mgmt->count++;	//increment the occupied count
	}
	else		//use page replacement strategy FIFO
	{
		//replace pages from frame
		frame = bp_mgmt->tail;
		do
		{
			//check if the page is in use, i.e. fixcount > 0
			//goto next frame whose fix count = 0, and replace the page
			if(frame->count != 0)
			{
				frame = frame->nextFrame;
			}
			else
			{
				//before replacing check for dirtyflag if dirty write back to disk and then replace
				if(frame->dirtyFlag == 1)
				{
					ensureCapacity(frame->pageNumber, &fh);
					if(writeBlock(frame->pageNumber,&fh, frame->data)!=RC_OK)
					{
						closePageFile(&fh);
						return RC_WRITE_FAILED;
					}
					bp_mgmt->numWrite++;
				}

				//update the frame and bufferPool attributes
				frame->pageNumber = pageNum;
				frame->count++;
				bp_mgmt->tail = frame->nextFrame;
				bp_mgmt->head = frame;

				break;
			}
		}while(frame!= bp_mgmt->head);
	}

	//ensure if the pageFile has the required number of pages, if not create those
	ensureCapacity((pageNum+1),&fh);

	//read the block into pageFrame
	if(readBlock(pageNum, &fh,frame->data)!=RC_OK)
	{
		closePageFile(&fh);
		return RC_READ_NON_EXISTING_PAGE;
	}

	//increment the num of read operations
	bp_mgmt->numRead++;

	//update the attributes and put the data onto the page
	page->pageNum = pageNum;
	page->data = frame->data;

	//close the pageFile
	closePageFile(&fh);

	return RC_OK;
}

// LRU Page Replacement Strategy Implementations
/*
 * This method implements LRU page replacement strategy
 * i.e. the page which is Least Recently Used will be replaced, with the new page
 */
RC pinPageLRU(BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum)
{
	BufferManager *bp_mgmt = bm->mgmtData;
	BufferFrame *frame = bp_mgmt->head;
	SM_FileHandle fh;

	openPageFile((char*)bm->pageFile,&fh);

	//check if frame already in buffer pool
	do
	{
		if(frame->pageNumber == pageNum)
		{
			//update the page and frame attributes
			page->pageNum = pageNum;
			page->data = frame->data;

			frame->pageNumber = pageNum;
			frame->count++;

			//point the head and tail for replacement
			bp_mgmt->tail = bp_mgmt->head->nextFrame;
			bp_mgmt->head = frame;
			return RC_OK;
		}

		frame = frame->nextFrame;

	}while(frame!= bp_mgmt->head);

	//if there are empty spaces in the bufferPool , then fill in those frames first
	if(bp_mgmt->count < bm->numPages)
	{

		frame = bp_mgmt->head;
		frame->pageNumber = pageNum;

		if(frame->nextFrame != bp_mgmt->head)
		{
			bp_mgmt->head = frame->nextFrame;
		}
		frame->count++;		//increment the fix count
		bp_mgmt->count++;	//increment the occupied Count
	}
	else
	{
		//replace pages from frame using LRU
		frame = bp_mgmt->tail;
		do
		{
			//check if page in use, move onto next page to be replaced
			if(frame->count != 0)
			{
				frame = frame->nextFrame;
			}
			else
			{
				//before replacing check if dirty flag set, write back content onto the disk
				if(frame->dirtyFlag == 1)
				{
					ensureCapacity(frame->pageNumber, &fh);
					if(writeBlock(frame->pageNumber,&fh, frame->data)!=RC_OK)
					{
						closePageFile(&fh);
						return RC_WRITE_FAILED;
					}
					bp_mgmt->numWrite++;	//increment number of writes performed
				}

				//find the least recently used page and replace that page
				if(bp_mgmt->tail != bp_mgmt->head)
				{
					frame->pageNumber = pageNum;
					frame->count++;
					bp_mgmt->tail = frame;
					//bp_mgmt->head = frame;
					bp_mgmt->tail = frame->nextFrame;
					break;
				}
				else
				{
					frame = frame->nextFrame;
					frame->pageNumber = pageNum;
					frame->count++;
					bp_mgmt->tail = frame;
					bp_mgmt->head = frame;
					bp_mgmt->tail = frame->prevFrame;
					break;
				}
			}
		}while(frame!= bp_mgmt->tail);
	}

	ensureCapacity((pageNum+1),&fh);
	if(readBlock(pageNum, &fh,frame->data)!=RC_OK)
	{
		return RC_READ_NON_EXISTING_PAGE;
	}
	bp_mgmt->numRead++;

	//update the page frame and its data
	page->pageNum = pageNum;
	page->data = frame->data;

	//close the pagefile
	closePageFile(&fh);

	return RC_OK;
}


// CLOCK Page Replacement Strategy Implementations
/*
 * This method implements Clock Page Replacement Strategy
 * it is more optimized version of FIFO, implements a circular queue
 * with a reference bit set for each page in pageFrame
 */
RC pinPageCLOCK(BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum)
{
	SM_FileHandle fh;
	BufferManager *bp_mgmt = bm->mgmtData;
	BufferFrame *frame = bp_mgmt->head;
	BufferFrame *temp;
	openPageFile((char*)bm->pageFile,&fh);

	// if frame already in buffer pool

	do
	{
		//if same page
		if(frame->pageNumber == pageNum)
		{
			page->pageNum = pageNum;
			page->data = frame->data;

			frame->refBit = 1;	//mark its reference bit as 1, as it is referred again
			frame ->pageNumber = pageNum;
			frame->count++;

			return RC_OK;
		}
		frame = frame->nextFrame;
	}while(frame!=bp_mgmt->head);

	//if space present will be executed at the start when all the frames are empty
	if(bp_mgmt->count < bm->numPages)
	{
		frame = bp_mgmt->head;

		frame->pageNumber = pageNum;
		frame->refBit = 1;	//and mark their reference bit as 1

		//all the insertions will be made at the Head,
		//every time the insert takes place head is moved to that node
		if(frame->nextFrame != bp_mgmt->head)
		{
			bp_mgmt->head = frame->nextFrame;
		}

		frame->count++;
		bp_mgmt->count++;

	}
	else
	{
		//if replacement reqd
		frame = bp_mgmt->head;

		do
		{
			//find a page whose reference bit is 0, to be replaced,
			//and along its way make other reference bit which are 1 to 0

			if(frame->count !=0)
			{
				frame = frame->nextFrame;
			}
			else
			{
				while(frame->refBit!=0)
				{
					frame->refBit = 0;
					frame= frame->nextFrame;
				}

				//if reference bit is 0 replace the page
				if(frame-> refBit == 0)
				{
					//check the page that is replaced,
					//if its dirty bit is set, then writeBlock and then replace
					if(frame->dirtyFlag == 1)
					{
						ensureCapacity(frame->pageNumber, &fh);
						if(writeBlock(frame->pageNumber,&fh, frame->data)!=RC_OK)
						{
							closePageFile(&fh);
							return RC_WRITE_FAILED;
						}
						bp_mgmt->numWrite++;
					}
					//update all the frame & page attributes
					frame->refBit = 1;
					frame->pageNumber = pageNum;
					frame->count++;
					bp_mgmt->head = frame->nextFrame;
					break;
				}
			}

		}while(frame!=bp_mgmt->head);
	}
	ensureCapacity((pageNum+1),&fh);

	if(readBlock(pageNum, &fh,frame->data)!=RC_OK)
	{
		closePageFile(&fh);
		return RC_READ_NON_EXISTING_PAGE;
	}

	bp_mgmt->numRead++;
	page->pageNum = pageNum;
	page->data = frame->data;


	closePageFile(&fh);

	return RC_OK;
}


/* Darek Nowak A20497998 + Ramya Krishnan(rkrishnan1@hawk.iit.edu) - A20506653
// 1. Passes data through CheckReplacementStrategy() in order to figure out which strategy it'll pin(LRU or FIFO)
*/
RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
	if (!CheckValidManagementData(bm))
		return RC_BUFFER_POOL_EXIST;

	RC IsPageExistsReturnCode;

	SM_FileHandle sm_FileHandle;
	BufferManager *bufferManager = bm->mgmtData;
	BufferFrame *frame = bufferManager->head;

	RC openPageReturnCode = openPageFile((char *)bm->pageFile, &sm_FileHandle);
	if (openPageReturnCode == RC_OK)
	{
		CheckReplacementStrategy(page, bufferManager, pageNum, bm->strategy, frame, sm_FileHandle, bm);
	}
	return RC_OK;
}

/*
Ramya Krishnan(rkrishnan1@hawk.iit.edu) - A20506653
1. This method gets the number of pages
2. Inputs- buffer pool object
3. returns - Returns number of pages in the buffer pool
*/
int GetPageCount(BM_BufferPool *const bm)
{
	return bm->numPages;
}

// Statistics Interface

PageNumber *getPNForFrameContent(BM_BufferPool *const bm, int page_count)
{
	PageNumber *frameContent = (PageNumber *)malloc(sizeof(PageNumber) * page_count);
	return frameContent;
}

/*
Ramya Krishnan(rkrishnan1@hawk.iit.edu) - A20506653
1. This method gets the frame content from the buffer pool
2. Inputs- buffer pool object
3. returns - Returns number of pages stored in the page frame
*/
PageNumber *getFrameContents(BM_BufferPool *const bm)
{
	int page_count = GetPageCount(bm);

	PageNumber *frameContent = getPNForFrameContent(bm,page_count);	
	BufferFrame *allFrames = ((BufferManager *)bm->mgmtData)->start;
	if (frameContent != NULL)
	{
		int i;
		//iterate through pages and gets the frame content from the buffer pool
		for (i = 0; i < page_count; i++)
		{
			frameContent[i] = allFrames->pageNumber;
			allFrames = allFrames->nextFrame;
		}
	}
	return frameContent;
}

/*
Ramya Krishnan(rkrishnan1@hawk.iit.edu) - A20506653
1. This method gets the dirty flag of all the pages from the buffer pool
2. Inputs- buffer pool object
3. returns - Returns true(if the page is modified)or false(if the page is not modified) for each pages in the buffer pool
*/
bool *getDirtyFlags(BM_BufferPool *const bm)
{
	int page_count = GetPageCount(bm);
	BufferFrame *currentFrame = ((BufferManager *)bm->mgmtData)->start;

	bool *dirtyFlag = (bool *)malloc(sizeof(bool) * page_count);
	//case exists dirtyflag
	if (dirtyFlag != NULL)
	{
		int i;
		for (i = 0; i < page_count; i++)
		{
			//iterates through and displays which are dirty 
			dirtyFlag[i] = currentFrame->dirtyFlag;
			currentFrame = currentFrame->nextFrame;
		}
	}
	return dirtyFlag;
}

/*
Ramya Krishnan(rkrishnan1@hawk.iit.edu) - A20506653
1. This method gets the dirty flag of all the pages from the buffer pool
2. Inputs- buffer pool object
3. returns - Returns fix count stored of the page stored in the page frame
*/
int *getFixCounts(BM_BufferPool *const bm)
{
	int page_count = GetPageCount(bm);

	BufferFrame *currentFrame = ((BufferManager *)bm->mgmtData)->start;

	int *fixCountResult = (int *)malloc(sizeof(int) * page_count);

	if (fixCountResult != NULL)
	{
		int i;
		for (i = 0; i < page_count; i++)
		{
			fixCountResult[i] = currentFrame->count;
			currentFrame = currentFrame->nextFrame;
		}
	}
	return fixCountResult;
}

/*
Ramya Krishnan(rkrishnan1@hawk.iit.edu) - A20506653
1. This method gets the number of pages read from the buffer pool since initialized
2. Inputs- buffer pool object
3. returns - Returns number of pages read from the buffer pool
*/
int getNumReadIO(BM_BufferPool *const bm)
{
	//checks if pool has been init
	if (!CheckValidManagementData(bm))
		return RC_BUFFER_POOL_NOT_INIT;
	//exists so returns number of pages read 
	else
		return ((BufferManager *)bm->mgmtData)->numRead;
}

/*
Ramya Krishnan(rkrishnan1@hawk.iit.edu) - A20506653
1. This method gets the number of pages write in to the buffer pool since initialized
2. Inputs- buffer pool object
3. returns - Returns number of pages write into the buffer pool
*/
int getNumWriteIO(BM_BufferPool *const bm)
{
	//checks if pool has been init
	if (!CheckValidManagementData(bm))
		return RC_BUFFER_POOL_NOT_INIT;
	//Returns number of pages writen into buffer
	else
		return ((BufferManager *)bm->mgmtData)->numWrite;
}
