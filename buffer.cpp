/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

BufMgr::BufMgr(std::uint32_t bufs)
: numBufs(bufs) {
bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  bufDescTable[i].frameNo = i;
  bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table
  
  clockHand = bufs - 1;
}

BufMgr::~BufMgr() {
  for(FrameID i = 0; i<numBufs; i++)
  {
    if(bufDescTable[i].dirty)
    {
      flushFile(bufDescTable[i].file); // flush dirty pages
    }
    advanceClock();
  }
  delete[] bufDescTable; //deallocate buffer pool
}

void BufMgr::advanceClock()
{
  clockHand = (clockHand+1)%numBufs;
}

void BufMgr::allocBuf(FrameId & frame) 
{
  
  int count = 0; // for checking if we cycle through the entire buffer pool
  
  while(!finished)
  {
    
    advanceClock(); // Advance Clock Pointer
    
    count++;
    if (count>numBufs){ // Have we done a full loop?
      throw BufferExceededException;
    }
    
    if(bufDescTable[clockHand].valid) // Valid set?
    {
      break; // no
     }
     
    if(bufDescTable[clockHand].refbit) // refbit set?
    {
      bufDescTable[clockHand].refbit = false; //Clear refbit
      continue;
    }
    
    if(bufDescTable[clockHand].pinCnt>0) // Page pinned?
    {
      continue;
    }
    
    if(bufDescTable[clockHand].dirty) // Dirty bit Set?
    {
      bufDescTable[i].file->writePage(bufDescTable[i].page); // Flush page to disk
      BufHashTbl->remove(bufDescTable[clockHand].file,  bufDescTable[clockHand].pageNo) // clear from hash table
    }
    finished = true; // we've reached the end
  }
  bufDescTable[clockHand]->Set(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo); // Call "Set()" on the Frame

  frame = &bufDescTable[clockHand]; // for return variable
}

void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
  FrameId location;
  try
  {
   BufHashTbl->lookup(file, pageNo, &location);
   // case 2
   bufDescTable[location].refbit = true;
   bufDescTable[location].pinCnt = bufDescTable[location].pinCnt + 1;
   page = &bufDescTable[location].page; // this may need to be &page = &
  } catch(HashNotFoundException e)
  {
    // case 1
    allocBuf(&location);
    bufDescTable[location].page = file->readPage(pageNo);
    BufHashTbl->insert(bufDescTable[location].file , bufDescTable[location].pageNo , location);
    bufDescTable[location]->Set(bufDescTable[location].file, bufDescTable[location].pageNo); // we are calling this twice with alocBuf, which I don't think is right
    page = &bufDescTable[location].page;
  }
}

void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
  FrameId location;
  try{
     BufHashTbl->lookup(file, pageNo, &location);
     if( bufDescTable[location].pinCnt == 0){
       throw PAGENOTPINNED;
     } else {
        bufDescTable[location].pinCnt =bufDescTable[location].pinCnt -1;
        if (dirty ==true){
          bufDescTable[location].dirty = true; // I would just have done bufDescTable[location].dirty = dirty, but that's not what the porgram specifications says to do
      }
     }
  }catch(HashNotFoundException e)
  {
    // tried to unPin something not in the hash table/ buffer pool
  }
}

void BufMgr::flushFile(const File* file) 
{
  
   for (FrameId i = 0; i < bufs; i++) 
  {
  if(bufDescTable[i].file == file){
      // part a
      if(bufDescTable[i].pinCnt>0){
        throw PagePinnedException;
      }
       if(!bufDescTable[i].valid){
        throw BadBufferException;
      }
      if(bufDescTable[i].dirty){
        file->writePage(bufDescTable[i].page);
        bufDescTable[i].dirty = false;
      }  
      // part b
      BufHashTbl->remove(bufDescTable[i].file,  bufDescTable[i].pageNo);
      // part c
      bufDescTable[i]->Clear();
    }
 
  }
  
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
  page = file->allocatePage();
  FrameId location;
  allocBuf(&location);
  bufDescTable[location].file = file;
  bufDescTable[location].pageNo = pageNo;
  BufHashTbl->insert(bufDescTable.file , bufDescTable.pageNo , location); 
  bufDescTable[location]->Set(bufDescTable[location].file, bufDescTable[location].pageNo);
  page = &bufDescTable[location].page; // return a newly allocated page
  pageNo = &bufDescTable[location].pageNo;
  
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
  
  // do we need to write it before deleting?
    FrameId location;
    try{
      lookup(file, pageNo, &location);
      BufHashTbl->remove(file, pageNo);
      bufDescTable[location].valid = false;
      file->deletePage(pageNo);
      
    }catch(HashNotFoundException e){
       file->deletePage(pageNo);
      
    }  
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
{
  tmpbuf = &(bufDescTable[i]);
std::cout << "FrameNo:" << i << " ";
tmpbuf->Print();

  if (tmpbuf->valid == true)
    validFrames++;
  }

std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}