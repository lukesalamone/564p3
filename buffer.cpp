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
  BufMgr::BufMgr(std::uint32_t bufs) : numBufs(bufs) {
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
    
    for(int i = 0; i<numBufs; i++)
    {
      if(bufDescTable[i].dirty)
      {
        flushFile(bufDescTable[i].file);
      }
      advanceClock();
    }
    
    delete[] bufDescTable;
    
  }

  void BufMgr::advanceClock()
  {
    clockHand = (clockHand+1)%numBufs;
  }

  void BufMgr::allocBuf(FrameId & frame) 
  {
    
    int count = 0;
    bool finished = false;
    
    while(!finished)
    {
      
      advanceClock();
      
      count++;
      if (count>numBufs){
        throw BufferExceededException;
      }
      
      if(bufDescTable[clockHand].valid)
      {
        break;
       }
       
      if(bufDescTable[clockHand].refbit)
      {
        bufDescTable[clockHand].refbit = false;
        continue;
      }
      
      if(bufDescTable[clockHand].pinCnt>0)
      {
        continue;
      }
      
      if(bufDescTable[clockHand].dirty)
      {
        flushFile(bufDescTable[clockHand].file);
        BufHashTbl->remove(bufDescTable[clockHand].file,  bufDescTable[clockHand].pageNo)
        // remove from hash table LUKE
      }
      finished = true;
    }
    Set(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
     // Not sure what "Use Frame" intails, may be done
    
  }

  void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
  {
    FrameId location;
    try{
     lookup(file, pageNo, &location);
     bufDescTable[location].refbit = true;
     bufDescTable[location].pinCnt = bufDescTable[location].pinCnt + 1;
     page = &bufDescTable[location]; // this may need to be &page = &
    
         
    }catch(HashNotFoundException e)
    {
      allocBuf();
      bufDescTable[clockHand].file = file->readPage();
      BufHashTbl->insert(bufDescTable.file , bufDescTable.pageNo , clockHand);
      Set(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo); // we are calling this twice with alocBuf, which I don't think is right
      
    }
  }


  void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
  {
    FrameId location;
    try{
       lookup(file, pageNo, &location);
       if( bufDescTable[location].pinCnt == 0){
         throw PAGENOTPINNED;
       } else {
          bufDescTable[location].pinCnt =bufDescTable[location].pinCnt -1;
          if (dirty ==true){
            bufDescTable[location].dirty = true; // I would just have done bufDescTable[location].dirty = dirty, but that's not what the porgram specifications say
        }
       }
    }catch(HashNotFoundException e)
    {
      // tryed to unPin something not in the hash table
    }
  }

  void BufMgr::flushFile(const File* file) 
  {
    
    
    
  }

  void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
  {
    page = file->allocatePage();
    allocBuf();
    bufDescTable[clockHand].file = file;
    bufDescTable[clockHand].pageNo = pageNo;
    BufHashTbl->insert(bufDescTable.file , bufDescTable.pageNo , clockHand); 
    Set(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
    
    
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