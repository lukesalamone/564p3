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
  int numBufs;


  BufMgr::BufMgr(std::uint32_t bufs) : numBufs(bufs) {
    bufDescTable = new BufDesc[bufs];

    for (int i = 0; i < (signed)bufs; i++){
      bufDescTable[i].frameNo = i;
      bufDescTable[i].valid = false;
    }

    bufPool = new Page[bufs];

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl(htsize);  // allocate the buffer hash table
    clockHand = bufs - 1;
  }//end BufMgr constructor

  BufMgr::~BufMgr() {
    for(int i = 0; i<(signed)numBufs; i++){
      if(bufDescTable[i].dirty)
      {
        // Piazza post title "flushFile action if exception thrown?" seems to suggest we should handle excpetions thrown from here
        flushFile(bufDescTable[i].file); // flush dirty pages
      }
      advanceClock();
    }
    delete[] bufDescTable; //deallocate buffer pool
    delete[] bufPool;
    delete[] hashTable;
  }

  void BufMgr::advanceClock(){
    clockHand = (clockHand+1)%numBufs;
  }

  /**
   * Allocate a free frame.  
   *
   * @param frame   Frame reference, frame ID of allocated frame returned via this variable
   * @throws        BufferExceededException If no such buffer is found which can be allocated
   */
  void BufMgr::allocBuf(FrameId & frame){
    bool finished = false;
    int count = 0; // for checking if we cycle through the entire buffer pool
    
    while(!finished){
      
      advanceClock(); // Advance Clock Pointer
      
      count++;
      if (count>(signed)numBufs){ // Have we done a full loop?
        throw BufferExceededException();
      }
      //Valid set?
      if(bufDescTable[clockHand].valid){
        break; // no
      }
      //refbit set? 
      if(bufDescTable[clockHand].refbit) {
        bufDescTable[clockHand].refbit = false; //Clear refbit
        continue;
      }
      //Page pinned?
      if(bufDescTable[clockHand].pinCnt>0){
        continue;
      }
      // Dirty bit Set? 
      if(bufDescTable[clockHand].dirty){
        bufDescTable[clockHand].file->writePage(bufPool[clockHand]); // Flush page to disk
        hashTable->remove(bufDescTable[clockHand].file,  bufDescTable[clockHand].pageNo); // clear from hash table
      }
      finished = true; // we've reached the end
    }//end finished loop

    bufDescTable[clockHand].Set(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo); // Call "Set()" on the Frame
    frame = clockHand; // for return variable
  }//end allocBuf method

  /**
   * Reads the given page from the file into a frame and returns the pointer to page.
   * If the requested page is already present in the buffer pool pointer to that frame is returned
   * otherwise a new frame is allocated from the buffer pool for reading the page.
   *
   * @param file    File object
   * @param PageNo  Page number in the file to be read
   * @param page    Reference to page pointer. Used to fetch the Page object in which requested page from file is read in.
   */
  void BufMgr::readPage(File* file, const PageId PageNo, Page*& page){
    FrameId location;
    try{
      hashTable->lookup(file, PageNo, location);
      // case 2
      bufDescTable[location].refbit = true;
      bufDescTable[location].pinCnt = bufDescTable[location].pinCnt + 1;
      page = &bufPool[location]; // this may need to be &page = &
    } catch(HashNotFoundException e){
      // case 1
      allocBuf(location);
      bufPool[location] = file->readPage(PageNo);
      hashTable->insert(file , PageNo , location);
      bufDescTable[location].Set(file, PageNo); // we are calling this twice with alocBuf, which I don't think is right
      page = &bufPool[location];
    }
  }//end readPage method

  void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty){
    FrameId location;
    try{
      hashTable->lookup(file, pageNo, location);
      if( bufDescTable[location].pinCnt == 0){
        throw PageNotPinnedException(file->filename(), pageNo, location);
      } else {
          bufDescTable[location].pinCnt = bufDescTable[location].pinCnt -1;
          if (dirty ==true){
            bufDescTable[location].dirty = true; // I would just have done bufDescTable[location].dirty = dirty, but that's not what the porgram specifications says to do
          }
       }
    }catch(HashNotFoundException e){
      // tried to unPin something not in the hash table/ buffer pool
    }
  }

  void BufMgr::flushFile(const File* file){
    FrameId location;
    for (FrameId i = 0; i < numBufs; i++){
      if(bufDescTable[i].file == file){
        // part a
        if(bufDescTable[i].pinCnt>0){
          throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, location);
        }
        if(!bufDescTable[i].valid){
          throw BadBufferException(i, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refBit);
        }
        if(bufDescTable[i].dirty){
          file->writePage(bufDescTable[i].page);
          bufDescTable[i].dirty = false;
        }  
        // part b
        hashTable->remove(bufDescTable[i].file,  bufDescTable[i].pageNo);
        // part c
        bufDescTable[i].Clear();
      }
    }//end for loop
  }//end flushFile method

  void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page){
    *page = file->allocatePage();// return a newly allocated page
    *pageNO = page->page_number();// return a newly allocated page number
    FrameId location;
    allocBuf(location);
    bufPool[location] = *page;
    bufDescTable[location].file = file;
    bufDescTable[location].pageNo = pageNo;
    hashTable->insert(bufDescTable.file , bufDescTable.pageNo , location); 
    bufDescTable[location].Set(bufDescTable[location].file, bufDescTable[location].pageNo);
  }//end allocPage method

  void BufMgr::disposePage(File* file, const PageId PageNo){
    // do we need to write it before deleting?
    FrameId location;
    try{
      hashTable->lookup(file, PageNo, location);
      hashTable->remove(file, PageNo);
      bufDescTable[location].valid = false;
      file->deletePage(PageNo);
      
    }catch(HashNotFoundException e){
       file->deletePage(PageNo);
    }  
  }

  void BufMgr::printSelf(void){
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
