#include <string>

#include "common/exception.h"

#include "common/logger.h"

#include "common/rid.h"

#include "storage/index/b_plus_tree.h"

#include "storage/page/header_page.h"

namespace bustub {
  INDEX_TEMPLATE_ARGUMENTS
  BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager * buffer_pool_manager,
      const KeyComparator & comparator,
        int leaf_max_size, int internal_max_size): index_name_(std::move(name)),
    root_page_id_(INVALID_PAGE_ID),
    buffer_pool_manager_(buffer_pool_manager),
    comparator_(comparator),
    leaf_max_size_(leaf_max_size),
    internal_max_size_(internal_max_size) {}

  /*
   * Helper function to decide whether current b+tree is empty
   */
  INDEX_TEMPLATE_ARGUMENTS
  auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
    return root_page_id_ == INVALID_PAGE_ID;
  }
  /*****************************************************************************
   * SEARCH
   *****************************************************************************/
  /*
   * Return the only value that associated with input key
   * This method is used for point query
   * @return : true means key exists
   */
  INDEX_TEMPLATE_ARGUMENTS
  auto BPLUSTREE_TYPE::GetValue(const KeyType & key, std::vector < ValueType > * result, Transaction * transaction) -> bool {
    if (transaction != nullptr) {
    rootLatch.RLock();
    transaction->AddIntoPageSet(nullptr);
    }
    // LOG_DEBUG("bringing the key:");
     LeafPage * leafPage = FindLeaf(key, root_page_id_, LOOKUP_TRAVERSE, transaction);
     if (leafPage == 0) { 
      // LOG_DEBUG("NO PLACE IN BPMBPMBPMBPMBPMBPMBPMBPMBPMBPMBPMBPMBPMBPMBPMBPM");
      ClearLatches(LOOKUP_TRAVERSE,transaction, false);
      return false;
    }
    //BUG
 
     bool isSucess  = leafPage -> GetValue(key, comparator_, result);
     if (transaction == nullptr) {
      buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), false);
     }
    //  LOG_DEBUG("Response is %d", isSucess);
     ClearLatches(LOOKUP_TRAVERSE,transaction, false);
    return  isSucess;

  }

  /*****************************************************************************
   * INSERTION
   *****************************************************************************/
  /*
   * Insert constant key & value pair into b+ tree
   * if current tree is empty, start new tree, update root page id and insert
   * entry, otherwise insert into leaf page.
   * @return: since we only support unique key, if user try to insert duplicate
   * keys return false, otherwise return true.
   */
  INDEX_TEMPLATE_ARGUMENTS
  template < typename T >
    auto BPLUSTREE_TYPE::BuildRootNode(int maxSize) -> T * {

      page_id_t currentPageId;
      Page * rawPage = buffer_pool_manager_ -> NewPage( & currentPageId);
      //assert(rawPage != nullptr);
 
      T * rootPage = reinterpret_cast < T * > (rawPage -> GetData());
      root_page_id_ = currentPageId;
      rootPage -> Init(currentPageId, INVALID_PAGE_ID, maxSize);

      UpdateRootPageId(0);
      return rootPage;
    }

  INDEX_TEMPLATE_ARGUMENTS
  auto BPLUSTREE_TYPE::Insert(const KeyType & key,
    const ValueType & value, Transaction * transaction) -> bool {
      if (transaction != nullptr) {
    LOG_DEBUG("Trying to aquire wLatch");
    rootLatch.WLock();
    transaction -> AddIntoPageSet(nullptr);
      }
    if (root_page_id_ == INVALID_PAGE_ID) {
      //1st Insertion
      LeafPage * rootPage = BuildRootNode < LeafPage > (leaf_max_size_);
      bool result = rootPage -> Insert(key, value, comparator_);
      buffer_pool_manager_->UnpinPage(rootPage->GetPageId(), true);
      std::cerr<<"llllllllllllllllllllllllllllllllllllllllllllllllllllllThis is the root page id = "<< rootPage->GetPageId()<<"\n";
      ClearLatches(INSERT_TRAVERSE, transaction, true);
      
      return result;
    }
 
    LeafPage * ourLeaf = FindLeaf(key, root_page_id_, INSERT_TRAVERSE, transaction);
    if (ourLeaf -> KeyExist(key, comparator_)) {
      ClearLatches(INSERT_TRAVERSE, transaction, false);
        if (transaction == nullptr) {
          buffer_pool_manager_->UnpinPage(ourLeaf->GetPageId(), true);
        }
      return false;
    }
    if (ourLeaf->GetMaxSize() == ourLeaf->GetSize() + 1) {
      //We Need to Split
      LeafPage * returnedLeaf;
      MappingType newPair = std::make_pair(key, value);
      SplitLeafNode(ourLeaf, & returnedLeaf, newPair);
      //assert(returnedLeaf != 0);
      page_id_t parentId = returnedLeaf -> GetParentPageId();
      InternalPage * parentPage;
      if (parentId == INVALID_PAGE_ID) {
        //Here i need to create new root 
        parentPage = BuildRootNode < InternalPage > (internal_max_size_);
        parentId = parentPage -> GetPageId();
        ourLeaf -> SetParentPageId(parentId);
        returnedLeaf -> SetParentPageId(parentId);
      } else {
        Page * rawParentPage = buffer_pool_manager_ -> FetchPage(parentId);
        if (rawParentPage == nullptr) throw Exception(ExceptionType::OUT_OF_MEMORY, "fail to fetch page1");
        parentPage = reinterpret_cast < InternalPage * > (rawParentPage -> GetData());
      }
      
      std::pair < KeyType, page_id_t > m = std::make_pair(returnedLeaf -> KeyAt(0), returnedLeaf -> GetPageId());
        buffer_pool_manager_ -> UnpinPage(returnedLeaf -> GetPageId(), true);
      InsertIntoParent(parentPage, m.first, returnedLeaf->GetPageId(), ourLeaf -> GetPageId(), transaction);
      ClearLatches(INSERT_TRAVERSE, transaction, true);
       buffer_pool_manager_ -> UnpinPage(parentId, true);
      // buffer_pool_manager_ -> UnpinPage(ourLeaf -> GetPageId(), true);
       

        if (transaction == nullptr) {
          buffer_pool_manager_->UnpinPage(ourLeaf->GetPageId(), true);
        }
       
      return true;
    } else {
      bool result = ourLeaf -> Insert(key, value, comparator_);
      //BUG
      // buffer_pool_manager_ -> UnpinPage(ourLeaf -> GetPageId(), result);
       ClearLatches(INSERT_TRAVERSE, transaction, result);
        if (transaction == nullptr) {
          buffer_pool_manager_->UnpinPage(ourLeaf->GetPageId(), true);
        }
      return result;
    }
  }

  INDEX_TEMPLATE_ARGUMENTS
  auto BPLUSTREE_TYPE::FindLeaf(KeyType key, page_id_t pageId, TRAVERSE_TYPE traverseType, Transaction * transaction) -> LeafPage * {
    //assert(pageId != INVALID_PAGE_ID);
    Page * rawPage = buffer_pool_manager_ -> FetchPage(pageId);
    // if (traverseType == LOOKUP_TRAVERSE) {
    //     LOG_DEBUG("Fetching Page Id %d", pageId);
    // }
   
    //assert(rawPage != nullptr);
    HandleLatches(rawPage, traverseType, transaction, false);
    BPlusTreePage * currentPage = reinterpret_cast < BPlusTreePage * > (rawPage -> GetData());
    if (currentPage -> IsLeafPage()) {
      LeafPage * myPage = reinterpret_cast < LeafPage * > (rawPage -> GetData());
      return myPage;
    } else {
      //This is internal node
      InternalPage * myPage = reinterpret_cast < InternalPage * > (rawPage -> GetData());
      bool found = false;
      page_id_t childPosition = -1;
      for (int i = 1; i < myPage -> GetArraySize(); i++) {
        if (comparator_(key, myPage -> KeyAt(i)) < 0) {
          found = true;
          childPosition = myPage -> ValueAt(i - 1);
          break;
        }
      }
      if (found == false) {
        childPosition = myPage -> ValueAt(myPage -> GetArraySize() - 1);
      }
      //BUG
      if (transaction == nullptr) {
      
      buffer_pool_manager_ -> UnpinPage(pageId, false);
      }
      return FindLeaf(key, childPosition, traverseType, transaction);
    }

  }
  INDEX_TEMPLATE_ARGUMENTS
  auto BPLUSTREE_TYPE::FindLeftMostLeaf(page_id_t pageId) -> LeafPage * {
    Page * rawPage = buffer_pool_manager_ -> FetchPage(pageId);
    if (rawPage == nullptr) return 0;

    BPlusTreePage * currentPage = reinterpret_cast < BPlusTreePage * > (rawPage -> GetData());
    if (currentPage -> IsLeafPage()) {
      LeafPage * myPage = reinterpret_cast < LeafPage * > (rawPage -> GetData());
      return myPage;
    } else {
      //This is internal node
      InternalPage * myPage = reinterpret_cast < InternalPage * > (rawPage -> GetData());
      page_id_t childPosition = myPage -> ValueAt(0);
      buffer_pool_manager_ -> UnpinPage(pageId, false);
      return FindLeftMostLeaf(childPosition);
    }

  }
  INDEX_TEMPLATE_ARGUMENTS
  auto BPLUSTREE_TYPE::SplitLeafNode(LeafPage * oldLeafPage, LeafPage ** returnedNewPage, std::pair < KeyType, ValueType > & newPair) -> void {
    std::vector < std::pair < KeyType, ValueType >> temporaryLeafPage;
    bool done = false;
    int arrayLength = oldLeafPage -> GetSize();
    for (int i = 0; i < arrayLength; i++) {
      MappingType m = oldLeafPage -> pop();
      if (!done && comparator_(newPair.first, m.first) > 0) {
        done = true;
        temporaryLeafPage.push_back(newPair);
      }
      temporaryLeafPage.push_back(m);
    }
    if (!done) {
      temporaryLeafPage.push_back(newPair);
      done = true;
    }
    // page_id_t newPageId;
    // Page * newPage = buffer_pool_manager_ -> NewPage( & newPageId);
    // if (newPage == nullptr) return;
    // LeafPage * newLeafPage = reinterpret_cast < LeafPage * > (newPage -> GetData());
    // newLeafPage -> Init(newPageId, oldLeafPage -> GetParentPageId(), oldLeafPage -> GetMaxSize());
    LeafPage * newLeafPage = MakeTwin < LeafPage > (oldLeafPage);
    //BUG LMA KANT IF CONDITION
    //assert (newLeafPage != nullptr); 
    newLeafPage -> SetNextPageId(oldLeafPage -> GetNextPageId());
    oldLeafPage -> SetNextPageId(newLeafPage -> GetPageId());
    int median = ceil((temporaryLeafPage.size() / 2.0));
    for (int i = 0; i < median; i++) {
      MappingType keyValuePair = temporaryLeafPage.back();
      temporaryLeafPage.pop_back();
      oldLeafPage -> Insert(keyValuePair.first, keyValuePair.second, comparator_);
    }
    int temporaryArraySize = temporaryLeafPage.size();
    for (int i = 0; i < temporaryArraySize; i++) {
      MappingType keyValuePair = temporaryLeafPage.back();
      temporaryLeafPage.pop_back();
      newLeafPage -> Insert(keyValuePair.first, keyValuePair.second, comparator_);
    }
    * returnedNewPage = newLeafPage;

    return;
  }

  INDEX_TEMPLATE_ARGUMENTS
  auto BPLUSTREE_TYPE::InsertInFullInternal(const KeyType & k,
    const page_id_t & Pointer, InternalPage * oldInternalPage, InternalPage ** returnedNewPage, int brotherId) -> std::pair < KeyType, page_id_t > {
    int arrayLength = oldInternalPage -> GetArraySize();
    if (arrayLength != oldInternalPage -> GetMaxSize() + 1) return GetInvalidPair();
    // page_id_t newPageId;
    // Page * newPage = buffer_pool_manager_ -> NewPage( & newPageId);
    // InternalPage * newInternalPage = reinterpret_cast < InternalPage * > (newPage -> GetData());
    // newInternalPage -> Init(newPageId, oldInternalPage -> GetParentPageId(), oldInternalPage -> GetMaxSize());
    InternalPage * newInternalPage = MakeTwin < InternalPage > (oldInternalPage);
    if (newInternalPage == nullptr) return GetInvalidPair();
    BPlusTreeInternalPage < KeyType,
    page_id_t,
    KeyComparator > temporaryPage;
    temporaryPage.Init(INVALID_PAGE_ID, INVALID_PAGE_ID, oldInternalPage -> GetMaxSize() + 1);
    // temporaryPage.SetMaxSize(oldInternalPage -> GetMaxSize() + 1); //Just For Holding all so i can split easily (This step is for simplification)
    for (int i = 1; i < arrayLength; i++) {

      page_id_t leftPointer = oldInternalPage -> ValueAt(i - 1);
      page_id_t rightPointer = oldInternalPage -> ValueAt(i);
      KeyType key = oldInternalPage -> KeyAt(i);
       temporaryPage.Insert(leftPointer, key, rightPointer, comparator_);
    }
    temporaryPage.InsertAndShift(k, Pointer, comparator_);

    while (oldInternalPage -> GetSize() > 0) {
      oldInternalPage -> pop();
    }
    int median = floor(temporaryPage.GetSize() / 2.0);

    for (int i = 1; i < median + 1; i++) {
      oldInternalPage -> Insert(temporaryPage.ValueAt(i - 1), temporaryPage.KeyAt(i), temporaryPage.ValueAt(i), comparator_);
    }
    for (int i = median + 2; i < temporaryPage.GetSize() + 1; i++) {
      newInternalPage -> Insert(temporaryPage.ValueAt(i - 1), temporaryPage.KeyAt(i), temporaryPage.ValueAt(i), comparator_);
    }
    for (int i = 0; i < newInternalPage -> GetArraySize(); i++) {
      //This Loop is responsible for updating parent pointers
      page_id_t currentPageId = newInternalPage -> ValueAt(i);
      Page * rawPage = buffer_pool_manager_ -> FetchPage(currentPageId);
  
      if (rawPage == nullptr) return GetInvalidPair();
           if (currentPageId != Pointer && currentPageId != brotherId) {
         rawPage->WLatch();
      }
      BPlusTreePage * BPage = reinterpret_cast < BPlusTreePage * > (rawPage -> GetData());
      BPage -> SetParentPageId(newInternalPage -> GetPageId());
      //BUG
      if (currentPageId != Pointer && currentPageId != brotherId) {
      rawPage->WUnlatch();
      }
      //  rawPage->WUnlatch();
      buffer_pool_manager_ -> UnpinPage(currentPageId, true);
    }
    //Median + 1 is sent to the caller so he insert it to the parent 
    std::pair < KeyType,
    page_id_t > returnPair = std::make_pair(temporaryPage.KeyAt(median + 1), newInternalPage -> GetPageId());
    * returnedNewPage = newInternalPage;
    return returnPair;
  }

  INDEX_TEMPLATE_ARGUMENTS
  auto BPLUSTREE_TYPE::InsertIntoParent(InternalPage * currentInternal, KeyType & key, page_id_t currentPageId, page_id_t brotherId, Transaction * transaction) -> void {
    if (currentInternal -> IsFull()) {
      InternalPage * brotherPage;
      InternalPage * parentPage;
      page_id_t parentPageId = currentInternal -> GetParentPageId();
      std::pair < KeyType, page_id_t > returnedPair = InsertInFullInternal(key, currentPageId, currentInternal, & brotherPage, brotherId);
      if (parentPageId == INVALID_PAGE_ID) {
        //Here i need to create new rgit oot 
        parentPage = BuildRootNode < InternalPage > (internal_max_size_);
        if (parentPage == 0) return;
        parentPageId = parentPage -> GetPageId();
      } else {
        Page * rawParentPage = buffer_pool_manager_ -> FetchPage(parentPageId);
        if (rawParentPage == nullptr) throw Exception(ExceptionType::OUT_OF_MEMORY, "fail to fetch page2");;
        parentPage = reinterpret_cast < InternalPage * > (rawParentPage -> GetData());
      }
      currentInternal -> SetParentPageId(parentPageId);
      brotherPage -> SetParentPageId(parentPageId);
      // currentPage -> SetParentPageId(brotherPage -> GetPageId());
      // currentPage->SetParentPageId(returnedPair.second);

      buffer_pool_manager_ -> UnpinPage(brotherPage -> GetPageId(), true);
      InsertIntoParent(parentPage, returnedPair.first, brotherPage->GetPageId(), currentInternal -> GetPageId(), transaction);
      //   parentPage->Insert(currentInternal->GetPageId(), returnedPair.first, brotherPage->GetPageId(), comparator_);
 
      buffer_pool_manager_ -> UnpinPage(parentPageId, true);
    } else {
      currentInternal -> Insert(brotherId, key, currentPageId, comparator_);
    }
  }

  /*****************************************************************************
   * REMOVE
   *****************************************************************************/
  /*
   * Delete key & value pair associated with input key
   * If current tree is empty, return immdiately.
   * If not, User needs to first find the right leaf page as deletion target, then
   * delete entry from leaf page. Remember to deal with redistribute or merge if
   * necessary.
   */
  INDEX_TEMPLATE_ARGUMENTS
  void BPLUSTREE_TYPE::Remove(const KeyType & key, Transaction * transaction) {
    if (transaction != nullptr) {
      rootLatch.WLock();
      transaction->AddIntoPageSet(nullptr);
    }
     if (root_page_id_ == INVALID_PAGE_ID) {
      ClearLatches(DELETE_TRAVERSE, transaction, false);
      return;}
    LeafPage * foundLeaf = FindLeaf(key, root_page_id_, DELETE_TRAVERSE, transaction);

    if (!foundLeaf -> KeyExist(key, comparator_)) {
       
      ClearLatches(DELETE_TRAVERSE, transaction, false);
 
      return;
    }
    //If The Root is the Leaf
    if (foundLeaf -> IsRootPage() && foundLeaf -> GetSize() >= 1) {
 
      foundLeaf -> Remove(key, comparator_);
      if (foundLeaf -> IsRootPage() && foundLeaf -> GetSize() == 0) {
        root_page_id_ = INVALID_PAGE_ID;
        UpdateRootPageId(0);
         transaction -> AddIntoDeletedPageSet(root_page_id_);
      }
         CleanupDeletedPages(transaction);
         ClearLatches(DELETE_TRAVERSE, transaction, true);
            return;
    }

    if (foundLeaf -> IsMin()) {
      //Here We Go
      HandleLeafDelete(foundLeaf, key, transaction);
    } else {
      foundLeaf -> Remove(key, comparator_);
    }

    CleanupDeletedPages(transaction);
    ClearLatches(DELETE_TRAVERSE, transaction, true);

  }
  INDEX_TEMPLATE_ARGUMENTS
  void BPLUSTREE_TYPE::HandleLeafDelete(BPlusTreePage * currentPage,
    const KeyType & key, Transaction * transaction) {
    page_id_t currentPageId = currentPage -> GetPageId();
    bool sucess = false;
    if (currentPage -> IsLeafPage()) {
      Page * rawParentPage = buffer_pool_manager_ -> FetchPage(currentPage -> GetParentPageId());
      if (rawParentPage == nullptr) throw Exception(ExceptionType::OUT_OF_MEMORY, "fail to fetch page4");;
      LeafPage * currentLeafPage = reinterpret_cast < LeafPage * > (currentPage);
      InternalPage * parentPage = reinterpret_cast < InternalPage * > (rawParentPage -> GetData());
      page_id_t leftBrotherId = parentPage -> GetLeftSibling(currentPageId);
      page_id_t rightBrotherId = parentPage -> GetRightSibling(currentPageId);
      //Check if one of them can give me  item so i become balanced

      LeafPage * leftBrotherPage;
      LeafPage * rightBrotherPage;
      Page * rawLeftBrother;
      Page * rawRightBrother;
      if (leftBrotherId != INVALID_PAGE_ID) {
         rawLeftBrother = buffer_pool_manager_ -> FetchPage(leftBrotherId);
        rawLeftBrother->WLatch();
        if (rawLeftBrother == nullptr) throw Exception(ExceptionType::OUT_OF_MEMORY, "fail to fetch page5");;
        leftBrotherPage = reinterpret_cast < LeafPage * > (rawLeftBrother -> GetData());
        if (!leftBrotherPage -> IsMin()) {
          //He will give me
          ReDistributeLeaf(leftBrotherPage, currentLeafPage, true);
          currentLeafPage -> Remove(key, comparator_);
          KeyType myKey = currentLeafPage -> KeyAt(0);
          parentPage -> ChangeKeyOfValue(currentPageId, myKey);
          sucess = true;
          buffer_pool_manager_ -> UnpinPage(parentPage -> GetPageId(), true);
        }
        rawLeftBrother->WUnlatch();
      }
      if (rightBrotherId != INVALID_PAGE_ID && !sucess) {
        //if the previous step failed try with the right brother
         rawRightBrother = buffer_pool_manager_ -> FetchPage(rightBrotherId);
        rawRightBrother->WLatch();
        if (rawRightBrother == nullptr) throw Exception(ExceptionType::OUT_OF_MEMORY, "fail to fetch page");;
        rightBrotherPage = reinterpret_cast < LeafPage * > (rawRightBrother -> GetData());
        if (!rightBrotherPage -> IsMin()) {
          //He will give me
          ReDistributeLeaf(rightBrotherPage, currentLeafPage, false);
          currentLeafPage -> Remove(key, comparator_);
          KeyType myKey = rightBrotherPage -> KeyAt(0);
          parentPage -> ChangeKeyOfValue(rightBrotherId, myKey);
          sucess = true;
          buffer_pool_manager_ -> UnpinPage(parentPage -> GetPageId(), true);
        }
         rawRightBrother->WUnlatch();
      }
      if (!sucess) {
        //Here i have to merge 

        //we will start looking for merge opportinutiy at the left
        if (leftBrotherId != INVALID_PAGE_ID) {
          rawLeftBrother->WLatch();
          currentLeafPage -> Remove(key, comparator_);
          Merge(currentLeafPage, leftBrotherPage);
          transaction -> AddIntoDeletedPageSet(currentPageId);
          // parentPage->Remove(currentPageId);
            rawLeftBrother->WUnlatch();
          HandleInternalDelete(parentPage, currentPageId, transaction);
          sucess = true;
         
        } else if (rightBrotherId != INVALID_PAGE_ID && !sucess) {
          rawRightBrother->WLatch();
          currentLeafPage -> Remove(key, comparator_);
          Merge(rightBrotherPage, currentLeafPage);
          transaction -> AddIntoDeletedPageSet(rightBrotherId);
            rawRightBrother->WUnlatch();
          HandleInternalDelete(parentPage, rightBrotherId, transaction);
          // parentPage->Remove(rightBrotherId);//
          sucess = true;
         
        }
  
      buffer_pool_manager_ -> UnpinPage(currentPage -> GetParentPageId(), true);
      }
      if (leftBrotherId != INVALID_PAGE_ID)  buffer_pool_manager_ ->UnpinPage(leftBrotherId, true);
      if (rightBrotherId != INVALID_PAGE_ID) buffer_pool_manager_ ->UnpinPage(rightBrotherId, true);
    }

   
 
  }

  INDEX_TEMPLATE_ARGUMENTS
  void BPLUSTREE_TYPE::HandleInternalDelete(BPlusTreePage * currentPage,
    const page_id_t value, Transaction * transaction) {

    if (currentPage -> IsRootPage()) {

      InternalPage * currentInternalPage = reinterpret_cast < InternalPage * > (currentPage);
      currentInternalPage -> Remove(value);
      if (currentPage -> GetSize() == 0) {
        page_id_t newRootId = reinterpret_cast < InternalPage * > (currentPage) -> ValueAt(0);
        Page * newRootPage = buffer_pool_manager_ -> FetchPage(newRootId);
        reinterpret_cast < BPlusTreePage * > (newRootPage) -> SetParentPageId(INVALID_PAGE_ID);
        root_page_id_ = newRootId;
        UpdateRootPageId(0);
        buffer_pool_manager_ -> UnpinPage(newRootId, true);
      }
      return;
    }
    if (currentPage -> IsInternalPage()) {
      InternalPage * currentInternalPage = reinterpret_cast < InternalPage * > (currentPage);
      if (!currentInternalPage -> IsMin()) {
        currentInternalPage -> Remove(value);
        return;
      }

      //Here we go
      page_id_t currentPageId = currentPage -> GetPageId();
      bool sucess = false;
      Page * rawParentPage = buffer_pool_manager_ -> FetchPage(currentPage -> GetParentPageId());
      if (rawParentPage == nullptr) throw Exception(ExceptionType::OUT_OF_MEMORY, "fail to fetch page6");;

      InternalPage * parentPage = reinterpret_cast < InternalPage * > (rawParentPage -> GetData());
      page_id_t leftBrotherId = parentPage -> GetLeftSibling(currentPageId);
      page_id_t rightBrotherId = parentPage -> GetRightSibling(currentPageId);
      InternalPage * leftBrotherPage;
      InternalPage * rightBrotherPage;
      Page * rawLeftBrother;
      Page * rawRightBrother;
      if (leftBrotherId != INVALID_PAGE_ID) {
         rawLeftBrother = buffer_pool_manager_ -> FetchPage(leftBrotherId);
          rawLeftBrother->WLatch();
        if (rawLeftBrother == nullptr) throw Exception(ExceptionType::OUT_OF_MEMORY, "fail to fetch page7");;
        leftBrotherPage = reinterpret_cast < InternalPage * > (rawLeftBrother -> GetData());
        if (!leftBrotherPage -> IsMin()) {
          currentInternalPage -> Remove(value);
          std::pair < KeyType, page_id_t > movedPairs = leftBrotherPage -> pop();
          //Say to the pooped element the new father
          currentInternalPage -> pushFront(movedPairs.first, movedPairs.second);

          //Then Swapping
          currentInternalPage -> SetKeyAt(1, parentPage -> GetKeyOfValue(currentPageId));
          parentPage -> ChangeKeyOfValue(currentPageId, movedPairs.first);
          Page * movedPage = buffer_pool_manager_ -> FetchPage(movedPairs.second);
          BPlusTreePage * BmovedPage = reinterpret_cast < BPlusTreePage * > (movedPage);
          BmovedPage -> SetParentPageId(currentInternalPage -> GetPageId());
          sucess = true;
          buffer_pool_manager_ -> UnpinPage(movedPairs.second, true);
 
        }
         rawLeftBrother->WUnlatch();
      }
      if (rightBrotherId != INVALID_PAGE_ID && !sucess) {
        //if the previous step failed try with the right brother
         rawRightBrother = buffer_pool_manager_ -> FetchPage(rightBrotherId);
         rawRightBrother->WLatch();
        if (rawRightBrother == nullptr) throw Exception(ExceptionType::OUT_OF_MEMORY, "fail to fetch page8");;
        rightBrotherPage = reinterpret_cast < InternalPage * > (rawRightBrother -> GetData());
        if (!rightBrotherPage -> IsMin()) {
          currentInternalPage -> Remove(value);
          std::pair < KeyType, page_id_t > movedPairs = rightBrotherPage -> popFront();
          //Say to the pooped element the new father
          currentInternalPage -> pushBack(movedPairs.first, movedPairs.second);

          //Then Swapping
          currentInternalPage -> SetKeyAt(currentInternalPage -> GetArraySize() - 1, parentPage -> GetKeyOfValue(rightBrotherId));
          parentPage -> ChangeKeyOfValue(rightBrotherId, movedPairs.first);
          Page * movedPage = buffer_pool_manager_ -> FetchPage(movedPairs.second);
          BPlusTreePage * BmovedPage = reinterpret_cast < BPlusTreePage * > (movedPage);
          BmovedPage -> SetParentPageId(currentInternalPage -> GetPageId());
          sucess = true;
          buffer_pool_manager_ -> UnpinPage(movedPairs.second, true);
         
        }
        rawRightBrother->WUnlatch();
      }
      if (!sucess) {
        //Here i have to merge 

        //we will start looking for merge opportinutiy at the left
        if (leftBrotherId != INVALID_PAGE_ID) {
          rawLeftBrother->WLatch();
          currentInternalPage -> Remove(value);
          KeyType topKey = parentPage -> GetKeyOfValue(currentPageId);
          page_id_t firstPageId = currentInternalPage -> ValueAt(0);
          leftBrotherPage -> InsertAndShift(topKey, firstPageId, comparator_);
          Page * child = buffer_pool_manager_ -> FetchPage(firstPageId);
          BPlusTreePage * page = reinterpret_cast < BPlusTreePage * > (child);
          page -> SetParentPageId(leftBrotherPage -> GetPageId());
           
          MergeInternalPage(currentInternalPage, leftBrotherPage);
          transaction -> AddIntoDeletedPageSet(currentPageId);
          // parentPage->Remove(currentPageId);
          rawLeftBrother->WUnlatch();
          HandleInternalDelete(parentPage, currentPageId, transaction);
          buffer_pool_manager_ -> UnpinPage(child -> GetPageId(), true);
          sucess = true;
       
        } else if (rightBrotherId != INVALID_PAGE_ID && !sucess) {
          rawRightBrother->WLatch();
          currentInternalPage -> Remove(value);
          KeyType topKey = parentPage -> GetKeyOfValue(rightBrotherId);
          page_id_t firstPageId = rightBrotherPage -> ValueAt(0);
          currentInternalPage -> InsertAndShift(topKey, firstPageId, comparator_);
          Page * child = buffer_pool_manager_ -> FetchPage(firstPageId);
          BPlusTreePage * page = reinterpret_cast < BPlusTreePage * > (child);
          page -> SetParentPageId(currentInternalPage -> GetPageId());
         
          MergeInternalPage(rightBrotherPage, currentInternalPage);
          transaction -> AddIntoDeletedPageSet(rightBrotherId);
          // parentPage->Remove(currentPageId);
          rawRightBrother->WUnlatch();
          HandleInternalDelete(parentPage, rightBrotherId, transaction);
          buffer_pool_manager_ -> UnpinPage(child -> GetPageId(), true);
          sucess = true;
     
        }
 
      }
          if (leftBrotherId != INVALID_PAGE_ID)  buffer_pool_manager_ ->UnpinPage(leftBrotherId, true);
      if (rightBrotherId != INVALID_PAGE_ID) buffer_pool_manager_ ->UnpinPage(rightBrotherId, true);
      buffer_pool_manager_ -> UnpinPage(currentPage -> GetParentPageId(), true);
    }
  }
  INDEX_TEMPLATE_ARGUMENTS
  void BPLUSTREE_TYPE::ReDistributeLeaf(LeafPage * src, LeafPage * destination, bool popBack) {
    if (popBack) {
      MappingType pair = src -> pop();
      destination -> Insert(pair.first, pair.second, comparator_);
    } else {
      MappingType pair = src -> popFront();
      destination -> Insert(pair.first, pair.second, comparator_);
    }
  }

  INDEX_TEMPLATE_ARGUMENTS
  void BPLUSTREE_TYPE::Merge(LeafPage * src, LeafPage * destination) {
    while (src -> GetSize() != 0) {
      MappingType pair = src -> pop();
      destination -> Insert(pair.first, pair.second, comparator_);
    }
    destination -> SetNextPageId(src -> GetNextPageId());
  }

  INDEX_TEMPLATE_ARGUMENTS
  void BPLUSTREE_TYPE::MergeInternalPage(InternalPage * src, InternalPage * destination) {
    int length = src -> GetArraySize();
    for (int i = 1; i < length; i++) {
      std::pair < KeyType, page_id_t > pair = src -> pop();
      destination -> InsertAndShift(pair.first, pair.second, comparator_);
      Page * child = buffer_pool_manager_ -> FetchPage(pair.second);
      BPlusTreePage * page = reinterpret_cast < BPlusTreePage * > (child);
      page -> SetParentPageId(destination -> GetPageId());
      buffer_pool_manager_ -> UnpinPage(child -> GetPageId(), true);
    }
  }
  /*****************************************************************************
   * INDEX ITERATOR
   *****************************************************************************/
  /*
   * Input parameter is void, find the leaftmost leaf page first, then construct
   * index iterator
   * @return : index iterator
   */
  INDEX_TEMPLATE_ARGUMENTS
  auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
    //THIS IS NOT MULTITHREADED SAFE!!
    //I will create iterator that has in its current state the most left leaf so i wanna give him this id !
    rootLatch.RLock();
    if (root_page_id_ == INVALID_PAGE_ID) {
    rootLatch.RUnlock();
    return INDEXITERATOR_TYPE(INVALID_PAGE_ID, buffer_pool_manager_, 0);
    }
    rootLatch.RUnlock();
    LeafPage * leftMostLeaf = FindLeftMostLeaf(root_page_id_);
    buffer_pool_manager_ -> UnpinPage(leftMostLeaf -> GetPageId(), false);
    return INDEXITERATOR_TYPE(leftMostLeaf -> GetPageId(), buffer_pool_manager_, 0);
  }

  /*
   * Input parameter is low key, find the leaf page that contains the input key
   * first, then construct index iterator
   * @return : index iterator
   */
  INDEX_TEMPLATE_ARGUMENTS
  auto BPLUSTREE_TYPE::Begin(const KeyType & key) -> INDEXITERATOR_TYPE {
    rootLatch.RLock();
    if (root_page_id_ == INVALID_PAGE_ID) {
        rootLatch.RUnlock();
       return INDEXITERATOR_TYPE(INVALID_PAGE_ID, buffer_pool_manager_, 0);
    }
        rootLatch.RUnlock();
    LeafPage * currentLeaf = FindLeaf(key, root_page_id_, LOOKUP_TRAVERSE);
    int index = currentLeaf -> KeyIndex(key, comparator_);
     buffer_pool_manager_ -> UnpinPage(currentLeaf -> GetPageId(), false);
    return INDEXITERATOR_TYPE(currentLeaf -> GetPageId(), buffer_pool_manager_, index);
  }

  /*
   * Input parameter is void, construct an index iterator representing the end
   * of the key/value pair in the leaf node
   * @return : index iterator
   */
  INDEX_TEMPLATE_ARGUMENTS
  auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
    return INDEXITERATOR_TYPE(INVALID_PAGE_ID, buffer_pool_manager_, 0);
  }

  /**
   * @return Page id of the root of this tree
   */
  INDEX_TEMPLATE_ARGUMENTS
  auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
    return root_page_id_;
  }

  /*****************************************************************************
   * UTILITIES AND DEBUG
   *****************************************************************************/
  /*
   * Update/Insert root page id in header page(where page_id = 0, header_page is
   * defined under include/page/header_page.h)
   * Call this method everytime root page id is changed.
   * @parameter: insert_record      defualt value is false. When set to true,
   * insert a record <index_name, root_page_id> into header page instead of
   * updating it.
   */
  INDEX_TEMPLATE_ARGUMENTS
  void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
    auto * header_page = static_cast < HeaderPage * > (buffer_pool_manager_ -> FetchPage(HEADER_PAGE_ID));
    if (insert_record != 0) {
      // create a new record<index_name + root_page_id> in header_page
      header_page -> InsertRecord(index_name_, root_page_id_);
    } else {
      // update root_page_id in header_page
      header_page -> UpdateRecord(index_name_, root_page_id_);
    }
    buffer_pool_manager_ -> UnpinPage(HEADER_PAGE_ID, true);
  }

  /*
   * This method is used for test only
   * Read data from file and insert one by one
   */
  INDEX_TEMPLATE_ARGUMENTS
  void BPLUSTREE_TYPE::InsertFromFile(const std::string & file_name, Transaction * transaction) {
    int64_t key;
    std::ifstream input(file_name);
    while (input) {
      input >> key;

      KeyType index_key;
      index_key.SetFromInteger(key);
      RID rid(key);
      Insert(index_key, rid, transaction);
    }
  }
  /*
   * This method is used for test only
   * Read data from file and remove one by one
   */
  INDEX_TEMPLATE_ARGUMENTS
  void BPLUSTREE_TYPE::RemoveFromFile(const std::string & file_name, Transaction * transaction) {
    int64_t key;
    std::ifstream input(file_name);
    while (input) {
      input >> key;
      KeyType index_key;
      index_key.SetFromInteger(key);
      Remove(index_key, transaction);
    }
  }

  /**
   * This method is used for debug only, You don't need to modify
   */
  INDEX_TEMPLATE_ARGUMENTS
  void BPLUSTREE_TYPE::Draw(BufferPoolManager * bpm,
    const std::string & outf) {
    if (IsEmpty()) {
      LOG_WARN("Draw an empty tree");
      return;
    }
    std::ofstream out(outf);
    out << "digraph G {" << std::endl;
    ToGraph(reinterpret_cast < BPlusTreePage * > (bpm -> FetchPage(root_page_id_) -> GetData()), bpm, out);
    out << "}" << std::endl;
    out.flush();
    out.close();
  }

  /**
   * This method is used for debug only, You don't need to modify
   */
  INDEX_TEMPLATE_ARGUMENTS
  void BPLUSTREE_TYPE::Print(BufferPoolManager * bpm) {
    if (IsEmpty()) {
      LOG_WARN("Print an empty tree");
      return;
    }
    ToString(reinterpret_cast < BPlusTreePage * > (bpm -> FetchPage(root_page_id_) -> GetData()), bpm);
  }

  /**
   * This method is used for debug only, You don't need to modify
   * @tparam KeyType
   * @tparam ValueType
   * @tparam KeyComparator
   * @param page
   * @param bpm
   * @param out
   */
  INDEX_TEMPLATE_ARGUMENTS
  void BPLUSTREE_TYPE::ToGraph(BPlusTreePage * page, BufferPoolManager * bpm, std::ofstream & out) const {
    std::string leaf_prefix("LEAF_");
    std::string internal_prefix("INT_");
    if (page -> IsLeafPage()) {
      auto * leaf = reinterpret_cast < LeafPage * > (page);
      // Print node name
      out << leaf_prefix << leaf -> GetPageId();
      // Print node properties
      out << "[shape=plain color=green ";
      // Print data of the node
      out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
      // Print data
      out << "<TR><TD COLSPAN=\"" << leaf -> GetSize() << "\">P=" << leaf -> GetPageId() << "</TD></TR>\n";
      out << "<TR><TD COLSPAN=\"" << leaf -> GetSize() << "\">" <<
        "max_size=" << leaf -> GetMaxSize() << ",min_size=" << leaf -> GetMinSize() << ",size=" << leaf -> GetSize() <<
        "</TD></TR>\n";
      out << "<TR>";
      for (int i = 0; i < leaf -> GetSize(); i++) {
        out << "<TD>" << leaf -> KeyAt(i) << "</TD>\n";
      }
      out << "</TR>";
      // Print table end
      out << "</TABLE>>];\n";
      // Print Leaf node link if there is a next page
      if (leaf -> GetNextPageId() != INVALID_PAGE_ID) {
        out << leaf_prefix << leaf -> GetPageId() << " -> " << leaf_prefix << leaf -> GetNextPageId() << ";\n";
        out << "{rank=same " << leaf_prefix << leaf -> GetPageId() << " " << leaf_prefix << leaf -> GetNextPageId() << "};\n";
      }

      // Print parent links if there is a parent
      if (leaf -> GetParentPageId() != INVALID_PAGE_ID) {
        out << internal_prefix << leaf -> GetParentPageId() << ":p" << leaf -> GetPageId() << " -> " << leaf_prefix <<
          leaf -> GetPageId() << ";\n";
      }
    } else {
      auto * inner = reinterpret_cast < InternalPage * > (page);
      // Print node name
      out << internal_prefix << inner -> GetPageId();
      // Print node properties
      out << "[shape=plain color=pink "; // why not?
      // Print data of the node
      out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
      // Print data
      out << "<TR><TD COLSPAN=\"" << inner -> GetArraySize() << "\">P=" << inner -> GetPageId() << "</TD></TR>\n";
      out << "<TR><TD COLSPAN=\"" << inner -> GetArraySize() << "\">" <<
        "max_size=" << inner -> GetMaxSize() << ",min_size=" << inner -> GetMinSize() << ",size=" << inner -> GetArraySize() <<
        "</TD></TR>\n";
      out << "<TR>";
      for (int i = 0; i < inner -> GetArraySize(); i++) {

        out << "<TD PORT=\"p" << inner -> ValueAt(i) << "\">";
        if (i > 0) {
          out << inner -> KeyAt(i);
        } else {
          out << " ";
        }
        out << "</TD>\n";
      }
      out << "</TR>";
      // Print table end
      out << "</TABLE>>];\n";
      // Print Parent link
      if (inner -> GetParentPageId() != INVALID_PAGE_ID) {
        out << internal_prefix << inner -> GetParentPageId() << ":p" << inner -> GetPageId() << " -> " << internal_prefix <<
          inner -> GetPageId() << ";\n";
      }
      // Print leaves
      for (int i = 0; i < inner -> GetArraySize(); i++) {
        auto child_page = reinterpret_cast < BPlusTreePage * > (bpm -> FetchPage(inner -> ValueAt(i)) -> GetData());
        ToGraph(child_page, bpm, out);
        if (i > 0) {
          auto sibling_page = reinterpret_cast < BPlusTreePage * > (bpm -> FetchPage(inner -> ValueAt(i - 1)) -> GetData());
          if (!sibling_page -> IsLeafPage() && !child_page -> IsLeafPage()) {
            out << "{rank=same " << internal_prefix << sibling_page -> GetPageId() << " " << internal_prefix <<
              child_page -> GetPageId() << "};\n";
          }
          bpm -> UnpinPage(sibling_page -> GetPageId(), false);
        }
      }
    }
    bpm -> UnpinPage(page -> GetPageId(), false);
  }

  /**
   * This function is for debug only, you don't need to modify
   * @tparam KeyType
   * @tparam ValueType
   * @tparam KeyComparator
   * @param page
   * @param bpm
   */
  INDEX_TEMPLATE_ARGUMENTS
  void BPLUSTREE_TYPE::ToString(BPlusTreePage * page, BufferPoolManager * bpm) const {
    if (page -> IsLeafPage()) {
      auto * leaf = reinterpret_cast < LeafPage * > (page);
      std::cout << "Leaf Page: " << leaf -> GetPageId() << " parent: " << leaf -> GetParentPageId() <<
        " next: " << leaf -> GetNextPageId() << std::endl;
      for (int i = 0; i < leaf -> GetSize(); i++) {
        std::cout << leaf -> KeyAt(i) << ",";
      }
      std::cout << std::endl;
      std::cout << std::endl;
    } else {
      auto * internal = reinterpret_cast < InternalPage * > (page);

      std::cout << "Internal Page: " << internal -> GetPageId() << " parent: " << internal -> GetParentPageId() << std::endl;
      for (int i = 0; i < internal -> GetSize() + 1; i++) {
        std::cout << internal -> KeyAt(i) << ": " << internal -> ValueAt(i) << ",";
      }
      std::cout << std::endl;
      std::cout << std::endl;

      for (int i = 0; i < internal -> GetSize() + 1; i++) {
        ToString(reinterpret_cast < BPlusTreePage * > (bpm -> FetchPage(internal -> ValueAt(i)) -> GetData()), bpm);
      }
    }
    bpm -> UnpinPage(page -> GetPageId(), false);
  }
  INDEX_TEMPLATE_ARGUMENTS
  auto BPLUSTREE_TYPE::testKeyExist(KeyComparator comparator) -> int {

    RID rid;
    RID rid2;
    KeyType index_key;
    KeyType index_key2;
    BPlusTreeInternalPage < KeyType, page_id_t, KeyComparator > internalPage = {};
    internalPage.Init((page_id_t) 1, (page_id_t) 0, 2);
    int64_t key = 42;
    int64_t key2 = 32;
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast < int32_t > (key), value);
    rid2.Set(static_cast < int32_t > (key2), value);
    index_key.SetFromInteger(key);
    index_key2.SetFromInteger(key2);
    page_id_t pageId1 = 1;
    page_id_t pageId2 = 2;
    internalPage.Insert(pageId1, index_key, pageId2, comparator);
    return 2;
  }

  INDEX_TEMPLATE_ARGUMENTS
  auto BPLUSTREE_TYPE::GetInvalidPair() -> std::pair < KeyType, page_id_t > {
    KeyType k;
    page_id_t invalidPage = INVALID_PAGE_ID;
    return std::make_pair(k, invalidPage);
  }

  INDEX_TEMPLATE_ARGUMENTS
  template < typename T >
    auto BPLUSTREE_TYPE::MakeTwin(T * oldNode) -> T * {
      page_id_t newPageId;
      Page * rawNewPage = buffer_pool_manager_ -> NewPage( & newPageId);
     //assert(rawNewPage != nullptr);
      if (rawNewPage == nullptr) throw Exception(ExceptionType::OUT_OF_MEMORY, "fail to fetch page9");;
      T * newPage = reinterpret_cast < T * > (rawNewPage -> GetData());
      newPage -> Init(newPageId, oldNode -> GetParentPageId(), oldNode -> GetMaxSize());
      return newPage;
    }

  INDEX_TEMPLATE_ARGUMENTS
  void BPLUSTREE_TYPE::HandleLatches(Page * page, TRAVERSE_TYPE type, Transaction * transaction, bool isChanged) {
    if (transaction == nullptr) return;
    BPlusTreePage * BPage = reinterpret_cast < BPlusTreePage * > (page);
    if (type == INSERT_TRAVERSE) {
      page -> WLatch();
      if ((BPage->GetSize() < BPage->GetMaxSize() - 1 || (!BPage->IsLeafPage() && BPage->GetSize() == BPage->GetMaxSize() - 1)) && !BPage->IsRootPage()) {
        ClearLatches(type, transaction, isChanged);
      }
      transaction -> AddIntoPageSet(page);
    } else if (type == DELETE_TRAVERSE) {
      page -> WLatch();
      if (!(BPage -> GetMinSize() == BPage -> GetSize()) && !BPage->IsRootPage()) {
        ClearLatches(type, transaction, isChanged);
      }
      transaction -> AddIntoPageSet(page);
    } else if (type == LOOKUP_TRAVERSE) {
      page -> RLatch();
      if (!BPage->IsRootPage()) {
      ClearLatches(type, transaction, false);
      }
      transaction -> AddIntoPageSet(page);
    }

  }
  INDEX_TEMPLATE_ARGUMENTS
  void BPLUSTREE_TYPE::ClearLatches(TRAVERSE_TYPE type, Transaction * transaction, bool isChanged) {
    if (transaction == nullptr) {
      return;
    }
    std::shared_ptr < std::deque < Page * >> queue = transaction -> GetPageSet();
    while (!queue -> empty()) {
      Page * page = queue -> front();
      queue -> pop_front();
      if (page == nullptr) {
        if (type == INSERT_TRAVERSE || type == DELETE_TRAVERSE) {
          rootLatch.WUnlock();
        } else if (type == LOOKUP_TRAVERSE) {
          rootLatch.RUnlock();
        }
        continue;
      }
      if (type == INSERT_TRAVERSE || type == DELETE_TRAVERSE) {
        page -> WUnlatch();
        buffer_pool_manager_ -> UnpinPage(page -> GetPageId(), isChanged);
      } else if (type == LOOKUP_TRAVERSE) {
        page -> RUnlatch();
        buffer_pool_manager_ -> UnpinPage(page -> GetPageId(), false);
      }

    }

    // if (BPage->IsRootPage()) {
    //   if (type == INSERT_TRAVERSE || type == DELETE_TRAVERSE) {
    //     LOG_DEBUG("IM UNLOCKING THE ROOT NOW ");
    //     rootLatch.WUnlock();
    //     page->WUnlatch();
    //     buffer_pool_manager_->UnpinPage(page->GetPageId(), isChanged);

    //   } else if (type == LOOKUP_TRAVERSE) {
    //             LOG_DEBUG("IM UNLOCKING THER READ LOCK ON  ROOT NOW ");
    //     rootLatch.RUnlock();
    //     page->RUnlatch();
    //     buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    //   }
    // } 
  }
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::CleanupDeletedPages(Transaction *transaction) {
      const auto deleted_pages = transaction->GetDeletedPageSet();
      for (const auto &page_id : *deleted_pages) {
        buffer_pool_manager_->DeletePage(page_id);
      }
      deleted_pages->clear();
    }

  template class BPlusTree < GenericKey < 4 > , RID, GenericComparator < 4 >> ;
  template class BPlusTree < GenericKey < 8 > , RID, GenericComparator < 8 >> ;
  template class BPlusTree < GenericKey < 16 > , RID, GenericComparator < 16 >> ;
  template class BPlusTree < GenericKey < 32 > , RID, GenericComparator < 32 >> ;
  template class BPlusTree < GenericKey < 64 > , RID, GenericComparator < 64 >> ;

} // namespace bustub

//Parentid shout updated in insertToPArent

//Insertion in internal node is wrong

/*

Test for mergining internal
3 3
i 1
i 2
i 3
i 4
i 5
i 6
i 7
i 8 
i 9 
i 10
i 11
i 12
d 3
d 4
d 5
d 6
i 12
i 13
d 2
d 7
d 8

*/