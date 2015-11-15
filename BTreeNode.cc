#include "BTreeNode.h"
#include <math.h>
#include <string.h>
#include <iostream>

using namespace std;

/*
 * Default Constructor for a BTLeafNode
 */
BTLeafNode::BTLeafNode()
{
	keyCount = 0;
	pageIdStart = (PageId*) buffer;
	entryStart = (Entry*) (buffer + sizeof(PageId));
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf)
{
	return pf.read(pid, buffer);
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf)
{
	return pf.write(pid, buffer);
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
RC BTLeafNode::getKeyCount()
{
	return keyCount;
}

/**
 * set the number of keys stored in the node.
 * @return 0 if successful. Return an error code if the number is of invalid size for the node
 */
RC BTLeafNode::setKeyCount(int number)
{
	if (number < 0 || number > MAX_LEAF_ENTRIES)
		return RC_INVALID_CURSOR;
	keyCount = number;
	return 0;
}


/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid)
{
	// If the node is full, return RC_NODE_FULL
	if (keyCount >= MAX_LEAF_ENTRIES)
		return RC_NODE_FULL;
	int eid;
	locate(key, eid);

	// When our leaf node is empty, just add the entry at the front
	if (keyCount == 0)
	{
		entryStart->key = key;
		entryStart->rid = rid;
	}
	// When key is larger than any other elements, we just need to add the entry at the end
	else if (eid + 1 == keyCount)
	{
		Entry* entry = entryStart + eid + 1;
		entry->key = key;
		entry->rid = rid;
	}
	// Otherwise, shift everything to the right then insert the entry
	else
	{
		Entry* entry = entryStart + keyCount;
		Entry* prevEntry = entry - 1;
		Entry* newEntry = entryStart + eid + 1;
		for (; prevEntry >= newEntry; entry--, prevEntry--)
		{
			entry->key = prevEntry->key;
			entry->rid = prevEntry->rid;
		}
		newEntry->key = key;
		newEntry->rid = rid;
	}

	// Update the count of the entries (keys)
	keyCount++;
	return 0;
}

/*
 * Insert the (key, rid) pair to the node
 * and split the node half and half with sibling.
 * The first key of the sibling node is returned in siblingKey.
 * @param key[IN] the key to insert.
 * @param rid[IN] the RecordId to insert.
 * @param sibling[IN] the sibling node to split with. This node MUST be EMPTY when this function is called.
 * @param siblingKey[OUT] the first key in the sibling node after split.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::insertAndSplit(int key, const RecordId& rid, 
                              BTLeafNode& sibling, int& siblingKey)
{ 
	if (keyCount < MAX_LEAF_ENTRIES)
		return RC_INVALID_CURSOR; // node is not full, does not need to be split
	if (sibling.getKeyCount() != 0)
		return RC_INVALID_CURSOR; // sibling node must be empty
	int eid;
	locate(key, eid); // find relative position of where our insertion should be
	bool insertIntoCurrent = false;
	double halfwayEntry = ((double) (keyCount-1)) /2.0;
	if (((double) eid) < halfwayEntry) // insert into current node
	{
		keyCount = (int) floor(((double) keyCount)/2.0);
		insertIntoCurrent = true;
	}
	else // insert into sibling node
	{
		keyCount = (int) ceil(((double) keyCount)/2.0);
	}
	// copy half of our values into sibling node
	sibling.setKeyCount(MAX_LEAF_ENTRIES - keyCount);
	memcpy((Entry*)sibling.getEntryStart(), entryStart + keyCount, sibling.getKeyCount() * sizeof(Entry) );
	sibling.setNextNodePtr(getNextNodePtr());
	setNextNodePtr(*sibling.getPageIDStart());
	if (insertIntoCurrent)
	{
		if (insert(key, rid) == RC_NODE_FULL)
			return RC_NODE_FULL;
	}
	else
	{
		if (sibling.insert(key, rid) == RC_NODE_FULL)
			return RC_NODE_FULL;
	}
	siblingKey = ((Entry*)sibling.getEntryStart())->key; // needs to be used to set parent node pointer
	return 0;
}

/**
 * If searchKey exists in the node, set eid to the index entry
 * with searchKey and return 0. If not, set eid to the index entry
 * of the largest index key that is smaller than searchKey,
 * and return the error code RC_NO_SUCH_RECORD.
 * Remember that keys inside a B+tree node are always kept sorted.
 * @param searchKey[IN] the key to search for.
 * @param eid[OUT] the index entry number with searchKey or immediately
                   behind the largest key smaller than searchKey.
 * @return 0 if searchKey is found. Otherwise return an error code.
 * EXAMPLE: If Node is [14|15|17|19|22] and you're trying to locate 18,
 * the eid[OUT] will be set to 17's eid
 */
RC BTLeafNode::locate(int searchKey, int& eid)
{
	Entry* entry = entryStart;
	for (eid = 0; eid < keyCount && entry->key < searchKey; eid++, entry++)
	{
		if (entry->key == searchKey)
			return 0;
	}
	eid--;
	return RC_NO_SUCH_RECORD;
}

/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid)
{
	if (eid < 0 || eid >= keyCount)
		return RC_INVALID_CURSOR;

	Entry* entry = (Entry*) entryStart;
	entry += eid;
	key = entry->key;
	rid = entry->rid;
	return 0;
}

/*
 * Return the pid of the next sibling node.
 * @return the PageId of the next sibling node 
 */
PageId BTLeafNode::getNextNodePtr()
{
	PageId pid;
	memcpy(&pid, pageIdStart, sizeof(PageId));
	return pid;
}

/*
 * Set the pid of the next sibling node.
 * @param pid[IN] the PageId of the next sibling node 
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid)
{
	memcpy(pageIdStart, &pid, sizeof(PageId));
	return 0;
}


/*
 * get a void pointer (needs to be converted) to the start of the entries in the node
 */
void* BTLeafNode::getEntryStart()
{
	return (void*) entryStart;
}
/*
 * Get a pointer to the start of the node
 */
PageId* BTLeafNode::getPageIDStart()
{
	return pageIdStart;
}

/* Print the contents of the nodes for debugging
 */
void BTLeafNode::printNode()
{
	Entry* entry = entryStart;
	cout << "[pageId|pid,sid,key|...|pid,sid,key]" << endl;
	cout << "[" << *pageIdStart << "|";
	for (int i = 0; i < keyCount; i++, entry++)
	{
		cout << (entry->rid).pid << "," << (entry->rid).sid << "," << entry->key;
		if (i+1 != keyCount)
			cout << "|";
	}
	cout << "]" << endl;
}




// START: BTNONLeafNode

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */

BTNonLeafNode::BTNonLeafNode()
{
	keyCount = 0;
	entryStart = (Entry*) buffer;
}

RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{
	return pf.read(pid, buffer);
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{
	return pf.write(pid, buffer);
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
RC BTNonLeafNode::getKeyCount()
{
	return keyCount;
}

/**
 * set the number of keys stored in the node.
 * @return 0 if successful. Return an error code if the number is of invalid size for the node
 */
RC BTNonLeafNode::setKeyCount(int number)
{
	if (number < 0 || number > MAX_NON_LEAF_ENTRIES)
		return RC_INVALID_CURSOR;
	keyCount = number;
	return 0;
}

/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
{
	// If the node is full, return RC_NODE_FULL
	if (keyCount >= MAX_NON_LEAF_ENTRIES)
		return RC_NODE_FULL;

	// This is needed for initializeRoot
	if (keyCount == 0)
	{
		entryStart->key = key;
		entryStart->pid = pid;
	}
	// Regular cases. We have to shift everything to the right
	// so that we can insert our element at the right position
	else
	{
		// get the position
		int pos = insertPosition(key);
		Entry* entry = entryStart + keyCount + 1;
		Entry* prevEntry = entry - 1;
		// shift the right most pid manually
		entry->pid = prevEntry->pid;
		entry--;
		prevEntry--;
		// the position of entry that we want to insert in
		Entry* newEntry = entryStart + pos;
		// shift everything to the right
		for (; prevEntry >= newEntry; entry--, prevEntry--)
		{
			entry->key = prevEntry->key;
			entry->pid = prevEntry->pid;
		}
		// insert our new key and pid
		newEntry->key = key;
		newEntry->pid = pid;
	}
	keyCount++;
	return 0;
}

/*
 * Insert the (key, pid) pair to the node
 * and split the node half and half with sibling.
 * The middle key after the split is returned in midKey.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @param sibling[IN] the sibling node to split with. This node MUST be empty when this function is called.
 * @param midKey[OUT] the key in the middle after the split. This key should be inserted to the parent node.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey)
{ 
	if (keyCount < MAX_NON_LEAF_ENTRIES)
		return RC_INVALID_CURSOR; // node is not full, does not need to be split
	if (sibling.getKeyCount() != 0)
		return RC_INVALID_CURSOR; // sibling node must be empty
	int pos = insertPosition(key); // find relative position of where our insertion should be
	bool insertIntoCurrent = false;
	double halfwayEntry = ((double) (keyCount-1)) /2.0;
	if (((double) pos) < halfwayEntry) // insert into current node
	{
		keyCount = (int) floor(((double) keyCount)/2.0);
		insertIntoCurrent = true;
	}
	else // insert into sibling node
	{
		keyCount = (int) ceil(((double) keyCount)/2.0);
	}
	// copy half of our values into sibling node
	sibling.setKeyCount(MAX_NON_LEAF_ENTRIES - keyCount);
	memcpy((Entry*)sibling.getEntryStart(), entryStart + keyCount, sibling.getKeyCount() * sizeof(Entry) );
	if (insertIntoCurrent)
	{
		if (insert(key, pid) == RC_NODE_FULL)
			return RC_NODE_FULL;
	}
	else
	{
		if (sibling.insert(key, pid) == RC_NODE_FULL)
			return RC_NODE_FULL;
	}
	midKey = ((Entry*)sibling.getEntryStart())->key; // needs to be used to set parent node pointer
	return 0;
}

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{
	Entry* entry = entryStart;
	// if the searchKey is smaller than our first key, return the left pointer of our first key
	if (entry->key > searchKey)
	{
		pid = entry->pid;
		return 0;
	}
	// Otherwise, find the position and return the correct pid
	for (int i = 0; i < keyCount && entry->key <= searchKey; i++, entry++) {}
	pid = entry->pid;
	return 0;
}

// return the position of the largest key that is smaller than key[IN]
int BTNonLeafNode::insertPosition(int key)
{
	Entry* entry = entryStart;
	for (int i = 0; i < keyCount; i++, entry++)
	{
		if (key < entry->key)
			return i;
	}
	return keyCount;
}

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{
	// keyCount should be empty
	if (keyCount != 0)
		return RC_INVALID_CURSOR;
	// insert our first element
	insert(key, pid1);
	Entry* entry = entryStart + 1;
	// insert pid
	entry->pid = pid2;
	return 0;
}

/*
 * get a void pointer (needs to be converted) to the start of the entries in the node
 */
void* BTNonLeafNode::getEntryStart()
{
  	return (void*) entryStart;
}

/* Print the contents of the nodes for debugging
 */
void BTNonLeafNode::printNode()
{
	Entry* entry = entryStart;
	cout << "[pageId,key|...|pageId]" << endl;
	cout << "[";
	for (int i = 0; i < keyCount; i++, entry++)
	{
		cout << entry->pid << "," << entry->key << "|";
	}
	cout << entry->pid << "]" << endl;
}
