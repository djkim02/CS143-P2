#include "BTreeNode.h"
#include <math.h>
#include <string.h>
#include <iostream>
#include <stdio.h>

using namespace std;

/*
 * Default Constructor for a BTLeafNode
 */
BTLeafNode::BTLeafNode()
{
	std::fill(buffer, buffer + PageFile::PAGE_SIZE, 0);
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
	printf("in write, pid is %d\n", pid);
	printf("in write, next node's pid is %d\n", getNextNodePtr());
	return pf.write(pid, buffer);
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount()
{
	int count = 0;
	Entry* entry = (Entry*) getEntryStart();
	for(int i = 0; i < MAX_LEAF_ENTRIES; i++)
	{
		if (entry->key == 0)
			break;
		count++;
		entry++;
	}
	return count;
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
	if (getKeyCount() >= MAX_LEAF_ENTRIES)
		return RC_NODE_FULL;
	int eid;
	locate(key, eid);

	// When our leaf node is empty, just add the entry at the front
	if (getKeyCount() == 0)
	{
		Entry* entry = (Entry*) getEntryStart();
		entry->key = key;
		entry->rid.pid = rid.pid;
		entry->rid.sid = rid.sid;
	}
	// When key is larger than any other elements, we just need to add the entry at the end
	else if (eid + 1 == getKeyCount())
	{
		Entry* entry = (Entry*) getEntryStart() + eid + 1;
		entry->key = key;
		entry->rid.pid = rid.pid;
		entry->rid.sid = rid.sid;
	}
	// Otherwise, shift everything to the right then insert the entry
	else
	{	
		Entry* entry = (Entry*) getEntryStart() + getKeyCount();
		Entry* prevEntry = entry - 1;
		Entry* newEntry = (Entry*) getEntryStart() + eid + 1;
		for (; prevEntry >= newEntry; entry--, prevEntry--)
		{
			entry->key = prevEntry->key;
			entry->rid.pid = prevEntry->rid.pid;
			entry->rid.sid = prevEntry->rid.sid;
		}
		newEntry->key = key;
		newEntry->rid.pid = rid.pid;
		newEntry->rid.sid = rid.sid;
	}
	printf("Key count is %d\n", getKeyCount());
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
	int oldKeyCount = getKeyCount();
	if (oldKeyCount < MAX_LEAF_ENTRIES)
		return RC_INVALID_CURSOR; // node is not full, does not need to be split
	if (sibling.getKeyCount() != 0)
		return RC_INVALID_CURSOR; // sibling node must be empty
	int eid;
	locate(key, eid); // find relative position of where our insertion should be
	bool insertIntoCurrent = false;
	double halfwayEntry = ((double) (oldKeyCount-1)) /2.0;
	int newKeyCount = 0;
	if (((double) eid) < halfwayEntry) // insert into current node
	{
		newKeyCount = ((int) floor(((double) oldKeyCount)/2.0));
		insertIntoCurrent = true;
	}
	else // insert into sibling node
	{
		newKeyCount = ((int) ceil(((double) oldKeyCount)/2.0));
	}
	// copy half of our values into sibling node
	int siblingKeyCount = (MAX_LEAF_ENTRIES - newKeyCount);
	memcpy((Entry*) sibling.getEntryStart(), (Entry*) getEntryStart() + newKeyCount, siblingKeyCount * sizeof(Entry));
	// clear old memory in current node
	memset((Entry*) getEntryStart() + newKeyCount, '\0', siblingKeyCount * sizeof(Entry));
	sibling.setNextNodePtr(getNextNodePtr());
	// current node's nextPointer needs to be set in the function that calls this during sibling node creation
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
	Entry* entry = (Entry*) getEntryStart();
	for (eid = 0; eid < getKeyCount() && entry->key < searchKey; eid++, entry++)
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
	if (eid < 0 || eid >= getKeyCount())
		return RC_INVALID_CURSOR;

	Entry* entry = (Entry*) getEntryStart();
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
	memcpy(&pid, buffer, sizeof(PageId));
	return pid;
}

/*
 * Set the pid of the next sibling node.
 * @param pid[IN] the PageId of the next sibling node 
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid)
{
	memcpy(buffer, &pid, sizeof(PageId));
	return 0;
}


/*
 * get a void pointer (needs to be converted) to the start of the entries in the node
 */
char* BTLeafNode::getEntryStart()
{
	return (char*) buffer + sizeof(PageId);
}
/*
 * Get a pointer to the start of the node
 */
PageId* BTLeafNode::getPageIDStart()
{
	return (PageId*) buffer;
}

/* Print the contents of the nodes for debugging
 */
void BTLeafNode::printNode()
{
	Entry* entry = (Entry*) getEntryStart();
	cout << "[pageId|pid,sid,key|...|pid,sid,key]" << endl;
	cout << "[" << *(getPageIDStart()) << "|";
	for (int i = 0; i < getKeyCount(); i++, entry++)
	{
		cout << (entry->rid).pid << "," << (entry->rid).sid << "," << entry->key;
		if (i+1 != getKeyCount())
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
	std::fill(buffer, buffer + PageFile::PAGE_SIZE, 0);
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
	printf("in nl write, pid is %d\n", pid);
	return pf.write(pid, buffer);
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount()
{
	int count = 0;
	Entry* entry = (Entry*) getEntryStart();
	for(int i = 0; i < MAX_NON_LEAF_ENTRIES; i++)
	{
		if (entry->key == 0)
			break;
		count++;
		entry++;
	}
	return count;
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
	if (getKeyCount() >= MAX_NON_LEAF_ENTRIES)
		return RC_NODE_FULL;

	// This is needed for initializeRoot
	if (getKeyCount() == 0)
	{
		Entry* entry = (Entry*) getEntryStart();
		entry->key = key;
		entry->pid = pid;
	}
	// Regular cases. We have to shift everything to the right
	// so that we can insert our element at the right position
	else
	{
		// get the position
		int pos = insertPosition(key);
		Entry* entry = (Entry*) getEntryStart() + getKeyCount() + 1;
		Entry* prevEntry = entry - 1;
		// shift the right most pid manually
		entry->pid = prevEntry->pid;
		entry--;
		prevEntry--;
		// the position of entry that we want to insert in
		Entry* newEntry = (Entry*) getEntryStart() + pos;
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
	printf("nl Key count is %d\n", getKeyCount());
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
	int oldKeyCount = getKeyCount();
	if (oldKeyCount < MAX_NON_LEAF_ENTRIES)
		return RC_INVALID_CURSOR; // node is not full, does not need to be split
	if (sibling.getKeyCount() != 0)
		return RC_INVALID_CURSOR; // sibling node must be empty
	int pos = insertPosition(key); // find relative position of where our insertion should be
	bool insertIntoCurrent = false;
	double halfwayEntry = ((double) (oldKeyCount-1)) /2.0;
	int newKeyCount = 0;
	if (((double) pos) <= halfwayEntry) // insert into current node
	{
		newKeyCount = ((int) floor(((double) oldKeyCount)/2.0));
		insertIntoCurrent = true;
	}
	else // insert into sibling node
	{
		newKeyCount = ((int) ceil(((double) oldKeyCount)/2.0));
	}
	// copy half of our values into sibling node
	int siblingKeyCount = (MAX_NON_LEAF_ENTRIES - newKeyCount);
	memcpy((Entry*)sibling.getEntryStart(), (Entry*) getEntryStart() + newKeyCount, siblingKeyCount * sizeof(Entry) + sizeof(PageId) );
	if (insertIntoCurrent)
	{
		if (insert(key, pid) == RC_NODE_FULL)
			return RC_NODE_FULL;
		midKey = ((Entry*) getEntryStart()+newKeyCount-1)->key; // needs to be moved up to parent node
		// delete last entry we're moving up along with all entries we copied to sibling
		memset(((Entry*) getEntryStart() + newKeyCount-1) + sizeof(PageId), '\0', siblingKeyCount+1 * sizeof(Entry) + sizeof(PageId));
	}
	else
	{
		if (sibling.insert(key, pid) == RC_NODE_FULL)
			return RC_NODE_FULL;
		midKey = ((Entry*)sibling.getEntryStart())->key; // needs to be moved up to parent node
		memcpy((Entry*)getEntryStart()+newKeyCount, (Entry*)sibling.getEntryStart(), sizeof(PageId)); // copy the PageId from midKey
		// shift all entries to the left one entry to overwrite midKey
		memmove((Entry*)sibling.getEntryStart(), ((Entry*)sibling.getEntryStart())+1, (siblingKeyCount-1) * sizeof(Entry) + sizeof(PageId));
		// zero out last entry in sibling
		memset(((Entry*)sibling.getEntryStart() + (siblingKeyCount-1)) + sizeof(PageId), '\0', sizeof(Entry));
		// clear copied entries in other node
		memset(((Entry*)getEntryStart() + newKeyCount) + sizeof(PageId), '\0', siblingKeyCount * sizeof(Entry) + sizeof(PageId));
	}
	
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
	Entry* entry = (Entry*) getEntryStart();
	// if the searchKey is smaller than our first key, return the left pointer of our first key
	if (entry->key > searchKey)
	{
		pid = entry->pid;
		printf("nl child pointer for %d is %d\n", searchKey, pid);
		return 0;
	}
	// Otherwise, find the position and return the correct pid
	for (int i = 0; i < getKeyCount() && entry->key <= searchKey; i++, entry++) {}
	pid = entry->pid;
	printf("nl child pointer for %d is %d\n", searchKey, pid);
	return 0;
}

// return the position of the largest key that is smaller than key[IN]
int BTNonLeafNode::insertPosition(int key)
{
	Entry* entry = (Entry*) getEntryStart();
	for (int i = 0; i < getKeyCount(); i++, entry++)
	{
		if (key < entry->key)
			return i;
	}
	return getKeyCount();
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
	if (getKeyCount() != 0)
		return RC_INVALID_CURSOR;
	// insert our first element
	insert(key, pid1);
	Entry* entry = (Entry*) getEntryStart() + 1;
	// insert pid
	entry->pid = pid2;
	return 0;
}

/*
 * get a void pointer (needs to be converted) to the start of the entries in the node
 */
char* BTNonLeafNode::getEntryStart()
{
	return (char*) buffer + sizeof(PageId);
}

/* Print the contents of the nodes for debugging
 */
void BTNonLeafNode::printNode()
{
	Entry* entry = entryStart;
	cout << "[pageId,key|...|pageId]" << endl;
	cout << "[";
	for (int i = 0; i < getKeyCount(); i++, entry++)
	{
		cout << entry->pid << "," << entry->key << "|";
	}
	cout << entry->pid << "]" << endl;
}
