/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */
 
#include "BTreeIndex.h"
#include "BTreeNode.h"
#include <string.h>
#include <stack>
#include <stdio.h>

using namespace std;

/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
    rootPid = -1;
    treeHeight = 0;
}

/*
 * Open the index file in read or write mode.
 * Under 'w' mode, the index file should be created if it does not exist.
 * @param indexname[IN] the name of the index file
 * @param mode[IN] 'r' for read, 'w' for write
 * @return error code. 0 if no error
 */
RC BTreeIndex::open(const string& indexname, char mode)
{
	RC errorMsg = pf.open(indexname, mode);
	if (errorMsg != 0)
		return errorMsg;

	// check if our PageFile is empty
	if (pf.endPid() == 0)
	{
		// store our default values to populate first pid in PageFile
		memcpy(buffer, &rootPid, sizeof(PageId));
		memcpy(buffer+sizeof(PageId), &treeHeight, sizeof(int));
		errorMsg = pf.write(BTREE_BOOT_UP_PID, buffer);
		if (errorMsg != 0)
			return errorMsg;
		return 0;
	}
	// otherwise read the content from disk
	errorMsg = pf.read(BTREE_BOOT_UP_PID, buffer);
	if (errorMsg != 0)
		return errorMsg;
	memcpy(&rootPid, buffer, sizeof(PageId));
	memcpy(&treeHeight, buffer+sizeof(PageId), sizeof(int));
	return 0;
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
	// save our values to disk before closing
	memcpy(buffer, &rootPid, sizeof(PageId));
	memcpy(buffer+sizeof(PageId), &treeHeight, sizeof(int));
	RC errorMsg = pf.write(BTREE_BOOT_UP_PID, buffer);
	if (errorMsg != 0)
		return errorMsg;
	return pf.close();
}

/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)
{
	if (treeHeight == 0)
	{
		BTLeafNode first = BTLeafNode();
		first.insert(key, rid);
		rootPid = pf.endPid();
		RC errorMsg = first.write(rootPid, pf);
		if (errorMsg != 0)
			return errorMsg;
		treeHeight++;
		return 0;
	}

	BTNonLeafNode nonLeafNode;
	PageId readPid = rootPid;
	stack<PageId> pids;		// used to find parent pids in the event of splits
	int height = treeHeight;
	// processing non-leaf nodes
	while (height > 1)
	{
		// read the node from Pagefile
		RC nonLeafRC = nonLeafNode.read(readPid, pf);

		// if read error, return the error code
		if (nonLeafRC != 0)
			return nonLeafRC;

		// save the parent pid in case of a node split
		pids.push(readPid);

		// locate the next node that we have to examine
		nonLeafRC = nonLeafNode.locateChildPtr(key, readPid);
		printf("readPid after locateChildPtr %d\n", readPid);
		// if locate fails, return the error code
		if (nonLeafRC != 0)
			return nonLeafRC;

		// examine the next level of the tree
		height--;
	}

	// if we reached here, we have gotten to our leaf node
	BTLeafNode leafNode;

	// read the node from Pagefile
	RC leafRC = leafNode.read(readPid, pf);

	// if read error, return the error code
	if (leafRC != 0)
		return leafRC;

	if (leafNode.insert(key, rid) != RC_NODE_FULL)
		return leafNode.write(readPid, pf);

	// create new sibling node
	BTLeafNode sibling;
	int siblingKey;

	RC errorMsg = leafNode.insertAndSplit(key, rid, sibling, siblingKey);
	if (errorMsg != 0)
		return errorMsg;

	// set next pointer to new sibling's pid
	errorMsg = leafNode.setNextNodePtr(pf.endPid());
	if (errorMsg != 0)
		return errorMsg;
	// they have the correct next node ptr
	printf("leaf node's next node ptr %d\n", leafNode.getNextNodePtr());

	// save updated node in memory
	printf("writing leaf node %d\n", sibling.getNextNodePtr());
	errorMsg = leafNode.write(readPid, pf);
	if (errorMsg != 0)
		return errorMsg;
	printf("readPid is %d\n", readPid);
	// but for some reason, it is not saved

	// save new node in memory
	errorMsg = sibling.write(pf.endPid(), pf);
	if (errorMsg != 0)
		return errorMsg;

	printf("sibling's next node ptr %d\n", sibling.getNextNodePtr());


	int newKey = siblingKey;
	// continually try to insert into parent non-leaf nodes and split if overflow
	while (!pids.empty())
	{
		BTNonLeafNode parent;

		// read the node from Pagefile
		PageId parentPid = pids.top();
		pids.pop();
		errorMsg = parent.read(parentPid, pf);
		if (errorMsg != 0)
			return errorMsg;

		if (parent.insert(newKey, pf.endPid()-1) != RC_NODE_FULL)
			return parent.write(parentPid, pf);

		BTNonLeafNode nonLeafSibling;
		int midKey;

		errorMsg = parent.insertAndSplit(newKey, pf.endPid()-1, nonLeafSibling, midKey);
		if (errorMsg != 0)
			return errorMsg;

		// save updated node in memory
		errorMsg = parent.write(parentPid, pf);
		if (errorMsg != 0)
			return errorMsg;

		// save new node in memory
		errorMsg = nonLeafSibling.write(pf.endPid(), pf);
		if (errorMsg != 0)
			return errorMsg;

		newKey = midKey;
	}
	// if we got here, we've overflowed the root node as well
	BTNonLeafNode newRoot;
	errorMsg = newRoot.initializeRoot(rootPid, newKey, pf.endPid()-1);
	if (errorMsg != 0)
		return errorMsg;
	rootPid = pf.endPid();
	errorMsg = newRoot.write(pf.endPid(), pf);
	if (errorMsg != 0)
		return errorMsg;
	treeHeight++;
    return 0;
}

/**
 * Run the standard B+Tree key search algorithm and identify the
 * leaf node where searchKey may exist. If an index entry with
 * searchKey exists in the leaf node, set IndexCursor to its location
 * (i.e., IndexCursor.pid = PageId of the leaf node, and
 * IndexCursor.eid = the searchKey index entry number.) and return 0.
 * If not, set IndexCursor.pid = PageId of the leaf node and
 * IndexCursor.eid = the index entry immediately after the largest
 * index key that is smaller than searchKey, and return the error
 * code RC_NO_SUCH_RECORD.
 * Using the returned "IndexCursor", you will have to call readForward()
 * to retrieve the actual (key, rid) pair from the index.
 * @param key[IN] the key to find
 * @param cursor[OUT] the cursor pointing to the index entry with
 *                    searchKey or immediately behind the largest key
 *                    smaller than searchKey.
 * @return 0 if searchKey is found. Othewise an error code
 */
RC BTreeIndex::locate(int searchKey, IndexCursor& cursor)
{
	BTNonLeafNode nonLeafNode;
	PageId readPid = rootPid;
	int height = treeHeight;
	printf("in locate rootPid is %d\n", readPid);
	printf("in locate height is %d\n", height);
	printf("in locate searchKey is %d\n", searchKey);

	// processing non-leaf nodes
	while (height > 1)
	{
		// read the node from Pagefile
		RC nonLeafRC = nonLeafNode.read(readPid, pf);

		// if read error, return the error code
		if (nonLeafRC != 0)
			return nonLeafRC;

		// locate the next node that we have to examine
		nonLeafRC = nonLeafNode.locateChildPtr(searchKey, readPid);

		// if locate fails, return the error code
		if (nonLeafRC != 0)
			return nonLeafRC;

		// examine the next level of the tree
		height--;
	}

	// if we reached here, we have gotten to our leaf node
	BTLeafNode leafNode;
	printf("in locate leafPid is %d\n", readPid);


	// read the node from Pagefile
	RC leafRC = leafNode.read(readPid, pf);

	// if read error, return the error code
	if (leafRC != 0)
		return leafRC;

	// Find the entry whose key value is larger than or equal to searchKey
	// We have to add 1 because our locate function works differently
	int eid;

	// Try to locate the searchKey
	RC locateRC = leafNode.locate(searchKey, eid);
	printf("locateRC is %d\n", locateRC);

	// set the cursor and return
	cursor.pid = readPid;
	cursor.eid = eid + 1;
	printf("in locate cursor.pid is %d\n", cursor.pid);
	printf("in locate cursor.eid is %d\n", cursor.eid);

    return 0;
}

/*
 * Read the (key, rid) pair at the location specified by the index cursor,
 * and move foward the cursor to the next entry.
 * @param cursor[IN/OUT] the cursor pointing to an leaf-node index entry in the b+tree
 * @param key[OUT] the key stored at the index cursor location.
 * @param rid[OUT] the RecordId stored at the index cursor location.
 * @return error code. 0 if no error
 */
RC BTreeIndex::readForward(IndexCursor& cursor, int& key, RecordId& rid)
{
    BTLeafNode leafNode;
    PageId pid = cursor.pid;
    int eid = cursor.eid;

    // read our leaf node from PageFile
    RC leafRC = leafNode.read(pid, pf);

    // if read error, return the error code
    if (leafRC != 0)
    	return leafRC;

    // read the entry from our leaf node
    leafRC = leafNode.readEntry(eid, key, rid);

    // if readEntry error, return the error code
    if (leafRC != 0)
    	return leafRC;

    // Move the cursor to the next entry
    eid++;

    // if the one that we just the last element of the node
    if (eid >= BTLeafNode::MAX_LEAF_ENTRIES)
    {
    	// move on to the next node
    	cursor.pid = leafNode.getNextNodePtr();

    	// set eid to 0, which denotes the first entry of a node
    	eid = 0;
    }

    // update cursor's eid
    cursor.eid = eid;
    return 0;
}

/**
  * @param count[OUT] total number of keys in the BTreeIndex
  * @return error code. 0 if no error
  */
RC BTreeIndex::getTotalKeyCount(int& count)
{
	printf("In total key count\n");

	int searchKey = -99999999;
	BTNonLeafNode nonLeafNode;
	PageId readPid = rootPid;
	int height = treeHeight;
	count = 0;

	printf("Height is %d\n", height);

	// processing non-leaf nodes
	while (height > 1)
	{
		// read the node from Pagefile
		RC nonLeafRC = nonLeafNode.read(readPid, pf);

		// if read error, return the error code
		if (nonLeafRC != 0)
			return nonLeafRC;

		// locate the next node that we have to examine
		nonLeafRC = nonLeafNode.locateChildPtr(searchKey, readPid);

		// if locate fails, return the error code
		if (nonLeafRC != 0)
			return nonLeafRC;

		// examine the next level of the tree
		height--;
	}

	// if we reached here, we have gotten to our leaf node
	BTLeafNode leafNode;

	// read the node from Pagefile
	RC leafRC = leafNode.read(readPid, pf);
	// if read error, return the error code
	if (leafRC != 0)
		return leafRC;
	count += leafNode.getKeyCount();
	readPid = leafNode.getNextNodePtr();

	while (readPid < pf.endPid() && readPid != 0)
	{
		// read the node from Pagefile
		leafRC = leafNode.read(readPid, pf);

		// if read error, return the error code
		if (leafRC != 0)
			return leafRC;
		count += leafNode.getKeyCount();
		readPid = leafNode.getNextNodePtr();
	}
	return 0;
}
