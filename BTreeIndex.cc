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
    return 0;
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
	return 0;
}

/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)
{
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

	// Find the entry whose key value is larger than or equal to searchKey
	// We have to add 1 because our locate function works differently
	int eid;

	// Try to locate the searchKey
	RC locateRC = leafNode.locate(searchKey, eid);

	// if locate error, return the error code
	if (locateRC != 0)
		return locateRC;

	// set the cursor and return
	cursor.pid = readPid;
	cursor.eid = eid;

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
}
