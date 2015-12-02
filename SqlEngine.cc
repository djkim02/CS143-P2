/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "BTreeIndex.h"

using namespace std;

// external functions and variables for load file and sql command parsing 
extern FILE* sqlin;
int sqlparse(void);


RC SqlEngine::run(FILE* commandline)
{
  fprintf(stdout, "Bruinbase> ");

  // set the command line input and start parsing user input
  sqlin = commandline;
  sqlparse();  // sqlparse() is defined in SqlParser.tab.c generated from
               // SqlParser.y by bison (bison is GNU equivalent of yacc)

  return 0;
}

/* We check if the queried table has an index and if so, we check the conditions
 * to make appropriate optimizations using our B+ Tree search algorithms
 */
RC SqlEngine::select(int attr, const string& table, const vector<SelCond>& cond)
{
  bool DEBUG = true;
  RecordFile rf;   // RecordFile containing the table
  RecordId   rid;  // record cursor for table scanning

  RC     rc;
  int    key;     
  string value;
  int    count;
  int    diff;

  // open the table file
  if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
    fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
    return rc;
  }

  BTreeIndex index;
  if ((index.open(table + ".idx", 'r')) != 0) {
    // no index exists for this table so we must
    // scan the table file from the beginning
    if (DEBUG)
      cout << "No index found..." << endl;
    no_index:
    rid.pid = rid.sid = 0;
    count = 0;
    while (rid < rf.endRid()) {
      // read the tuple
      if ((rc = rf.read(rid, key, value)) < 0) {
        fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
        goto exit_select;
      }

      // check the conditions on the tuple
      for (unsigned i = 0; i < cond.size(); i++) {
        // compute the difference between the tuple value and the condition value
        switch (cond[i].attr) {
        case 1:
          diff = key - atoi(cond[i].value);
          break;
        case 2:
          diff = strcmp(value.c_str(), cond[i].value);
          break;
        }

        // skip the tuple if any condition is not met
        switch (cond[i].comp) {
          case SelCond::EQ:
            if (diff != 0) goto next_tuple;
            break;
          case SelCond::NE:
            if (diff == 0) goto next_tuple;
            break;
          case SelCond::GT:
            if (diff <= 0) goto next_tuple;
            break;
          case SelCond::LT:
            if (diff >= 0) goto next_tuple;
            break;
          case SelCond::GE:
            if (diff < 0) goto next_tuple;
            break;
          case SelCond::LE:
            if (diff > 0) goto next_tuple;
            break;
        }
      }

      // the condition is met for the tuple. 
      // increase matching tuple counter
      count++;

      // print the tuple 
      switch (attr) {
      case 1:  // SELECT key
        fprintf(stdout, "%d\n", key);
        break;
      case 2:  // SELECT value
        fprintf(stdout, "%s\n", value.c_str());
        break;
      case 3:  // SELECT *
        fprintf(stdout, "%d '%s'\n", key, value.c_str());
        break;
      }

      // move to the next tuple
      next_tuple:
      ++rid;
    }

      // print matching tuple count if "select count(*)"
    if (attr == 4) {
      fprintf(stdout, "%d\n", count);
    }
    rc = 0;
  }
  else {
    // use the index to speed up searching
    if (DEBUG)
      cout << "Found index!" << endl;
    rid.pid = rid.sid = 0;
    count = 0;
    int searchKey = -99999999;
    int maxKey = 99999999;
    bool isEqualityComparison = false;
    bool isReadVal = false;
    bool isOnlyNotEqualsComparisons = true;
    bool isOnlyCountStar = false;

    // go through conditions and reduce our searching appropriately
    for (unsigned i = 0; i < cond.size(); i++) {
      // set starting point for our key searching based on highest minimum value we can start at
      if (cond[i].attr == 1 && (cond[i].comp == SelCond::GT || 
                                cond[i].comp == SelCond::GE)) {
        if (searchKey < atoi(cond[i].value))
          searchKey = atoi(cond[i].value);
      }
      // set ending point for our key searching based on lowest maximum value we can end at  
      if (cond[i].attr == 1 && (cond[i].comp == SelCond::LT || 
                                cond[i].comp == SelCond::LE)) {
        if (maxKey > atoi(cond[i].value))
          maxKey = atoi(cond[i].value);
      }
      // check if we can ignore checking the value to reduce PageFile reads
      if (cond[i].attr == 2)
        isReadVal = true;
      // check if all our conditions are NOT EQUALS, 
      // then we should just iterate through all elements without using the index
      if (cond[i].comp != SelCond::NE)
        isOnlyNotEqualsComparisons = false;
      // immediately search for this key if there is an equality comparison
      if (cond[i].attr == 1 && cond[i].comp == SelCond::EQ) {
        isEqualityComparison = true;
        searchKey = atoi(cond[i].value);
        break;
      }
    }
    // check if only count(*) to save from doing unnecessary PageFile reads
    if (cond.size() == 0 && attr == 4)
    {
      isOnlyCountStar = true;
      isOnlyNotEqualsComparisons = false;
    }

    if (DEBUG)
    {
      cout << "Conditions equal:" << endl;
      cout << "isEqualityComparison: " << isEqualityComparison << endl;
      cout << "isReadVal: " << isReadVal << endl;
      cout << "isOnlyNotEqualsComparisons: " << isOnlyNotEqualsComparisons << endl; 
      cout << "isOnlyCountStar: " << isOnlyCountStar << endl;
    }
    if (isOnlyNotEqualsComparisons && !isEqualityComparison)
      goto no_index;

    IndexCursor cursor;
    index.locate(searchKey, cursor);
    if (DEBUG)
    {
      cout << "searchKey: " << searchKey << endl;
      cout << "cursor.pid: " << cursor.pid << endl;
      cout << "cursor.eid: " << cursor.eid << endl;
    }

    if (isEqualityComparison)
    {
        bool isEqual = true;
        RC errorMsg = index.readForward(cursor, searchKey, rid);
        if (errorMsg != 0)
          return errorMsg;

        if (DEBUG)
        {
          cout << "searchKey: " << searchKey << endl;
          cout << "rid.pid: " << rid.pid << endl;
          cout << "rid.sid: " << rid.sid << endl;
        }
        // read the tuple
        if ((rc = rf.read(rid, searchKey, value)) < 0) {
          fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
          goto exit_select;
        } 

        if (DEBUG)
        {
          cout << "searchKey: " << searchKey << endl;
          cout << "rid.pid: " << rid.pid << endl;
          cout << "rid.sid: " << rid.sid << endl;
          cout << "value: " << value << endl;
          cout << "cond.size: " << cond.size() << endl;
        }

        // check the conditions on the tuple
        for (unsigned i = 0; i < cond.size(); i++) {
          // compute the difference between the tuple value and the condition value
          switch (cond[i].attr) {
          case 1:
            diff = searchKey - atoi(cond[i].value);
            break;
          case 2:
            diff = strcmp(value.c_str(), cond[i].value);
            break;
          }
          if (DEBUG)
          {
            cout << "i: " << i << endl;
            cout << "seachKey: " << searchKey << endl;
            cout << "cond[i].value: " << cond[i].value << endl;
          }

          // skip the search if any condition is not met
          switch (cond[i].comp) {
            case SelCond::EQ:
              if (diff != 0) isEqual = false;
              break;
            case SelCond::NE:
              if (diff == 0) isEqual = false;
              break;
            case SelCond::GT:
              if (diff <= 0) isEqual = false;
              break;
            case SelCond::LT:
              if (diff >= 0) isEqual = false;
              break;
            case SelCond::GE:
              if (diff < 0) isEqual = false;
              break;
            case SelCond::LE:
              if (diff > 0) isEqual = false;
              break;
          }
          if (!isEqual)
            break;
        }
        if (isEqual)
        {
          // the conditions are met for the tuple. 
          // increase matching tuple counter
          count++;

          // print the tuple 
          switch (attr) {
          case 1:  // SELECT key
            fprintf(stdout, "%d\n", searchKey);
            break;
          case 2:  // SELECT value
            fprintf(stdout, "%s\n", value.c_str());
            break;
          case 3:  // SELECT *
            fprintf(stdout, "%d '%s'\n", searchKey, value.c_str());
            break;
          }
        }
    }
    else if (isOnlyCountStar)
    {
      RC errorMsg = index.getTotalKeyCount(count);
      if (errorMsg != 0)
        return errorMsg;
    }
    else if (isReadVal)
    {
      while (index.readForward(cursor, searchKey, rid) == 0 && searchKey < maxKey)  {
        // read the tuple
        if ((rc = rf.read(rid, searchKey, value)) < 0) {
          fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
          goto exit_select;
        }
        // check the conditions on the tuple
        for (unsigned i = 0; i < cond.size(); i++) {
          // compute the difference between the tuple value and the condition value
          switch (cond[i].attr) {
          case 1:
            diff = searchKey - atoi(cond[i].value);
            break;
          case 2:
            diff = strcmp(value.c_str(), cond[i].value);
            break;
          }

          // skip the tuple if any condition is not met
          switch (cond[i].comp) {
            case SelCond::EQ:
              if (diff != 0) continue;
              break;
            case SelCond::NE:
              if (diff == 0) continue;
              break;
            case SelCond::GT:
              if (diff <= 0) continue;
              break;
            case SelCond::LT:
              if (diff >= 0) continue;
              break;
            case SelCond::GE:
              if (diff < 0) continue;
              break;
            case SelCond::LE:
              if (diff > 0) continue;
              break;
          }
        }

        // the condition is met for the tuple. 
        // increase matching tuple counter
        count++;

        // print the tuple 
        switch (attr) {
        case 1:  // SELECT key
          fprintf(stdout, "%d\n", searchKey);
          break;
        case 2:  // SELECT value
          fprintf(stdout, "%s\n", value.c_str());
          break;
        case 3:  // SELECT *
          fprintf(stdout, "%d '%s'\n", searchKey, value.c_str());
          break;
        }

      }
    }
    else  // don't read values from PageFile unless we have a match
    {
      while (index.readForward(cursor, searchKey, rid) == 0 && searchKey < maxKey)  {
        // check the conditions on the tuple
        for (unsigned i = 0; i < cond.size(); i++) {
          // compute the difference between the tuple value and the condition value
          diff = searchKey - atoi(cond[i].value);

          // skip the tuple if any condition is not met
          switch (cond[i].comp) {
            case SelCond::EQ:
              if (diff != 0) continue;
              break;
            case SelCond::NE:
              if (diff == 0) continue;
              break;
            case SelCond::GT:
              if (diff <= 0) continue;
              break;
            case SelCond::LT:
              if (diff >= 0) continue;
              break;
            case SelCond::GE:
              if (diff < 0) continue;
              break;
            case SelCond::LE:
              if (diff > 0) continue;
              break;
          }
        }

        // the condition is met for the tuple. 
        // increase matching tuple counter
        count++;

        // read the tuple
        if ((rc = rf.read(rid, searchKey, value)) < 0) {
          fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
          goto exit_select;
        }

        // print the tuple 
        switch (attr) {
        case 1:  // SELECT key
          fprintf(stdout, "%d\n", searchKey);
          break;
        case 2:  // SELECT value
          fprintf(stdout, "%s\n", value.c_str());
          break;
        case 3:  // SELECT *
          fprintf(stdout, "%d '%s'\n", searchKey, value.c_str());
          break;
        }

      } 
    }

    // print matching tuple count if "select count(*)"
    if (attr == 4) {
      fprintf(stdout, "%d\n", count);
    }
    rc = 0;

    index.close();
  }

  // close the table file and return
  exit_select:
  rf.close();
  return rc;
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
  RecordFile* recordFile = new RecordFile(table + ".tbl", 'w');
  ifstream fileName(loadfile.c_str());
  string line;
  int key;
  string value;
  RecordId recordId;

  if (index)
  {
    BTreeIndex btree;
    btree.open(table + ".idx", 'w');

    while (getline(fileName, line))
    {
      if (parseLoadLine(line, key, value) == 0)
      {
        if (recordFile->append(key, value, recordId) != 0)
        {
          return RC_INVALID_ATTRIBUTE;
        }
        if (btree.insert(key, recordId) != 0)
        {
          return RC_FILE_WRITE_FAILED;
        }
      }
      else
      {
        return RC_INVALID_ATTRIBUTE;
      }
    }
    
    btree.close();
  }
  else
  {
    while (getline(fileName, line))
    {
      if (parseLoadLine(line, key, value) == 0)
      {
        if (recordFile->append(key, value, recordId) == 0)
        {

        }
        else
        {
          return RC_INVALID_ATTRIBUTE;
        }
      }
      else
      {
        return RC_INVALID_ATTRIBUTE;
      }
    }
  }

  fileName.close();
  recordFile->close();
  delete(recordFile);

  return 0;
}

RC SqlEngine::parseLoadLine(const string& line, int& key, string& value)
{
    const char *s;
    char        c;
    string::size_type loc;
    
    // ignore beginning white spaces
    c = *(s = line.c_str());
    while (c == ' ' || c == '\t') { c = *++s; }

    // get the integer key value
    key = atoi(s);

    // look for comma
    s = strchr(s, ',');
    if (s == NULL) { return RC_INVALID_FILE_FORMAT; }

    // ignore white spaces
    do { c = *++s; } while (c == ' ' || c == '\t');
    
    // if there is nothing left, set the value to empty string
    if (c == 0) { 
        value.erase();
        return 0;
    }

    // is the value field delimited by ' or "?
    if (c == '\'' || c == '"') {
        s++;
    } else {
        c = '\n';
    }

    // get the value string
    value.assign(s);
    loc = value.find(c, 0);
    if (loc != string::npos) { value.erase(loc); }

    return 0;
}
