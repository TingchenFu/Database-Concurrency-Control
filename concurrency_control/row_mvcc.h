
#ifndef ROW_MVCC_H
#define ROW_MVCC_H

typedef uint64_t ts_t; // used for timestamp
#if ALGO==MVCC
// corresponding versions of a record, using a linked list.
struct RowVersionEntry {
	ts_t wts;
	ts_t rts;
	sem_t status_lock;
	version_t status;
	RowData *version_data;	// data
	RowVersionEntry * past;	// pointer to the next version according to version chain
	RowVersionEntry * future;  // pointer to the previous version
	//RowVersionEntry():wts(0),rts(0),status_lock(),version_data(NULL),past(NULL),future(NULL){}
};

class Row_mvcc {
public:
	void init(RowData* initrow); // init function
	/*
	 * access() function:
	 * To read or write a particular version of this record, 
	 * as well as do concurrency control.
	 */
	RC access(access_t type, TxnMgr * txn, RowData * row);
	/*
	 * commit() function:
	 * To be called when a transaction is committing.
	 * It may include two parts:
	 * 1. Call insert_version() to write a new version, which can also be called in access()
	 *    instead.
	 * 2. Call clear_version_list() to cleanup outdated version.
	 */
	RC commit(access_t type, TxnMgr * txn, RowData* root, RowData * data);
	/*
	 * abort() function:
	 * To be called when a transaction is aborting.
	 * It may include two parts:
	 * 1. Cleanup the intermediate version that is produced when processing a transaction.
	 * 2. Call clear_version_list() to cleanup outdated version.
	 */
	RC abort(access_t type, TxnMgr * txn, RowData* root);

 	//pthread_mutex_t * latch;
	//sem_t sem;
	/*
	 * insert_version() function:
	 * To insert a new version.
	 */
	void insert_version(ts_t rts,ts_t wts, version_t status,RowData *row);
	/*
	 * clear_version_list() function:
	 * To clean up outdated versions in the version chain.
	 */
	RC clear_version_list(access_t type, ts_t ts);
	~Row_mvcc();
	sem_t list_sem;
    std::vector<RowVersionEntry*> *row_version_list;	// version chain
	std::map<ts_t,int> *rvm; //map a txn's start timestamp to the index of row_version_list that it read
	std::map<ts_t,int> *wvm;
	std::map<ts_t,int> *cvm;
	uint64_t row_version_len;		// length of version chain
	
};

#endif
#endif
