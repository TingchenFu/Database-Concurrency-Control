#ifndef ROW_MyOCC_H
#define ROW_MyOCC_H

class table_t;
class Catalog;
class TxnMgr;
class RowData;
struct TsReqEntry;
#if ALGO==OCC
class Row_occ {
public:
	void 				init(RowData * row);
	RC 					access(TxnMgr * txn,access_t type);
	void 				latch();
	bool				validate(uint64_t valid_ts);
	void				write(RowData * data, uint64_t ts);
	void 				release();
	bool				locked();
private:
 	sem_t 				_semaphore;
	RowData * 			_row;
	ts_t 				wts;
};
#endif

#endif