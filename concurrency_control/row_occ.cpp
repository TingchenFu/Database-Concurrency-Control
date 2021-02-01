
#include "txn.h"
#include "row.h"
#include "row_occ.h"
#include "mem_alloc.h"

#if ALGO==OCC
void Row_occ::init(RowData * row) {
	_row = row;
    sem_init(&_semaphore, 0, 1);
	wts = 0;
}

RC Row_occ::access(TxnMgr * txn,access_t type) {
	RC rc = RCOK;
	sem_wait(&_semaphore);
	if (txn->get_start_timestamp() < wts && type==RD) {
		rc = Abort;
	}
	else { 
		txn->cur_row->copy(_row);
		rc = RCOK;
	}
	sem_post(&_semaphore);
	return rc;
}


void Row_occ::latch() {
  sem_wait(&_semaphore);
}

bool Row_occ::validate(uint64_t ts) {
	return ts>=wts;
}

void Row_occ::write(RowData * data, uint64_t ts) {
	_row->copy(data);
	wts = ts;
}


void Row_occ::release() {
  sem_post(&_semaphore);
}

bool Row_occ::locked(){
	int semvalue=1;
	sem_getvalue(&_semaphore,&semvalue);
	return semvalue<=0;
}

#endif