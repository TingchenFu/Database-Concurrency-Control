#ifndef _MyOCC_H_
#define _MyOCC_H_
#include "row.h"
#include "semaphore.h"

class TxnMgr;

#if ALGO==OCC
class MyOCC {
public:
	void init();
	RC validate(TxnMgr * txn);
	void finish(RC rc, TxnMgr * txn);
private:
	volatile uint64_t tnc;
 	sem_t 	valid_semaphore;
};

#endif
#endif