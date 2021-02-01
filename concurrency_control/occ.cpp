#include "global.h"
#include "universal.h"
#include "txn.h"
#include "occ.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row_occ.h"

#if ALGO==OCC
void MyOCC::init() {
    sem_init(&valid_semaphore, 0, 1);
	tnc = 0;
}


RC MyOCC::validate(TxnMgr * txn) {
	RC rc=RCOK;
  	uint64_t starttime = acquire_ts();
    INC_STATS(txn->read_thd_id(),txn_validate_time,acquire_ts() - starttime);

	for(uint64_t i=0;i<txn->access_get_cnt()-1;i++){
		for(uint64_t j=i+1;j<txn->access_get_cnt();j++){
			if(txn->access_get_original_row(i)->get_row_id() > txn->access_get_original_row(j)->get_row_id()){
				txn->swap_accesses(i,j);
			}
		}
	}

	std::set<uint64_t> write_rowid;
	std::vector<uint64_t> read_accessid;
	for(uint64_t i=0;i<txn->access_get_cnt();i++){
		if(txn->access_get_type(i)==WR){
			write_rowid.insert(txn->access_get_original_row(i)->get_row_id());
			txn->access_get_original_row(i)->manager->latch();
		}
		else if(txn->access_get_type(i)==RD){
			read_accessid.push_back(i);
		}
	}

	for(uint64_t i=0;i<read_accessid.size();i++){
		if(!txn->access_get_original_row(read_accessid[i])->manager->validate( txn->get_start_timestamp())){
			rc=Abort;
			break;
		}
		if(write_rowid.find(txn->access_get_original_row(read_accessid[i])->get_row_id())==write_rowid.end() 
		&& txn->access_get_original_row(read_accessid[i])->manager->locked()){
			rc=Abort;
			break;
		}		
	}

	return rc;
}

/*
RC MyOCC::validate(TxnMgr * txn){
	RC rc=RCOK;
  	uint64_t starttime = acquire_ts();
    INC_STATS(txn->read_thd_id(),txn_validate_time,acquire_ts() - starttime);
	bool qualified=true;
	sem_wait(&valid_semaphore);
	for (uint64_t i = 0; i < txn->access_get_cnt() && qualified; i++) {
		if(txn->access_get_type(i)==RD){
			//txn->access_get_original_row(i)->manager->latch();
			qualified = txn->access_get_original_row(i)->manager->validate( txn->get_start_timestamp() );
			//txn->access_get_original_row(i)->manager->release();
		}
	}
	sem_post(&valid_semaphore);
	rc=qualified?RCOK:Abort;
	return rc;
}*/





void MyOCC::finish(RC rc, TxnMgr * txn) {
  if(rc == RCOK) {
		txn->set_end_timestamp(global_manager.get_ts( txn->read_thd_id() ));
  }
}

#endif