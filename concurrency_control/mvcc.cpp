#include "global.h"
#include "universal.h"
#include "txn.h"
#include "mvcc.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row_mvcc.h"

#if ALGO==MVCC
void MVOCC::init() {
    sem_init(&valid_semaphore, 0, 1);
	tnc = 0;
}

RC MVOCC::validate(TxnMgr * txn){
    RC rc=RCOK;
    //sem_wait(&valid_semaphore);
    //insert pending version
    for(size_t i=0;i<txn->access_get_cnt();i++){
        if(txn->access_get_type(i)!=WR)
            continue;
        sem_wait(&(txn->access_get_original_row(i)->manager->list_sem));
        //std::unique_lock lock(txn->access_get_original_row(i)->manager->list_mutex);
        std::map<ts_t,int>* wvmp=txn->access_get_original_row(i)->manager->wvm;
        std::map<ts_t,int>* cvmp=txn->access_get_original_row(i)->manager->cvm;
        std::vector<RowVersionEntry*> * list_ptr=txn->access_get_original_row(i)->manager->row_version_list; 
        size_t index=(*wvmp)[txn->get_start_timestamp()];
        if( ( (index!=list_ptr->size()-1) && (list_ptr->at(list_ptr->size()-1)->rts!=txn->get_start_timestamp()) ) 
        || list_ptr->at(index)->rts > txn->get_start_timestamp()){
            //sem_post(&valid_semaphore);
            //std::unique_lock unlock(txn->access_get_original_row(i)->manager->list_mutex);
            sem_post(&(txn->access_get_original_row(i)->manager->list_sem));
            return Abort;
        }
        else{
            RowVersionEntry* new_version=(RowVersionEntry*)alloc_memory.alloc(sizeof(RowVersionEntry));
            new_version->status=PENDING;
            new_version->rts=new_version->wts=txn->get_start_timestamp();
            list_ptr->push_back(new_version);
            (*cvmp)[txn->get_start_timestamp()]=list_ptr->size()-1;
            //std::unique_lock unlock(txn->access_get_original_row(i)->manager->list_mutex);
            sem_post(&(txn->access_get_original_row(i)->manager->list_sem));
        }
    }
    //update read timestamp
    for(size_t i=0;i<txn->access_get_cnt();i++){
        if(txn->access_get_type(i)!=RD)
            continue;
        sem_wait(&(txn->access_get_original_row(i)->manager->list_sem));
        //std::shared_lock lock(txn->access_get_original_row(i)->manager->list_mutex);
        std::map<ts_t,int>* rvmp=txn->access_get_original_row(i)->manager->rvm;
        std::vector<RowVersionEntry*> * list_ptr=txn->access_get_original_row(i)->manager->row_version_list; 
        size_t index=(*rvmp)[txn->get_start_timestamp()];
        if(list_ptr->at(index)->rts<txn->get_start_timestamp())
            list_ptr->at(index)->rts=txn->get_start_timestamp();
        sem_post(&(txn->access_get_original_row(i)->manager->list_sem));
        //std::shared_lock unlock(txn->access_get_original_row(i)->manager->list_mutex);
    }
    //check version consistency
    for(size_t i=0;i<txn->access_get_cnt();i++){
        //std::shared_lock lock(txn->access_get_original_row(i)->manager->list_mutex);
        sem_wait(&(txn->access_get_original_row(i)->manager->list_sem));
        std::map<ts_t,int>* rvmp=txn->access_get_original_row(i)->manager->rvm;
        std::vector<RowVersionEntry*> * list_ptr=txn->access_get_original_row(i)->manager->row_version_list; 
        std::map<ts_t,int>* wvmp=txn->access_get_original_row(i)->manager->wvm;
        if(txn->access_get_type(i)==RD){
            size_t index=(*rvmp)[txn->get_start_timestamp()];
            for(size_t j=index+1;j<list_ptr->size();j++){
                if(list_ptr->at(j)->wts<=txn->get_start_timestamp()){
                    //sem_post(&valid_semaphore);
                    //std::shared_lock unlock(txn->access_get_original_row(i)->manager->list_mutex);
                    sem_post(&(txn->access_get_original_row(i)->manager->list_sem));
                    return Abort;
                }
            }
        }
        else if(txn->access_get_type(i)==WR){
            size_t index=(*wvmp)[txn->get_start_timestamp()];
            if(list_ptr->at(index)->rts>txn->get_start_timestamp()){
                //sem_post(&valid_semaphore);
                //std::shared_lock unlock(txn->access_get_original_row(i)->manager->list_mutex);
                sem_post(&(txn->access_get_original_row(i)->manager->list_sem));
                return Abort;
            }
        }
        sem_post(&(txn->access_get_original_row(i)->manager->list_sem));
    }
    //sem_post(&valid_semaphore);
    return rc;
}
#endif