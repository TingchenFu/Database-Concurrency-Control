
#include "row.h"
#include "txn.h"
#include "row_mvcc.h"
#include "mem_alloc.h"
#include "manager.h"
#include "universal.h"

#if ALGO==MVCC
void Row_mvcc::init(RowData* initrow) {
	//latch = (pthread_mutex_t *)alloc_memory.alloc(sizeof(pthread_mutex_t));
	//pthread_mutex_init(latch, NULL);
	row_version_list=new std::vector<RowVersionEntry*>();
	rvm=new std::map<ts_t,int>();
	wvm=new std::map<ts_t,int>();
	cvm=new std::map<ts_t,int>();
	struct RowVersionEntry* new_version=(RowVersionEntry*) alloc_memory.alloc(sizeof(RowVersionEntry));
	new_version->rts=new_version->wts=0;
	new_version->status=COMMITED;
	new_version->version_data=initrow;
	sem_init(&(new_version->status_lock),0,0);
	row_version_list->push_back(new_version);
	row_version_len = 1;
	sem_init(&list_sem,0,1);
}

RC Row_mvcc::clear_version_list(access_t type, ts_t ts) {
	RC rc = RCOK;
	return rc;
}


void Row_mvcc::insert_version(ts_t rts,ts_t wts, version_t status,RowData *row) {
	/*
	RowVersionEntry* curr=row_version_list;
	RowVersionEntry* last=NULL;
	RowVersionEntry* head=row_version_list;
	RowVersionEntry* new_version=(RowVersionEntry*)alloc_memory(sizeof(RowVersionEntry));
	new_version->start_vts=start_vts;
	new_version->end_vts=end_vts;
	new_version->version_data=(RowData*) alloc_memory.alloc(sizeof(RowData));
	new_version->version_data->copy(row);
	new_version->prev=new_version->next=NULL;
	sem_init(&(new_version->vsem),0,1)
	row_version_len++;
	*/
	/* insert by start version timestamp 
	//if(head==NULL){
	//	row_version_list=new_version;
	//	return;
	//}
	while(curr!=NULL && curr->start_vts<start_vts){
		last=curr;
		curr=curr->next;
	}
	new_version->next=curr;
	new_version->prev=last;

	if(curr!=NULL){
		curr->prev=new_version;
	}
	if(last!=NULL){
		last->next=new_version;
	}
	if(head->prev!=NULL)
		head=head->prev;
	*/
	//insert at the begin
	/*
	new_version->next=head;
	head->prev=new_version;
	row_version_list=new_version;
	*/
}

RC Row_mvcc::access(access_t type, TxnMgr * txn, RowData * row) {
	RC rc = Abort;
	int i;
	//printf("version chain length: %lu\n",row_version_list->size());
	//std::shared_lock lock(list_mutex);
	sem_wait(&list_sem);
	for(i=row_version_list->size()-1;i>=0;i--){
		if(row_version_list->at(i)->wts>txn->get_start_timestamp())
			continue;
		if(row_version_list->at(i)->status==PENDING)
			sem_wait(&(row_version_list->at(i)->status_lock));
		if(row_version_list->at(i)->status==ABORTED)
			continue;
		if(row_version_list->at(i)->status==COMMITED){
			rc=RCOK;
			row->copy(row_version_list->at(i)->version_data);
			if(type==RD)
				(*rvm)[txn->get_start_timestamp()]=i;
			else if(type==WR)
				(*wvm)[txn->get_start_timestamp()]=i;
			//printf("successfully find %d version which is %lu\n",i,row_version_list->at(i)->version_data->get_primary_key());
			break;
		}
	}
	//std::shared_lock unlock(list_mutex);
	sem_post(&list_sem);
	return rc;
}

RC Row_mvcc::commit(access_t type, TxnMgr * txn, RowData* root, RowData * data) {
	RC rc = RCOK;
	int semvalue;
	if(type==RD)
		return rc;
	//std::shared_lock lock(list_mutex);
	sem_wait(&list_sem);
	size_t index=(*cvm)[txn->get_start_timestamp()];
	row_version_list->at(index)->version_data=(RowData*)alloc_memory.alloc(sizeof(RowData));
	row_version_list->at(index)->version_data->init(root->get_table(), root->get_part_id());
	row_version_list->at(index)->version_data->copy(data);
	row_version_list->at(index)->status=COMMITED;
	while(!sem_getvalue(&(row_version_list->at(index)->status_lock),&semvalue) && semvalue<0)
		sem_post(&(row_version_list->at(index)->status_lock));
	//std::shared_lock unlock(list_mutex);
	sem_post(&list_sem);
	return rc;
}

RC Row_mvcc::abort(access_t type, TxnMgr * txn, RowData* root){
	RC rc = RCOK;
	int semvalue;
	if(type==RD)
		return rc;
	//std::shared_lock lock(list_mutex);
	sem_wait(&list_sem);
	size_t index=(*cvm)[txn->get_start_timestamp()];
	row_version_list->at(index)->status=ABORTED;
	while(!sem_getvalue(&(row_version_list->at(index)->status_lock),&semvalue) && semvalue<0)
		sem_post(&(row_version_list->at(index)->status_lock));
	//std::shared_lock unlock(list_mutex);
	sem_post(&list_sem);
	return rc;
}

Row_mvcc::~Row_mvcc(){
	for(size_t i=0;i<row_version_list->size();i++){
		alloc_memory.free(row_version_list->at(i)->version_data,sizeof(RowData));
		alloc_memory.free(row_version_list->at(i),sizeof(RowVersionEntry));
	}
}

#endif