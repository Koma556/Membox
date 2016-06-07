/*
 * membox Progetto del corso di LSO 2016 
 *
 * Dipartimento di Informatica Università di Pisa
 * Docenti: Pelagatti, Torquati
 * 
 */
/**
 * @file membox.c
 * @brief File principale del server membox
 */
#define _POSIX_C_SOURCE 200809L
#define UNIX_PATH_MAX  64
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <pthread.h>

/* inserire gli altri include che servono */

//#include "connections.c"
#include <connections.h>
#include <stats.h>
#include <icl_hash.h>
//#include "icl_hash.c"
#include <message.h>
#include <sys/types.h>
#include <sys/socket.h> 
#include <sys/un.h>
#include <time.h>
#include <errno.h>
#include <ops.h>

typedef struct listS{
	int sokAddr;
	struct listS* next;
}listSimple;

static char *socketpath, *statfilepath;
static int maxconnections, threadsinpool, storagesize, storagebyte, maxobjsize;
static int overlord = 1, activethreads = 0, queueLength = 0, replock = -1;
static FILE* descriptr;
// Struttura dati condivisa
static icl_hash_t* dataTable;
static listSimple* connectionQueue, *head;
// mutex for active threads
static pthread_mutex_t actvth = PTHREAD_MUTEX_INITIALIZER;
// mutex to be called before a mboxStats update
static pthread_mutex_t stCO = PTHREAD_MUTEX_INITIALIZER;
// mutex for connectionQueue
static pthread_mutex_t coQU = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t coQUwait = PTHREAD_COND_INITIALIZER;
// mutex for dataTable
static pthread_mutex_t dataMUTEX = PTHREAD_MUTEX_INITIALIZER;
/* struttura che memorizza le statistiche del server, struct statistics 
 * e' definita in stats.h.
 *
 */
struct statistics  mboxStats = { 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0 };

static inline unsigned int fnv_hash_function( void *key, int len ) {
    unsigned char *p = (unsigned char*)key;
    unsigned int h = 2166136261u;
    int i;
    for ( i = 0; i < len; i++ )
    {
        h = ( h * 16777619 ) ^ p[i];
    }
    return h;
}
static inline unsigned int ulong_hash_function( void *key ) {
    int len = sizeof(unsigned long);
    unsigned int hashval = fnv_hash_function( key, len );
    return hashval;
}
static inline int ulong_key_compare( void *key1, void *key2  ) {
    return ( *(unsigned long*)key1 == *(unsigned long*)key2 );
}

void printLog(){
	int err2;
	
	if(statfilepath != NULL)
	{
		err2 = -1;
		//printList(head);
		do
			{
			if((err2 = pthread_mutex_lock(&stCO)) == 0)
				printStats(descriptr);
			pthread_mutex_unlock(&stCO);
		}
		while(err2 != 0);
		
	}
}

/**
 * @function statOP
 * @brief increases the mboxStats values tied to the operations
 * 
 * @param opResult	the integer resulting from an OP function
 * @param op 		the operation which we need to stat
 */ //TODO: inplement mutex
int statOP(int opResult, op_t op, int length){
	if(opResult == 0)
	{
		switch(op)
		{		
			case PUT_OP:
			{
				mboxStats.nput++;
				mboxStats.current_objects++;
				mboxStats.current_size += length*sizeof(char);
				if(mboxStats.max_size < mboxStats.current_size)
					mboxStats.max_size = mboxStats.current_size;
				if(mboxStats.max_objects < mboxStats.current_objects)
					mboxStats.max_objects = mboxStats.current_objects;
			}
			break;
			case UPDATE_OP: mboxStats.nupdate++;
			break;
			case LOCK_OP: mboxStats.nlock++;
			break;
			case GET_OP: mboxStats.nget++;
			break;
			case REMOVE_OP: 
			{
				mboxStats.nremove++;
				mboxStats.current_objects--;
				mboxStats.current_size -= length*sizeof(char);
			}
			break;
			
			default:
			{
				return -1;
			}
		}
	}
	else
	{
		switch(op)
		{
			case PUT_OP: 
			{
				mboxStats.nput++;
				mboxStats.nput_failed++;
			}
			break;
			case UPDATE_OP: 
			{
				mboxStats.nupdate++;
				mboxStats.nupdate_failed++;
			}
			break;
			case LOCK_OP: 
			{
				mboxStats.nlock++;
				mboxStats.nlock_failed++;
			}
			break;
			case GET_OP: 
			{
				mboxStats.nget++;
				mboxStats.nget++mboxStats.nget_failed++;
			}
			break;
			case REMOVE_OP: 
			{
				mboxStats.nremove++;
				mboxStats.nremove_failed++;
			}
			break;
			default:
			{
				return -1;
			}
		}
	}
	
	return 0;
}

/**
 * @function statConnections
 * @brief handles the mboxStats concurrent connections
 * 
 * @param side		flag to identify if a connection has been opened or closed
 */
int statConnections(int side){
	if(side == 0) mboxStats.concurrent_connections++;
	else mboxStats.concurrent_connections--;
	if(mboxStats.concurrent_connections < 0)
	{
		errno = EIO;
		return -1;
	}
	else return 0;
}

int sendReply(op_t oldop, message_t *msg, int socID){
	printf("[sendReply] oldop: %u\n", (unsigned int)oldop);
	switch(oldop)
	{
		case PUT_OP: return sendHeader(socID, msg);
		break;
		case UPDATE_OP: return sendHeader(socID, msg);
		break;
		case GET_OP:
		{
			if(sendHeader(socID, msg) < 0) return -1;
			if(sendData(socID, msg) < 0) return -1;
			return 0;
		}
		case REMOVE_OP: return sendHeader(socID, msg);
		break;
		case LOCK_OP: return sendHeader(socID, msg);
		break;
		case UNLOCK_OP: return sendHeader(socID, msg);
		break;
		default: return -1;
	}
	return -1;
}

/**
 * @function statStructure
 * @brief handles byte size and obj number stats for mboxStats
 * 
 * @param size 		size of object I just handled
 * @param side		flag to control if I'm removing or adding objects
 */
int statStructure(int size, int side){
	if(side == 1)
	{
		if(++mboxStats.current_objects > mboxStats.max_objects) mboxStats.max_objects = mboxStats.current_objects;
		if((mboxStats.current_size += size) > mboxStats.max_size) mboxStats.max_size = mboxStats.current_size;
	}
	else
	{
		if((mboxStats.current_objects--) < 0) return -1;
		if((mboxStats.current_size-= size) < 0) return -1;
	}
	return 0;
}

char* readLocation(char** args, int argc){
	char *tmp;
	int i;
	
	for(i = 0; i < argc; i++)
	{
		tmp=strchr(args[i], '-');
		if(tmp != NULL && tmp[1] == 'f')
			return args[i+1];
	}
	
	return NULL;
}

char* readLine(FILE* fd){
	char *str, *p, *tmp, *ret;
	
	str = calloc(UNIX_PATH_MAX+40, sizeof(char));
	tmp = str;
	
	do
	{
		if(fgets(str, UNIX_PATH_MAX+30, fd) == NULL)
		{
			errno = EIO;
			free(tmp);
			return NULL;
		}
	}while(str[0] == '#' || str[0] == '\n' || str[0] == ' ');
	
	str = strchr(str, '=');
	if(str == NULL)
	{
		errno = EINVAL;
		free(tmp);
		return NULL;
	}
	
	// rimuovo gli spazi
	do{str++;
	}while(str[0] == ' ');
	
	// cerco ed eventualmente rimuovo newline
	p = strchr(str, '\n');
	if(p != NULL) *p = '\0';
	
	ret = calloc(strlen(str)+1, sizeof(char));
	strcpy(ret, str);
	free(tmp);
	return ret;
}

void readConfig(FILE* fd, int *conf){
	int i;
	char* str, *tmp;
	
	str = calloc(UNIX_PATH_MAX, (sizeof(char)));
	
	for(i = 0; i < 5; i++)
	{
		do{
			if(fgets(str, UNIX_PATH_MAX, fd) == NULL){
				errno = EIO;
			}
		}while(str[0] == '#' || str[0] == '\n');
		tmp = strchr(str, '=');
		if(tmp != NULL){
			tmp++;
			conf[i] = atoi(tmp);
		}
		else i--;
	}
	free(str);
}

void cleaninFun(void* arg){
	free(arg);
}

void cleaninData(void* arg){
	message_data_t *tmp = arg;
	free(tmp->buf);
	free(tmp);
}

/**
 * @function selectorOP
 * @brief deals with whatever operation the machine asked for
 * 
 * @param msg	puntatore al messaggio gia' spacchettato 
 * @param socID ID del socket sul quale sono connesso
 */
void selectorOP(message_t *msg, int socID, unsigned int oldop){
	int err, result = 0;
	unsigned int *newkey;
    message_data_t *newdata, *olddata = NULL;
    
	do
	{
		if((err = pthread_mutex_lock(&dataMUTEX)) == 0)
		{
			if(replock != socID && replock != -1)
			{
				msg->hdr.op = OP_LOCKED;
				result = 1;
			}
			pthread_mutex_unlock(&dataMUTEX);
		}
	}
	while(err != 0);
	
	switch(msg->hdr.op)
	{
		case PUT_OP:
		{
			if(maxobjsize != 0 && msg->data.len > maxobjsize)
			{					
				msg->hdr.op = OP_PUT_SIZE;
				result = 1;
			}
			else if(storagebyte != 0 && msg->data.len + mboxStats.current_size > storagebyte){
				msg->hdr.op = OP_PUT_REPOSIZE;
				result = 1;
			}
			else if(storagesize != 0 && mboxStats.current_objects == storagesize){
				msg->hdr.op = OP_PUT_TOOMANY;
				result = 1;
			}
			if(result != 1)
			{
				newkey = calloc(1, sizeof(unsigned int*));
				*newkey = msg->hdr.key;
				newdata = calloc(1, sizeof(message_data_t));
				newdata->len = msg->data.len;
				newdata->buf = calloc(msg->data.len, sizeof(char));
				memcpy(newdata->buf, msg->data.buf, sizeof(char)*(msg->data.len));
				do
				{
					if((err = pthread_mutex_lock(&dataMUTEX)) == 0)
					{
						if((icl_hash_insert(dataTable, newkey, (void*)newdata)) == NULL)
						{
							pthread_mutex_unlock(&dataMUTEX);
							// dato che non posso modificare le funzioni in icl_hash, cerco di nuovo
							if(icl_hash_find(dataTable, newkey) != NULL)
								msg->hdr.op = OP_PUT_ALREADY;
							else
								// not enough memory
								msg->hdr.op = OP_FAIL; 
							result = 1;
							free(newdata->buf);
							free(newdata);
							free(newkey);
						}
						else
						{
							pthread_mutex_unlock(&dataMUTEX);
							msg->hdr.op = OP_OK;
							result = 0;
							printf("ho inserito\n");
						}
						
					}
				}
				while(err != 0);
			}
		}
		break;
		case UPDATE_OP:
		{
			newkey = calloc(1, sizeof(unsigned int*));
			*newkey = msg->hdr.key;
			newdata = calloc(1, sizeof(message_data_t));
			newdata->len = msg->data.len;
			newdata->buf = calloc(msg->data.len, sizeof(char));
			memcpy(newdata->buf, msg->data.buf, sizeof(char)*(msg->data.len));
			do
			{
				if((err = pthread_mutex_lock(&dataMUTEX)) == 0)
				{
					if((olddata = icl_hash_find(dataTable, &msg->hdr.key)) != NULL)
					{
						if(olddata->len == msg->data.len)
						{
							icl_hash_delete(dataTable, &msg->hdr.key, cleaninFun, cleaninData);
							icl_hash_insert(dataTable, newkey, newdata);
							pthread_mutex_unlock(&dataMUTEX);
							msg->hdr.op = OP_OK;
							result = 0;
						}
						else
						{
							pthread_mutex_unlock(&dataMUTEX);
							msg->hdr.op = OP_UPDATE_SIZE;
							free(newdata->buf);
							free(newdata);
							free(newkey);
							result = 1;
						}
					}
					else
					{
						pthread_mutex_unlock(&dataMUTEX);
						msg->hdr.op = OP_UPDATE_NONE;
						free(newdata->buf);
						free(newdata);
						free(newkey);
						result = 1;
					}
				}
			}
			while(err != 0);
		}
		break;
		case GET_OP: 
		{
			do
			{
				if((err = pthread_mutex_lock(&dataMUTEX)) == 0)
				{
					if((olddata = icl_hash_find(dataTable, &msg->hdr.key)) == NULL)
					{
						msg->hdr.op = OP_GET_NONE;
						result = 1;
					}
					else
					{
						msg->hdr.op = OP_OK;
						msg->data.buf = olddata->buf;
						result = 0;
					}
					pthread_mutex_unlock(&dataMUTEX);
				 }
			}
			while(err != 0);
		}
		break;
		//	REMOVE_OP       = 3,   /// eliminazione di un oggetto dal repository
		case REMOVE_OP:
		{
			do
			{		
				if((err = pthread_mutex_lock(&dataMUTEX)) == 0)
				{	
					if((icl_hash_delete(dataTable, &msg->hdr.key, cleaninFun, cleaninData)) == -1)
					{
						pthread_mutex_unlock(&dataMUTEX);
						msg->hdr.op = OP_REMOVE_NONE;
						result = 1;
					}
					else
					{
						pthread_mutex_unlock(&dataMUTEX);
						msg->hdr.op = OP_OK;
						result = 0;
					}
				}	
			}
			while(err != 0);
		}
		// 	LOCK_OP         = 4,   /// acquisizione di una lock su tutto il repository
		break;
		case LOCK_OP: 
		{
			do
			{
				if((err = pthread_mutex_lock(&dataMUTEX)) == 0)
				{
					replock = socID;
					pthread_mutex_unlock(&dataMUTEX);
					msg->hdr.op = OP_OK;
					result = 0;
				}
			}
			while(err != 0);
		}
		// rilascio della lock su tutto il repository
		break;
		case UNLOCK_OP: 
		{
			do
			{
				if((err = pthread_mutex_lock(&dataMUTEX)) == 0)
				{
					if(replock != -1)
					{
						replock = -1;
						pthread_mutex_unlock(&dataMUTEX);
						result = 0;
						msg->hdr.op = OP_OK;
					}
					else
					{
						pthread_mutex_unlock(&dataMUTEX);
						msg->hdr.op = OP_LOCK_NONE;
						result = 1;
					}
				}
			}
			while(err != 0);
		}
		// Invalid OP code
		break;
		default: 
		{
			perror("OP not recognized.\n");
			exit(EXIT_FAILURE);
		}
	}
	// switch end
	do
	{
		if((err = pthread_mutex_lock(&stCO)) == 0)
		{
			statOP(result, oldop, msg->data.len);
			pthread_mutex_unlock(&stCO);
		}
	}
	while(err != 0);
}
/*
void selectorOP(message_t *msg, int socID, int thrdnumber){
	message_t* reply;
	unsigned int *newkey;
    message_data_t *newdata, *olddata = NULL;
	int result = 1, err = -1;
	
	printf("[selectorOP] msg->hdr.key: %u\n", (unsigned int)msg->hdr.key);
	do
	{
		if((err = pthread_mutex_lock(&dataMUTEX)) == 0)
		{
			if(replock != socID && replock != -1)
			{
				msg->hdr.op = OP_LOCKED;
				result = 1;
			}
			else
			{
				switch(msg->hdr.op)
				{
					// inserimento di un oggetto nel repository
					case PUT_OP:
					{
						newkey = calloc(1, sizeof(unsigned int*));
						*newkey = msg->hdr.key;
						newdata = calloc(1, sizeof(message_data_t));
						newdata->len = msg->data.len;
						newdata->buf = calloc(msg->data.len, sizeof(char));
						memcpy(newdata->buf, msg->data.buf, sizeof(char)*(msg->data.len));
						
						if(maxobjsize != 0 && msg->data.len > maxobjsize)
						{					
							msg->hdr.op = OP_PUT_SIZE;
							result = 1;
							free(newdata->buf);
							free(newdata);
							free(newkey);
						}
						else if(storagebyte != 0 && msg->data.len + mboxStats.current_size > storagebyte){
							msg->hdr.op = OP_PUT_REPOSIZE;
							result = 1;
							free(newdata->buf);
							free(newdata);
							free(newkey);
						}
						else if(storagesize != 0 && mboxStats.current_objects == storagesize){
							msg->hdr.op = OP_PUT_TOOMANY;
							result = 1;
							free(newdata->buf);
							free(newdata);
							free(newkey);
						}
						else if((icl_hash_insert(dataTable, newkey, (void*)newdata)) == NULL)
						{
							// dato che non posso modificare le funzioni in icl_hash, cerco di nuovo
							if(icl_hash_find(dataTable, newkey) != NULL)
								msg->hdr.op = OP_PUT_ALREADY;
							else
								msg->hdr.op = OP_FAIL; // not enough memory
							result = 1;
							free(newdata->buf);
							free(newdata);
							free(newkey);
						}
						else
						{
							msg->hdr.op = OP_OK;
							result = 0;
							printf("ho inserito\n");
						}
					}
					break;
					// aggiornamento di un oggetto gia' presente
					case UPDATE_OP: 
					{
						newkey = calloc(1, sizeof(unsigned int*));
						*newkey = msg->hdr.key;
						newdata->len = msg->data.len;
						newdata = calloc(1, sizeof(message_data_t));
						newdata->buf = calloc(msg->data.len, sizeof(char));
						memcpy(newdata->buf, msg->data.buf, sizeof(char)*(msg->data.len));
						//find old entry
						//compare len dei due data
						//replace if ==
						printf("[selectorOP] UPDATE_OP for data of len: %d\n", msg->data.len);
						olddata = icl_hash_find(dataTable, &msg->hdr.key);
						if(olddata != NULL)
						{
							if(olddata->len == msg->data.len)
							{
								icl_hash_delete(dataTable, &msg->hdr.key, cleaninFun, cleaninData);
								icl_hash_insert(dataTable, newkey, newdata);
								msg->hdr.op = OP_OK;
								result = 0;
							}
							else
							{
								msg->hdr.op = OP_UPDATE_SIZE;
								free(newdata->buf);
								free(newdata);
								free(newkey);
								result = 1;
							}
						}
						else
						{
							msg->hdr.op = OP_UPDATE_NONE;
							free(newdata->buf);
							free(newdata);
							free(newkey);
							result = 1;
						}
					}
					break;
					// recupero di un oggetto dal repository
					case GET_OP: 
					{
						if((olddata = icl_hash_find(dataTable, newkey)) == NULL)
						{
							msg->hdr.op = OP_GET_NONE;
							result = 1;
						}
						else
						{
							msg->hdr.op = OP_OK;
							msg->data.buf = olddata->buf;
							result = 0;
						}
					}
					break;
					//	REMOVE_OP       = 3,   /// eliminazione di un oggetto dal repository
					case REMOVE_OP:
					{
						if((icl_hash_delete(dataTable, &msg->hdr.key, cleaninFun, cleaninData)) == -1)
						{
							msg->hdr.op = OP_REMOVE_NONE;
							result = 1;
						}
						else
						{
							msg->hdr.op = OP_OK;
							result = 0;
						}
					}
					// 	LOCK_OP         = 4,   /// acquisizione di una lock su tutto il repository
					break;
					case LOCK_OP: 
					{
						replock = socID;
						msg->hdr.op = OP_OK;
						result = 0;
					}
					// rilascio della lock su tutto il repository
					break;
					case UNLOCK_OP: 
					{
						if(replock != -1)
						{
							replock = -1;
							result = 0;
							msg->hdr.op = OP_OK;
						}
						else
						{
							msg->hdr.op = OP_LOCK_NONE;
							result = 1;
						}
					}
					// Invalid OP code
					break;
					default: 
					{
						perror("OP not recognized.\n");
						exit(EXIT_FAILURE);
					}
				}
			}
			pthread_mutex_unlock(&dataMUTEX);
		}
	}
	while(err != 0);
	err = -1;
	do
	{
		if((err = pthread_mutex_lock(&stCO)) == 0)
		{
			statOP(result, msg->hdr.op, msg->data.len);
			pthread_mutex_unlock(&stCO);
		}
	}
	while(err != 0);
	printf("sto chiudendo selectorOP\n");
}

*/
/*
message_t* selectorOP(message_t *msg, int socID, int thrdnumber){
	message_t* reply;
	unsigned int *newkey;
    char *newdata, *olddata = NULL;
	int result = 1, err = -1;
	
	reply = calloc(1, sizeof(message_t));
	reply->hdr.key = msg->hdr.key;
	reply->data.len = msg->data.len;
	reply->data.buf = msg->data.buf;
	
	// alloco/setto le variabili da passare per l'ashing
	//TODO: NEED TO FIND A WAY TO FREE THEM!!!
	
	do
	{
		if((err = pthread_mutex_lock(&dataMUTEX)) == 0)
		{
			printf("thread %d locked mutex on dataTable\n", thrdnumber);
			if(replock != socID && replock != -1)
			{
				reply->hdr.op = OP_LOCKED;
				result = 1;
			}
			else
			{
				switch(msg->hdr.op)
				{
					// inserimento di un oggetto nel repository
					case PUT_OP:
					{
						newkey = calloc(1, sizeof(unsigned int*));
						*newkey = msg->hdr.key;
						newdata = calloc(msg->data.len+1, sizeof(char));
						memcpy(newdata, msg->data.buf, sizeof(char)*(msg->data.len));
						
						if(maxobjsize != 0 && msg->data.len > maxobjsize)
						{					
							reply->hdr.op = OP_PUT_SIZE;
							result = 1;
						}
						else if(storagebyte != 0 && msg->data.len + mboxStats.current_size > storagebyte){
							reply->hdr.op = OP_PUT_REPOSIZE;
							result = 1;
						}
						else if(storagesize != 0 && mboxStats.current_objects == storagesize){
							reply->hdr.op = OP_PUT_TOOMANY;
							result = 1;
						}
						else if((icl_hash_insert(dataTable, newkey, (void*)newdata)) == NULL)
						{
							// dato che non posso modificare le funzioni in icl_hash, cerco di nuovo
							if(icl_hash_find(dataTable, newkey) != NULL)
								reply->hdr.op = OP_PUT_ALREADY;
							else
								reply->hdr.op = OP_FAIL; // not enough memory
							result = 1;
						}
						else
						{
							reply->hdr.op = OP_OK;
							printf("strlen data inserted: %s\n", (int)strlen(newdata));
							result = 0;
						}
					}
					break;
					// aggiornamento di un oggetto gia' presente
					case UPDATE_OP: 
					{
						newkey = calloc(1, sizeof(unsigned int*));
						*newkey = msg->hdr.key;
						newdata = calloc(msg->data.len+1, sizeof(char));
						memcpy(newdata, msg->data.buf, sizeof(char)*(msg->data.len));
						//find old entry
						//compare len dei due data
						//replace if ==
						olddata = icl_hash_find(dataTable, &msg->hdr.key);
						if(olddata != NULL)
						{
							printf("olddata len: %u\n", (unsigned int)strlen(olddata));
							printf("olddata sizeof: %d\n", sizeof(olddata));
							if((unsigned int)strlen(olddata) == msg->data.len)
							{
								icl_hash_delete(dataTable, &msg->hdr.key, cleaninFun, cleaninFun);
								icl_hash_insert(dataTable, newkey, newdata);
								reply->hdr.op = OP_OK;
								result = 0;
							}
							else
							{
								reply->hdr.op = OP_UPDATE_SIZE;
								free(newdata);
								free(newkey);
								result = 1;
							}
						}
						else
						{
							reply->hdr.op = OP_UPDATE_NONE;
							free(newdata);
							free(newkey);
							result = 1;
						}
					}
					break;
					// recupero di un oggetto dal repository
					case GET_OP: 
					{
						if((olddata = icl_hash_find(dataTable, newkey)) == NULL)
						{
							reply->hdr.op = OP_GET_NONE;
							result = 1;
						}
						else
						{
							reply->hdr.op = OP_OK;
							reply->data.buf = olddata;
							result = 0;
						}
					}
					break;
					//	REMOVE_OP       = 3,   /// eliminazione di un oggetto dal repository
					case REMOVE_OP:
					{
						if((icl_hash_delete(dataTable, &msg->hdr.key, cleaninFun, cleaninFun)) == -1)
						{
							reply->hdr.op = OP_REMOVE_NONE;
							result = 1;
						}
						else
						{
							reply->hdr.op = OP_OK;
							result = 0;
						}
					}
					// 	LOCK_OP         = 4,   /// acquisizione di una lock su tutto il repository
					break;
					case LOCK_OP: 
					{
						replock = socID;
						reply->hdr.op = OP_OK;
						result = 0;
					}
					// rilascio della lock su tutto il repository
					break;
					case UNLOCK_OP: 
					{
						if(replock != -1)
						{
							replock = -1;
							result = 0;
							reply->hdr.op = OP_OK;
						}
						else
						{
							reply->hdr.op = OP_LOCK_NONE;
							result = 1;
						}
					}
					// Invalid OP code
					break;
					default: 
					{
						perror("OP not recognized.\n");
						exit(EXIT_FAILURE);
					}
				}
			}
			pthread_mutex_unlock(&dataMUTEX);
			printf("thread %d unlocked mutex on dataTable\n", thrdnumber);
		}
		else
		{
			pthread_mutex_unlock(&dataMUTEX);
			printf("thread %d unlocked mutex on dataTable\n", thrdnumber);
		}
	}
	while(err != 0);
	err = -1;
	do
	{
		if((err = pthread_mutex_lock(&stCO)) == 0)
		{
			statOP(result, msg->hdr.op, msg->data.len);
			pthread_mutex_unlock(&stCO);
		}
		else
			pthread_mutex_unlock(&stCO);
	}
	while(err != 0);
	return reply;
}
*/


void printMsg(message_t* msg, int thrd){
	//printf("==============THREAD#%d==============\nHeader OP: %d\nHeader Key: %li\nData Len: %u\nData Payload: %s\n", thrd, msg->hdr.op, msg->hdr.key, msg->data.len, msg->data.buf); 
	printf("msg number %li to thread %d success\n", msg->hdr.key, thrd);
	fflush(stdout);
}

void printList(listSimple* head){
	listSimple* curr = head;
	printf("\t\n");
	for(;curr != NULL; curr = curr->next)
	{
		printf("%d -> ", curr->sokAddr);
	}
	printf("\nqueueLength: %d", queueLength);
}

int initActivity(int flag){
	int err = -1, thrdnumber;
	do
	{
		if((err = pthread_mutex_lock(&actvth)) == 0)
		{
			activethreads+=flag;
			if(flag == 1)
			{
				thrdnumber = activethreads;
				printf("I'm thread %d\n",thrdnumber); 
			}
			pthread_mutex_unlock(&actvth);
		}
	}
	while(err != 0);
	return thrdnumber;
}

void* dealmaker (void* args){
	int err, err2, soktAcc, connected = 1, thrdnumber = (intptr_t) args;
	message_t *messg;
	listSimple *tmp;
	op_t tmpop;
	
	initActivity(1);
	
	while(overlord)
	{
		do
		{
			if((err = pthread_mutex_lock(&coQU)) == 0)
			{
				while(queueLength == 0)
					pthread_cond_wait(&coQUwait, &coQU);
				soktAcc = head->sokAddr;
				tmp = head;
				head = head->next;
				free(tmp);
				queueLength--;
				pthread_mutex_unlock(&coQU);
			}
			pthread_mutex_unlock(&coQU);
		}
		while(err != 0);
		do
		{
			if((err2 = pthread_mutex_lock(&stCO)) == 0)
			{
				if(statConnections(0) != 0)
				{
					printf("%s\n", strerror(errno));
					exit(EXIT_FAILURE);
				}
				pthread_mutex_unlock(&stCO);
			}
		}
		while(err2 != 0);
		while(connected)
		{
			if((messg = calloc(1, sizeof(message_t))) == NULL)
				{
					errno = ENOMEM;
					printf("Can't allocate memory for receiver\n");
					exit(EXIT_FAILURE);
				}
			printf("[dealmaker%d] messg alloc'd\n", thrdnumber);
			if(readHeader(soktAcc, &messg->hdr) < 0) 
				break;
			if(readData(soktAcc, &messg->data) < 0) 
				break;
			printf("[dealmaker%d] msg->hdr.op: %u\t msg->data.len: %u\n", thrdnumber, messg->hdr.op, messg->data.len);
			tmpop = messg->hdr.op;
			selectorOP(messg, soktAcc, tmpop);
			if(sendReply(tmpop, messg, soktAcc) != 0)
					printf("[dealmaker%d] sendReply returned failure state\n", thrdnumber);
			//free(messg->data.buf);
			//free(messg);
			printf("[dealmaker%d] messg free'd\n", thrdnumber);
		}
		if(replock == soktAcc) replock = -1;
		do
		{
			if((err2 = pthread_mutex_lock(&stCO)) == 0)
			{
				if(statConnections(1) != 0)
				{
					printf("%s\n", strerror(errno));
					exit(EXIT_FAILURE);
				}
				pthread_mutex_unlock(&stCO);
			}
		}
		while(err2 != 0);
		printLog();
	}
	initActivity(-1);
	pthread_exit(NULL);
}

/*
void *dealmaker(void* arg){
	int err, quit, thrdnumber, soktAcc, socID = (intptr_t) arg;
	int err2;
	listSimple* tmp;
	message_t* receiver, *reply;
	
	//lock e aggiungo al numero di thread attivi
	thrdnumber = initActivity(1);
	
	while(1)
	{
		do
		{
			err = -1;
			if(queueLength > 0 && (err = pthread_mutex_lock(&coQU)) == 0)
			{
				printf("thread %d unlocked mutex on coQU\n", thrdnumber);
				while(queueLength == 0)
				{
					printf("thread %d waits on coQUwait\n", thrdnumber);
					pthread_cond_wait(&coQUwait, &coQU);
				}
				
				// altrimenti opero sulla testa della coda
				soktAcc = head->sokAddr;
				tmp = head;
				head = head->next;
				free(tmp);
				queueLength--;
				
				//rilascio la mutex sulla coda
				pthread_mutex_unlock(&coQU);
				printf("thread %d unlocked mutex on coQU\n", thrdnumber);
			}
		}
		while(err != 0);
		// controllo d'aver preso un socket valido
		if(soktAcc > 0)
		{
			// appena accetto una connessione faccio la mutex per aggiornare mboxStats
			err2 = -1;
			do
			{
				if((err2 = pthread_mutex_lock(&stCO)) == 0)
				{
					printf("thread %d locked mutex on stCO\n", thrdnumber);
					if(statConnections(0) != 0)
					{
						printf("%s\n", strerror(errno));
						exit(EXIT_FAILURE);
					}
					pthread_mutex_unlock(&stCO);
					printf("thread %d unlocked mutex on stCO\n", thrdnumber);
				}
			}
			while(err2 != 0);
				
			// mi metto in un loop per leggere il socket aperto da questo client
			quit = 0;
			while(quit != 1)
			{
				// alloco la struttura dati che riceve il messaggio
				if((receiver = calloc(1, sizeof(message_t))) == NULL)
				{
					errno = ENOMEM;
					printf("Can't allocate memory for receiver\n");
					exit(EXIT_FAILURE);
				}
				
				// Leggo quello che il client mi scrive
				if(readHeader(soktAcc, &receiver->hdr)!=0)
				{
					quit = 1;
				}
				if(readData(soktAcc, &receiver->data)!=0)
				{
					quit = 1;
				}
					
				// mando il messaggio a selectorOP che si occupa del resto
				printf("quit: %d\n");
				if(quit != 1)
				{
					if((reply = selectorOP(receiver, soktAcc, thrdnumber)) == NULL && quit != 0)
					{
						perror("selectorOP fluked\n");
						exit(EXIT_FAILURE);
					}
					else
					{
						printf("return value of sendrequest is: %d\n", sendRequest(soktAcc, reply));
						printf("reply->hdr.op: %u\treply->data.len: %u\n", (unsigned int)reply->hdr.op, reply->data.len);
						fflush(stdout);
						//printf("Reply->hdr.op: %u\tkey: %u\tlen: %d\t data: %s\n", reply->hdr.op, reply->hdr.key, reply->data.len, reply->data.buf);
						free(reply);
						printMsg(receiver, thrdnumber);
					}
				}
				free(receiver->data.buf);
				free(receiver);
			}					
			// elimino eventuale lock della repository creata da questo client
			if(replock == soktAcc) replock = -1;
			
			// chiudo il socket e rimuovo la connessione dalle attive
			err = -1;
			do
			{
				if((err = pthread_mutex_lock(&stCO)) == 0)
				{
					printf("thread %d locked mutex on stCO\n", thrdnumber);
					close(soktAcc);
					statConnections(1);
					pthread_mutex_unlock(&stCO);
					printf("thread %d unlocked mutex on stCO\n", thrdnumber);
				}
			}
			while(err != 0);
		}
		//TODO: proper error handling here
		else
		{
			sleep(1);
		}
	}
	
	initActivity(-1);
	pthread_exit(NULL);
}
*/

void* dispatcher(void* args){
	int socID = (intptr_t) args, err3;
	int err = -1;
	while(overlord)
	{
		if((err = pthread_mutex_lock(&coQU)) == 0)
		{
			//printf("Producer has mutex\n");
			if(queueLength + mboxStats.concurrent_connections < maxconnections)
			{
				pthread_mutex_unlock(&coQU);
				if((connectionQueue->sokAddr = accept(socID, NULL, 0)) != -1)
				{	
					printf("Producer has accepted a new socket!\n");
					err3 = -1;
					do
					{
						if((err3 = pthread_mutex_lock(&coQU)) == 0)
						{
							printf("Producer has coQU mutex to append socID to the queue\n");
							connectionQueue->next = calloc(1, sizeof(listSimple));
							queueLength++;
							connectionQueue = connectionQueue->next;
							connectionQueue->sokAddr = -1;
							connectionQueue->next = NULL;
							pthread_cond_signal(&coQUwait);
							pthread_mutex_unlock(&coQU);
							printf("producer broadcasted\n");
						}
					}
					while(err3 != 0);
				}
			}
			else
			{
				pthread_mutex_unlock(&coQU);
				printf("Producer released mutex because queue is full\n");
			}
		}
	}
	pthread_exit(NULL);
}

int main(int argc, char *argv[]) {
	int config[5], socID, err, i = 0;
	char *configfilepath;
	pthread_t* thrds, disp;
	FILE *fp;
	
	//TODO: mask signals
	// SIGUSR1: close all threads and then quit
	// SIGUSR2: stamp mboxStat to file if file exists
	
	// apro il file di configurazione
	configfilepath = readLocation(argv, argc);
	fp = fopen(configfilepath, "r");
	if(fp == NULL) 
	{
		errno = EIO;
		printf("server shutting down, no config file\n");
		return(-1);
    }
    
    // leggo il file di configurazione
    socketpath = readLine(fp);
    fflush(stdout);
    readConfig(fp, config);
    statfilepath = readLine(fp);
    fclose(fp);
    
    // assegno i valori dell'array config a variabili GLOBALI
    // gestisco bad config
    
    if((maxconnections = config[0]) < 1){perror("Bad config: Less than 1 connection allowed."); exit(EXIT_FAILURE);}
    if((threadsinpool = config[1]) < 1){perror("Bad config: Less than 1 thread allocated."); exit(EXIT_FAILURE);}
    if((storagesize = config[2]) < 0){perror("Bad config: Storagesize is negative."); exit(EXIT_FAILURE);}
    if((storagebyte = config[3]) < 0){perror("Bad config: Storagebyte is negative."); exit(EXIT_FAILURE);}
    if((maxobjsize = config[4]) < 0){perror("Bad config: MaxObjSize is negative."); exit(EXIT_FAILURE);}
    if(maxconnections < threadsinpool)
    {
		printf("WARNING: You have set more threads than are allowed connections. MaxConnections has been automatically set equal to ThreadsInPool.\n"); 
		maxconnections = threadsinpool;
	}
    
    //DEBUG: print all it read from config file.
    printf("%s\t%d\t%d\t%d\t%d\t%d\t%s\n", socketpath, config[0], config[1], config[2], config[3], config[4], statfilepath);
    
    // array dove salvo i pid dei thread
    if((thrds = calloc(threadsinpool, sizeof(pthread_t))) == NULL)
    {
		errno = ENOMEM;
		printf("%s\n", strerror(errno));
		return -1;
	}
    printf("thrds alloc'd\n");
	// array dove salvo le connessioni in attesa
	if((connectionQueue = calloc(1, sizeof(listSimple))) == NULL)
	{
		errno = ENOMEM;
		printf("%s\n", strerror(errno));
		return -1;
	}
	connectionQueue->sokAddr = -1;
	connectionQueue->next = NULL;
	head = connectionQueue;
	printf("connectionQueue alloc'd\n");
	
    // alloco la struttura dati d'hash condivisa
    if((dataTable = icl_hash_create(1087, ulong_hash_function, ulong_key_compare)) == NULL)
    {
		errno = ENOMEM;
		printf("%s\n", strerror(errno));
		return -1;
	}
    printf("dataTable alloc'd\n");
	
    // apro il file di log
    if(statfilepath != NULL)
		descriptr = fopen(statfilepath, "w+");
    printf("statfile open\n");    
    
    // creo il socket
    remove(socketpath);//printf("Cleaned up old Socket.\nPossible recovery after crash?\n");
    if((socID = startConnection(socketpath)) == -1)
	{
		printf("Couldn't create socket. Resource busy.\n");
		exit(EXIT_FAILURE);
	}
	else printf("Socket open.\n");
	
	// alloco il dispatcher
    if((err = pthread_create(&disp, NULL, dispatcher, (void*) (intptr_t) socID)) != 0)
    {
		perror("Unable to create dispatcher thread!\n");
		exit(EXIT_FAILURE);
	}
	
	// ALLOCATE THREAD POOL
	for(i = 0; i < threadsinpool; i++)
	{
		if((err = pthread_create(&thrds[i], NULL, dealmaker , (void*) (intptr_t) i)) != 0)
		{
			perror("Unable to create client thread!\n");
			exit(EXIT_FAILURE);
		}
	}
	
	//TODO: gently close all threads
	while(1)
		sleep(1);
	fclose(descriptr);
	//TODO: remove this block, statfile has to be printed only in case of SIGUSR2 signal
		
	icl_hash_destroy(dataTable, cleaninFun, cleaninData);
	free(statfilepath);
	free(socketpath);
	//TODO: JOIN THREADS, only memory leak left open
	free(thrds);
	
    return 0;
}
