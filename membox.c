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

#include </home/groot/Downloads/Git/Membox/connections.c> 
#include </home/groot/Downloads/Git/Membox/stats.h>
#include </home/groot/Downloads/Git/Membox/icl_hash.h>
#include </home/groot/Downloads/Git/Membox/test_hash.c>
#include </home/groot/Downloads/Git/Membox/icl_hash.c>
#include </home/groot/Downloads/Git/Membox/message.h> //TEST SU IDE RIMUOVERE PER IL MAKE
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h> 
#include <sys/un.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>

typedef struct listS{
	int sokAddr;
	struct listS* next;
}listSimple;

static char *socketpath, *statfilepath;
static int maxconnections, threadsinpool, storagesize, storagebyte, maxobjsize, activethreads = 0, queueLength = 0, count = 0;
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
				mboxStats.nget++;
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
			case PUT_OP: mboxStats.nget_failed++;
			break;
			case UPDATE_OP: mboxStats.nupdate_failed++;
			break;
			case LOCK_OP: mboxStats.nlock_failed++;
			break;
			case GET_OP: mboxStats.nget_failed++;
			break;
			case REMOVE_OP: mboxStats.nremove_failed++;
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
	}while(str[0] == '#');
	
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
		}while(str[0] == '#');
		tmp = strchr(str, '=');
		if(tmp == NULL){
			errno = EINVAL;
		}
		tmp++;
		conf[i] = atoi(tmp);
	}
}

/**
 * @function selectorOP
 * @brief deals with whatever operation the machine asked for
 * 
 * @param msg	puntatore al messaggio gia' spacchettato 
 */
message_t* selectorOP(message_t *msg, int socID, int* quit){
	message_t* reply;
	icl_entry_t* slot;
	char* tmp = NULL;
	int result = 1, err = -1;
	
	reply = calloc(1, sizeof(message_t));
	reply->hdr.key = msg->hdr.key;
	reply->data.len = msg->data.len;
	reply->data.buf = msg->data.buf;
	
	do
	{
		if((err = pthread_mutex_lock(&dataMUTEX)) == 0)
		{
			if(dataTable->lock != socID && dataTable->lock != -1)
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
						else if((icl_hash_insert(dataTable, &msg->hdr.key, &msg->data.len, &msg->data.buf)) == NULL)
						{
							reply->hdr.op = OP_FAIL;
							printf("couldn't allocate datatable space\n");
							result = 1;
						}
						else
						{
							reply->hdr.op = OP_OK;
							result = 0;
						}
					}
					break;
					// aggiornamento di un oggetto gia' presente
					case UPDATE_OP: 
					{
						if((icl_hash_update_insert(dataTable, &msg->hdr.key, &msg->data.len, &msg->data.buf, (void**) tmp)) == NULL)
						{
							reply->hdr.op = OP_UPDATE_SIZE;
							result = 1;
						}
						else
						{
							if(tmp != NULL)
							{
								reply->hdr.op = OP_OK;
								result = 0;
							}
							else
							{
								reply->hdr.op = OP_UPDATE_NONE;
								result = 0;
							}
						}
					}
					break;
					// recupero di un oggetto dal repository
					case GET_OP: 
					{
						if((slot = icl_hash_find(dataTable, (void*) msg->hdr.key)) == NULL)
						{
							reply->hdr.op = OP_GET_NONE;
							result = 1;
						}
						else
						{
							reply->hdr.op = OP_OK;
							reply->data.len = (intptr_t) slot->len;
							reply->data.buf = slot->data;
							result = 0;
						}
					}
					break;
					//	REMOVE_OP       = 3,   /// eliminazione di un oggetto dal repository
					case REMOVE_OP:
					{
						if((icl_hash_delete(dataTable, (void*) msg->hdr.key, *free, *free, *free)) == -1)
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
						dataTable->lock = socID;
						reply->hdr.op = OP_OK;
						result = 0;
					}
					// rilascio della lock su tutto il repository
					break;
					case UNLOCK_OP: 
					{
						if(dataTable->lock != -1)
						{
							dataTable->lock = -1;
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
		}
		else
		{
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
		else
			pthread_mutex_unlock(&stCO);
	}
	while(err != 0);
	return reply;
}

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

void *dealmaker(void* arg){
	int err, quit, thrdnumber, soktAcc, socID = (intptr_t) arg;
	int err2;
	listSimple* tmp;
	message_t* receiver, *reply;
	
	//lock e aggiungo al numero di thread attivi
	thrdnumber = initActivity(1);
		
	// alloco la struttura dati che riceve il messaggio
	if((receiver = calloc(1, sizeof(message_t))) == NULL)
	{
		errno = ENOMEM;
		exit(EXIT_FAILURE);
	}
	
	while(1)
	{
		do
		{
			err = -1;
			if(queueLength > 0 && (err = pthread_mutex_lock(&coQU)) == 0)
			{
				while(queueLength == 0)
				{
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
					if(statConnections(0) != 0)
					{
						printf("%s\n", strerror(errno));
						exit(EXIT_FAILURE);
					}
					pthread_mutex_unlock(&stCO);
				}
			}
			while(err2 != 0);
				
			// mi metto in un loop per leggere il socket aperto da questo client
			quit = 0;
			while(quit != 1)
			{
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
				if(quit != 1)
				{
					if((reply = selectorOP(receiver, soktAcc, &quit)) == NULL && quit != 0)
					{
						perror("selectorOP fluked\n");
						exit(EXIT_FAILURE);
					}
					else
					{
						sendRequest(socID, reply);
						printMsg(receiver, thrdnumber);
					}
					sleep(1);
				}
			}					
			// elimino eventuale lock della repository creata da questo client
			if(dataTable->lock == soktAcc) dataTable->lock = -1;
			
			// chiudo il socket e rimuovo la connessione dalle attive
			err = -1;
			do
			{
				if((err = pthread_mutex_lock(&stCO)) == 0)
				{
					close(soktAcc);
					statConnections(1);
					pthread_mutex_unlock(&stCO);
				}
			}
			while(err != 0);
		}
		//TODO: proper error handling here
		else
		{
			err = count;
			err++;
			printf("Queue socket reading error in thread %d at iteration %d\n", thrdnumber, err);
			fflush(stdout);
			exit(EXIT_FAILURE);
		}
	}
	
	initActivity(-1);
	pthread_exit(NULL);
}

void* dispatcher(void* args){
	int socID = (intptr_t) args, err3;
	int err = -1;
	while(1)
	{
		if((err = pthread_mutex_lock(&coQU)) == 0)
		{
			if(queueLength + mboxStats.concurrent_connections < maxconnections)
			{
				pthread_mutex_unlock(&coQU);
				if((connectionQueue->sokAddr = accept(socID, NULL, 0)) != -1)
				{	
					err3 = -1;
					do
					{
						if((err3 = pthread_mutex_lock(&coQU)) == 0)
						{
							connectionQueue->next = calloc(1, sizeof(listSimple));
							queueLength++;
							connectionQueue = connectionQueue->next;
							connectionQueue->sokAddr = -1;
							connectionQueue->next = NULL;
							pthread_cond_signal(&coQUwait);
							pthread_mutex_unlock(&coQU);
						}
					}
					while(err3 != 0);
				}
			}
			else
				pthread_mutex_unlock(&coQU);
		}
	}
}

int main(int argc, char *argv[]) {
	int config[5], socID, err, err2, i = 0;
	pthread_t* thrds, disp;
	FILE *fp;
	
	// apro il file di configurazione
	fp = fopen("config.txt", "r");
	if(fp == NULL) 
	{
		errno = EIO;
		return(-1);
    }
    
    // leggo il file di configurazione
    socketpath = readLine(fp);
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
    
    // array dove salvo i pid dei thread
    if((thrds = calloc(threadsinpool, sizeof(pthread_t))) == NULL)
    {
		errno = ENOMEM;
		return -1;
	}
    
	// array dove salvo le connessioni in attesa
	if((connectionQueue = calloc(1, sizeof(listSimple))) == NULL)
	{
		errno = ENOMEM;
		return -1;
	}
	connectionQueue->sokAddr = -1;
	connectionQueue->next = NULL;
	head = connectionQueue;
	
    // alloco la struttura dati d'hash condivisa
    if((dataTable = icl_hash_create(maxconnections, &ulong_hash_function, &ulong_key_compare)) == NULL)
    {
		errno = ENOMEM;
		return -1;
	}
    
    // apro il file di log
    descriptr = fopen(statfilepath, "w+");
        
    // creo il socket
    if(remove(socketpath) == 0)printf("Cleaned up old Socket.\nPossible recovery after crash?\n");
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
		if((err = pthread_create(&thrds[i], NULL, dealmaker , (void*) (intptr_t) socID)) != 0)
		{
			perror("Unable to create client thread!\n");
			exit(EXIT_FAILURE);
		}
	}
	
	getchar();
	err2 = -1;
	printList(head);
	do
		{
		if((err2 = pthread_mutex_lock(&stCO)) == 0)
			printStats(descriptr);
		pthread_mutex_unlock(&stCO);
	}
	while(err2 != 0);
	fclose(descriptr);
	
	getchar();
	
	
	
    return 0;
}
