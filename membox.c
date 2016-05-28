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
// Struttura dati condivisa
static icl_hash_t* dataTable;
static listSimple* connectionQueue, *head;
// mutex for active threads
static pthread_mutex_t actvth = PTHREAD_MUTEX_INITIALIZER;
// mutex to be called before a mboxStats update
static pthread_mutex_t stOP = PTHREAD_MUTEX_INITIALIZER, stCO = PTHREAD_MUTEX_INITIALIZER, stST = PTHREAD_MUTEX_INITIALIZER;
// mutex for connectionQueue
static pthread_mutex_t coQU = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t coQUwait;
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
 */
int statOP(int opResult, op_t op){
	if(opResult == 0)
	{
		switch(op)
		{
			case PUT_OP: mboxStats.nget++;
			case UPDATE_OP: mboxStats.nupdate++;
			case LOCK_OP: mboxStats.nlock++;
			case GET_OP: mboxStats.nget++;
			case REMOVE_OP: mboxStats.nremove++;
			default: return -1;
		}
	}
	else
	{
		switch(op)
		{
			case PUT_OP: mboxStats.nget_failed++;
			case UPDATE_OP: mboxStats.nupdate_failed++;
			case LOCK_OP: mboxStats.nlock_failed++;
			case GET_OP: mboxStats.nget_failed++;
			case REMOVE_OP: mboxStats.nremove_failed++;
			default: return -1;
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
	char *str, *p;
	
	str = calloc(UNIX_PATH_MAX+40, sizeof(char));
	
	do
	{
		if(fgets(str, UNIX_PATH_MAX+30, fd) == NULL)
		{
			errno = EIO;
			return NULL;
		}
	}while(str[0] == '#');
	
	str = strchr(str, '=');
	if(str == NULL)
	{
		errno = EINVAL;
		return NULL;
	}
	
	// rimuovo gli spazi
	do{str++;
	}while(str[0] == ' ');
	
	// cerco ed eventualmente rimuovo newline
	p = strchr(str, '\n');
	if(p != NULL) *p = '\0';
	
	return str;
}

int* readConfig(FILE* fd){
	int i;
	int *conf;
	char* str;
	
	conf = calloc(5, sizeof(int));
	str = calloc(UNIX_PATH_MAX, sizeof(char));
	for(i = 0; i < 5; i++){
		do{
			if(fgets(str, UNIX_PATH_MAX, fd) == NULL){
				errno = EIO;
				free(conf);
				return NULL;
			}
		}while(str[0] == '#');
		
		str = strchr(str, '=');
		if(str == NULL){
			errno = EINVAL;
			free(conf);
			return NULL;
		}
		str++;
		conf[i] = atoi(str);
	}
	
	return conf;
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
	int result = -1; //TODO: set result = 0 if op success, -1 if failure
	
	//TODO: allocare reply
	reply = (message_t*)malloc(sizeof(message_t));
	
	if(msg->hdr.op == END_OP)
	{
		// rimuovo eventuali lock sulla repository
		if(dataTable->lock == socID) dataTable->lock = -1;
		quit = 0;
		return NULL;
	}
	if(dataTable->lock != socID || dataTable->lock != -1)
	{
		//TODO: tell client repository is locked and return
	}
	switch(msg->hdr.op)
	{
		//	PUT_OP          = 0,   /// inserimento di un oggetto nel repository
		case PUT_OP:
		{
			if((slot = icl_hash_insert(dataTable, (void*) (intptr_t) msg->hdr.key, (void*) (intptr_t) msg->data.len, (void*) (intptr_t) msg->data.buf)) == NULL)
			{
				//TODO: key esiste già, mandare errore a client
			}
			else
			{
				//TODO: dire al client che ho inserito
			}
		}
		//	UPDATE_OP       = 1,   /// aggiornamento di un oggetto gia' presente
		case UPDATE_OP: 
		{
			if((slot = icl_hash_update_insert(dataTable, (void*) (intptr_t) msg->hdr.key, (void*) (intptr_t) msg->data.len, (void*) (intptr_t) msg->data.buf, (void**) tmp)) == NULL)
			{
				//TODO: dire al client che l'elemento non combacia!
			}
			else
			{
				if(tmp != NULL)
				{
					//TODO: dire al client che ho rimpiazzato tmp?
				}
				else
				{
					//TODO: dire al client che ho inserito da zero
				}
			}
		}
		//	GET_OP          = 2,   /// recupero di un oggetto dal repository
		case GET_OP: 
		{
			if((slot = icl_hash_find(dataTable, (void*) msg->hdr.key)) == NULL)
			{
				//TODO: message not found
			}
			else
			{
				//TODO: send the data to client
			}
		}
		//	REMOVE_OP       = 3,   /// eliminazione di un oggetto dal repository
		case REMOVE_OP:
		{
			if((icl_hash_delete(dataTable, (void*) msg->hdr.key, *free, *free, *free)) == -1)
			{
				//TODO: couldn't find/remove the entry
			}
			else
			{
				//TODO: send all green
			}
		}
		// 	LOCK_OP         = 4,   /// acquisizione di una lock su tutto il repository
		case LOCK_OP: 
		{
			dataTable->lock = socID;
			result = 0;
		}
		// 	UNLOCK_OP       = 5,   /// rilascio della lock su tutto il repository
		case UNLOCK_OP: 
		{
			dataTable->lock = -1;
			result = 0;
		}
		// Invalid OP code
		default: 
		{
			perror("OP not recognized.\n");
			exit(EXIT_FAILURE);
		}
	}
	statOP(result, msg->hdr.op);
	return reply;
}

void printMsg(message_t* msg, int thrd){
	//printf("==============THREAD#%d==============\nHeader OP: %d\nHeader Key: %d\nData Len: %lu\nData Payload: %s\n", thrd, msg->hdr.op, msg->hdr.key, msg->data.len, msg->data.buf); 
	printf("msg number %d to thread %d success\n", msg->hdr.key, thrd);
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
	}while(err != 0);
	return thrdnumber;
}

void *dealmaker(void* arg){
	int err, quit, thrdnumber, soktAcc, socketConfirmation, socID = (intptr_t) arg;
	int err1, err2, err3, check = 0;
	listSimple* tmp;
	message_t* receiver, reply;
	
	//lock e aggiungo al numero di thread attivi
	thrdnumber = initActivity(1);
	
	// alloco la struttura dati che riceve il messaggio
	if((receiver = (message_t*)malloc(sizeof(message_t))) == NULL)
	{
		errno = ENOMEM;
		exit(EXIT_FAILURE);
	}
	
	while(1)
	{
		// apro la variabile condivisa e leggo la sua length
		if(queueLength > 0)
		{
			// cerco di acquisire il lock sulla coda
			do
			{	
				err1 = -1;
				if((err1 = pthread_mutex_lock(&coQU)) == 0)
				{
					// se nel mentre la coda si è svuotata interrompo il loop
					// ricordandomi di rilasciare la mutex sulla coda
					if(queueLength <= 0)
					{
						pthread_mutex_unlock(&coQU);
						break;					
					}
					
					// altrimenti opero sulla testa della coda
					soktAcc = head->sokAddr;
					tmp = head;
					head = head->next;
					free(tmp);
					queueLength--;
					
					//rilascio la mutex sulla coda
					pthread_mutex_unlock(&coQU);
					
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
							//TODO: acquire mutex on repository
							err3 = -1;
							do
							{
								if((err3 = pthread_mutex_lock(&dataMUTEX)) == 0)
								{
									printMsg(receiver, thrdnumber);
									/*
									if((reply = selectorOP(receiver, soktAcc, &quit)) == NULL && quit != 0)
									{
										perror("selectorOP fluked\n");
										exit(EXIT_FAILURE);
									}
									else
									{
										//TODO: send reply to client
									}
									*/
									pthread_mutex_unlock(&dataMUTEX);
								}
							}
							while(err3 != 0);
						}					
						// elimino eventuale lock della repository creata da questo client
						if(dataTable->lock == soktAcc) dataTable->lock = -1;
						
						// chiudo il socket e rimuovo la connessione dalle attive
						err = -1;
						do
						{
							if((err = pthread_mutex_lock(&stCO)) == 0)
							{
								//pthread_cond_init(&stCOwait, NULL);
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
			}
			while(err1 != 0);
		}
		else
		{
			// se la coda è vuota si occupa lui d'ascoltare
			if((soktAcc = accept(socID, NULL, 0)) == -1)
			{
				errno = EIO;
				exit(EXIT_FAILURE);
			}
			else
			{
				// appena accetto una connessione faccio la mutex per aggiornare mboxStats
				err = -1;
				do
				{
					if((err = pthread_mutex_lock(&stCO)) == 0)
					{
						//pthread_cond_init(&stCOwait, NULL);
						if(statConnections(0) != 0)
						{
							printf("%s\n", strerror(errno))	;
							exit(EXIT_FAILURE);
						}
						printf("Completed statConnections+ thread %d\n", thrdnumber);
						fflush(stdout);
						pthread_mutex_unlock(&stCO);
					}
				}
				while(err != 0);
				check = 0;
				while(check == 0)
				{
					// Leggo quello che il client mi scrive
					
					//TODO: NON FALLISCE MAI E MANDA TUTTO IN LOOP
					if(readHeader(soktAcc, &receiver->hdr)!=0)
					{
						check = 1;
					}
					if(readData(soktAcc, &receiver->data)!=0)
					{
						check = 1;
					}
					
					// mando il messaggio a selectorOP che si occupa del resto
					//selectorOP(receiver);
					err3 = -1;
					do
					{
						if((err3 = pthread_mutex_lock(&dataMUTEX)) == 0)
						{
							printMsg(receiver, thrdnumber);
							/*
							if((reply = selectorOP(receiver, soktAcc, &quit)) == NULL && quit != 0)
							{
								perror("selectorOP fluked\n");
								exit(EXIT_FAILURE);
							}
							else
							{
								//TODO: send reply to client
							}
							*/
							pthread_mutex_unlock(&dataMUTEX);
						}
					}
					while(err3 != 0);
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
						//TODO: remove this print and flush
						printf("Completed statConnections- thread %d\n", thrdnumber);
						fflush(stdout);
						pthread_mutex_unlock(&stCO);
					}
				}
				while(err != 0);
				// TODO: BREAK se ricevo il segnale dall'utente
			}
		}
	}
	initActivity(-1);
	pthread_exit(NULL);
}

int main(int argc, char *argv[]) {
	int *config, socID, err, i = 0;
	pthread_t* thrds;
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
    config = readConfig(fp);
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
    free(config);
    
    // array dove salvo i pid dei thread
    if((thrds = calloc(threadsinpool, sizeof(pthread_t))) == NULL)
    {
		errno = ENOMEM;
		return -1;
	}
    
	// array dove salvo le connessioni in attesa
	if((connectionQueue = (listSimple*)malloc(sizeof(listSimple))) == NULL)
	{
		errno = ENOMEM;
		return -1;
	}
	connectionQueue->sokAddr = -1;
	connectionQueue->next = NULL;
	head = connectionQueue;
	
    // alloco la struttura dati d'hash condivisa
    if((dataTable = icl_hash_create(maxconnections*10, ulong_hash_function, ulong_key_compare)) == NULL)
    {
		errno = ENOMEM;
		return -1;
	}
        
    // creo il socket
    if(remove(socketpath) == 0)printf("Cleaned up old Socket.\nPossible recovery after crash?\n");
    if((socID = startConnection(socketpath)) == -1)
	{
		printf("Couldn't create socket. Resource busy.\n");
		exit(EXIT_FAILURE);
	}
	else printf("Socket open.\n");
	
	// ALLOCATE THREAD POOL
	for(i = 0; i < threadsinpool; i++)
	{
		if((err = pthread_create(&thrds[i], NULL, dealmaker , (void*) (intptr_t) socID)) != 0)
		{
			perror("Unable to create client thread!\n");
			exit(EXIT_FAILURE);
		}
	}
	
	// accept connections and store them into the shared struct
    while(1)
	{
		if(mboxStats.concurrent_connections >= maxconnections-1) //was <
		{
			// mutex for producer
			//TODO: solve hanging problem on last element: maybe solved
			err = -1;
			if((err = pthread_mutex_lock(&coQU)) == 0){
				printf("main has the mutex\n");
				if(maxconnections > queueLength + threadsinpool)
				{
					if((connectionQueue->sokAddr = accept(socID, NULL, 0)) == -1)
					{
						pthread_mutex_unlock(&coQU);
						exit(EXIT_FAILURE);
					}
					else
					{
						connectionQueue->next = (listSimple*)malloc(sizeof(listSimple));
						connectionQueue = connectionQueue->next;
						connectionQueue->sokAddr = -1;
						connectionQueue->next = NULL;
						queueLength++;
						pthread_cond_signal(&coQUwait);
						pthread_mutex_unlock(&coQU);
					}
				}
				else
				{
					pthread_mutex_unlock(&coQU);
					sleep(1);
				}
			}
		}
	}
	
    return 0;
}
