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
 * @author Giuseppe Crea 501922  
 *   Si dichiara che il contenuto di questo file e' in ogni sua parte opera  
 *   originale dell'autore.  
 */  

#define _POSIX_C_SOURCE 200809L
#define UNIX_PATH_MAX  64
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <pthread.h>
#include <signal.h>

/* inserire gli altri include che servono */

#include <connections.h>
#include <stats.h>
#include <icl_hash.h>
#include <message.h>
#include <sys/types.h>
#include <sys/socket.h> 
#include <sys/un.h>
#include <sys/select.h>
#include <time.h>
#include <errno.h>
#include <ops.h>

typedef struct listS{
	int sokAddr;
	struct listS* next;
}listSimple;

static char *socketpath, *statfilepath;
static int maxconnections, threadsinpool, storagesize, storagebyte, maxobjsize;
static volatile sig_atomic_t overlord = 1;
static int activethreads = 0, queueLength = 0, replock = -1;
static int highSocID = 0;
static FILE* descriptr;
static pthread_t disp;
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

void shutDown(){
	overlord = 0;
	shutdown(highSocID, SHUT_RD);
}

void printLog(){
	int err2;
	
	if(statfilepath != NULL)
	{
		err2 = -1;
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
				mboxStats.current_size += length;
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
				mboxStats.current_size = mboxStats.current_size - length;
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
				mboxStats.nget_failed++;
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

void cleanList(listSimple *head){
	listSimple* tmp;
	
	while(head != NULL)
	{
		tmp = head;
		head = head->next;
		free(tmp);
	}
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
	
	// if repo is locked I jump over the switch
	if(result != 1)
	{
		switch(msg->hdr.op)
		{
			case PUT_OP:
			{
				do
				{
					if((err = pthread_mutex_lock(&stCO)) == 0)
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
						else if(storagesize != 0 && mboxStats.current_objects >= storagesize){
							msg->hdr.op = OP_PUT_TOOMANY;
							result = 1;
						}
						pthread_mutex_unlock(&stCO);
					}
				}
				while(err != 0);
				
				if(result != 1)
				{
					newkey = calloc(1, sizeof(unsigned int*));
					*newkey = msg->hdr.key;
					newdata = calloc(1, sizeof(message_data_t));
					newdata->len = msg->data.len;
					newdata->buf = (char*)calloc(msg->data.len+1, sizeof(char));
					memcpy(newdata->buf, msg->data.buf, sizeof(char)*(msg->data.len));
					do
					{
						if((err = pthread_mutex_lock(&dataMUTEX)) == 0)
						{
							if((icl_hash_insert(dataTable, newkey, (void*)newdata)) == NULL)
							{
								if(icl_hash_find(dataTable, newkey) != NULL)
									msg->hdr.op = OP_PUT_ALREADY;
								else
								{
									msg->hdr.op = OP_FAIL; 
								}
								pthread_mutex_unlock(&dataMUTEX);
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
				newdata->buf = calloc(msg->data.len+1, sizeof(char));
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
							msg->data.buf = calloc(olddata->len+1, sizeof(char));
							memcpy(msg->data.buf, olddata->buf, olddata->len);
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
						if((olddata = icl_hash_find(dataTable, &msg->hdr.key)) == NULL)
						{
							pthread_mutex_unlock(&dataMUTEX);
							msg->hdr.op = OP_REMOVE_NONE;
							result = 1;
						}
						else
						{
							msg->data.len = olddata->len;
							icl_hash_delete(dataTable, &msg->hdr.key, cleaninFun, cleaninData);
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
				msg->hdr.op = OP_FAIL;
				result = 1;
			}
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
			}
			pthread_mutex_unlock(&actvth);
		}
	}
	while(err != 0);
	return thrdnumber;
}

void* dealmaker (void* args){
	int err, err2, soktAcc, thrdnumber = (intptr_t) args;
	message_t *messg;
	listSimple *tmp;
	op_t tmpop;
	
	initActivity(1);
	
	while(overlord == 1)
	{
		do
		{
			if((err = pthread_mutex_lock(&coQU)) == 0)
			{
				while(queueLength == 0)
				{
					if(overlord == 1)
					{
						pthread_cond_wait(&coQUwait, &coQU);
					}
					else if(overlord == 0)
					{
						pthread_mutex_unlock(&coQU);
						initActivity(-1);
						pthread_exit(NULL);
					}
				}
				soktAcc = head->sokAddr;
				tmp = head;
				head = head->next;
				free(tmp);
				queueLength--;
				pthread_mutex_unlock(&coQU);
			}
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
		while(1)
		{
			if((messg = calloc(1, sizeof(message_t))) == NULL)
				{
					errno = ENOMEM;
					exit(EXIT_FAILURE);
				}
			if(readHeader(soktAcc, &messg->hdr) < 0)
			{
				free(messg);
				break;
			}
			if(readData(soktAcc, &messg->data) < 0) 
			{
				free(messg->data.buf);
				free(messg);
				break;
			}
			tmpop = messg->hdr.op;
			selectorOP(messg, soktAcc, tmpop);
			if(sendReply(tmpop, messg, soktAcc) != 0)
					printf("[dealmaker%d] sendReply returned failure state\n", thrdnumber);
			free(messg->data.buf);
			free(messg);
		}
		if(replock == soktAcc) replock = -1;
		close(soktAcc);
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
	}
	
	initActivity(-1);
	pthread_exit(NULL);
}

void* dispatcher(void* args){
	int tmpSockt;
	int err = -1;
	message_t *msg;
	
	while(overlord)
	{
		if((tmpSockt = accept(highSocID, NULL, 0)) > 0)
		{				
			do
			{
				if((err = pthread_mutex_lock(&coQU)) == 0)
				{
					//printf("Producer has mutex\n");
					if(queueLength + mboxStats.concurrent_connections < maxconnections)
					{
						connectionQueue->sokAddr = tmpSockt;
						connectionQueue->next = calloc(1, sizeof(listSimple));
						queueLength++;
						connectionQueue = connectionQueue->next;
						connectionQueue->sokAddr = -1;
						connectionQueue->next = NULL;
						pthread_cond_signal(&coQUwait);
						pthread_mutex_unlock(&coQU);
					}
					else
					{
						pthread_mutex_unlock(&coQU);
						msg = calloc(1, sizeof(message_t));
						msg->hdr.op = OP_FAIL;
						msg->hdr.key = -1;
						msg->data.buf = calloc(20, sizeof(char));
						sprintf(msg->data.buf, "connection refused\n");
						sendReply(msg->hdr.op, msg, tmpSockt);
						free(msg->data.buf);
						free(msg);
						close(tmpSockt);
					}
				}
			}
			while(err != 0);
		}
	}
	pthread_cond_broadcast(&coQUwait);
	pthread_exit(NULL);
}

int main(int argc, char *argv[]) {
	int config[5], err, i = 0;
	char *configfilepath;
	pthread_t* thrds;
	FILE *fp;
	struct sigaction s;
	struct sigaction r;
		
	memset(&s, 0, sizeof(s));
	memset(&r, 0, sizeof(r));	
		
	s.sa_handler = shutDown;
	r.sa_handler = printLog;
	sigaction(SIGUSR1, &r, NULL);
	sigaction(SIGUSR2, &s, NULL);
	sigaction(SIGQUIT, &s, NULL);
	sigaction(SIGTERM, &s, NULL);
	sigaction(SIGINT, &s, NULL);
	
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
		printf("%s\n", strerror(errno));
		return -1;
	}
    
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
	
    // alloco la struttura dati d'hash condivisa
    if((dataTable = icl_hash_create(1087, ulong_hash_function, ulong_key_compare)) == NULL)
    {
		errno = ENOMEM;
		printf("%s\n", strerror(errno));
		return -1;
	}
	
    // apro il file di log
    if(statfilepath != NULL)
		descriptr = fopen(statfilepath, "w+");  
    
    // creo il socket
    remove(socketpath);
    if((highSocID = startConnection(socketpath)) == -1)
	{
		printf("Couldn't create socket. Resource busy.\n");
		exit(EXIT_FAILURE);
	}
	
	// alloco il dispatcher
    if((err = pthread_create(&disp, NULL, dispatcher, NULL)) != 0)
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
	
	// gently close all threads
	
	pthread_join(disp, NULL);
	for(i = 0; i < threadsinpool; i++)
	{
		pthread_join(thrds[i], NULL);
	}
	
	printLog();
	
	if(statfilepath != NULL)
	{
		fclose(descriptr);
	}
	cleanList(head);
	remove(socketpath);
	icl_hash_destroy(dataTable, cleaninFun, cleaninData);
	free(statfilepath);
	free(socketpath);
	free(thrds);
	printf("All done!\n");
    return 0;
}
