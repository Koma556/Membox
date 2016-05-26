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

static char *socketpath, *statfilepath;
static int maxconnections, threadsinpool, storagesize, storagebyte, maxobjsize, activethreads = 0, quit = 1;
// Struttura dati condivisa
static icl_hash_t* dataTable;
// mutex
static pthread_mutex_t dT = PTHREAD_MUTEX_INITIALIZER, actvth = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t actwait, tabwait;
// mutex to be called before a mboxStats update
static pthread_mutex_t stOP = PTHREAD_MUTEX_INITIALIZER, stCO = PTHREAD_MUTEX_INITIALIZER, stST = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t stOPwait, stCOwait, stSTwait;
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
	do{*str++;
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
		*str++;	
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
message_t selectorOP(message_t *msg){
	message_t reply;
	unsigned int shkey;
	
	switch(msg->hdr.op)
	{
		//	PUT_OP          = 0,   /// inserimento di un oggetto nel repository
		case PUT_OP:
		{
			
		}
		//	UPDATE_OP       = 1,   /// aggiornamento di un oggetto gia' presente
		case UPDATE_OP: 
		{
		}
		//	GET_OP          = 2,   /// recupero di un oggetto dal repository
		case GET_OP: 
		{
		}
		//	REMOVE_OP       = 3,   /// eliminazione di un oggetto dal repository
		case REMOVE_OP:
		{
		}
		// 	LOCK_OP         = 4,   /// acquisizione di una lock su tutto il repository
		case LOCK_OP: 
		{
		}
		// 	UNLOCK_OP       = 5,   /// rilascio della lock su tutto il repository
		case UNLOCK_OP: 
		{
		}
		// 	END_OP
		case END_OP: 
		{
		}
		// Invalid OP code
		default: 
		{
			perror("OP not recognized.\n");
			EXIT_FAILURE;
		}
	}
	return reply;
}

void *dealmaker(void* arg){
	int err, thrdnumber;
	int sokt = (int)arg, soktAcc;
	message_t* receiver;
	
	//lock e aggiungo al numero di thread attivi
	if((err = pthread_mutex_lock(&actvth)) == 0)
	{
		pthread_cond_init(&actwait, NULL);
		activethreads++;
		thrdnumber = activethreads;
		pthread_mutex_unlock(&actvth);
	}
	else(pthread_cond_wait(&actwait, &actvth));
	
	// alloco la struttura dati che riceve il messaggio
	if((receiver = (message_t*)malloc(sizeof(message_t))) == NULL)
	{
		errno = ENOMEM;
		exit(EXIT_FAILURE);
	}
	
	while(1)
	{
		// TODO list of pending connections
		
		printf("Thread %d primed and ready!\n", thrdnumber);
		if((soktAcc = accept(sokt, NULL, 0)) == -1)
		{
			strerror(errno);
			exit(EXIT_FAILURE);
		}
	
		// appena accetto una connessione faccio la mutex per aggiornare mboxStats
		if((err = pthread_mutex_lock(&stOP)) == 0)
		{
			pthread_cond_init(&actwait, NULL);
			if(statConnections(0) != 0)
			{
				strerror(errno);
				exit(EXIT_FAILURE);
			}
			pthread_mutex_unlock(&stOP);
		}else(pthread_cond_wait(&stOPwait, &stOP));
		
		// Leggo quello che il client mi scrive
		if(readHeader(soktAcc, &receiver->hdr)!=0)
		{
			errno = EIO;
			exit(EXIT_FAILURE);
		}
		if(readData(soktAcc, &receiver->data)!=0)
		{
			errno = EIO;
			exit(EXIT_FAILURE);
		}
		
		// mando il messaggio a selectorOP che si occupa del resto
		selectorOP(receiver);
		
		// chiudo il socket e rimuovo la connessione dalle attive
		if((err = pthread_mutex_lock(&stOP)) == 0)
		{
			pthread_cond_init(&actwait, NULL);
			close(soktAcc);
			statConnections(1);
			pthread_mutex_unlock(&stOP);
		}else(pthread_cond_wait(&stOPwait, &stOP));
		
		// TODO: BREAK se ricevo il segnale dall'utente
	}
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
    thrds = calloc(threadsinpool, sizeof(pthread_t));
    
    // alloco la struttura dati d'hash condivisa
    dataTable = icl_hash_create(maxconnections*10, ulong_hash_function, ulong_key_compare);
        
    // creo il socket
    if(remove(socketpath) == 0)printf("Cleaned up old Socket.\nPossible recovery after crash?\n");
    if((socID = startConnection(socketpath)) == -1)
	{
		printf("Couldn't create socket.\n");
		exit(EXIT_FAILURE);
	}
	else printf("Socket open.\n");
	
	// ALLOCATE THREAD POOL
	for(i = 0; i < threadsinpool; i++)
	{
		if((err = pthread_create(&thrds[0], NULL, dealmaker , (void*)socID)) != 0)
		{
			perror("Unable to create client thread!\n");
			exit(EXIT_FAILURE);
		}
	}
	
	// wait on child threads
    while (activethreads > 0)
	{
		sleep(1);
	}
	
    return 0;
}
