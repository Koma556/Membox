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

/* struttura che memorizza le statistiche del server, struct statistics 
 * e' definita in stats.h.
 *
 */
struct statistics  mboxStats = { 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0 };

char* readLine(FILE* fd){
	char *str, *p;
	
	str = calloc(UNIX_PATH_MAX+40, sizeof(char));
	
	do{
		if(fgets(str, UNIX_PATH_MAX+30, fd) == NULL){
			errno = EIO;
			return NULL;
		}
	}while(str[0] == '#');
	
	str = strchr(str, '=');
	if(str == NULL){
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

void *threadd(){
	printf("Hi\n");
}

int main(int argc, char *argv[]) {
	int *config, socpath, err, i = 0;
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
    
    // assegno i valori dell'array config a variabili GLOBALI
    
    maxconnections = config[0];
    threadsinpool = config[1];
    storagesize = config[2];
    storagebyte = config[3];
    maxobjsize = config[4];
    free(config);
    // array dove salvo i pid dei thread
    thrds = calloc(threadsinpool, sizeof(pthread_t));
    
    // controlli configurazione
    if(threadsinpool > maxconnections) printf("Bad config: Too few max connections\n");
    // DEBUGGING PRINTS
    {	printf("%s\n", socketpath);
    	printf("%d\n%d\n%d\n%d\n%d\n", maxconnections, threadsinpool, storagesize, storagebyte, maxobjsize);
    	printf("%s\n", statfilepath);
    }// END DEBUGGING PRINTS
    
    //creo il socket
    if((socpath = startConnection(socketpath)) == -1)
	{
		printf("Couldn't create socket.\n");
		exit(EXIT_FAILURE);
	}else printf("Socket open.\n");
	
	// DEBUGGING PRINTS	
	{	if(close(socpath) == 0) printf("Socket closed.\n");
		else printf("Couldn't close socket.\n");
		if(remove(socketpath) == 0) printf("Socket removed.\n");
		else printf("Couldn't remove socket.\n");
	}// END DEBUGGING PRINTS
	
	// Main Loop
	while(quit){
		while(maxconnections-threadsinpool >= 0)
		{
			//accetta connessione e salva i dati
			if(activethreads < threadsinpool){
				//lancia nuovo thread che si occupa d'un client
				if((err = pthread_create(&thrds[i], NULL, threadd , (void*) 1)) != 0)
				{
					perror("Unable to create client thread!\n");
					exit(EXIT_FAILURE);
				}else
				{
					i++;
					activethreads++;
				}
			}else
			{
				//metti in coda la connessione
			}
		}
    }
    
    return 0;
}
