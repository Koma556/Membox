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
	//rimuovo gli spazi
	do{*str++;
	}while(str[0] == ' ');
	//cerco ed eventualmente rimuovo newline
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
				return NULL;
			}
		}while(str[0] == '#');
		
		str = strchr(str, '=');
		if(str == NULL){
			errno = EINVAL;
			return NULL;
		}
		do{*str++;
		}while(str[0] == ' ');
		conf[i] = atoi(str);
	}
	
	return conf;
}

int main(int argc, char *argv[]) {
	int* config;
	char* socketpath;
	char* statfilepath;
	int i;
	FILE *fp;
	
	//apro il file di configurazione
	fp = fopen("config.txt", "r");
	if(fp == NULL) 
	{
		errno = EIO;
		return(-1);
    }
    
    //leggo il file di configurazione
    socketpath = readLine(fp);
    config = readConfig(fp);
    statfilepath = readLine(fp);
    
    /** DEBUGGING PRINTS
    *	printf("%s\n", socketpath);
    *	for(i = 0; i < 5; i++) printf("%d\n", config[i]);
    *	printf("%s\n", statfilepath);
    **/
    
    return 0;
}
