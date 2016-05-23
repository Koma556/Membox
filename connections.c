/*
 * membox Progetto del corso di LSO 2016 
 *
 * Dipartimento di Informatica Università di Pisa
 * Docenti: Pelagatti, Torquati
 * 
 */
#ifndef CONNECTIONS_H_
#define CONNECTIONS_H_

#define MAX_RETRIES     10
#define MAX_SLEEPING     3
#if !defined(UNIX_PATH_MAX)
#define UNIX_PATH_MAX  64
#endif

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

/**
 * @file  connection.h
 * @brief Contiene le funzioni che implementano il protocollo 
 *        tra i clients ed il server membox
 */

/**@function waitFor
 * @brief Aspetta un certo numero di secondi. Presa da stackoverflow.
 * 
 * @param secs tempo da aspettare
 * 
 */

void waitFor (unsigned int secs) {
    unsigned int retTime = time(0) + secs;
    while (time(0) < retTime);
}

/**
 * @function openConnection
 * @brief Apre una connessione AF_UNIX verso il server membox.
 *
 * @param path Path del socket AF_UNIX 
 * @param ntimes numero massimo di tentativi di retry
 * @param secs tempo di attesa tra due retry consecutive
 *
 * @return il descrittore associato alla connessione in caso di successo
 *         -1 in caso di errore
 */
int openConnection(char* path, unsigned int ntimes, unsigned int secs){
	int i = 0;
	int ck;
	struct sockaddr_un addr;
	
	if(strlen(path) > UNIX_PATH_MAX) { errno = E2BIG; return -1;}
	
	strncpy(addr.sun_path, path, UNIX_PATH_MAX);
	addr.sun_family = AF_UNIX;
	
	if((ck = socket(AF_UNIX, SOCK_STREAM,0)) == -1) return -1;
	if((bind(ck,(struct sockaddr *)&addr, sizeof(addr))) == -1) return -1;
	if((listen(ck, SOMAXCONN)) == -1) return -1;
	
	//ciclo di retries
	while(i < ntimes || ((listen(ck, SOMAXCONN)) == -1)){
		waitFor(secs);
		i++;
	}
		
	return ck;
}

// -------- server side ----- 

/**
 * @function readHeader
 * @brief Legge l'header del messaggio
 *
 * @param fd     descrittore della connessione
 * @param hdr    puntatore all'header del messaggio da ricevere
 *
 * @return 0 in caso di successo -1 in caso di errore
 */
int readHeader(long fd, message_hdr_t *hdr){
	int ck = 0;
	char *storage;
	
	//alloco storage per ospitare sia op che chiave
	if((storage = (char*)malloc(sizeof(op_t)+sizeof(membox_key_t))) == NULL){
		errno = ENOMEM; 
		return -1;
	}
	
	//leggo l'intera connessione
	ck = read(fd, storage, sizeof(op_t)+sizeof(membox_key_t));
	if(ck < 0){	
		free(storage);
		return -1;
	}
		
	//salvo storage in hdr
	memcpy(&hdr->op, storage, sizeof(op_t));
	memcpy(&hdr->key, storage+sizeof(op_t), sizeof(membox_key_t));
	
	free(storage);
	return 0;
}

/**
 * @function readData
 * @brief Legge il body del messaggio
 *
 * @param fd     descrittore della connessione
 * @param data   puntatore al body del messaggio
 *
 * @return 0 in caso di successo -1 in caso di errore
 */
int readData(long fd, message_data_t *data){
	int ck = 0;
	unsigned int dim;
	char *storage;
	
	//leggo dimensione di data
	ck = read(fd, &dim, sizeof(unsigned int));
	if(ck < 0)	
		return -1;
		
	//salvo dimensione di data
	memcpy(&data->len, &dim, sizeof(unsigned int));
	
	//alloco storage per ospitare data
	if((storage = (char*)malloc(sizeof(char)*dim)) == NULL){
		errno = ENOMEM; 
		return -1;
	}
	
	//leggo data dal socket 
	ck = read(fd, storage, (sizeof(char)*data->len));
	if(ck < 0){	
		free(storage);
		return -1;
	}
	
	//alloco data->buf e vi salvo storage
	if((data->buf = (char*)malloc(sizeof(char)*data->len)) == NULL){
		errno = ENOMEM;
		free(storage);
		return -1;
	}
	memcpy(&data->buf, storage, sizeof(char)*data->len);
	
	free(storage);
	return 0;
}


/* da completare da parte dello studente con altri metodi di interfaccia */



// ------- client side ------
/**
 * @function sendRequest
 * @brief Invia un messaggio di richiesta al server membox
 *
 * @param fd     descrittore della connessione
 * @param msg    puntatore al messaggio da inviare
 *
 * @return 0 in caso di successo -1 in caso di errore
 */
int sendRequest(long fd, message_t *msg){
	char* storage;
	
	//preparo hdr
	if((storage = (char*)malloc(sizeof(op_t)+sizeof(membox_key_t))) == NULL){
		errno = ENOMEM; 
		return -1;
	}
		
	memcpy(storage, &msg->hdr.op, sizeof(op_t));
	memcpy(storage+sizeof(op_t), &msg->hdr.key, sizeof(membox_key_t));
	
	//mando hdr
	if((write(fd, storage, sizeof(op_t)+sizeof(membox_key_t))) == -1){
		free(storage);
		return -1;
	}
	free(storage);
	
	//preparo data
	if((storage = (char*)malloc(sizeof(unsigned int)+(sizeof(char)*msg->data.len))) == NULL){
		errno = ENOMEM; 
		return -1;
	}
		
	memcpy(storage, &msg->data.len, sizeof(unsigned int));
	memcpy(storage+sizeof(op_t), msg->data.buf, sizeof(char)*msg->data.len);
	
	//mando data
	if((write(fd, storage, sizeof(unsigned int)+(sizeof(char)*msg->data.len))) == -1){
		free(storage);
		return -1;
	}
	free(storage);
	
	return 0;
}

/**
 * @function readReply
 * @brief Legge un messaggio di risposta dal server membox
 *
 * @param fd     descrittore della connessione
 * @param msg    puntatore al messaggio da ricevere
 *
 * @return 0 in caso di successo -1 in caso di errore
 */
int readReply(long fd, message_t *msg);

#endif /* CONNECTIONS_H_ */
int main()
{
    // TODO: implementation
    return 0;
}
