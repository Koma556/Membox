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
	int fd, i = 0, ck;
	struct sockaddr_un addr;
	
	if(strlen(path) > UNIX_PATH_MAX)
	{
		errno = E2BIG;
		return -1;
	}
	
	strncpy(addr.sun_path, path, UNIX_PATH_MAX);
	addr.sun_family = AF_UNIX;
	
	if((fd = socket(AF_UNIX,SOCK_STREAM,0)) == -1){
		errno = EIO;
		return -1;
	}
	
	while ( (ck = connect(fd,(struct sockaddr*)&addr, sizeof(addr))) == -1 && i++ < ntimes) 
	{
		printf("Connecting, please wait...\n");
		sleep(secs);
	}
	
	if(ck == 0) return fd;
	else
	{
		close(fd);
		printf("Failed to Connect.\n");
		return -1;
	}
}

// -------- server side ----- 

/**
 * @function startConnection
 * @brief Crea un socket AF_UNIX alla path specificata
 *
 * @param path Path del socket AF_UNIX da creare
 *
 * @return 0 in caso di successo -1 in caso di errore
 */
int startConnection(char* path){
	int ck;
	struct sockaddr_un addr;
	
	if(strlen(path) > UNIX_PATH_MAX){ 
		errno = E2BIG; 
		return -1;
	}
	
	strncpy(addr.sun_path, path, UNIX_PATH_MAX);
	addr.sun_family = AF_UNIX;
	
	if((ck = socket(AF_UNIX, SOCK_STREAM,0)) == -1) return -1;
	if((bind(ck,(struct sockaddr *)&addr, sizeof(addr))) == -1) return -1;
	if((listen(ck, SOMAXCONN)) == -1) return -1;
	// needs the accept foo too
	return ck;
}

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
	if((storage = (char*)malloc(sizeof(op_t)+sizeof(membox_key_t))) == NULL)
	{
		errno = ENOMEM; 
		return -1;
	}
	
	//leggo l'intera connessione
	ck = read(fd, storage, sizeof(op_t)+sizeof(membox_key_t));
	if(ck <= 0)
	{	
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
	unsigned int length;
	char *storage;
	
	//leggo dimensione di data
	/*
	if((storage = (char*)malloc(sizeof(unsigned int))) == NULL){
		errno = ENOMEM; 
		return -1;
	}*/
	ck = read(fd, &length, sizeof(unsigned int));
	if(ck <= 0)
	{	
		//free(storage);
		return -1;
	}
		
	//salvo dimensione di data
	memcpy(&data->len, &length, sizeof(unsigned int));
	
	//alloco storage per ospitare data
	if((storage = (char*)malloc(sizeof(char)*(data->len))) == NULL)
	{
		errno = ENOMEM; 
		return -1;
	}
	
	//leggo data dal socket 
	ck = 0;
	do
	{
		ck = read(fd, storage+(sizeof(char)*ck), (sizeof(char)*length));
		length =- ck;
	}while(ck > 0 && ck != length);
	if(ck < 0){	
		free(storage);
		return -1;
	}
	data->buf = storage;
	
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
int readReply(long fd, message_t *msg){
		if(readHeader(fd, &msg->hdr) || readData(fd, &msg->data) != 0)
			return -1;
		return 0;
}

#endif /* CONNECTIONS_H_ */
/*int main()
{
    // TODO: implementation
    return 0;
}*/
