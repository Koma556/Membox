#
# membox Progetto del corso di LSO 2016 
# 
# Dipartimento di Informatica Università di Pisa
# Docenti: Pelagatti, Torquati
#
#

##########################################################
# IMPORTANTE: completare la lista dei file da consegnare
# 
FILE_DA_CONSEGNARE=membox.c message.h ops.h stats.h DATA/membox.conf1 DATA/membox.conf2
# inserire il nome del tarball: es. NinoBixioGiuseppeGaribaldi
TARNAME=NomeCognome
#
###########################################################

###################################################################
# NOTA: Il nome riportato in UNIX_PATH deve corrispondere al nome 
#       usato per l'opzione UnixPath nel file di configurazione del 
#       server mbox (vedere i file nella directory DATA).
#       Lo stesso vale per il nome riportato in STAT_PATH che deve 
#       corrispondere con l'opzione StatFileName.
#
# ATTENZIONE: se il codice viene sviluppato sulle macchine del 
#             laboratorio utilizzare come nomi, nomi unici, 
#             ad esempo /tmp/mbox_sock_<numero-di-matricola> e
#             /tmp/mbox_stats_<numero-di-matricola>.
#
###################################################################
UNIX_PATH       = /tmp/mbox_socket
STAT_PATH       = /tmp/mbox_stats.txt


CC		=  gcc
AR              =  ar
CFLAGS	        += -std=c99 -Wall -pedantic -g -DMAKE_VALGRIND_HAPPY
ARFLAGS         =  rvs
INCLUDES	= -I.
LDFLAGS 	= -L.
OPTFLAGS	= #-O3 
LIBS            = -pthread

# aggiungere qui altri targets se necessario
TARGETS		= membox        \
		  client        

# aggiungere qui i file oggetto da compilare
OBJECTS		= connections.o	\
		  icl_hash.o

# aggiungere qui gli altri include 
INCLUDE_FILES   = connections.h \
		  message.h     \
		  ops.h	  	\
		  stats.h	\
		  icl_hash.h	


.PHONY: all clean cleanall test1 test2 test3 test4 test5 test6 test7 consegna
.SUFFIXES: .c .h

%: %.c
	$(CC) $(CFLAGS) $(INCLUDES) $(OPTFLAGS) -o $@ $< $(LDFLAGS) 

%.o: %.c
	$(CC) $(CFLAGS) $(INCLUDES) $(OPTFLAGS) -c -o $@ $<

all		: $(TARGETS)


membox: membox.o libmbox.a $(INCLUDE_FILES)
	$(CC) $(CFLAGS) $(INCLUDES) $(OPTFLAGS) $(LDFLAGS) -o $@ $^ $(LIBS)

client: client.o connections.o message.h
	$(CC) $(CFLAGS) $(INCLUDES) $(OPTFLAGS) $(LDFLAGS) -o $@ $^ $(LIBS)

test_hash: test_hash.o icl_hash.o
	$(CC) $(CFLAGS) $(INCLUDES) $(OPTFLAGS) $(LDFLAGS) -o $@ $^


############################ non modificare da qui in poi

libmbox.a: $(OBJECTS)
	$(AR) $(ARFLAGS) $@ $^

clean		: 
	rm -f $(TARGETS)

cleanall	: clean
	\rm -f *.o *~ libmbox.a valgrind_out $(STAT_PATH) $(UNIX_PATH)

killmbox:
	killall -9 membox

# test base
test1: 
	make cleanall
	make all
	./membox -f DATA/membox.conf1&
	./client -l $(UNIX_PATH) -c 0:0:8192
	./client -l $(UNIX_PATH) -c 0:1:8192
	./client -l $(UNIX_PATH) -c 0:2:0
	./client -l $(UNIX_PATH) -c 0:3:0
	./client -l $(UNIX_PATH) -c 0:0:1048576
	./client -l $(UNIX_PATH) -c 0:2:0
	./client -l $(UNIX_PATH) -c 0:0:1048576; test $$? -eq 243
	./client -l $(UNIX_PATH) -c 0:3:0
	killall -USR2 -w membox
	@echo "********** Test1 superato!"

# test put concorrenti
test2:
	make cleanall
	make all
	./membox -f DATA/membox.conf1&
	./testput.sh $(UNIX_PATH) $(STAT_PATH)
	killall -QUIT -w membox
	@echo "********** Test2 superato!"

# test funzionamento del protocollo di lock
test3:
	make cleanall
	make all
	./membox -f DATA/membox.conf1&
	./testlock.sh $(UNIX_PATH)
	killall -USR2 -w membox
	@echo "********** Test3 superato!"

# gestione dei parametri sulle size e sul n. di connessioni
test4:
	make cleanall
	make all
	./membox -f DATA/membox.conf2&
	./testsize.sh $(UNIX_PATH) $(STAT_PATH)
	@echo "********** Test4 superato!"

# corretto funzionamento delle statistiche
test5:
	make cleanall
	make all
	./membox -f DATA/membox.conf1&
	./teststats.sh $(UNIX_PATH) $(STAT_PATH)
	killall -USR2 -w membox
	@echo "********** Test5 superato!"

# verifica di memory leaks 
test6:
	make cleanall
	make all
	./testleaks.sh $(UNIX_PATH)
	@echo "********** Test6 superato!"

# stress test
test7:
	make cleanall
	make all
	./membox -f DATA/membox.conf1&
	./teststress.sh $(UNIX_PATH)
	killall -USR2 -w membox
	@echo "********** Test7 superato!"

SUBJECTA="lso16: frammento 2 - corso A"
SUBJECTB="lso16: frammento 2 - corso B"
# target per la consegna
consegna:
	make test1
	sleep 3
	make test2
	sleep 3
	make test3
	sleep 3
	make test4
	sleep 3
	make test5
	sleep 3
	make test6
	sleep 3
	make test7
	sleep 3
	./gruppo-check.pl < gruppo.txt
	tar -cvf $(TARNAME)-membox.tar ./gruppo.txt ./Makefile $(FILE_DA_CONSEGNARE) 
	@echo "*** FRAMMENTO: TAR PRONTO $(TARNAME)-membox.tar "
	@echo "inviare come allegato a lso.di@listgateway.unipi.it con subject:"
	@echo "$(SUBJECTA) oppure"
	@echo "$(SUBJECTB)"
