#if !defined(MEMBOX_STATS_)
#define MEMBOX_STATS_

#include <stdio.h>
#include <time.h>

/**
 *  @struct statistics
 *  @brief Struttura dati contenente le informazioni da monitorare.
 */
struct statistics {
    unsigned long nput;                         // n. di put
    unsigned long nput_failed;                  // n. di put fallite 
    unsigned long nupdate;                      // n. di update
    unsigned long nupdate_failed;               // n. di update fallite
    unsigned long nget;                         // n. di get
    unsigned long nget_failed;                  // n. di get fallite
    unsigned long nremove;                      // n. di remove
    unsigned long nremove_failed;               // n. di remove fallite
    unsigned long nlock;                        // n. di lock
    unsigned long nlock_failed;                 // n. di lock fallite
    unsigned long concurrent_connections;       // n. di connessioni concorrenti
    unsigned long current_size;                 // size attuale del repository (bytes)
    unsigned long max_size;                     // massima size raggiunta dal repository
    unsigned long current_objects;              // numero corrente di oggetti
    unsigned long max_objects;                  // massimo n. di oggetti raggiunto
};

/**
 * @function printStats
 * @brief Stampa le statistiche nel file passato come argomento
 *
 * @param fout descrittore del file aperto in append.
 *
 * @return 0 in caso di successo, -1 in caso di fallimento 
 */
static inline int printStats(FILE *fout) {
    extern struct statistics mboxStats;

    if (fprintf(fout, "%ld - %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld\n",
		(unsigned long)time(NULL),
		mboxStats.nput, mboxStats.nput_failed,
		mboxStats.nupdate, mboxStats.nupdate_failed,
		mboxStats.nget, mboxStats.nget_failed,
		mboxStats.nremove, mboxStats.nremove_failed,
		mboxStats.nlock, mboxStats.nlock_failed,
		mboxStats.concurrent_connections,
		mboxStats.current_size, mboxStats.max_size,
		mboxStats.current_objects, mboxStats.max_objects) < 0) return -1;
    fflush(fout);
    return 0;
}

#endif /* MEMBOX_STATS_ */
