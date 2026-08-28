#ifndef UTILS_H
#define UTILS_H

#include <stdio.h>
#include <stdlib.h>
#include "mr.h"
#include <sys/types.h>   
#include <stdatomic.h>   
#include <semaphore.h>   
#include <stdio.h>       
#include <time.h>       

#define MAX_LUNGHEZZA_RIGA 1024*1024
#define MAX_LUNGHEZZA_TOKEN 256
#define MAX_LUNGHEZZA_VALUE 1024*1024 //1 MB
#define MAX_LUNGHEZZA_RESULT (MAX_LUNGHEZZA_VALUE * 10) //10 MB, alla fine la result si ottiene prendendo come argomenti tutti i value, quindi è corretto aspettarsi che abbia una dimensione maggiore dei singoli value, per esempio se si parla di concatenazione

/*
mr_t è di tipo opaco, la sua implementazione non può essere osservata dal programma utente.
dato che mr_t è un puntatore alla struct mr, definisco quest'ultima all'interno di fun.h.
*/
struct mr{
    size_t numero_elaborazione; //ogni mr avrà un ID univoco che rappresenta il numero di elaborazione per ADDENDUM
    mr_attr_t* attributi;
    mr_mapper_t mapper_fun;
    mr_reducer_t reducer_fun;
    void *user_arg;

    FILE *f_log;
    FILE *f_stat;
    int main_to_mapper [2];
    int mapper_to_reducer [2];
    int reducer_to_main [2];
    /*
    Dal momento che nell'ADDENDUM devo considerare l'esecuzione di più elaborazioni del framework, è opportuno salvare i fd della pipe all'interno della mr.
    In questo modo ogni elaborazione si riferirà alle proprie fd interessate e può andare a chiudere le altre nei processi figli.
    */

    pid_t pid_mapper;
    pid_t pid_reducer;

    //variabili in cui vengono salvati valori che andranno a comporre il file delle statistiche finali dell ADDENDUM
    size_t contatore_righe_lette;
    size_t contatore_coppie;      
    atomic_size_t contatore_token_distinti; //modificato da più thread worker reducer, necessaria variabile atomica
    size_t contatore_risultati; 
    struct timespec *inizio;
    struct timespec *fine;

    sem_t *log_sem;
    sem_t *stat_sem;
};

typedef struct{
    int token_len;
    int value_len;
}mr_pair_header_map_to_red_t; //struct usata nella comunicazione interprocesso tra Mapper e Reducer

typedef struct{
    int token_len;
    int result_len;
}mr_pair_header_red_to_main_t;


void scrivi_log(mr_t mr, char *messaggio, char *processo, size_t numero_thread);


ssize_t readn(int fd, void *buf, size_t n);
ssize_t writen(int fd, void *buf, size_t n);

int mapper_process_main(mr_t mr);
int reducer_process_main(mr_t mr);


#endif