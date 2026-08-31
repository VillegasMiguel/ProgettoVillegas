#define _POSIX_C_SOURCE 200809L  //include negli header i prototipi delle funzione definite dallo standard di sistema POSIX, utile per poter usare localtime_r
#include "../include/mr.h"
#include "../include/macro.h"
#include "../include/mr_utils.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdatomic.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <semaphore.h>
#include <threads.h>
#include <fcntl.h>
#include <time.h>
#include <dirent.h>
#include <limits.h>

typedef struct{
    mr_file_line_t **coda_mapper;
    mr_t mr;

    int eof;  //flag per permettere di comunicare ai thread che la coda è chiusa, eof=0 non chiusa, eof=1 chiusa
    int errore; //flag per segnalare ai vari thread che uno di loro presenta un errore, così tutti i thread devono terminare

    size_t head;
    size_t tail;
    size_t count;
    size_t capacity;

    mtx_t mtx_coda;
    cnd_t empty;
    cnd_t full;

    mtx_t mtx_pipe; //per sincronizzare la scrittura verso la pipe tra i vari thread worker
}Coda_mapper_t;

typedef struct{
    Coda_mapper_t *coda;
    size_t thrd_id;
}Args_mapper_t;


static void segnala_errore_thread(Coda_mapper_t *coda){ //utile per segnalare a tutti i thread che è presente un errore e che quindi devono terminare
    mtx_lock(&(coda->mtx_coda));
    coda->errore = 1;
    cnd_broadcast(&(coda->empty));
    cnd_signal(&(coda->full));  // sveglia anche reader se bloccato su full
    mtx_unlock(&(coda->mtx_coda));
}

static int crea_msg(Args_mapper_t *arg_ptr, mr_file_line_t **dest){
    mr_t mr = arg_ptr->coda->mr;
    size_t id_thread = arg_ptr->thrd_id;


    int lunghezza_nome_file_int;
    size_t file_name_len;
    int lunghezza_linea_int;
    size_t line_len;
    unsigned long line_number;
    
    mr_file_line_t *msg; 
    SYSNCALLC(msg=malloc(sizeof(mr_file_line_t)), "errore malloc mr_file_line_t", scrivi_log(mr, "errore malloc mr_file_line_t", "MAPPER", id_thread));
    //lettura lunghezza nome file
    ssize_t n_read_lunghezza_nome_file = readn(STDIN_FILENO, &lunghezza_nome_file_int, sizeof(int));
    if(n_read_lunghezza_nome_file==0){ //arrivati all'EOF
        free(msg);
        return 1;
    }
    SYSLETTMAPC(mr, n_read_lunghezza_nome_file, sizeof(int), "errore lettura lunghezza nome file dallo stdin di mapper", free(msg));

    if(lunghezza_nome_file_int>0)file_name_len=(size_t)lunghezza_nome_file_int;
    else{
        perror("errore lunghezza_nome_file_int ha valore negativo");
        scrivi_log(mr, "errore lunghezza_nome_file_int ha valore negativo", "MAPPER", id_thread);
        free(msg);
        return -1;
    }

    char *file_name;
    SYSNCALLC(file_name=malloc(file_name_len), "errore malloc file_name",{
        scrivi_log(mr, "errore malloc file_name", "MAPPER", id_thread);
        free(msg);
    });

    //lettura nome del file
    ssize_t n_read_file_name=readn(STDIN_FILENO, file_name, file_name_len);
    SYSLETTMAPC(mr, n_read_file_name, file_name_len, "errore lettura nome file dallo stdin di mapper", {free(msg);free(file_name);});


    //lettura numero linea
    ssize_t n_read_numero_linea=readn(STDIN_FILENO, &line_number, sizeof(unsigned long));
    SYSLETTMAPC(mr, n_read_numero_linea, sizeof(unsigned long), "errore lettura numero linea dallo stdin di mapper", {free(msg);free(file_name);});



    //lettura lunghezza linea
    ssize_t n_read_lunghezza_linea = readn(STDIN_FILENO, &lunghezza_linea_int, sizeof(int));
    SYSLETTMAPC(mr, n_read_lunghezza_linea, sizeof(int), "errore lettura lunghezza linea dallo stdin di mapper", {free(msg);free(file_name);});



    if(lunghezza_linea_int>=0)line_len=(size_t)lunghezza_linea_int;
    else{
        perror("errore lunghezza_linea_int ha valore negativo");
        scrivi_log(mr, "errore lunghezza_linea_int ha valore negativo", "MAPPER", id_thread);
        free(msg);
        free(file_name);
        return -1;
    }

    //lettura linea
    char *line;
    if(lunghezza_linea_int==0)line=NULL; //line è vuota.
    else{
        SYSNCALLC(line=malloc(line_len), "errore malloc line", {
            scrivi_log(mr, "errore malloc line", "MAPPER", id_thread);
            free(file_name);
            free(msg);
        });
        ssize_t n_read_line = readn(STDIN_FILENO, line, line_len);
        SYSLETTMAPC(mr, n_read_line, line_len, "errore lettura linea dallo stdin di mapper", {free(msg); free(file_name); free(line);});
    }

    //inizializzazione del messagio

    msg->file_name=file_name;
    msg->file_name_len=file_name_len;
    msg->line=line;
    msg->line_len=line_len;
    msg->line_number=line_number;

    (*dest)=msg;

    return 0;
}

static int emit(const char *token, const void *value, size_t value_size, void *emit_arg){
    Args_mapper_t *args_ptr = (Args_mapper_t*) emit_arg;
    Coda_mapper_t *coda = args_ptr->coda;
    mtx_t *mtx_pipe_ptr = &(args_ptr->coda->mtx_pipe);
    size_t id_thread = args_ptr->thrd_id;
    mr_t mr = args_ptr->coda->mr;


    mr_pair_header_map_to_red_t len_coppia;
    size_t len_token = strlen(token);
    if(len_token>MAX_LUNGHEZZA_TOKEN){
        perror("errore lunghezza token supera limite");
        scrivi_log(mr, "errore lunghezza token supera limite", "MAPPER", id_thread);
        segnala_errore_thread(coda);
        return -1;
    }
    len_coppia.token_len = (int)len_token;

    if(value_size > MAX_LUNGHEZZA_VALUE){
        perror("errore lunghezza value supera limite");
        scrivi_log(mr, "errore lunghezza value supera limite", "MAPPER", id_thread);
        segnala_errore_thread(coda);
        return -1;
    }
    len_coppia.value_len=(int)value_size;

    //creazione messaggio da mandare alla pipe
    char *msg;
    size_t len_msg = sizeof(mr_pair_header_map_to_red_t)+len_coppia.token_len+len_coppia.value_len;
    SYSNCALLC(msg = malloc(len_msg), "errore malloc messaggio in emit mapper", {
        scrivi_log(mr, "errore malloc messaggio in emit mapper", "MAPPER", id_thread);
        segnala_errore_thread(coda);
    });
    size_t offset=0;

    memcpy(msg, &len_coppia, sizeof(mr_pair_header_map_to_red_t));
    offset+=sizeof(mr_pair_header_map_to_red_t);

    memcpy(msg+offset, token, len_coppia.token_len);
    offset+=len_coppia.token_len;

    memcpy(msg+offset, value, len_coppia.value_len);
    
    //inserimento messaggio nella pipe
    SYSTHCALLC(mtx_lock(mtx_pipe_ptr), "errore mtx_lock in emit mapper", {
        scrivi_log(mr, "errore mtx_lock in emit mapper", "MAPPER", id_thread);
        segnala_errore_thread(coda);
    });

    ssize_t n_writen = writen(STDOUT_FILENO, msg, len_msg);
    if(n_writen<0 || (size_t)n_writen!=len_msg){
        scrivi_log(mr, "errore scrittura nella pipe da parte di emit", "MAPPER", id_thread);
        free(msg);
        mtx_unlock(mtx_pipe_ptr);
        segnala_errore_thread(coda);
        return -1;
    }

    SYSTHCALLC(mtx_unlock(mtx_pipe_ptr), "errore mtx_unlock in emit mapper", {
        scrivi_log(mr, "errore mtx_unlock in emit mapper", "MAPPER", id_thread);
        segnala_errore_thread(coda);
        free(msg);
    });
    free(msg);
    mr->contatore_coppie++;
    return 0;
}


static int reader_mapper ( void * arg ){
    Args_mapper_t *arg_ptr = (Args_mapper_t*) arg;
    scrivi_log(arg_ptr->coda->mr, "avvio reader mapper thread", "MAPPER", arg_ptr->thrd_id);
    mr_file_line_t **coda = arg_ptr->coda->coda_mapper;
    size_t *tail = &(arg_ptr->coda->tail);
    size_t *count = &(arg_ptr->coda->count);
    size_t *capacity = &(arg_ptr->coda->capacity);
    mr_t mr = arg_ptr->coda->mr;
    size_t id_thread = arg_ptr->thrd_id;
    mtx_t *mtx_coda_ptr = &(arg_ptr->coda->mtx_coda);
    cnd_t *full_ptr = &(arg_ptr->coda->full);
    cnd_t *empty_ptr = &(arg_ptr->coda->empty);


    while(1){
        //controllo flag errore
        SYSTHCALLC(mtx_lock(mtx_coda_ptr), "errore mtx_lock della coda da parte di reader_mapper", {
            scrivi_log(mr, "errore mtx_lock della coda da parte di reader_mapper", "MAPPER", id_thread);
            segnala_errore_thread(arg_ptr->coda);
            scrivi_log(mr, "terminato con errore reader mapper thread", "MAPPER", id_thread);
        });
        if(arg_ptr->coda->errore){
            SYSTHCALLC(mtx_unlock(mtx_coda_ptr), "errore mtx_unlock della coda da parte di reader_mapper", scrivi_log(mr, "errore mtx_unlock della coda da parte di reader_mapper", "MAPPER", id_thread));
            return-1;
        }
        SYSTHCALLC(mtx_unlock(mtx_coda_ptr), "errore mtx_unlock della coda da parte di reader_mapper", scrivi_log(mr, "errore mtx_unlock della coda da parte di reader_mapper", "MAPPER", id_thread));

        //creazione messaggio
        mr_file_line_t *msg;
        int rit_crea_msg=crea_msg(arg, &msg);
        if(rit_crea_msg==1){ //arrivati all'EOF, il thread termina
            SYSTHCALLC(mtx_lock(mtx_coda_ptr), "errore mtx_lock della coda da parte di reader_mapper", {
                scrivi_log(mr, "errore mtx_lock della coda da parte di reader_mapper", "MAPPER",id_thread);
                segnala_errore_thread(arg_ptr->coda);
                scrivi_log(mr, "terminato con errore reader mapper thread", "MAPPER", id_thread);
            });
            arg_ptr->coda->eof=1; //segnalo agli altri thread che la coda è chiusa

            SYSTHCALLC(cnd_broadcast(empty_ptr),"errore cnd_broadcast in reader mapper",{
                scrivi_log(mr, "errore cnd_broadcast in reader_mapper", "MAPPER", id_thread);
                mtx_unlock(mtx_coda_ptr);
                segnala_errore_thread(arg_ptr->coda);
                scrivi_log(mr, "terminato con errore reader mapper thread", "MAPPER", id_thread);
            });
            SYSTHCALLC(mtx_unlock(mtx_coda_ptr), "errore mtx_unlock della coda da parte di reader_mapper", {
                scrivi_log(mr, "errore mtx_unlock della coda da parte di reader_mapper", "MAPPER", id_thread);
                segnala_errore_thread(arg_ptr->coda);
                scrivi_log(mr, "terminato con errore reader mapper thread", "MAPPER", id_thread);
            });
            scrivi_log(mr, "terminato con successo reader mapper thread", "MAPPER", id_thread);
            return 0;
        }
        
        if (rit_crea_msg == -1) { //gestire la presenza di errore in crea_msg
            scrivi_log(mr, "errore in crea_msg", "MAPPER", id_thread);
            
            // Entro nel mutex per impostare la chiusura forzata ed evitare che i worker rimangano appesi
            SYSTHCALLC(mtx_lock(mtx_coda_ptr), "errore mtx_lock per gestione errore in reader_mapper", {
                scrivi_log(mr, "errore mtx_lock per gestione errore in reader_mapper", "MAPPER", id_thread );
                segnala_errore_thread(arg_ptr->coda);
                scrivi_log(mr, "terminato con errore reader mapper thread", "MAPPER", id_thread);
            });
            arg_ptr->coda->errore = 1; 
            cnd_broadcast(empty_ptr); //prevenire deadlock
            mtx_unlock(mtx_coda_ptr);
            scrivi_log(mr, "terminato con errore reader mapper thread", "MAPPER", id_thread);
            return -1;
        }


        //inserimento messaggio
        SYSTHCALLC(mtx_lock(mtx_coda_ptr), "errore mtx_lock della coda da parte di reader_mapper", {
            scrivi_log(mr, "errore mtx_lock della coda da parte di reader_mapper", "MAPPER",id_thread );
            segnala_errore_thread(arg_ptr->coda);
            scrivi_log(mr, "terminato con errore reader mapper thread", "MAPPER", id_thread);
        });   
        while(*count == *capacity && !(arg_ptr->coda->errore)){
            SYSTHCALLC(cnd_wait(full_ptr, mtx_coda_ptr), "errore cnd_wait in reader_mapper",{
                scrivi_log(mr, "errore cnd_wait in reader_mapper", "MAPPER", id_thread);
                mtx_unlock(mtx_coda_ptr);
                free((void*)msg->file_name);free((void*)msg->line);free(msg);
                segnala_errore_thread(arg_ptr->coda);
                scrivi_log(mr, "terminato con errore reader mapper thread", "MAPPER", 1);
            });
        }

        //ricontrollo il flag di errore perché è possibile che sia uscito dal while proprio per via del flag
        if(arg_ptr->coda->errore){
            SYSTHCALLC(mtx_unlock(mtx_coda_ptr), "errore mtx_unlock della coda da parte di reader_mapper", scrivi_log(mr, "errore mtx_unlock della coda da parte di reader_mapper", "MAPPER", id_thread));
            free((void*)msg->file_name);free((void*)msg->line);free(msg);
            scrivi_log(mr, "terminato con errore reader mapper thread", "MAPPER", 1);
            return-1;
        }
        
        coda[*tail] = msg;
        *tail = ((*tail)+1) % (*capacity);
        (*count)++;

        SYSTHCALLC(cnd_broadcast(empty_ptr), "errore cnd_signal di empty_ptr in reader_mapper",{
            scrivi_log(mr, "errore cnd_signal di empty_ptr in reader_mapper", "MAPPER", id_thread);
            mtx_unlock(mtx_coda_ptr);
            free((void*)msg->file_name);free((void*)msg->line);free(msg);
            segnala_errore_thread(arg_ptr->coda);
            scrivi_log(mr, "terminato con errore reader mapper thread", "MAPPER", 1);
        });

        SYSTHCALLC(mtx_unlock(mtx_coda_ptr), "errore mtx_unlock della coda da parte di reader_mapper", {
            scrivi_log(mr, "errore mtx_unlock della coda da parte di reader_mapper", "MAPPER", id_thread);
            segnala_errore_thread(arg_ptr->coda);
            scrivi_log(mr, "terminato con errore reader mapper thread", "MAPPER", 1);
        });
    }
};


static int mapper_worker_main ( void * arg ){
    Args_mapper_t *arg_ptr = (Args_mapper_t*) arg;
    scrivi_log(arg_ptr->coda->mr, "avvio worker mapper thread", "MAPPER", arg_ptr->thrd_id);
    mr_file_line_t **coda = arg_ptr->coda->coda_mapper;
    size_t *head = &(arg_ptr->coda->head);
    size_t *count = &(arg_ptr->coda->count);
    size_t *capacity = &(arg_ptr->coda->capacity);
    mr_t mr = arg_ptr->coda->mr;
    size_t id_thread = arg_ptr->thrd_id;
    mtx_t *mtx_coda_ptr = &(arg_ptr->coda->mtx_coda);
    cnd_t *full_ptr = &(arg_ptr->coda->full);
    cnd_t *empty_ptr = &(arg_ptr->coda->empty);

    while(1){
        //estrarre elemento dalla coda
        mr_file_line_t* msg;
        SYSTHCALLC(mtx_lock(mtx_coda_ptr), "errore lock mutex coda in mapper_worker", {scrivi_log(mr, "errore lock mutex coda in mapper_worker", "MAPPER", id_thread);segnala_errore_thread(arg_ptr->coda);});
        while((*count)==0 && !(arg_ptr->coda->errore) && !(arg_ptr->coda->eof)){
            SYSTHCALLC(cnd_wait(empty_ptr, mtx_coda_ptr), "errore cnd_wait in mapper_worker", {
                scrivi_log(mr, "errore cnd_wait in mapper_worker", "MAPPER", id_thread);segnala_errore_thread(arg_ptr->coda);
                scrivi_log(mr, "terminato con errore worker mapper thread", "MAPPER", id_thread);
            });
        }
        if(arg_ptr->coda->errore){
            SYSTHCALLC(mtx_unlock(mtx_coda_ptr), "errore unlock mutex coda in mapper_worker", {scrivi_log(mr, "errore unlock mutex coda in mapper_worker", "MAPPER", id_thread);segnala_errore_thread(arg_ptr->coda);});
            scrivi_log(mr, "terminato con errore worker mapper thread", "MAPPER", id_thread);
            return -1;
        }
        
        if(arg_ptr->coda->eof && (*count)==0){//anche se mandato EOF, bisogna comunque liberare tutta la coda prima di far terminare i vari thread worker
            SYSTHCALLC(mtx_unlock(mtx_coda_ptr), "errore unlock mutex coda in mapper_worker",{
                scrivi_log(mr, "errore unlock mutex coda in mapper_worker", "MAPPER", id_thread);
                segnala_errore_thread(arg_ptr->coda);
                scrivi_log(mr, "terminato con errore worker mapper thread", "MAPPER", id_thread);
            });
            scrivi_log(mr, "terminato con successo worker mapper thread", "MAPPER", id_thread);
            return 0;
        }

        msg = coda[*head];
        *head = ((*head)+1) % (*capacity);
        (*count)--;
        SYSTHCALLC(cnd_signal(full_ptr), "errore cnd_signal in mapper_worker",{
            scrivi_log(mr, "errore cnd_signal in mapper_worker", "MAPPER", id_thread);
            scrivi_log(mr, "terminato con errore worker mapper thread", "MAPPER", id_thread);
        });
        SYSTHCALLC(mtx_unlock(mtx_coda_ptr), "errore unlock mutex coda in mapper_worker", {scrivi_log(mr, "errore unlock mutex coda in mapper_worker", "MAPPER", id_thread);segnala_errore_thread(arg_ptr->coda);});

        //chiamata alla funzione mapper
        if(mr->mapper_fun(msg, emit, arg_ptr, mr->user_arg)==-1){
            free((void*)msg->file_name);free((void*)msg->line);free(msg);
            scrivi_log(mr, "errore chiamata funzione mapper", "MAPPER", id_thread);
            scrivi_log(mr, "terminato con errore worker mapper thread", "MAPPER", id_thread);
            return -1;
        };
        free((void*)msg->file_name);free((void*)msg->line);free(msg);
    }
}

void mtx_cnd_destroy(Coda_mapper_t *coda){
    mtx_destroy(&coda->mtx_pipe);
    mtx_destroy(&coda->mtx_coda);
    cnd_destroy(&coda->empty);
    cnd_destroy(&coda->full);
    free(coda->coda_mapper);
}


int mapper_process_main(mr_t mr){
    mr_file_line_t **coda_mapper;  //creazione coda per comunicazione fra thread
    SYSNCALLC(coda_mapper=malloc(sizeof(mr_file_line_t*) * mr->attributi->queue_size), "errore malloc coda_mapper", {
        scrivi_log(mr,"errore malloc coda_mapper", "MAPPER", 0 );
        close(STDOUT_FILENO);
    });
    Coda_mapper_t coda;
    coda.coda_mapper=coda_mapper;
    coda.mr=mr;
    coda.head=0;
    coda.tail=0;
    coda.count=0;
    coda.capacity=mr->attributi->queue_size;
    coda.eof=0;
    coda.errore=0;
    SYSTHCALLC(mtx_init(&coda.mtx_pipe, mtx_plain), "errore inizializzazione mutex pipe mapper", {
        scrivi_log(mr, "errore inizializzazione mutex pipe mapper", "MAIN", 0);
        free(coda_mapper);
        close(STDOUT_FILENO);
    });
    SYSTHCALLC(mtx_init(&coda.mtx_coda, mtx_plain), "errore inizializzazione mutex coda mapper", {
        scrivi_log(mr, "errore inizializzazione mutex coda mapper", "MAIN", 0);
        close(STDOUT_FILENO);
        mtx_destroy(&(coda.mtx_pipe));
        free(coda_mapper);
    });
    SYSTHCALLC(cnd_init(&coda.empty), "errore inizializzazione cnd empty coda mapper", {
        scrivi_log(mr, "errore inizializzazione cnd empty coda mapper", "MAIN", 0);
        close(STDOUT_FILENO);
        mtx_destroy(&(coda.mtx_pipe));
        mtx_destroy(&(coda.mtx_coda));
        free(coda_mapper);
    });
    SYSTHCALLC(cnd_init(&coda.full), "errore inizializzazione cnd full coda mapper", {
        scrivi_log(mr, "errore inizializzazione cnd full coda mapper", "MAIN", 0);
        close(STDOUT_FILENO);
        mtx_destroy(&(coda.mtx_pipe));
        mtx_destroy(&(coda.mtx_coda));
        cnd_destroy(&(coda.empty));
        free(coda_mapper);
    });




    size_t num_threads_worker = mr->attributi->mapper_threads;
    thrd_t lista_thrd[num_threads_worker+1];
    Args_mapper_t lista_args[num_threads_worker+1];

    for(size_t i=0;i<num_threads_worker+1;i++){
        lista_args[i].coda=&coda;
        lista_args[i].thrd_id=i+1;  //i+1 perché il thread 0 è il thread che esegue mapper_process_main. Quindi il thread 1 è il thread lettore e i rimanenti sono i thread worker
    }

    int ret_create;
    ret_create = thrd_create(&lista_thrd[0], reader_mapper, &lista_args[0]); //salvo in una variabile il risultato, perché per come è stato definito SYSTHCALL, se passassi direttamente l'istruzione, verrebbe eseguita due volte thrd_create
    SYSTHCALLC(ret_create, "errore creazione thread mapper lettura", {
        scrivi_log(mr, "errore creazione thread mapper lettura", "MAPPER", 0);
        close(STDOUT_FILENO);
    }); //creazione thread mapper lettura
    


    size_t thread_creati=1; //controllo numero di thread creati, così se una thread_create fallisce, posso fare la join di quelli già esistenti
    for(size_t i=1;i<num_threads_worker+1;i++){
        if(thrd_create(&lista_thrd[i], mapper_worker_main, &lista_args[i]) == thrd_error){
            scrivi_log(mr, "errore creazione thread mapper worker", "MAPPER", 0);
            segnala_errore_thread(&coda);
            break;
        }
        thread_creati++;
    }

    if(thread_creati<num_threads_worker+1){
        for(size_t i=0;i<thread_creati;i++){
            if(thrd_join(lista_thrd[i],NULL)==thrd_error){
                scrivi_log(mr, "errore terminazione thread in mapper nella pulizia thread in cui thread creati < thread richiesti", "MAPPER", i+1);
            }
        }
        close(STDOUT_FILENO);
        mtx_cnd_destroy(&coda);
        return -1;
    }

    scrivi_log(mr, "creazione di tutti i thread", "MAPPER", 0);

    int risultato=0;
    int errore_trovato=0;

    //attesa terminazione thread reader
    if(thrd_join(lista_thrd[0], &risultato)==thrd_error){
        scrivi_log(mr, "errore terminazione thread reader", "MAPPER", 1);
        errore_trovato=-1;
    }
    if(risultato ==-1)errore_trovato=-1;


    //attesa terminazione thread worker
    for(size_t i=1;i<num_threads_worker+1;i++){
        if(thrd_join(lista_thrd[i], &risultato)==thrd_error){
            scrivi_log(mr, "errore terminazione thread worker", "MAPPER", i+1);
            errore_trovato=-1;
        }
        if(risultato ==-1)errore_trovato=-1;
    }

    scrivi_log(mr, "terminazione di tutti i thread", "MAPPER", 0);
    char msg_contatore[64];
    sprintf(msg_contatore, "numero di coppie prodotte dal mapper: %zu", mr->contatore_coppie);
    scrivi_log(mr, msg_contatore, "MAPPER", 0);

    close(STDOUT_FILENO); //segnalare EOF al reducer

    if(errore_trovato==-1){
        mtx_cnd_destroy(&coda);
        return -1;
    }
    mtx_cnd_destroy(&coda);
    return 0;
}