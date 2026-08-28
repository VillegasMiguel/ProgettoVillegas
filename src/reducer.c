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
    char* token;
    size_t len_token;
    mr_value_t info_value;
}Coppia_t;

//uso  variabili atomiche globali per eof e errore, per poter segnalare a tutti i thread che è presente un errore o la fine dell'input
static atomic_int eof=0;
static atomic_int errore=0;

typedef struct{
    Coppia_t **coda_reducer;

    mr_t mr;

    size_t head;
    size_t tail;
    size_t count;
    size_t capacity;

    mtx_t mtx_coda;
    cnd_t empty;
    cnd_t full;

    mtx_t *mtx_pipe;


}Coda_reducer_t;

typedef struct{
    Coda_reducer_t *coda;
    size_t thrd_id;

    //campi utili per poter segnalare gli errori agli altri thread 
    Coda_reducer_t **Array_coda_thread_worker;
}Args_reducer_t;

static void mtx_cnd_coda_destroy(Coda_reducer_t **coda, size_t num, mtx_t *mtx_pipe){
    for(size_t i=0; i<num;i++){
        mtx_destroy(&(coda[i]->mtx_coda));
        cnd_destroy(&(coda[i]->full));
        cnd_destroy(&(coda[i]->empty));
        free(coda[i]->coda_reducer);
        free(coda[i]);
        //non elimino mtx_pipe dentro questa funzione perché tutti i thread si riferiscono alla stessa mtx_pipe, quindi la elimino una singola volta.
    }
    mtx_destroy(mtx_pipe);
    //free coda viene eseguita dal chiamante,
    // perché può capitare di dover liberare anche coda[i+1] ma se viene liberata la coda in questa funzione, poi non sono più in grado di accedere a coda[i+1]

}

static void libera_coppia(Coppia_t *coppia){
    free((void*)coppia->info_value.data);
    free(coppia->token);
    free(coppia);
}

static void segnala_errore_reducer(Coda_reducer_t **array_coda_thread){
    atomic_store(&errore, 1);
    size_t num_thread = array_coda_thread[0]->mr->attributi->reducer_threads;
    for(size_t i = 0; i < num_thread; i++){
        mtx_lock(&array_coda_thread[i]->mtx_coda);
        cnd_signal(&array_coda_thread[i]->empty);
        cnd_signal(&array_coda_thread[i]->full);
        mtx_unlock(&array_coda_thread[i]->mtx_coda);
    }
}





static int reader_reducer(void *arg){
    Coda_reducer_t **array_coda_thread_worker = arg;
    scrivi_log(array_coda_thread_worker[0]->mr, "avvio reader reducer thread", "REDUCER",1);
    size_t num_thread = array_coda_thread_worker[0]->mr->attributi->reducer_threads;
    mr_t mr = array_coda_thread_worker[0]->mr;
    mr_hash_t hash = mr->attributi->hash;
    mr_pair_header_map_to_red_t len_coppia;

    while(1){
        //creazione messaggio da mandare in una coda
        if(atomic_load(&errore))return -1;
        ssize_t n_read_len_coppia=readn(STDIN_FILENO, &len_coppia, sizeof(mr_pair_header_map_to_red_t));
        if(n_read_len_coppia==0){
            atomic_fetch_add(&eof, 1);
            for(size_t i=0; i<num_thread; i++){
                mtx_lock(&array_coda_thread_worker[i]->mtx_coda);
                cnd_signal(&array_coda_thread_worker[i]->empty);
                mtx_unlock(&array_coda_thread_worker[i]->mtx_coda);
            }
            scrivi_log(mr, "terminato con successo reader reducer thread", "REDUCER", 1);
            return 0;
        }
        SYSLETTREDC(mr, n_read_len_coppia, sizeof(mr_pair_header_map_to_red_t), "errore lettura lunghezza coppia token value", {
            atomic_fetch_add(&errore,1);
            scrivi_log(mr, "terminato con errore reader reducer thread", "REDUCER", 1);
        });

        if(len_coppia.token_len<=0 || len_coppia.token_len>MAX_LUNGHEZZA_TOKEN){
            scrivi_log(mr, "errore dimensione lunghezza token", "REDUCER",1);
            segnala_errore_reducer(array_coda_thread_worker);
            scrivi_log(mr, "terminato con errore reader reducer thread", "REDUCER", 1);
            return -1;
        }

        size_t len_token = (size_t)len_coppia.token_len;

        if(len_coppia.value_len<0 || len_coppia.value_len>MAX_LUNGHEZZA_VALUE){
            scrivi_log(mr, "errore dimensione lunghezza value", "REDUCER", 1);
            segnala_errore_reducer(array_coda_thread_worker);
            scrivi_log(mr, "terminato con errore reader reducer thread", "REDUCER", 1);
            return -1;
        }

        size_t len_value = (size_t)len_coppia.value_len;

        char *token;
        SYSNCALLC(token=malloc(len_token+1), "errore malloc token in reducer", {
            scrivi_log(mr, "errore malloc token in reducer","REDUCER", 1);
            segnala_errore_reducer(array_coda_thread_worker);
            scrivi_log(mr, "terminato con errore reader reducer thread", "REDUCER", 1);
        });

        ssize_t n_read_token=readn(STDIN_FILENO, token, len_token);
        SYSLETTREDC(mr, n_read_token, len_token, "errore lettura token in reducer", {
            segnala_errore_reducer(array_coda_thread_worker);
            free(token);
            scrivi_log(mr, "terminato con errore reader reducer thread", "REDUCER", 1);
        });
        token[len_token]='\0';

        void *value = NULL;
        if(len_value>0){
            SYSNCALLC(value=malloc(len_value), "errore malloc value in reducer", {
                scrivi_log(mr, "errore malloc value in reducer", "REDUCER",1);
                segnala_errore_reducer(array_coda_thread_worker);
                free(token);
                scrivi_log(mr, "terminato con errore reader reducer thread", "REDUCER", 1);
            });
            ssize_t n_read_value = readn(STDIN_FILENO, value, len_value);
            SYSLETTREDC(mr, n_read_value, len_value, "errore lettura value in reducer", {
                segnala_errore_reducer(array_coda_thread_worker);
                free(token);
                free(value);
                scrivi_log(mr, "terminato con errore reader reducer thread", "REDUCER", 1);
            });
        }
        

        mr_value_t info_value;


        info_value.data=value;
        info_value.size=len_value;

        Coppia_t *coppia;
        SYSNCALLC(coppia=malloc(sizeof(Coppia_t)), "errore allocazione di Coppia_t",{
            scrivi_log(mr, "errore allocazione Coppia_t", "REDUCER", 1);
            segnala_errore_reducer(array_coda_thread_worker);
            free(token);
            free(value);
            scrivi_log(mr, "terminato con errore reader reducer thread", "REDUCER", 1);
        });

        coppia->token=token;
        coppia->len_token=len_token;
        coppia->info_value=info_value;

        //inserimento della coppia in una coda del thread scelto grazie alla funzione hash

        size_t val_hash = hash(coppia->token, coppia->len_token, mr->attributi->hash_arg);
        size_t indice = val_hash % num_thread;

        Coda_reducer_t *thread_bersaglio = array_coda_thread_worker[indice];
        Coppia_t **coda_coppie = thread_bersaglio->coda_reducer;
        size_t capacity_coda_bersaglio = thread_bersaglio->capacity;
        
        SYSTHCALLC(mtx_lock(&(thread_bersaglio->mtx_coda)), "errore mtx_lock coda del thread bersaglio",{
            scrivi_log(mr,"errore mtx_lock coda del thread bersaglio", "REDUCER", 1);
            segnala_errore_reducer(array_coda_thread_worker);
            libera_coppia(coppia);
            scrivi_log(mr, "terminato con errore reader reducer thread", "REDUCER", 1);
        });

        while(thread_bersaglio->count==capacity_coda_bersaglio && !atomic_load(&errore)){
            SYSTHCALLC(cnd_wait(&(thread_bersaglio->full), &(thread_bersaglio->mtx_coda)), "errore cnd_wait thread bersaglio", {
                scrivi_log(mr, "errore cnd_wait thread bersaglio", "REDUCER",1);
                mtx_unlock(&(thread_bersaglio->mtx_coda));
                libera_coppia(coppia);
                segnala_errore_reducer(array_coda_thread_worker);
                scrivi_log(mr, "terminato con errore reader reducer thread", "REDUCER", 1);
            });
        }

        //se viene rivelato un errore, la coppia creata deve essere liberata e non inserita in una coda

        if(atomic_load(&errore)){
            libera_coppia(coppia);
            mtx_unlock(&(thread_bersaglio->mtx_coda));
            scrivi_log(mr, "terminato con errore reader reducer thread", "REDUCER", 1);
            return -1;
        }

        coda_coppie[thread_bersaglio->tail]=coppia;
        thread_bersaglio->tail = ((thread_bersaglio->tail) + 1) % capacity_coda_bersaglio;
        thread_bersaglio->count++;
        
        SYSTHCALLC(cnd_signal(&(thread_bersaglio->empty)), "errore cnd_signal empty thread bersaglio",{
            scrivi_log(mr, "errore cnd_signal empty thread bersaglio", "REDUCER",1);
            scrivi_log(mr, "terminato con errore reader reducer thread", "REDUCER", 1);
        });

        SYSTHCALLC(mtx_unlock(&(thread_bersaglio->mtx_coda)), "errore mtx_unlock thread bersaglio", {
            scrivi_log(mr, "errore mtx_unlock thread bersaglio", "REDUCER", 1);
            scrivi_log(mr, "terminato con errore reader reducer thread", "REDUCER", 1);
        });
        mr->contatore_coppie++; //fuori dalla lock, solamente il thread reader ha il ruolo di incrementare il contatore coppie
    }
}



static int emit(const char *token, const void *result, size_t result_size, void *emit_arg){
    Args_reducer_t *args_reducer = (Args_reducer_t*) emit_arg;
    Coda_reducer_t* arg_coda = args_reducer->coda;
    mr_t mr = arg_coda->mr;
    size_t len_token = strlen(token);
    size_t id_thread = args_reducer->thrd_id;

    //devo passare le lunghezze come interi nella pipe, quindi devo anche controllare la loro lunghezza non superi il limite
    if(len_token > MAX_LUNGHEZZA_TOKEN){
        scrivi_log(mr, "errore token supera limite in emit reducer", "REDUCER", id_thread);
        segnala_errore_reducer(args_reducer->Array_coda_thread_worker);
        return -1;
    }   
    if(result_size > MAX_LUNGHEZZA_RESULT){
        scrivi_log(mr, "errore result supera limite in emit reducer", "REDUCER", id_thread);
        segnala_errore_reducer(args_reducer->Array_coda_thread_worker);
        return -1;
    }
    mr_pair_header_red_to_main_t header_msg;
    header_msg.token_len = (int)len_token;
    header_msg.result_len = (int)result_size;

    char *msg;
    size_t len_msg = sizeof(mr_pair_header_red_to_main_t) + header_msg.token_len + header_msg.result_len;
    SYSNCALLC(msg = malloc(len_msg), "errore malloc messaggio in emit reducer",{
        scrivi_log(mr, "errore malloc messaggio in emit reducer", "REDUCER", id_thread);
        segnala_errore_reducer(args_reducer->Array_coda_thread_worker);
    });


    header_msg.result_len=result_size;
    header_msg.token_len=len_token;    

    size_t offset=0;
    memcpy(msg, &header_msg, sizeof(mr_pair_header_red_to_main_t));
    offset+=sizeof(mr_pair_header_red_to_main_t);

    memcpy(msg+offset, token, len_token);
    offset+=len_token;
    if(result_size>0)memcpy(msg+offset, result, result_size); //solamente se la result ha lunghezza positiva copio la result

    SYSTHCALLC(mtx_lock(arg_coda->mtx_pipe), "errore mtx_lock pipe in emit reducer", {
        scrivi_log(mr, "errore mtx_lock pipe in emit reducer", "REDUCER", id_thread);
        free(msg);
        segnala_errore_reducer(args_reducer->Array_coda_thread_worker);
    });

    ssize_t n_write_msg = writen(STDOUT_FILENO, msg, len_msg);
    if(n_write_msg <0 || (size_t)n_write_msg != len_msg){
        scrivi_log(mr, "errore writen in emit reducer", "REDUCER", id_thread);
        mtx_unlock(arg_coda->mtx_pipe);
        free(msg);
        segnala_errore_reducer(args_reducer->Array_coda_thread_worker);
        return -1;
    }
    

    SYSTHCALLC(mtx_unlock(arg_coda->mtx_pipe), "errore mtx_unlock pipe in emit reducer", {
        scrivi_log(mr, "errore mtx_unlock pipe in emit reducer", "REDUCER", id_thread);
        free(msg);
        segnala_errore_reducer(args_reducer->Array_coda_thread_worker);
    });

    free(msg);
    return 0;
}
 



//struct utilizzati per creare le coppie <token, lista_values>
//utilizzati per poter viaggiare nei vari token associati al singolo thread worker e ad assegnare al token corretto la value corrispettiva

typedef struct Nodo_value{
    struct Nodo_value *next;
    void *value;
    size_t value_size;
}Nodo_value_t;

typedef struct Nodo_token{
    struct Nodo_token *next;
    char *token;
    Nodo_value_t *Head_lista_value;
    size_t count_values;
}Nodo_token_t;


static Nodo_token_t *trova_token(Nodo_token_t *head, char *token){
    while(head!=NULL){
        if(strcmp(head->token, token)==0){
            return head;
        }
        else{
            head = head->next;
        }
    }
    return NULL;
}

static int crea_token_nodo(Nodo_token_t **head_ptr, char *token, Args_reducer_t *arg_ptr){
    size_t id_thread = arg_ptr->thrd_id;
    mr_t mr = arg_ptr->coda->mr;
    size_t len = strlen(token);

    Nodo_token_t *nodo;
    SYSNCALLC(nodo=malloc(sizeof(Nodo_token_t)), "errore creazione nodo token", {
        scrivi_log(mr, "errore creazione nodo token", "REDUCER", id_thread);
        segnala_errore_reducer(arg_ptr->Array_coda_thread_worker);
    });
    
    SYSNCALLC(nodo->token = malloc(len + 1), "errore malloc token in crea_token_nodo", {
        scrivi_log(mr, "errore malloc token in crea_token_nodo", "REDUCER", id_thread);
        free(nodo);
        segnala_errore_reducer(arg_ptr->Array_coda_thread_worker);
    });
    memcpy(nodo->token, token, len + 1);
    
    nodo->count_values=0;
    nodo->Head_lista_value=NULL;
    nodo->next=*head_ptr;
    *head_ptr = nodo; 
    return 0;
}

static int crea_value_nodo(Nodo_value_t **head_ptr, const void *value, size_t value_size, Args_reducer_t *arg_ptr){
    size_t id_thread = arg_ptr->thrd_id;
    mr_t mr = arg_ptr->coda->mr;

    Nodo_value_t *nodo;
    SYSNCALLC(nodo = malloc(sizeof(Nodo_value_t)), "errore creazione nodo value", {
        scrivi_log(mr, "errore creazione nodo value", "REDUCER", id_thread);
        segnala_errore_reducer(arg_ptr->Array_coda_thread_worker);
    });

    if(value_size==0){
        nodo->value= NULL;
    }
    else{
        void *value_copy;
        SYSNCALLC(value_copy = malloc(value_size), "errore malloc copia value nel nodo value", {
            scrivi_log(mr, "errore copia value nel nodo value", "REDUCER", id_thread);
            segnala_errore_reducer(arg_ptr->Array_coda_thread_worker);
            free(nodo);
        });
        memcpy(value_copy, value, value_size);
        nodo->value=value_copy;
    }

    nodo->value_size=value_size;
    nodo->next = *head_ptr;
    *head_ptr = nodo;
    return 0;
}


static void free_token_value_list(Nodo_token_t **head){
    while((*head)!=NULL){
        Nodo_value_t *nodo_value = (*head)->Head_lista_value;
        while(nodo_value != NULL){
            free(nodo_value->value);
            Nodo_value_t *nodo_value_succ = nodo_value->next;
            free(nodo_value);
            nodo_value = nodo_value_succ;
        }
        Nodo_token_t *nodo_token_succ = (*head)->next;
        free((*head)->token);
        free(*head);
        (*head) = nodo_token_succ;
    }
}

static void free_lista_arg_thread(Args_reducer_t **lista, size_t num){
    for(size_t i=0; i<num;i++){
        free(lista[i]);
    }
}

static int esegui_reduce(mr_t mr, Nodo_token_t **head_token_list, Args_reducer_t *arg_ptr){
    size_t id_thread = arg_ptr->thrd_id;
    size_t token_distinti_thread = 0;
    
    while(*head_token_list != NULL){
        token_distinti_thread++;
        mr_value_t *values;
        SYSNCALLC(values = malloc(sizeof(mr_value_t) * (*head_token_list)->count_values), "errore malloc values per la funzione reducer",{
            scrivi_log(mr, "errore malloc values per funzione reducer", "REDUCER", id_thread);
            segnala_errore_reducer(arg_ptr->Array_coda_thread_worker);
            free_token_value_list(head_token_list);
            scrivi_log(mr, "terminato con errore worker reducer thread", "REDUCER", id_thread);
        });
        Nodo_value_t *value_nodo = (*head_token_list)->Head_lista_value;
        for(size_t i = 0; i < (*head_token_list)->count_values; i++){
            values[i].data = value_nodo->value;
            values[i].size = value_nodo->value_size;
            value_nodo = value_nodo->next;
        }
        if(mr->reducer_fun((*head_token_list)->token, values, (*head_token_list)->count_values, emit, arg_ptr, mr->user_arg) == -1){
            scrivi_log(mr, "errore chiamata funzione reducer", "REDUCER", id_thread);
            segnala_errore_reducer(arg_ptr->Array_coda_thread_worker);
            free_token_value_list(head_token_list);
            free(values);
            scrivi_log(mr, "terminato con errore worker reducer thread", "REDUCER", id_thread);
            return -1;
        }
        free(values);
        Nodo_token_t *nodo_tok_succ = (*head_token_list)->next;
        Nodo_value_t *nodo_val = (*head_token_list)->Head_lista_value;
        while(nodo_val != NULL){
            Nodo_value_t *nodo_val_succ = nodo_val->next;
            free(nodo_val->value);
            free(nodo_val);
            nodo_val = nodo_val_succ;
        }
        free((*head_token_list)->token);
        free(*head_token_list);
        *head_token_list = nodo_tok_succ;
    }
    atomic_fetch_add(&mr->contatore_token_distinti, token_distinti_thread);
    scrivi_log(mr, "terminato con successo worker reducer thread", "REDUCER", id_thread);
    return 0;
}

static int reducer_worker_main(void *arg){
    Args_reducer_t *arg_ptr = (Args_reducer_t*) arg;
    scrivi_log(arg_ptr->coda->mr, "avvio worker reducer thread", "REDUCER", arg_ptr->thrd_id);
    Coda_reducer_t *coda_arg = arg_ptr->coda;
    Coppia_t **coda = coda_arg->coda_reducer;
    size_t capacity = coda_arg->capacity;
    size_t id_thread = arg_ptr->thrd_id;
    mr_t mr = coda_arg->mr;
    Nodo_token_t *head_token_list = NULL;

    while(1){
        SYSTHCALLC(mtx_lock(&coda_arg->mtx_coda), "errore mtx_lock di mtx_coda", {
            scrivi_log(mr, "errore mtx_lock di mtx_coda", "REDUCER", id_thread);
            segnala_errore_reducer(arg_ptr->Array_coda_thread_worker);
            free_token_value_list(&head_token_list);
            scrivi_log(mr, "terminato con errore worker reducer thread", "REDUCER", id_thread);
        });

        while(coda_arg->count == 0 && !atomic_load(&eof) && !atomic_load(&errore)){
            SYSTHCALLC(cnd_wait(&coda_arg->empty, &coda_arg->mtx_coda), "errore cnd_wait", {
                scrivi_log(mr, "errore cnd_wait di empty", "REDUCER", id_thread);
                mtx_unlock(&coda_arg->mtx_coda);
                segnala_errore_reducer(arg_ptr->Array_coda_thread_worker);
                free_token_value_list(&head_token_list);
                scrivi_log(mr, "terminato con errore worker reducer thread", "REDUCER", id_thread);
            });
        }

        if(atomic_load(&errore)){
            mtx_unlock(&coda_arg->mtx_coda);
            free_token_value_list(&head_token_list);
            scrivi_log(mr, "terminato con errore worker reducer thread", "REDUCER", id_thread);
            return -1;
        }

        // controllo EOF con coda vuota dentro il lock
        if(atomic_load(&eof) && coda_arg->count == 0){
            mtx_unlock(&coda_arg->mtx_coda);
            return esegui_reduce(mr, &head_token_list, arg_ptr);
        }

        //caso in cui la coda non è vuota
        Coppia_t *coppia_pescata = coda[coda_arg->head];
        coda_arg->head = (coda_arg->head + 1) % capacity;
        coda_arg->count--;
        cnd_signal(&coda_arg->full);
        
        size_t num_el_coda = coda_arg->count;
        SYSTHCALLC(mtx_unlock(&coda_arg->mtx_coda), "errore mtx_unlock", {
            scrivi_log(mr, "errore mtx_unlock di mtx_coda", "REDUCER", id_thread);
            segnala_errore_reducer(arg_ptr->Array_coda_thread_worker);
            free_token_value_list(&head_token_list);
            scrivi_log(mr, "terminato con errore worker reducer thread", "REDUCER", id_thread);
        });

        Nodo_token_t *nodo_token = trova_token(head_token_list, coppia_pescata->token);
        if(nodo_token == NULL){
            if(crea_token_nodo(&head_token_list, coppia_pescata->token, arg_ptr) == -1){
                libera_coppia(coppia_pescata);
                free_token_value_list(&head_token_list);
                scrivi_log(mr, "terminato con errore worker reducer thread", "REDUCER", id_thread);
                return -1;
            }
            if(crea_value_nodo(&head_token_list->Head_lista_value, coppia_pescata->info_value.data, coppia_pescata->info_value.size, arg_ptr) == -1){
                free_token_value_list(&head_token_list);
                scrivi_log(mr, "terminato con errore worker reducer thread", "REDUCER", id_thread);
                return -1;
            }
            head_token_list->count_values++;
        } else {
            if(crea_value_nodo(&nodo_token->Head_lista_value, coppia_pescata->info_value.data, coppia_pescata->info_value.size, arg_ptr) == -1){
                free_token_value_list(&head_token_list);
                scrivi_log(mr, "terminato con errore worker reducer thread", "REDUCER", id_thread);
                return -1;
            }
            nodo_token->count_values++;
        }
        libera_coppia(coppia_pescata);

        //secondo punto: dopo inserimento, se eof e coda vuota
        if(atomic_load(&eof) && num_el_coda == 0){
            return esegui_reduce(mr, &head_token_list, arg_ptr);
        }
    }
}




static int controlla_writen(mr_t mr, ssize_t dim_ottenuto, size_t dim_corretto, char *msg, Coda_reducer_t **array_coda_thread_worker, size_t num_thread_reducer_worker, mtx_t *mutex_pipe, Args_reducer_t **arg_thrd_lista){
     
    if(dim_ottenuto<0 || (size_t)dim_ottenuto != dim_corretto){
        scrivi_log(mr, msg, "REDUCER", 0);
        mtx_cnd_coda_destroy(array_coda_thread_worker, num_thread_reducer_worker, mutex_pipe);
        free(array_coda_thread_worker);
        free_lista_arg_thread(arg_thrd_lista, num_thread_reducer_worker);
        close(STDOUT_FILENO);
        return -1;
    }
    return 0;
}


int reducer_process_main(mr_t mr){
    Coda_reducer_t **array_coda_thread_worker;
    size_t num_thread_reducer_worker = mr->attributi->reducer_threads;
    size_t capacity = mr->attributi->queue_size;
    SYSNCALLC(array_coda_thread_worker=malloc(sizeof(Coda_reducer_t*) * num_thread_reducer_worker), "errore malloc array_coda_thread_worker", {
        scrivi_log(mr, "errore malloc array_coda_thread_worker", "REDUCER", 0);
        close(STDOUT_FILENO);
    });

    //inizializzo mutex_pipe fuori dal for perché tutti i thread presentano la stessa mutex per la pipe
    mtx_t mutex_pipe;
    SYSTHCALLC(mtx_init(&mutex_pipe, mtx_plain), "errore mtx_init mutex_pipe in reducer",{
        scrivi_log(mr, "errore mtx_init di mutex_pipe in reducer", "REDUCER", 0);
        free(array_coda_thread_worker);
        close(STDOUT_FILENO);
    });



    //creazione della coda_reducer_t per ogni thread.
    for(size_t i=0;i<num_thread_reducer_worker;i++){
        SYSNCALLC(array_coda_thread_worker[i] = malloc(sizeof(Coda_reducer_t)), "errore malloc coda_reducer_t", {
            scrivi_log(mr, "errore malloc coda_reducer_t", "REDUCER", 0);
            mtx_cnd_coda_destroy(array_coda_thread_worker, i, &mutex_pipe);
            free(array_coda_thread_worker);
            close(STDOUT_FILENO);
        });

        Coppia_t **coda;
        SYSNCALLC(coda=malloc(sizeof(Coppia_t*) * capacity), "Errore malloc coda thread reducer", {
            scrivi_log(mr, "errore malloc coda thread reducer", "REDUCER", 0);
            mtx_cnd_coda_destroy(array_coda_thread_worker, i, &mutex_pipe);
            free(array_coda_thread_worker[i]);
            free(array_coda_thread_worker);
            close(STDOUT_FILENO);
        });

        SYSTHCALLC(mtx_init(&(array_coda_thread_worker[i]->mtx_coda), mtx_plain), "errore mtx_init di mtx_coda thread reducer",{
            scrivi_log(mr, "errore mtx_init di mtx_coda thread reducer", "REDUCER", 0);
            mtx_cnd_coda_destroy(array_coda_thread_worker, i, &mutex_pipe);
            free(array_coda_thread_worker[i]);
            free(array_coda_thread_worker);
            free(coda);
            close(STDOUT_FILENO);
        });

        SYSTHCALLC(cnd_init(&(array_coda_thread_worker[i]->empty)),"errore cnd_init di empty thread reducer",{
            scrivi_log(mr, "errore cnd_init di empty thread reducer", "REDUCER", 0);
            mtx_destroy(&(array_coda_thread_worker[i]->mtx_coda));
            mtx_cnd_coda_destroy(array_coda_thread_worker, i, &mutex_pipe);
            free(array_coda_thread_worker[i]);
            free(array_coda_thread_worker);
            free(coda);
 
            close(STDOUT_FILENO);
        });

        SYSTHCALLC(cnd_init(&(array_coda_thread_worker[i]->full)),"errore cnd_init di full thread reducer",{
            scrivi_log(mr, "errore cnd_init di full thread reducer", "REDUCER", 0);
            mtx_destroy(&(array_coda_thread_worker[i]->mtx_coda));
            cnd_destroy(&(array_coda_thread_worker[i]->empty));
            mtx_cnd_coda_destroy(array_coda_thread_worker, i, &mutex_pipe);
            free(array_coda_thread_worker[i]);
            free(array_coda_thread_worker);
            free(coda);
            close(STDOUT_FILENO);
        });

        array_coda_thread_worker[i]->capacity = capacity;
        array_coda_thread_worker[i]->head=0;
        array_coda_thread_worker[i]->tail=0;
        array_coda_thread_worker[i]->count=0;
        array_coda_thread_worker[i]->mtx_pipe=&mutex_pipe;
        array_coda_thread_worker[i]->coda_reducer=coda;
        array_coda_thread_worker[i]->mr=mr;
    }


    thrd_t thread_lista[num_thread_reducer_worker+1];
    int ret_create;
    ret_create=thrd_create(&(thread_lista[0]), reader_reducer, array_coda_thread_worker);
    SYSTHCALLC(ret_create, "errore creazione thread reader reducer",{
        scrivi_log(mr, "errore creazione thread reader", "REDUCER", 0);
        mtx_cnd_coda_destroy(array_coda_thread_worker, num_thread_reducer_worker, &mutex_pipe);
        free(array_coda_thread_worker);
        close(STDOUT_FILENO);
    });

    Args_reducer_t *arg_thrd_lista[num_thread_reducer_worker];
    size_t thread_creati = 1;
    for(size_t i=0; i<num_thread_reducer_worker;i++){
        SYSNCALLC(arg_thrd_lista[i]=malloc(sizeof(Args_reducer_t)), "errore malloc argomenti per singolo thread", {
            scrivi_log(mr, "errore malloc argomenti per singolo thread", "REDUCER", 0);
            segnala_errore_reducer(array_coda_thread_worker);
            break;
        });
        arg_thrd_lista[i]->Array_coda_thread_worker=array_coda_thread_worker;
        arg_thrd_lista[i]->thrd_id=i+2; //thrd_id = 0 --> thread principale, thrd_id = 1--> thread lettura
        arg_thrd_lista[i]->coda = array_coda_thread_worker[i];

        if(thrd_create(&(thread_lista[i+1]), reducer_worker_main, arg_thrd_lista[i]) == thrd_error){
            scrivi_log(mr, "errore creazione thread mapper worker", "MAPPER", 0);
            segnala_errore_reducer(array_coda_thread_worker);
            free(arg_thrd_lista[i]);
            break;
        }
        thread_creati++;
    }

    if(thread_creati != (num_thread_reducer_worker+1)){
        for(size_t i=0;i<thread_creati;i++){
            if(thrd_join(thread_lista[i],NULL)==thrd_error){
                scrivi_log(mr, "errore terminazione thread in mapper nella pulizia thread in cui thread creati < thread richiesti", "REDUCER", i+1);
            }
        }
        close(STDOUT_FILENO);
        mtx_cnd_coda_destroy(array_coda_thread_worker, num_thread_reducer_worker, &mutex_pipe);
        free(array_coda_thread_worker);
        free_lista_arg_thread(arg_thrd_lista, thread_creati-1);
        return -1;
    }


    scrivi_log(mr, "creazione di tutti i thread", "REDUCER", 0);

    int risultato;
    int errore_trovato=0;

    //attesa terminazione thread reader
    if(thrd_join(thread_lista[0], &risultato)==thrd_error){
        scrivi_log(mr, "errore terminazione thread reader", "REDUCER", 1);
        errore_trovato=-1;
    }
    if(risultato ==-1)errore_trovato=-1;


    //attesa terminazione thread worker
    for(size_t i=1;i<num_thread_reducer_worker+1;i++){
        if(thrd_join(thread_lista[i], &risultato)==thrd_error){
            scrivi_log(mr, "errore terminazione thread worker", "REDUCER", i+1);
            errore_trovato=-1;
        }
        if(risultato ==-1)errore_trovato=-1;
    }


    scrivi_log(mr, "terminazione di tutti i thread", "REDUCER", 0);

    char msg[64];
    sprintf(msg, "numero di token distinti: %zu", atomic_load(&mr->contatore_token_distinti));
    scrivi_log(mr, msg, "REDUCER", 0);


    //scrittura nella pipe dei contatori coppia e token distinti per il file delle statistiche
    mr_pair_header_red_to_main_t header_finale;
    header_finale.result_len=0;
    header_finale.token_len=0;

    ssize_t n_write_header_finale = writen(STDOUT_FILENO, &header_finale, sizeof(mr_pair_header_red_to_main_t));
    if(controlla_writen(mr, n_write_header_finale, sizeof(mr_pair_header_red_to_main_t),"errore scrittura header_finale", array_coda_thread_worker, num_thread_reducer_worker, &mutex_pipe, arg_thrd_lista) ==-1)return -1;
    
    ssize_t n_write_contatore_coppia = writen(STDOUT_FILENO, &(mr->contatore_coppie), sizeof(size_t));
    if(controlla_writen(mr,  n_write_contatore_coppia, sizeof(size_t),"errore scrittura contatore coppia nella pipe", array_coda_thread_worker, num_thread_reducer_worker, &mutex_pipe, arg_thrd_lista) ==-1)return -1;
    
    size_t token_distinti = atomic_load(&(mr->contatore_token_distinti));
    ssize_t n_write_contatore_token_distinti = writen(STDOUT_FILENO, &token_distinti, sizeof(size_t));
    if(controlla_writen(mr,  n_write_contatore_token_distinti, sizeof(size_t),"errore scrittura contatore token distinti nella pipe", array_coda_thread_worker, num_thread_reducer_worker, &mutex_pipe, arg_thrd_lista) ==-1)return -1;

    close(STDOUT_FILENO); //segnalare EOF al main

    if(errore_trovato==-1){
        mtx_cnd_coda_destroy(array_coda_thread_worker, num_thread_reducer_worker, &mutex_pipe);
        free(array_coda_thread_worker);
        free_lista_arg_thread(arg_thrd_lista, num_thread_reducer_worker);
        return -1;
    }
    mtx_cnd_coda_destroy(array_coda_thread_worker,num_thread_reducer_worker, &mutex_pipe);
    free(array_coda_thread_worker);
    free_lista_arg_thread(arg_thrd_lista, num_thread_reducer_worker);
    return 0;

}