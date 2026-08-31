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

#define N_MAPPER_THREADS_DEFAULT 1
#define N_REDUCER_THREADS_DEFAULT 1
#define DIM_QUEUE_DEFAULT 64
#define NAME_LOG_DEFAULT "mr.log"
#define NAME_STAT_FILE "statistiche_finali.txt"
#define NAME_SEM_LOG "/sem_log"
#define NAME_SEM_STAT "/sem_stat"
#define CLEANUP_LETTURA \
    libera_nodi_risultati(head); \
    close(mr->reducer_to_main[0]); \
    waitpid(mr->pid_mapper, NULL, 0); \
    waitpid(mr->pid_reducer, NULL, 0)

/*
Dal momento che vi possono essere più elaborazioni nello stesso processo chiamante,
è necessario conoscere il numero di elaborazioni attive per poter determinare quando eseguire l'unlink dei semafori.
*/

static atomic_size_t numero_elaborazioni_create = 0; //per ADDENDUM, per distinguere le diverse elaborazioni.

static atomic_size_t numero_elaborazioni_attive =0;


//funzione utile da chiamare ogniqualvolta è necessario ricavare il tempo corrente, come per la scrittura nel log o nel file delle statistiche
static char* tempo_corrente(){
    char *risultato;
    SYSNCALL_PTR(risultato=malloc(64), "malloc per creazione tempo corrente");
    char tempo_no_ms[64];
    struct timespec tempo;
    struct tm tempo_leggibile; //presenta gli attributi necessari per formattare il tempo in una struttura classica per il file log

    SYSTIMECALLC_PTR(timespec_get(&tempo, TIME_UTC), "errore in timespec_get", {free(risultato);});

    //uso localtime_r rispetto a localtime perché quest'ultimo non è thread safe dal momento che usa un buffer statico
    SYSNCALLC_PTR(localtime_r(&tempo.tv_sec, &tempo_leggibile), "localtime", {free(risultato);});

    int millisecondi = tempo.tv_nsec/1000000;

    SYSTIMECALLC_PTR(strftime(tempo_no_ms, sizeof(tempo_no_ms), "%d-%m-%Y %H:%M:%S", &tempo_leggibile), "strftime nel tempo corrente()", {free(risultato);});

    SYSCALLC_PTR(snprintf(risultato,64, "%s.%03d", tempo_no_ms, millisecondi), "sprintf nel tempo corrente()", {free(risultato);});
    return risultato;
}


//funzione per scrivere all'interno del file log
void scrivi_log(mr_t mr, char *messaggio, char *processo, size_t numero_thread){
    /*
    formato dei messaggi del log ha la forma
    [timestamp] [numero elaborazione] [processo] [thread] messaggio
    */
    sem_wait(mr->log_sem);
    FILE *f_log = fopen(mr->attributi->log_file, "a");
    char* timestamp;
    timestamp = tempo_corrente();
    if(timestamp==NULL){
        sem_post(mr->log_sem);
        return;
    }

    fprintf(f_log, "[%s] [%zu] [%s] [%zu] [%s]\n", timestamp, mr->numero_elaborazione, processo, numero_thread, messaggio);
    free(timestamp);
    fclose(f_log);
    sem_post(mr->log_sem);
    return;
}





//funzione per chiudere i descrittori inutilizzati nei processi figli
static int chiudi_fd_inutilizzati(){
    
    int maxfd = (int)sysconf(_SC_OPEN_MAX);
    if(maxfd < 0) maxfd = 1024;
    for(int fd = 3; fd < maxfd; fd++){
        close(fd); /* se il fd non è aperto ritorna EBADF, ignorato */
    }
    return 0;
}

//hash_default utilizzata è la Additive Hash
size_t hash_default(const char *token, size_t token_len, void *hash_arg){  //non è necessario controllare che token sia NULL, perché la hash viene chiamata durante l'esecuzione del processo reducer, e se è arrivato a quel punto del codice, significa che si sono già fatti i controlli sulla token
    (void)hash_arg;
    size_t ris=0;
    for(size_t i=0;i<token_len;i++){
        ris += (unsigned char)token[i]; //unsigned char per garantire che il valore ottenuto sia positivo.
    }
    return ris;
}


int mr_attr_init(mr_attr_t *attr){
    if(attr == NULL)return -1;
    attr->mapper_threads=N_MAPPER_THREADS_DEFAULT;
    attr->reducer_threads=N_REDUCER_THREADS_DEFAULT;
    attr->log_file=NAME_LOG_DEFAULT;
    attr->queue_size=DIM_QUEUE_DEFAULT;

    attr->hash=hash_default;
    attr->hash_arg=NULL;
    return 0;
}

int mr_attr_set_mapper_threads(mr_attr_t *attr, size_t n){
    if(attr==NULL || n==0)return -1;
    attr->mapper_threads=n;
    return 0;
}

int mr_attr_set_reducer_threads(mr_attr_t *attr, size_t n){
    if(attr==NULL || n==0)return -1;
    attr->reducer_threads=n;
    return 0;
}

int mr_attr_set_queue_size(mr_attr_t *attr, size_t n){
    if(attr==NULL || n==0)return -1;
    attr->queue_size=n;
    return 0;
}

int mr_attr_set_log_file(mr_attr_t *attr, const char *path){
    if(attr==NULL)return -1;
    attr->log_file = (path!=NULL) ? path : NAME_LOG_DEFAULT;
    return 0;
}

int mr_attr_set_hash_function(mr_attr_t *attr,mr_hash_t hash,void *hash_arg){
    if(attr==NULL)return -1;
    attr->hash_arg=hash_arg;
    attr->hash = (hash != NULL) ? hash : hash_default;
    return 0;
}


int mr_create(mr_t *mr, const mr_attr_t *attr, mr_mapper_t mapper, mr_reducer_t reducer, void *user_arg){
    if(mr == NULL || attr==NULL || reducer==NULL || mapper == NULL)return -1;


    /*
    nel programma utente, mr è soltanto dichiarato, quindi è un wild pointer
    necessario allocare la memoria a cui punterà mr
    */
    SYSNCALL((*mr)=malloc(sizeof(struct mr)), "malloc per struct mr");  //mr è puntatore al tipo mr_t che a sua volta è un puntatore al tipo mr


    
    (*mr)->numero_elaborazione=atomic_fetch_add(&numero_elaborazioni_create,1)+1; //perché atomic_fetch_add restituisce il valore precedente

    atomic_fetch_add(&numero_elaborazioni_attive,1); //non presenta valore di errore da controllare

    (*mr)->mapper_fun=mapper;
    (*mr)->reducer_fun=reducer;
    (*mr)->user_arg=user_arg;

    mr_attr_t *mr_attr;
    SYSNCALLC(mr_attr=malloc(sizeof(mr_attr_t)), "malloc per mr_attr", {
        free(*mr);
        atomic_fetch_sub(&numero_elaborazioni_attive,1);
    });  //prima di uscire dalla funzione, libero la memoria dinamica

    mr_attr->mapper_threads=attr->mapper_threads;
    mr_attr->reducer_threads=attr->reducer_threads;
    mr_attr->queue_size=attr->queue_size;
    mr_attr->hash = attr->hash;
    mr_attr->hash_arg=attr->hash_arg;

    char* log_name;
    SYSNCALLC(log_name=strdup(attr->log_file), "strdup copia profonda di log_name in mr_create", {
        free(*mr);free(mr_attr);
        atomic_fetch_sub(&numero_elaborazioni_attive,1);
    });
    mr_attr->log_file=log_name;


    /*
    Necessario l'utilizzo di semafori per gestire sincronizzazione dell'accesso al file log.
    */

    SYSSEMCALLC((*mr)->log_sem=sem_open(NAME_SEM_LOG, O_CREAT, 0666,1), "errore apertura semaforo log",{
        free(mr_attr);free(log_name);
        atomic_fetch_sub(&numero_elaborazioni_attive,1);
        free(*mr);
    });
    SYSSEMCALLC((*mr)->stat_sem=sem_open(NAME_SEM_STAT, O_CREAT, 0666,1), "errore apertura semaforo statistiche",{
        free(mr_attr);free(log_name);

        SYSCALL(sem_close((*mr)->log_sem), "errore sem_close di log_sem nella gestione errore di sem_open del sem_stat");
        if(atomic_fetch_sub(&numero_elaborazioni_attive,1)==1){
            SYSCALL(sem_unlink(NAME_SEM_LOG), "errore unlink di sem_log nella gestione errore di sem_open del sem_stat");
        }

        free(*mr);
    });   

    SYSNCALLC((*mr)->inizio = malloc(sizeof(struct timespec)), "errore malloc mr->inizio", {
        free(mr_attr);free(log_name);

        SYSCALL(sem_close((*mr)->stat_sem), "errore sem_close di stat_sem nella gestione errore di malloc tempo fine");
        SYSCALL(sem_close((*mr)->log_sem), "errore sem_close di log_sem nella gestione errore di malloc tempo inizio");
        if(atomic_fetch_sub(&numero_elaborazioni_attive,1)==1){
            SYSCALL(sem_unlink(NAME_SEM_LOG), "errore unlink di sem_log nella gestione errore di malloc tempo inizio");
            SYSCALL(sem_unlink(NAME_SEM_STAT), "errore unlink di sem_stat nella gestione errore di malloc tempo inizio");
        }

        free(*mr);
    });
    SYSNCALLC((*mr)->fine = malloc(sizeof(struct timespec)), "errore malloc mr->fine", {
        free(mr_attr);free(log_name);free((*mr)->inizio);

        SYSCALL(sem_close((*mr)->log_sem), "errore sem_close di log_sem nella gestione errore di malloc tempo fine");
        if(atomic_fetch_sub(&numero_elaborazioni_attive,1)==1){
            SYSCALL(sem_unlink(NAME_SEM_LOG), "errore unlink di sem_log nella gestione errore di malloc tempo fine");
            SYSCALL(sem_unlink(NAME_SEM_STAT), "errore unlink di sem_stat nella gestione errore di malloc tempo fine");
        }

        free(*mr);
    });

    (*mr)->contatore_coppie=0;
    (*mr)->contatore_righe_lette=0;
    (*mr)->contatore_risultati=0;
    (*mr)->contatore_token_distinti=0;

    (*mr)->attributi=mr_attr;
    (*mr)->f_stat = NAME_STAT_FILE;


    return 0;
}


int mr_attr_destroy(mr_attr_t *attr){
    if(attr==NULL)return -1;
   return 0;
}


int mr_destroy(mr_t mr){
    if(mr == NULL) return -1;
    free((void*)mr->attributi->log_file);
    free(mr->attributi);
    free(mr->inizio);
    free(mr->fine);
    sem_close(mr->stat_sem);
    sem_close(mr->log_sem);
    if(atomic_fetch_sub(&numero_elaborazioni_attive, 1) == 1){
        sem_unlink(NAME_SEM_LOG);
        sem_unlink(NAME_SEM_STAT);
    }
    free(mr);
    return 0;
}


//lettura riga, per gestire la situazione in cui la dimensione della riga supera la dimensione prefissata del buffer
static char* leggi_riga(mr_t mr, FILE *f) {
    size_t dim = 1024;
    size_t len = 0;
    char *buffer;
    SYSNCALLC_PTR(buffer=malloc(dim), "errore malloc buffer leggi_riga", {scrivi_log(mr, "errore malloc buffer leggi_riga", "MAIN", 0);});
    

    while (fgets(buffer + len, dim - len, f) != NULL) {
        len += strlen(buffer + len);

        if (len>0 && buffer[len - 1] == '\n') {
            return buffer;
        }

        if(feof(f))return buffer; //arrivato all'EOF e non è assicurato che l'ultima riga finisca con \n

        //viene raddoppiato il buffer se non trovo \n
        dim *= 2;
        char *temp;
        SYSNCALLC_PTR(temp=realloc(buffer,dim), "errore realloc del buffer in leggi_riga", {
            scrivi_log(mr, "errore realloc del buffer in leggi_riga", "MAIN", 0);
            free(buffer);
        });
        buffer = temp;
    }

    if (len == 0) { //len non è mai aumentata, significa che il file è vuoto.
        free(buffer);
        return NULL;
    }

    return buffer;
}

static char* serializza_riga(mr_t mr, const char *fp, char* riga, unsigned long numero_riga, size_t *dim_riga){
    //controlli per la lunghezza massima già presenti in scan_file_reg e in scansione_files
    size_t lunghezza_fp=strlen(fp);
    size_t lunghezza_riga=strlen(riga);
    if (lunghezza_riga > 0 && riga[lunghezza_riga - 1] == '\n') {
        riga[lunghezza_riga - 1] = '\0';
        lunghezza_riga--;
    }


    //prima calcolo dimensione del buffer   
    //dim = numero byte per lunghezza nome_file + strlen(nome_file) + numero byte per numero_riga + numero byte per lunghezza linea + lunghezza linea
    size_t dim=sizeof(int) + lunghezza_fp + sizeof(unsigned long) + sizeof(int) + lunghezza_riga;
    //uso unsigned long per numero_riga per renderlo coerente con la struct mr_file_line_t
    //progetto richiede che le lunghezze mandate mediante la pipe siano di tipo int
    
    *dim_riga=dim;

    char *buffer;
    SYSNCALLC_PTR(buffer=malloc(dim), "errore malloc buffer in serializza riga", {
        scrivi_log(mr, "errore malloc buffer in serializza riga", "MAIN", 0);
    });

    size_t offset=0;
    
    //copia della lunghezza nome file
    memcpy(buffer+offset, &lunghezza_fp, sizeof(int));
    offset +=sizeof(int);

    //copia del nome file
    memcpy(buffer+offset, fp, lunghezza_fp);
    offset+=lunghezza_fp;

    //copia numero riga
    memcpy(buffer+offset, &numero_riga, sizeof(unsigned long));
    offset+=sizeof(unsigned long);

    //copia lunghezza riga
    memcpy(buffer+offset, &lunghezza_riga, sizeof(int));
    offset+=sizeof(int);

    //copia riga
    memcpy(buffer+offset, riga, lunghezza_riga);
    
    return buffer;
}

ssize_t writen(int fd, void *buf, size_t n){
    size_t tot=0;
    ssize_t caratteri_scritti;
    char *buf_castato = buf;  //casting che permette di poter utilizzare l'aritmetica dei puntatori.
    while((caratteri_scritti=write(fd, buf_castato+tot,n-tot))>0){
        tot+=caratteri_scritti;
        if(tot==n)break;
    }
    if(tot!=n)return -1;
    return tot;
}

ssize_t readn(int fd, void *buf, size_t n){
    size_t tot=0;
    ssize_t caratteri_letti;
    char* buf_castato = buf;
    while((caratteri_letti=read(fd, buf_castato+tot, n-tot))>0){
        tot+=caratteri_letti;
        if(tot==n)break;
    }
    if(caratteri_letti == -1) return -1;
    return tot; // se tot==0 raggiunto EOF
}

//funzione che permette di leggere le righe di un file regolare
int scan_file_reg(mr_t mr, const char *fp){
    FILE *f_input;
    unsigned long numero_riga=0;
    SYSNCALLC(f_input=fopen(fp, "r"), "errore apertura file input", scrivi_log(mr, "errore apertura file input", "MAIN", 0));
    char msg[PATH_MAX + 24];
    SYSCALLC(snprintf(msg,sizeof(msg), "apertura del file %s", fp), "errore snprintf apertura file", scrivi_log(mr, "errore snprintf apertura file", "MAIN", 0));
    scrivi_log(mr, msg, "MAIN",0);
    char* riga;
    while((riga = leggi_riga(mr, f_input))!=NULL){
        if(strlen(riga)>MAX_LUNGHEZZA_RIGA){
            scrivi_log(mr,"riga ha superato la lunghezza massima documentata", "MAIN", 0);
            return -1;
        }
        numero_riga++;
        char *riga_serializzata;
        size_t dim_riga_serializzata;
        SYSNCALLC(riga_serializzata=serializza_riga(mr, fp, riga, numero_riga, &dim_riga_serializzata), "errore serializzazione riga per mapper", {scrivi_log(mr, "errore serializzazione riga per mapper", "MAIN", 0); free(riga);fclose(f_input);});
        if(writen(mr->main_to_mapper[1],riga_serializzata, dim_riga_serializzata)==-1){
            scrivi_log(mr, "errore write verso mapper", "MAIN", 0);
            free(riga);
            free(riga_serializzata);
            SYSCALLC(fclose(f_input), "errore fclose all'interno di scansione_files", scrivi_log(mr, "errore fclose all'interno di scansione_files", "MAIN", 0));
            char msg[100];
            SYSCALLC(snprintf(msg,sizeof(msg), "chiusura del file %s", fp), "errore snprintf chiusura file", scrivi_log(mr, "errore snprintf chiusura file", "MAIN", 0));
            scrivi_log(mr, msg, "MAIN",0);
            return -1;
        };
        mr->contatore_righe_lette++;
        free(riga);
        free(riga_serializzata);
    }
    if(riga==NULL && !feof(f_input)){
        scrivi_log(mr, "errore lettura riga", "MAIN", 0);
        SYSCALLC(fclose(f_input), "errore fclose all'interno di scansione_files", scrivi_log(mr, "errore fclose all'interno di scansione_files", "MAIN", 0));
        char msg_fine[100];
        SYSCALLC(snprintf(msg_fine,sizeof(msg_fine), "chiusura del file %s", fp), "errore snprintf chiusura file", scrivi_log(mr, "errore snprintf chiusura file", "MAIN", 0));
        scrivi_log(mr, msg_fine, "MAIN",0);
        return -1;
    }
    SYSCALLC(fclose(f_input), "errore fclose all'interno di scansione_files", scrivi_log(mr, "errore fclose all'interno di scansione_files", "MAIN", 0));
    char msg_fine[100];
    SYSCALLC(snprintf(msg_fine,sizeof(msg_fine), "chiusura del file %s", fp), "errore snprintf chiusura file", scrivi_log(mr, "errore snprintf chiusura file", "MAIN", 0));
    scrivi_log(mr, msg_fine, "MAIN",0);
    return 0;
}



//scansione files
static int scansione_files(mr_t mr,const char *fp){
    int stato_operazione = 0;
    if(fp==NULL)return -1;
    struct stat info;
    SYSCALLC(stat(fp, &info), "stat in scansione_files", scrivi_log(mr, "errore stat in scansione_files", "MAIN", 0)); //stat permette di salvare i metadati del input_path nella struct info

    if(S_ISREG(info.st_mode)){ //controllo se input_path è un file regolare
        return scan_file_reg(mr, fp);
    }
    if(S_ISDIR(info.st_mode)){
        struct dirent **arr_dirent_ptr;  //dove vengono salvati i vari puntatori ai metadati di ogni file che appartiene al primo livello della directory, in ordine lessicografico grazie a alphasort
        int len_arr;
        SYSCALLC(len_arr=scandir(fp, &arr_dirent_ptr, NULL, alphasort), "errore utilizzo scandir", scrivi_log(mr, "errore utilizzo scandir", "MAIN", 0));
        size_t len_arr_cast = len_arr;
        for(size_t i=0;i<len_arr_cast;i++){
            char *nome = arr_dirent_ptr[i]->d_name;
            if(strcmp(nome, ".")==0 || strcmp(nome, "..")==0)continue;  //per evitare la presenza di cicli infiniti.
            char path_completo[PATH_MAX]; //mi serve il path completo per determinare con stat se il file che sto considerando è regolare o una directory

            /*
            nelle SYSCALL in questo caso metto la break, perché così posso liberare tutta la memoria dinamica fuori dal ciclo anche se ho rilevato un errore.
            il programma saprà comunque che si è presentato un errore grazie alla variabile stato_operazione
            */

            int ris_path = snprintf(path_completo,sizeof(path_completo), "%s/%s", fp, nome);
            if(ris_path<0 || (size_t)ris_path >=sizeof(path_completo)){ //può capitare che il numero di byte da copiare sia più grande della dimensione del buffer.
                scrivi_log(mr, "erorre snprintf in scansione_files", "MAIN",0);
                stato_operazione=-1;
                break;
            }
            SYSCALLC(stat(path_completo, &info), "stat in scansione_files directory", {
                scrivi_log(mr, "errore stat in scansione_files directory", "MAIN", 0);
                stato_operazione=-1;
                break;
            });
            if(S_ISREG(info.st_mode)){
                if(scan_file_reg(mr, path_completo)==-1){
                    stato_operazione=-1;
                    break;
                }
                continue;
            }
            if(S_ISDIR(info.st_mode)){
                if(scansione_files(mr, path_completo)==-1){
                    stato_operazione=-1;
                    break;
                };
            }
        }
        for(size_t i=0;i<len_arr_cast;i++){
            free(arr_dirent_ptr[i]);  //libero memoria dinamica allocata con scandir
        }
        free(arr_dirent_ptr);
    }
    return stato_operazione;
}

typedef struct Nodo_risultato_t{
    struct Nodo_risultato_t *next;
    char *token;
    void *result;
    size_t size_result;
    size_t token_len;
}Nodo_risultato_t;

static int ordina_stringhe(const void *el1, const void *el2){
    const Nodo_risultato_t *nodo1 = *(const Nodo_risultato_t**)el1;  //la compar della qsort prende come argomenti i puntatori agli elementi dell'array, quindi Nodo_risultato**
    const Nodo_risultato_t *nodo2 = *(const Nodo_risultato_t**)el2;

    return strcmp(nodo1->token, nodo2->token);  //l'ordinamento di coppie <token, result> con token uguali e result diversi sarà inverso rispetto all'ordine di emissione dalla pipe red_to_main dal momento che l'inserimento dei nodi è in testa nella linked list
}




static void libera_nodi_risultati(Nodo_risultato_t *head){
    while(head!=NULL){
        Nodo_risultato_t *succ = head->next;
        free(head->result);
        free(head->token);
        free(head);
        head = succ;
    }
}


static int controlla_fwrite(mr_t mr, size_t dim_ottenuto, size_t dim_corretto, char *msg, Nodo_risultato_t *head, Nodo_risultato_t **arr, FILE* f_out){
    if(dim_ottenuto < dim_corretto){
        scrivi_log(mr, msg, "MAIN", 0);
        libera_nodi_risultati(head);
        free(arr);
        fclose(f_out);
        return -1;
    }
    return 0;
}

int mr_start(mr_t mr, const char *input_path, const char *output_path){

    if(input_path==NULL || strlen(input_path)> PATH_MAX || output_path==NULL || strlen(output_path)>PATH_MAX){
        scrivi_log(mr, "argomenti passati a mr_start non rispettano le condizioni stabilite", "MAIN", 0);
        return -1;
    }
    //partenza timer per calcolare tempo di esecuzione che sarà presente in f_stat
    SYSTIMECALLC(timespec_get(mr->inizio, TIME_UTC), "errore avvio del timer con timespec_get", scrivi_log(mr, "errore avvio del timer con timespec_get", "MAIN", 0));

    //Creazione Pipe
    SYSCALL(pipe((mr)->main_to_mapper), "pipe per main_to_mapper");
    if(pipe((mr)->mapper_to_reducer)==-1){
        perror("pipe per mapper_to_reducer");
        SYSCALL(close(((mr)->main_to_mapper)[0]), "close di main_to_mapper[0]");
        SYSCALL(close(((mr)->main_to_mapper)[1]), "close di main_to_mapper[1]");
        return -1;
    }
    if(pipe((mr)->reducer_to_main)==-1){
        perror("pipe per recuder_ro_main");
        SYSCALL(close(((mr)->main_to_mapper)[0]), "close di main_to_mapper[0]");   
        SYSCALL(close(((mr)->main_to_mapper)[1]), "close di main_to_mapper[1]");   
        SYSCALL(close(((mr)->mapper_to_reducer)[0]), "close di mapper_to_reducer[0]");
        SYSCALL(close(((mr)->mapper_to_reducer)[1]), "close di mapper_to_reducer[1]");
        return -1;
    }
    
    scrivi_log(mr,"creazione pipe", "MAIN", 0);


    SYSCALLC(mr->pid_mapper=fork(), "errore creazione figlio mapper", scrivi_log(mr, "errore creazione figlio mapper", "MAIN", 0));

    if(mr->pid_mapper==0){
        scrivi_log(mr, "creazione processo mapper", "MAPPER", 0);
        SYSCALLC(dup2(mr->main_to_mapper[0], STDIN_FILENO), "errore dup2 per redirezione STDIN di MAPPER", scrivi_log(mr,"errore dup2 per redirezione STDIN di MAPPER", "MAPPER", 0));
        SYSCALLC(dup2(mr->mapper_to_reducer[1], STDOUT_FILENO), "errore dup2 per redirezione STDOUT di MAPPER", scrivi_log(mr, "errore dup2 per redirezione STDOUT di MAPPER", "MAPPER", 0));

        chiudi_fd_inutilizzati();
        
        int ret=mapper_process_main(mr);  //esecuzione del processo mapper
        
        _exit(ret==0? 0 : EXIT_FAILURE);
    }

    SYSCALLC(mr->pid_reducer=fork(), "errore creazione figlio reducer", scrivi_log(mr, "errore creazione figlio reducer", "MAIN", 0));

    if(mr->pid_reducer==0){
        scrivi_log(mr, "creazione processo reducer", "REDUCER", 0);
        SYSCALLC(dup2(mr->mapper_to_reducer[0], STDIN_FILENO), "errore dup2 per redirezione STDIN di REDUCER", scrivi_log(mr, "errore dup2 per redirezione STDIN di REDUCER", "REDUCER", 0));
        SYSCALLC(dup2(mr->reducer_to_main[1], STDOUT_FILENO), "errore dup2 per redirezione STDOUT di REDUCER", scrivi_log(mr, "errore dup2 per redirezione STDOUT di REDUCER", "REDUCER", 0));

        chiudi_fd_inutilizzati();

        int ret=reducer_process_main(mr);
    
        _exit(ret==0? 0 : EXIT_FAILURE);
    }

    //chiusura descrittori non utilizzati nel processo principale
    SYSCALLC(close(mr->main_to_mapper[0]), "errore close main_to_mapper[0]", scrivi_log(mr, "errore close main_to_mapper[0]", "MAIN",0));
    SYSCALLC(close(mr->reducer_to_main[1]), "errore close reducer_to_main[1]", scrivi_log(mr, "errore close reducer_to_main[1]", "MAIN",0));
    SYSCALLC(close(mr->mapper_to_reducer[0]), "errore close mapper_to_reducer[0]", scrivi_log(mr, "errore close mapper_to_reducer[0]", "MAIN", 0));
    SYSCALLC(close(mr->mapper_to_reducer[1]), "errore close mapper_to_reducer[1]", scrivi_log(mr, "errore close mapper_to_reducer[1]", "MAIN", 0));



    //scansione files con ADDENDUM
    if(scansione_files(mr, input_path)==-1){
        scrivi_log(mr, "errore durante la scansione dei file", "MAIN", 0);
      
        
        SYSCALLC(close(mr->main_to_mapper[1]), "errore close main_to_mapper[1]", scrivi_log(mr, "errore close main_to_mapper[1]", "MAIN", 0)); //mapper riceve EOF sul proprio standard input
        SYSCALLC(close(mr->reducer_to_main[0]),"errore close reducer_to_main[0]", scrivi_log(mr, "errore close reducer_to_main[0]", "MAIN", 0)); //prevenire il deadlock segnalando al reducer che non c'è più nessun lettore
                                                                                                                                                //sennò si potrebbe presentare la situazione del buffer pieno e il reducer che si blocca aspettando che processo principale legga 
                                                                                                                                                //portando il processo principale ad essere bloccato nella waitpid
        SYSCALLC(waitpid(mr->pid_mapper, NULL, 0), "errore waitpid di mr->pid_mapper", scrivi_log(mr, "errore waitpid di mr->pid_mapper", "MAIN", 0));
        SYSCALLC(waitpid(mr->pid_reducer, NULL, 0), "errore waitpid di mr->pid_reducer", scrivi_log(mr, "errore waitpid di mr->pid_reducer", "MAIN", 0));

        return -1;
    }


    //terminata la scansione del file di input, mando il segnale di fine input al mapper
    SYSCALLC(close(mr->main_to_mapper[1]), "errore close main_to_mapper[1]", scrivi_log(mr, "errore close main_to_mapper[1]", "MAIN", 0));



    //lettura della pipe reducer_to_main, e creazione della linked list contenente le coppia <token, result>
    Nodo_risultato_t *head=NULL;
    mr_pair_header_red_to_main_t header;
    size_t contatore_coppie;
    size_t contatore_token_distinti;
    
    while(1){
        ssize_t n_header = readn(mr->reducer_to_main[0], &header, sizeof(mr_pair_header_red_to_main_t));
        if(n_header==0)break;
        SYSLETTMAINC(mr, n_header, sizeof(mr_pair_header_red_to_main_t), "errore lettura header nella reducer_to_main[0]", {CLEANUP_LETTURA;});
        if(header.token_len==0){ //token_len == 0 solamente se è l'elemento finale contenente i contatori
            ssize_t n_contatore_coppie = readn(mr->reducer_to_main[0], &contatore_coppie, sizeof(size_t));
            SYSLETTMAINC(mr, n_contatore_coppie, sizeof(size_t), "errore lettura contatore_coppie nella reducer_to_main[0]", {CLEANUP_LETTURA;});

            ssize_t n_contatore_token_distinti = readn(mr->reducer_to_main[0], &contatore_token_distinti, sizeof(size_t));
            SYSLETTMAINC(mr, n_contatore_token_distinti, sizeof(size_t), "errore lettura contatore_token_distinti nella reducer_to_main[0]", {CLEANUP_LETTURA;});
            break;
        }

        Nodo_risultato_t *nodo;
        SYSNCALLC(nodo = malloc(sizeof(Nodo_risultato_t)), "errore malloc nodo risultato", {
            scrivi_log(mr, "errore malloc nodo risultato", "MAIN", 0);
            CLEANUP_LETTURA;
        });

        if(header.token_len<0 || header.token_len > MAX_LUNGHEZZA_TOKEN){
            scrivi_log(mr, "errore dimensione lunghezza token", "MAIN", 0);
            CLEANUP_LETTURA;
            return -1;
        }

        if(header.result_len <0 || header.result_len>MAX_LUNGHEZZA_RESULT){
            scrivi_log(mr, "errore dimensione lunghezza result", "MAIN", 0);
            CLEANUP_LETTURA;
            return -1;
        }

        char *token;
        SYSNCALLC(token = malloc(header.token_len +1), "errore malloc token durante lettura reducer_to_main[0]", {
            scrivi_log(mr, "errore malloc token durante lettura reducer_to_main[0]", "MAIN", 0);
            free(nodo);
            CLEANUP_LETTURA;
        });
        ssize_t n_token = readn(mr->reducer_to_main[0], token, (size_t)header.token_len);
        SYSLETTMAINC(mr, n_token, header.token_len, "errore lettura token nella reducer_to_main[0]", {
            free(token);
            free(nodo);
            CLEANUP_LETTURA;
        });
        token[header.token_len] = '\0';

        void *result = NULL;
        if(header.result_len>0){
            SYSNCALLC(result = malloc(header.result_len), "errore malloc result durante lettura reducer_to_main[0]", {
                scrivi_log(mr, "errore malloc result durante lettura reducer_to_main[0]", "MAIN", 0);
                free(token);
                free(nodo);
                CLEANUP_LETTURA;
            });
            ssize_t n_result = readn(mr->reducer_to_main[0], result, (size_t)header.result_len);
            SYSLETTMAINC(mr, n_result, header.result_len, "errore lettura result nella reducer_to_main[0]", {
                free(token);
                free(nodo);
                free(result);
                CLEANUP_LETTURA;
            });
        }


        nodo->next=head;
        nodo->token=token;
        nodo->token_len=(size_t)header.token_len;
        nodo->result=result;
        nodo->size_result = (size_t)header.result_len;
        head = nodo;
        mr->contatore_risultati++;
    }

    //terminata lettura della pipe reducer_to_main
    //chiudo il processo reducer e controllo il risultato ottenuto

    SYSCALLC(close(mr->reducer_to_main[0]), "errore close reducer_to_main[0]", scrivi_log(mr, "errore close reducer_to_main[0]", "MAIN", 0));

    int status_map;
    SYSCALLC(waitpid(mr->pid_mapper, &status_map, 0), "errore waitpid di mr->pid_mapper", scrivi_log(mr, "errore waitpid di mr->pid_mapper", "MAIN", 0)); //waitpid per evitare che il mapper diventi un processo zombie

    if (WIFSIGNALED(status_map)) { //mapper terminato per errore di sistema o un segnale
        scrivi_log(mr, "processo mapper ucciso da un segnale", "MAIN", 0);  //SIGKILL, SIGTERM, SIGSEGV
        waitpid(mr->pid_reducer, NULL, 0);
        return -1;
    }else if (WIFEXITED(status_map) && WEXITSTATUS(status_map) != 0) {
        scrivi_log(mr, "processo mapper terminato con errore", "MAIN", 0); //mapper restituito -1
        waitpid(mr->pid_reducer, NULL, 0);
        return -1;
    }

    char msg[64];
    SYSCALLC(snprintf(msg, sizeof(msg), "numero di righe inviate al mapper: %zu", mr->contatore_righe_lette), "errore snprintf numero righe inviate al mapper", scrivi_log(mr, "errore snprintf numero righe inviate al mapper", "MAIN", 0));
    scrivi_log(mr, msg, "MAIN", 0);


    int status_red;
    SYSCALLC(waitpid(mr->pid_reducer, &status_red, 0), "errore waitpid di mr->pid_reducer", scrivi_log(mr, "errore waitpid di mr->pid_reducer", "MAIN", 0)); 

    if (WIFSIGNALED(status_red)) { //reducer terminato per errore di sistema o un segnale
        scrivi_log(mr, "processo reducer ucciso da un segnale", "MAIN", 0);  //SIGKILL, SIGTERM, SIGSEGV
        libera_nodi_risultati(head);
        return -1;
    }else if (WIFEXITED(status_red) && WEXITSTATUS(status_red) != 0) {
        scrivi_log(mr, "processo reducer terminato con errore", "MAIN", 0); //reducer restituito -1
        libera_nodi_risultati(head);
        return -1;
    }

    char msg_ris_finali[64];
    sprintf(msg_ris_finali, "numero di risultati finali prodotti: %zu", mr->contatore_risultati);
    scrivi_log(mr, msg_ris_finali, "MAIN", 0);


    //conversione da linked list ad array per poter usare la qsort e fare l'ordinamento lessicografico delle coppie <token, result>

    Nodo_risultato_t **arr_risultati;
    SYSNCALLC(arr_risultati=malloc(sizeof(Nodo_risultato_t*) * mr->contatore_risultati), "errore malloc array risultati", {
        scrivi_log(mr, "errore malloc array risultati", "MAIN", 0);
        libera_nodi_risultati(head);
    });
    
    Nodo_risultato_t *nodo_temp = head;

    for(size_t i=0;i<mr->contatore_risultati;i++){
        arr_risultati[i] = nodo_temp;
        nodo_temp = nodo_temp->next;
    }

    qsort(arr_risultati, mr->contatore_risultati, sizeof(Nodo_risultato_t*), ordina_stringhe);



    //scrittura nel file di output dei risultati ottenuti
    //con struttura: [token_len: int] [token: token_len byte] [result_len: int] [result: result_len byte]

    FILE *f_output;
    SYSNCALLC(f_output = fopen(output_path, "wb"), "errore apertura file di output", {
        scrivi_log(mr, "errore apertura file di output", "MAIN", 0);
        libera_nodi_risultati(head);
        free(arr_risultati);
    });

    for(size_t i=0; i<mr->contatore_risultati;i++){

        //nella chiamata a fwrite inserisco come secondo argomento 1 e come terzo la dimensione x dell'elemento
        //perché così fwrite considera l'elemento da scrivere come un insieme di x elementi da 1 byte
        //così restituisce effettivamente il numero di byte scritti e non il numero di elementi scritti.

        int token_len_int = (int)arr_risultati[i]->token_len;
        size_t n_write_token_len= fwrite(&(token_len_int), 1, sizeof(int), f_output);
        if(controlla_fwrite(mr, n_write_token_len, sizeof(int), "errore scrittura token len nel file output", head, arr_risultati, f_output)==-1)return -1;

        size_t n_write_token = fwrite(arr_risultati[i]->token, 1, token_len_int, f_output);
        if(controlla_fwrite(mr, n_write_token, arr_risultati[i]->token_len, "errore scrittura token nel file output", head, arr_risultati, f_output)==-1)return -1;;

        int size_result_int = (int)arr_risultati[i]->size_result;
        size_t n_write_size_result = fwrite(&(size_result_int), 1, sizeof(int), f_output);
        if(controlla_fwrite(mr, n_write_size_result, sizeof(int), "errore scrittura size result nel file output", head, arr_risultati, f_output)==-1)return -1;

        if(size_result_int > 0){
            size_t n_write_size = fwrite(arr_risultati[i]->result, 1, size_result_int, f_output);
            if(controlla_fwrite(mr, n_write_size, arr_risultati[i]->size_result, "errore scrittura result nel file output", head, arr_risultati, f_output)==-1) return -1;
        }
    }

    libera_nodi_risultati(head);
    free(arr_risultati);
    SYSCALLC(fclose(f_output), "errore chiusura file di output", scrivi_log(mr, "errore chiusura file di output", "MAIN", 0));
    scrivi_log(mr, "chiusura file di output", "MAIN", 0);



    SYSTIMECALLC(timespec_get(mr->fine, TIME_UTC), "errore chiusura del timer con timespec_get", scrivi_log(mr, "errore chiusura del timer con timespec_get", "MAIN", 0));
    double tempo_ms = ((mr->fine->tv_sec - mr->inizio->tv_sec) * 1000.0) + ((mr->fine->tv_nsec - mr->inizio->tv_nsec) / 1000000.0);

    //scrittura nel file delle statistiche
    sem_wait(mr->stat_sem);
    FILE *f_log = fopen(mr->f_stat, "a");
    fprintf(f_log, "=======================================================================\n");
    fprintf(f_log, "STATISTICHE FINALI ELABORAZIONE NUMERO %zu\n", mr->numero_elaborazione);
    fprintf(f_log, "tempo di esecuzione: %.3f ms\n", tempo_ms);
    fprintf(f_log, "righe lette: %zu\n", mr->contatore_righe_lette);
    fprintf(f_log, "coppie prodotte: %zu\n", contatore_coppie);
    fprintf(f_log, "token distinti: %zu\n", contatore_token_distinti);
    fprintf(f_log, "risultati emessi: %zu\n", mr->contatore_risultati);
    fclose(f_log);
    sem_post(mr->stat_sem);
    
    return 0;
}