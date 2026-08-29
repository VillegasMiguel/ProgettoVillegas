#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <threads.h>
#include "mr.h"


typedef struct{
    char *input;
    char *output;
    size_t num_threads_reducer;
    size_t num_threads_mapper;
    size_t dim_coda;
}arg_thread;




static int my_mapper( const mr_file_line_t *line, mr_emit_pair_t emit, void *emit_arg, void *user_arg){
    (void)user_arg; //non utilizzato 

    const char *linea = line->line;
    size_t i=0;
    size_t valore=1;
    while(i<line->line_len){
        while(i<line->line_len && !isalnum((unsigned char)linea[i])){
            i++;
        }

        size_t start = i;
        while(i<line->line_len && isalnum((unsigned char)linea[i])){
            i++;
        }
        if(i > start){ //se i==start significa che l'intera linea non ha caratteri alfanumerici dalla posizione corrente, quindi non viene chiamata emit e la funzione termina
            char parola[(i-start)+1];
            memcpy(parola, linea+start, i-start);
            parola[i-start]='\0';
            if(emit(parola, &valore, sizeof(size_t), emit_arg)==-1)return -1;;
        }
    }
    return 0;
}



static int my_reducer(const char *token, const mr_value_t *values, size_t values_count, mr_emit_result_t emit, void *emit_arg, void *user_arg){
    (void)user_arg;

    size_t somma = 0;
    size_t val;
    for(size_t i=0; i<values_count;i++){
        memcpy(&val, values[i].data, sizeof(size_t));
        somma += val;
    }
    if(emit(token, &somma, sizeof(size_t), emit_arg)==-1)return -1;
    return 0;
}


int crea_elaborazione(void *arg){
    arg_thread *arg_ptr = (arg_thread*)arg;
    mr_t mr ;
    mr_attr_t attr;
    if ( mr_attr_init (& attr ) == -1) {
        perror (" mr_attr_init ") ;
        return 1;
    }



    if ( mr_attr_set_mapper_threads (& attr , arg_ptr->num_threads_mapper) == -1) {
        perror (" mr_attr_set_mapper_threads ") ;
        mr_attr_destroy (& attr ) ;
        return 1;
    }


    if ( mr_attr_set_reducer_threads (& attr , arg_ptr->num_threads_reducer) == -1) {
        perror (" mr_attr_set_reducer_threads ") ;
        mr_attr_destroy (& attr ) ;
        return 1;
    }


    if ( mr_attr_set_queue_size (& attr , arg_ptr->dim_coda) == -1) {
        perror (" mr_attr_set_queue_size ") ;
        mr_attr_destroy (& attr ) ;
        return 1;
    }


    if ( mr_attr_set_log_file (& attr , NULL) == -1) {
        perror (" mr_attr_set_log_file ") ;
        mr_attr_destroy (& attr ) ;
        return 1;
    }


    if ( mr_create (& mr , & attr , my_mapper , my_reducer , NULL ) == -1) {
        perror (" mr_create ") ;
        mr_attr_destroy (& attr ) ;
        return 1;
    }

    if ( mr_start ( mr , arg_ptr->input, arg_ptr->output) == -1) {
        perror (" mr_start ");
        mr_destroy ( mr ) ;
        mr_attr_destroy (& attr ) ;
        return 1;
    }

    mr_destroy ( mr ) ;
    mr_attr_destroy (& attr ) ;

    return 0;

}


int main ( int argc , char ** argv ) {

    //mandati 4 elementi in argv oltre al nome del programma
    //argv[1] input del thread1, argv[2] output del thread1, argv[3] input del thread2, argv[4] input del thread2
    if(argc != 5){
        fprintf(stderr, "Uso: %s <input> <output>\n", argv[0]);
        return 1;
    }


    thrd_t threads[2];
    arg_thread lista_arg[2];
    lista_arg[0].input=argv[1];
    lista_arg[0].output=argv[2];
    lista_arg[0].dim_coda=3;
    lista_arg[0].num_threads_mapper=3;
    lista_arg[0].num_threads_reducer=3;
    lista_arg[1].input=argv[3];
    lista_arg[1].output=argv[4];
    lista_arg[1].dim_coda=2;
    lista_arg[1].num_threads_mapper=4;
    lista_arg[1].num_threads_reducer=4;

    for(size_t i=0;i<2;i++){
        if(thrd_create(&threads[i], crea_elaborazione, &lista_arg[i])==thrd_error)return 1;
    }



    for(size_t i=0;i<2;i++){
       thrd_join(threads[i], NULL);
    }


    return 0;

}