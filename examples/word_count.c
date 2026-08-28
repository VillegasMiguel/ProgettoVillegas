#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include "mr.h"

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

int main ( int argc , char ** argv ) {

    if(argc != 3){
        fprintf(stderr, "Uso: %s <input> <output>\n", argv[0]);
        return 1;
    }

    mr_t mr ;
    mr_attr_t attr ;
    if ( mr_attr_init (& attr ) == -1) {
        perror (" mr_attr_init ") ;
        return 1;
    }   

    if ( mr_attr_set_mapper_threads (& attr , 4) == -1) {
        perror (" mr_attr_set_mapper_threads ") ;
        mr_attr_destroy (& attr ) ;
        return 1;
    }

    if ( mr_attr_set_reducer_threads (& attr , 4) == -1) {
        perror (" mr_attr_set_reducer_threads ") ;
        mr_attr_destroy (& attr ) ;
        return 1;
    }

    if ( mr_attr_set_queue_size (& attr , 64) == -1) {
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

    if ( mr_start ( mr , argv[1], argv[2]) == -1) {
        perror (" mr_start ");
        mr_destroy ( mr ) ;
        mr_attr_destroy (& attr ) ;
        return 1;
    }

    mr_destroy ( mr ) ;
    mr_attr_destroy (& attr ) ;

    return 0;
}