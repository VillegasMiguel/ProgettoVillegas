#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include "../include/mr.h"

typedef struct{
    double voto_finale;
    size_t numero_recensioni;
}Risultato_t;

static int my_mapper(const mr_file_line_t *line, mr_emit_pair_t emit, void *emit_arg, void *user_arg){
    (void)user_arg;
    if(line==NULL)return -1;

    const char* linea = line->line;

    size_t len_nome_film = strcspn(linea, ":");
    char nome_film[len_nome_film +1];
    strncpy(nome_film, linea, len_nome_film);
    nome_film[len_nome_film]= '\0';

    double voto = atof(linea + len_nome_film +1);

    return emit(nome_film, &voto, sizeof(double), emit_arg);
}


static int my_reducer(const char *token, const mr_value_t *values, size_t values_count, mr_emit_result_t emit, void *emit_arg, void *user_arg){
    (void)user_arg;

    Risultato_t ris;
    double somma=0;
    ris.numero_recensioni=values_count;

    for(size_t i=0; i<values_count;i++){
        double val;
        memcpy(&val, values[i].data, sizeof(double));
        somma += val;
    }

    double media = somma / ris.numero_recensioni;
    ris.voto_finale=media;

    return emit(token, &ris, sizeof(Risultato_t), emit_arg);
}

int main(int argc, char **argv){
    if(argc != 3)return 1;
    mr_t mr;
    mr_attr_t attr;

    if ( mr_attr_init (& attr ) == -1) {
        perror (" mr_attr_init ") ;
        return 1;
    }   

    if ( mr_attr_set_mapper_threads (& attr , 3) == -1) {
        perror (" mr_attr_set_mapper_threads ") ;
        mr_attr_destroy (& attr ) ;
        return 1;
    }

    if ( mr_attr_set_reducer_threads (& attr , 3) == -1) {
        perror (" mr_attr_set_reducer_threads ") ;
        mr_attr_destroy (& attr ) ;
        return 1;
    }

    if ( mr_attr_set_queue_size (& attr , 5) == -1) {
        perror (" mr_attr_set_queue_size ") ;
        mr_attr_destroy (& attr ) ;
        return 1;
    }

    if ( mr_attr_set_log_file (& attr , "rec.log") == -1) {
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