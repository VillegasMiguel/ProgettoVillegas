#include <stdio.h>
#include "mr.h"

int main ( int argc , char ** argv ) {
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

if ( mr_attr_set_log_file (& attr , "mr.log ") == -1) {
    perror (" mr_attr_set_log_file ") ;
    mr_attr_destroy (& attr ) ;
    return 1;
}

if ( mr_create (& mr , & attr , my_mapper , my_reducer , NULL ) == -1) {
    perror (" mr_create ") ;
    mr_attr_destroy (& attr ) ;
    return 1;
}

if ( mr_start ( mr , " input ", " output . mro ") == -1) {
    perror (" mr_start ");
    mr_destroy ( mr ) ;
    mr_attr_destroy (& attr ) ;
    return 1;
}

mr_destroy ( mr ) ;
mr_attr_destroy (& attr ) ;

return 0;
}



