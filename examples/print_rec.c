#include <stdio.h>
#include <stdlib.h>
#include <string.h>

//con struttura: [token_len: int] [token: token_len byte] [result_len: int] [result: result_len byte]

typedef struct{
    double voto_finale;
    size_t numero_recensioni;
}Risultato_t;

int main(int argc, char **argv){
    if(argc!=2)return 1;
    FILE* file = fopen(argv[1], "rb");

    int token_len;
    int result_len;

    while(fread(&token_len, sizeof(int), 1, file)>0){
        char nome[token_len+1];

        fread(nome, token_len, 1, file);
        nome[token_len]='\0';

        fread(&result_len, sizeof(int), 1, file);

        Risultato_t value;
        fread(&value, result_len, 1, file);

        printf("%s : %.2f, numero recensioni: %zu\n", nome, value.voto_finale, value.numero_recensioni);
    }
    return 0;
}