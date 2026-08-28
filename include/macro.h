#ifndef SYS_H
#define SYS_H

#include <stdio.h>
#include <stdlib.h>
#include "mr_utils.h"

#define SYSCALL(s,m)do{\
    if((s)==-1){perror(m);return -1;}\
}while(0)

#define SYSCALLC(s,m,c)do{\
    if((s)==-1){perror(m);(c);return -1;}\
}while(0)

#define SYSCALLC_PTR(s,m,c)do{\
    if((s)==-1){perror(m);(c);return NULL;}\
}while(0)


#define SYSNCALL(s,m)do{\
    if((s)==NULL){perror(m);return -1;}\
}while(0)

#define SYSNCALL_PTR(s,m)do{\
    if((s)==NULL){perror(m);return NULL;}\
}while(0)

#define SYSNCALLC(s,m,c)do{\
    if((s)==NULL){perror(m);(c);return -1;}\
}while(0)

#define SYSNCALLC_PTR(s,m,c)do{\
    if((s)==NULL){perror(m);(c);return NULL;}\
}while(0)

#define SYSTHCALL(s,m)do{\
    int systhcall_ret = (s);\
    if(systhcall_ret==thrd_error || systhcall_ret==thrd_nomem){perror(m);return -1;}\
}while(0)

#define SYSTHCALLC(s,m,c)do{\
    int systhcallc_ret = (s);\
    if(systhcallc_ret==thrd_error || systhcallc_ret==thrd_nomem){perror(m);(c);return -1;}\
}while(0)


#define SYSMAPCALL(s,m)do{\
    if((s)==MAP_FAILED){perror(m);return -1;}\
}while(0)

#define SYSSEMCALL(s,m)do{\
    if((s)==SEM_FAILED){perror(m);return -1;}\
}while(0)

#define SYSSEMCALLC(s,m,c)do{\
    if((s)==SEM_FAILED){perror(m);(c);return -1;}\
}while(0)

#define SYSTIMECALL(s,m)do{\
    if((s)==0){perror(m);return -1;}\
}while(0)

#define SYSTIMECALLC(s,m,c)do{\
    if((s)==0){perror(m);(c);return -1;}\
}while(0)

#define SYSTIMECALLC_PTR(s,m,c)do{\
    if((s)==0){perror(m);(c);return NULL;}\
}while(0)

//Macro per controllare la corretta lettura della pipe da parte del thread lettura mapper
#define SYSLETTMAPC(mr, dim_ottenuto, dim, m, c) do {\
    if ((dim_ottenuto) < 0) {\
        perror(m);\
        scrivi_log(mr, m, "MAPPER", 1);\
        (c);\
        return -1;\
    }\
    if ((size_t)(dim_ottenuto) != (size_t)(dim)) {\
        perror(m);\
        scrivi_log(mr, m, "MAPPER", 1);\
        (c);\
        return -1;\
    }\
} while (0)
//non posso confrontare un tipo che può essere solo positivo con un tipo che può essere anche negativo se è presente il flag -Wall e -Wextra, per questo mi serve la guardia per controllare se è negativo

#define SYSLETTREDC(mr, dim_ottenuto, dim, m, c) do {\
    if ((dim_ottenuto) < 0) {\
        perror(m);\
        scrivi_log(mr, m, "REDUCER", 1);\
        (c);\
        return -1;\
    }\
    if ((size_t)(dim_ottenuto) != (size_t)(dim)) {\
        perror(m);\
        scrivi_log(mr, m, "REDUCER", 1);\
        (c);\
        return -1;\
    }\
} while (0)

#define SYSLETTMAINC(mr, dim_ottenuto, dim, m, c) do {\
    if ((dim_ottenuto) < 0) {\
        perror(m);\
        scrivi_log(mr, m, "MAIN", 0);\
        (c);\
        return -1;\
    }\
    if ((size_t)(dim_ottenuto) != (size_t)(dim)) {\
        perror(m);\
        scrivi_log(mr, m, "MAIN", 0);\
        (c);\
        return -1;\
    }\
} while (0)




#endif