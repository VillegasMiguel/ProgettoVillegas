Cos'è questo progetto?
Come lo compilo?
Come lo eseguo?
Che input vuole?
Cosa produce?
Ci sono particolarità o limitazioni?

# Libreria Map-Reduce per l'esame di Laboratorio 2 dell'anno 2025/2026
Libreria che ha lo scopo di analizzare file testuali mediante due funzioni mapper e reducer passati dal programma utente che utilizza la libreria.

# Compilazione
per compilare la libreria e i vari codici usati per la simulazione, viene fatto affidamento al Makefile presente nella directory principale.
I target principali utilizzati all'interno del Makefile sono:

    all: Permette la compilazione della libreria e dei programmi usati per la simulazione (Word_count.c Word_count_addendum.c media_rec e print_rec.c)
         essendo il primo target presente nel Makefile, è sufficiente  eseguire make nel terminale
    
    