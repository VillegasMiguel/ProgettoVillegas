CC = gcc
CFLAGS = -Wall -Wextra -g -std=c11
AR = ar
ARFLAGS = rcs

#file sorgenti del framework
OBJS = src/mr.o src/mapper.o src/reducer.o

#compila tutto
all: libmr.a examples/word_count examples/word_count_addendum examples/media_recensione examples/print_rec

#libreria statica
libmr.a: $(OBJS)
	$(AR) $(ARFLAGS) $@ $^


#compilazione file oggetto
src/mr.o: src/mr.c
	$(CC) $(CFLAGS) -c $< -o $@

src/mapper.o: src/mapper.c
	$(CC) $(CFLAGS) -c $< -o $@

src/reducer.o: src/reducer.c
	$(CC) $(CFLAGS) -c $< -o $@


#word_count
examples/word_count: examples/word_count.c libmr.a
	$(CC) $(CFLAGS) -Iinclude $< -L. -lmr -o $@


#word_count_addendum
examples/word_count_addendum: examples/word_count_addendum.c libmr.a
	$(CC) $(CFLAGS) -Iinclude $< -L. -lmr -o $@


#recensione
examples/media_recensione: examples/media_rec.c libmr.a
	$(CC) $(CFLAGS) -Iinclude $< -L. -lmr -o $@

examples/print_rec: examples/print_rec.c
	$(CC) $(CFLAGS) $< -o $@

#esecuzione del test
test: all
	@rm -f mr.log statistiche_finali.txt rec.log
	@echo "****** Esecuzione test 1 word_count ******"
	@./examples/word_count tests/input1.txt tests/out_1.mro && echo "TEST 1 SUCCESS" || echo "TEST 1 FAIL"
	@echo "****** Esecuzione test 1 terminato ******"
	@echo "****** LOG ******"
	@cat mr.log
	@echo
	@echo "****** STATISTICHE ******"
	@cat statistiche_finali.txt
	@rm -f mr.log statistiche_finali.txt

	@echo
	@echo "****** Esecuzione test 2 word_count_addendum ******"
	@./examples/word_count_addendum tests/cartella_input1 tests/out_add_1.mro tests/cartella_input2 tests/out_add_2.mro && echo "TEST 2 SUCCESS" || echo "TEST 2 FAIL"
	@echo "****** Esecuzione test 2 terminato ******"
	@echo "****** LOG ******"
	@cat mr.log
	@echo
	@echo "****** STATISTICHE ******"
	@cat statistiche_finali.txt
	@rm -f mr.log statistiche_finali.txt
	
	@./examples/word_count_addendum tests/cartella_input1 tests/out_verifica_1.mro tests/cartella_input1 tests/out_verifica_2.mro
	
	@echo
	@echo "usando il programma word_count_addendum con lo stesso input ma file di output diversi,"
	@echo "confronto i file di output byte per byte per confermare che il risultato del framework sia deterministico"
	@if cmp -s tests/out_verifica_1.mro tests/out_verifica_2.mro;then \
	echo "risultato ottenuto: output deterministico"; \
	else \
	echo "risultato ottenuto: output non deterministico"; \
	fi
	@rm -f mr.log statistiche_finali.txt
	
	@echo
	@echo "****** Esecuzione test 3 recensioni ******"
	@./examples/media_recensione tests/rec_input.txt tests/rec_out.mro && echo "TEST 3 SUCCESS" || echo "TEST 3 FAIL"
	@echo "****** Esecuzione test 3 terminato ******"
	@echo "****** LOG ******"
	@cat rec.log
	@echo
	@echo "****** STATISTICHE ******"
	@cat statistiche_finali.txt
	@echo
	@echo "Risultato testuale recensioni"
	@echo
	@./examples/print_rec tests/rec_out.mro
	@echo
	
	




#pulizia
clean:
	rm -f src/mr.o src/mapper.o src/reducer.o libmr.a \
	examples/word_count examples/word_count_addendum examples/media_recensione examples/print_rec \
	tests/out_1.mro tests/out_add_1.mro tests/out_add_2.mro tests/out_verifica_1.mro  tests/out_verifica_2.mro  tests/rec_out.mro \
	mr.log rec.log statistiche_finali.txt

.PHONY: all test clean

