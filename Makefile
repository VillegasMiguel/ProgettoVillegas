CC = gcc
CFLAGS = -Wall -Wextra -g -std=c11
AR = ar
ARFLAGS = rcs

#file sorgenti del framework
SRCS = src/mr.c src/mapper.c src/reducer.c
OBJS = src/mr.o src/mapper.o src/reducer.o

#compila tutto
all: libmr.a examples/word_count examples/word_count_addendum

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



#esecuzione del test
test: all
	@rm -f mr.log statistiche_finali.txt
	@echo "****** Esecuzione test 1 word_count ******"
	@./examples/word_count tests/cartella_input tests/output1.mro && echo "TEST 1 SUCCESS" || echo "TEST 1 FAIL"
	@echo "****** Esecuzione test 1 terminato ******"
	@echo "****** LOG ******"
	@cat mr.log
	@echo "****** STATISTICHE ******"
	@cat statistiche_finali.txt
	@rm -f statistiche_finali.txt

	@echo "****** Esecuzione test 2 word_count_addendum ******"
	@./examples/word_count_addendum tests/input_addendum.txt tests/output_addendum1.mro tests/cartella_input tests/output_addendum2.mro && echo "TEST 2 SUCCESS" || echo "TEST 2 FAIL"
	@echo "****** Esecuzione test 2 terminato ******"
	@echo "****** LOG ******"
	@cat mr.log
	@echo "****** STATISTICHE ******"
	@cat statistiche_finali.txt

#pulizia
clean:
	rm -f src/mr.o src/mapper.o src/reducer.o libmr.a examples/word_count examples/word_count_addendum tests/output1.mro tests/output_addendum1.mro tests/output_addendum2.mro mr.log statistiche_finali.txt 

.PHONY: all test clean

