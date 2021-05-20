all: psort

psort.o: psort.c helper.h
	gcc -c -g -Wall -std=gnu99 psort.c -o psort.o

helper.o: helper.c helper.h
	gcc -c -g -Wall -std=gnu99 helper.c -o helper.o

psort: helper.o psort.o
	gcc helper.o psort.o -o psort

.PHONY: clean
clean:
	rm psort *.o