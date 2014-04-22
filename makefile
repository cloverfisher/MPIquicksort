all: quicksortparallel input

quicksortparallel: quicksortparallel.c
	mpicc -o quicksortparallel quicksortparallel.c

input: input.c
	mpicc -o input input.c

clean:
	/bin/rm input quicksortparallel
