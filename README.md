# Multi-process dictionary sorting

The program psort takes in 3 paramters - filename of the input file,
filename of the output file, and the number of processes that will be 
performing pieces of work of sorting rec structs based on freq stored
in them. This program also uses quicksort operation to sort the array
and merge operation that combines the result of child processes.