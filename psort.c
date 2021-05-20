#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdlib.h>
#include "helper.h"
#include <unistd.h>
#include <sys/wait.h>

/*Free the memory that was used merge and create_children functions.*/
void deallocate_process_data(int num, int **fds, int *nums_rec, int *positions, int *results){
    for(int i = 0; i < num; i++){
    	free(fds[i]);
    }
    free(fds);
    free(nums_rec);
    free(positions);
    free(results);
}


/* Read num_read struct recs from binary file with address input_file starting at 
position, sort the rec structs based on frequency using quicksort and
then write them to the pipe using given file descriptors fd. */
void read_binary_file(int position, int num_read, char *input_file, int *fd){
	int num_to_read = num_read;
	if(close(fd[0]) == -1){
        perror("pipe");
        exit(1);
    }
    struct rec *r = malloc(sizeof(struct rec) * num_read);
    if(num_read > 0){
	    FILE *data_in = fopen(input_file, "rb");
	    if(data_in == NULL){
	    	fprintf(stderr, "Error opening file\n");
            exit(1);
	    }
	    // seek to the provided position and read data from file
	    int err0 = fseek(data_in, sizeof(struct rec) * position, SEEK_SET);
	    if(err0 != 0){
	    	fprintf(stderr, "Error: failed moving through file\n");
            exit(1);
	    }
        int err1 = fread(r, sizeof(struct rec), num_to_read, data_in);
        if(err1 != num_to_read){
            fprintf(stderr, "Error: failed reading data from file\n");
            exit(1);
        }
        // sort the array using qsort and write sorted data to file
        qsort(r, num_read, sizeof(struct rec), compare_freq);
        for(int i = 0; i < num_read; i++){
            if(write(fd[1], &(r[i]), sizeof(struct rec)) == -1){
            	perror("write to pipe");
                exit(1);
            }
        }
        int error = fclose(data_in);
        if(error != 0){
        	fprintf(stderr, "Error closing file\n");
            exit(1);
        }
    }
    if(close(fd[1]) == -1){
        perror("pipe");
        exit(1);
    }
    free(r);
}


/* Close pipes and wait for children to terminate after merge operation. Return whether
child processes terminated abnormally */
int wait_for_children(int num, int **fds, int *results){
    int ret = 0;
    for(int i = 0; i < num; i++){
        if(close(fds[i][0]) == -1){
            perror("pipe");
            exit(1);
        }
        if(wait(&(results[i])) != -1){
            
            // wait accepts pid of a child process so that we know for what to wait
            
            if(!WIFEXITED(results[i])){ // WIFEXITED returns 1 if child terminated normally
                // and 0 if child terminated abnormally

                // Here I am using fprintf for printing errors to standard error

                fprintf(stderr, "Child terminated abnormally\n");
                ret = 1;
            }
        }
        else{
            perror("wait");
            ret = 1;
        }
    }
    return ret;         
}


/* Read rec structs from pipes using file descriptors fds and array of 
integers nums_rec that stores information about how many structs were sorted
and written to pipe by each process. After reading, combine merge structs
read from different pipes so that they appear in increasing order based
on their frequency. Finally, write the result to the output file. */
void merge(int file_size, int num, int **fds, char *output_file, int *nums_rec){
    struct rec *result = malloc(sizeof(struct rec) * num);
    int *statuses = malloc(sizeof(int) * num);
    int found;
    int index = 0;    
    int min_freq = 0;
    FILE *data_out = fopen(output_file, "wb");
	if(data_out == NULL){
        fprintf(stderr, "Error opening file\n");
        exit(1);
	} 
    for(int j = 0; file_size > 0; j++){
    	found = -1;
    	for(int i = 0; i < num; i++){
    		// read data from each pipe during the first iteration
    		if(j == 0){
    			statuses[i] = 0;
    			if(nums_rec[i] > 0){
                    if(close(fds[i][1]) == -1){
                        perror("pipe");
                        exit(1);
                    }
    				if(read(fds[i][0], &(result[i]), sizeof(struct rec)) == -1){
                        perror("read from pipe");
                        exit(1);
    				}
    			}
    			nums_rec[i]--;
    		}
    		// search for struct rec with minimum freq in the array
    		else if(j > 0){
    			if(statuses[i] != 1){
                    if(found == -1 || result[i].freq <= min_freq){
                    	min_freq = result[i].freq;
                    	found = i;
                    	index = i;
                    }
                }    
    		}
    	}
    	// write struct rec with minimum freq to output_file
        if(j > 0){
    	    if(fwrite(&(result[index]), sizeof(struct rec), 1, data_out) != 1){
                fprintf(stderr, "Error: failed reading data from file\n");
    	    }
    	    file_size--;
            if(nums_rec[index] == 0){
    		    statuses[index] = 1;
    	    }
    	    // if there is still data left in the pipe corresponding to the element 
            // of the array that was just written to the file, read from that pipe
    	    if(nums_rec[index] > 0){
    	        read(fds[index][0], &(result[index]), sizeof(struct rec));
    	        nums_rec[index]--;
    	    }         
        }
    }
    free(result);
    free(statuses);
    int error = fclose(data_out);
    if(error != 0){
        fprintf(stderr, "Error closing file\n");
        exit(1);
    }
}    


/*Create num child processes and make them all read data from input file.
Make parent process wait until children processes exit and then perform
Merge operation. */
void create_children(int num, char *input_file, char *output_file){
    
    // calclulate the # of structs in the file by dividing file size by size of struct
	int file_size = get_file_size(input_file) / sizeof(struct rec);
    int initial_size = file_size;
    
    // if file is not empty and we are asking for more than 0 processes to be created
    if(initial_size > 0 && num > 0){
        int *nums_rec = malloc(sizeof(int) * num);
        if(nums_rec == NULL){
            perror("malloc");
            exit(1);
        }
        // allocate file descriptors for each process that will be created 
        int **fds = malloc(sizeof(int *) * num);
        if(fds == NULL){
            perror("malloc");
            exit(1);
        }   
        for(int q = 0; q < num; q++){
    	    nums_rec[q] = 0;
    	    fds[q] = malloc(sizeof(int) * 2);
            if(fds[q] == NULL){
                perror("malloc");
                exit(1);
            }
        }
        int *positions = malloc(sizeof(int) * num);
        // distribute structs between processes as evenly as possible
        for(int j = 0; file_size != 0; file_size--){
    	    nums_rec[j]++;
            if(j == num - 1){
                j = 0;
            }
            else{
        	    j++;
            }
        }
        // compute positions and create pipes
        for(int j = 0; j < num; j++){
            if(nums_rec[j] > 0){

                // call pipe on each array containing file descriptors for our future children
                if(pipe(fds[j]) == -1){
                   perror("pipe");
                   exit(1);
                }
            }    
            if(j == 0){
                positions[j] = 0;
            }
            else{
                positions[j] = positions[j-1] + nums_rec[j-1];
            }
        }


        int *results = malloc(sizeof(int) * num);
        int result = 0;
        int merge_num = 0;

        // create child processes and make each of them call read_binary_file
        for(int i = 0; i < num; i++){
            if(nums_rec[i] > 0){
                merge_num++;

                // each time we are creating a child process
    	        result = fork();
    	        
                if(result > 0){
                    // if process is parent
                    results[i] = result;  // save the process if pid of the child for future use                
    	        }
    	        else if(result == 0){
                    // if process is child
                    read_binary_file(positions[i], nums_rec[i], input_file, fds[i]);
                    deallocate_process_data(num, fds, nums_rec, positions, results);
                    break;
    	        }
    	        else{
                    perror("fork");
                    exit(1);
    	        }
            }    
        }
        if(result > 0){
            // if process is parent
            merge(initial_size, merge_num, fds, output_file, nums_rec);
            
            // here we are waiting for children before exiting
            int wait_res = wait_for_children(merge_num, fds, results);

            deallocate_process_data(num, fds, nums_rec, positions, results);
            if(wait_res != 0){
                exit(1);
            }
        }
    }
    else{
        FILE *data_out = fopen(output_file, "wb");
        if(data_out == NULL){
            fprintf(stderr, "Error opening file\n");
            exit(1);
        } 
        int error = fclose(data_out);
        if(error != 0){
            fprintf(stderr, "Error closing file\n");
            exit(1);
        }
        exit(0);
    }
}    


/*This program takes in 3 paramters - filename of the input file,
filename of the output file, and the number of processes that will be 
performing pieces of work of sorting rec structs based on freq stored
in them. This program also uses quicksort operation to sort the array
and merge operation that combines the result of child processes.*/
int main(int argc, char *argv[]) {
    int par; 
    int num_proc;
    char *input_file;
    char *output_file;
    int invalid = 0;
    if(argc != 7){
    	invalid = 1;
    }
    // process command line arguments
    while((par = getopt(argc, argv, ":n:f:o:?")) != -1){  
        switch(par){  
            case 'f':  
                input_file = optarg;
                break;  
            case 'o':   
                output_file = optarg;
                break;      
            case 'n':  
                num_proc = strtol(optarg, NULL, 10);
                break;   
            case '?':
                invalid = 1;
                break;                 
            default:
                invalid = 1;
                break;
        }  
    }
    if(invalid){
    	fprintf(stderr, "Usage: psort -n <number of processes> -f <inputfile> -o <outputfile>\n");
    	exit(1);
    }
    // begin by creating child processes
    create_children(num_proc, input_file, output_file);
    return 0;
}	