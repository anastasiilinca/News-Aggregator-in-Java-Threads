#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

int main (int argc, char *argv[])
{   
    srand(time(NULL)); 
    int  numtasks, rank;

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
    MPI_Comm_rank(MPI_COMM_WORLD,&rank);

    int recv_num;

    // First process starts the circle.
    if (rank == 0) {
        // First process starts the circle.
        // Generate a random number.
        // Send the number to the next process.
        int tok = rand();
        MPI_Send(&tok, 1, MPI_INT, rank + 1, 0, MPI_COMM_WORLD);
        MPI_Recv(&tok, 1, MPI_INT, numtasks - 1, 0, MPI_COMM_WORLD, NULL);
        printf("Rank %d - token %d\n", rank, tok);
    } else if (rank == numtasks - 1) {
        // Last process close the circle.
        // Receives the number from the previous process.
        // Increments the number.
        // Sends the number to the first process.
        int tok;
        MPI_Recv(&tok, 1, MPI_INT, rank - 1, 0, MPI_COMM_WORLD, NULL);
        printf("Rank %d - token %d\n", rank, tok);
        tok += 2;
        MPI_Send(&tok, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
    } else {
        // Middle process.
        // Receives the number from the previous process.
        // Increments the number.
        // Sends the number to the next process.
        int tok;
        MPI_Recv(&tok, 1, MPI_INT, rank - 1, 0, MPI_COMM_WORLD, NULL);
        printf("Rank %d - token %d\n", rank, tok);
        tok += 2;
        MPI_Send(&tok, 1, MPI_INT, rank + 1, 0, MPI_COMM_WORLD);
    }

    MPI_Finalize();

}

