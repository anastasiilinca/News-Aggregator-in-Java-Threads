#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>

#define MASTER 0

int main (int argc, char *argv[])
{
    int procs, rank;

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &procs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int value = rank;
    int x;

    for (int pas = 1; pas < procs; pas *= 2) {
        if (rank + pas < procs) {
            MPI_Send(&value, 1, MPI_INT, rank + pas, 0, MPI_COMM_WORLD);
        }
        if (rank - pas >= 0) {
            MPI_Recv(&x, 1, MPI_INT, rank - pas, 0, MPI_COMM_WORLD, NULL);
            value += x;
        }
    }

    printf("Process [%d] has result = %d\n", rank, value);

    MPI_Finalize();

}
