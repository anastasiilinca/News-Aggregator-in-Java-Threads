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

    for (int pas = 2; pas <= procs; pas *= 2) {
        // TODO
        if (rank % pas == 0) {
            MPI_Recv(&x, 1, MPI_INT, rank + pas / 2, 0, MPI_COMM_WORLD, NULL);
            value += x;
        } else if (rank % (pas / 2) == 0) {
            MPI_Send(&value, 1, MPI_INT, rank - pas / 2, 0, MPI_COMM_WORLD);
        }
    }

    if (rank == MASTER) {
        printf("Result = %d\n", value);
    }

    MPI_Finalize();

}
