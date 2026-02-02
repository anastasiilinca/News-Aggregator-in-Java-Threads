#include<mpi.h>
#include<stdio.h>
#include<stdlib.h>
#include<math.h>

#define N 1000
#define MASTER 0

void compareVectors(int * a, int * b) {
	// DO NOT MODIFY
	int i;
	for(i = 0; i < N; i++) {
		if(a[i]!=b[i]) {
			printf("Sorted incorrectly\n");
			return;
		}
	}
	printf("Sorted correctly\n");
}

void displayVector(int * v) {
	// DO NOT MODIFY
	int i;
	int displayWidth = 2 + log10(v[N-1]);
	for(i = 0; i < N; i++) {
		printf("%*i", displayWidth, v[i]);
	}
	printf("\n");
}

int min(int a, int b) {
	if (a < b)
		return a;
	return b;
}

int cmp(const void *a, const void *b) {
	// DO NOT MODIFY
	int A = *(int*)a;
	int B = *(int*)b;
	return A-B;
}
 
int main(int argc, char * argv[]) {
	int rank, i, j;
	int nProcesses;
	MPI_Init(&argc, &argv);
	int pos[N];
	int auxPos[N];
	int sorted = 0;
	int *v = (int*)malloc(sizeof(int)*N);
	int *newV = (int *)malloc(sizeof(int) * N);
	int *vQSort = (int*)malloc(sizeof(int)*N);

	for (i = 0; i < N; i++)
		pos[i] = 0;

	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &nProcesses);
	printf("Hello from %i/%i\n", rank, nProcesses);

    if (rank == MASTER) {
        // generate random vector
		for (int i = 0; i < N; i++) {
			v[i] = rand() % 1000;
		}

		for (int i = 1; i < nProcesses; i++) {
			MPI_Send(v, N, MPI_INT, i, 0, MPI_COMM_WORLD);
		}
    }


	if(rank == 0) {
		// DO NOT MODIFY
		displayVector(v);

		// make copy to check it against qsort
		// DO NOT MODIFY
		for(i = 0; i < N; i++)
			vQSort[i] = v[i];
		qsort(vQSort, N, sizeof(int), cmp);

		// sort the vector v
		
        // recv the new pozitions
		for (int i = 1; i < nProcesses; i++) {
			MPI_Recv(auxPos, N, MPI_INT, i, 0, MPI_COMM_WORLD, NULL);

			for (int j = 0; j < N; j++) {
				if (auxPos[j] != -1) {
					pos[j] = auxPos[j];
				}
			}
		}

		for (int i = 0; i < N; i++) {
			newV[pos[i]] = v[i];
		}

		for (int i = 0; i < N; i++) {
			v[i] = newV[i];
		}

		displayVector(v);
		compareVectors(v, vQSort);
	} else {
		
        // compute the positions
        // send the new positions to process MASTER
		MPI_Recv(v, N, MPI_INT, 0, 0, MPI_COMM_WORLD, NULL);

		int start = (rank - 1) * ((double)N / (nProcesses - 1));
		int end = min(N, rank * ((double)N / (nProcesses - 1)));

		for (int i = 0; i < N; i++) {
			auxPos[i] = -1;
		}

		for (int i = start; i < end; i++) {
			for (int j = 0; j < N; j++) {
				if (v[i] > v[j] || (v[i] == v[j] && i > j)) {
					if (auxPos[i] == -1) {
						auxPos[i] = 1;
					} else {
						auxPos[i]++;
					}
				}
			}
		}

		MPI_Send(auxPos, N, MPI_INT, 0, 0, MPI_COMM_WORLD);
	}

	MPI_Finalize();
	return 0;
}