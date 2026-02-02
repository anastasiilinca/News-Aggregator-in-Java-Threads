#include<mpi.h>
#include<stdio.h>
#include <math.h>
#include <string.h>
#include <stdlib.h>

#define CONVERGENCE_COEF 100
#define TAG_SONDA 0
#define TAG_ECOU 1

/**
 * Run: mpirun -np 12 ./a.out
 */

static int num_neigh;
static int *neigh;

void read_neighbours(int rank) {
    FILE *fp;
    char file_name[15];
    sprintf(file_name, "./files/%d.in", rank);

    fp = fopen(file_name, "r");
	fscanf(fp, "%d", &num_neigh);

	neigh = malloc(sizeof(int) * num_neigh);

	for (size_t i = 0; i < num_neigh; i++)
		fscanf(fp, "%d", &neigh[i]);
}

int* get_dst(int rank, int numProcs, int leader) {
	MPI_Status status;
	MPI_Request request;

	/* Vectori de parinti */
	int *v = malloc(sizeof(int) * numProcs);
	int *vRecv = malloc(sizeof(int) * numProcs);
	/* O valoare aleatoare pentru a fi folosita ca sonda.
 	 * MPI permite și mesaje de lungime 0, dar pentru 
         * a da mai multa claritate codului vom folosi aceasta valoare.
 	*/ 
	int sonda = 42;
	int *partial_v = (int *)malloc(sizeof(int) * numProcs);

	for (int i = 0; i < numProcs; i++) {
		partial_v[i] = -1;
	}

	memset(v, -1, sizeof(int) * numProcs);
	memset(vRecv, -1, sizeof(int) * numProcs);
	
	if (rank == leader)
		v[rank] = -1;
	else {
		/* Daca procesul curent nu este liderul, inseamna ca va astepta un mesaj de la un parinte */
		MPI_Recv(partial_v, numProcs, MPI_INT, MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, &status);
		v[rank] = status.MPI_SOURCE;
	}


	/*
	*  TODO2: Pentru fiecare proces vecin care nu este parintele procesului curent,
	*		  voi trimite o sonda. 
	*/
	for (int i = 0; i < num_neigh; i++) {
		/* Trimitem sonda tuturor vecinilor care nu sunt parintele. */
		if (neigh[i] != v[rank]) {
			MPI_Send(partial_v, numProcs, MPI_INT, neigh[i], 1, MPI_COMM_WORLD);
		}
	}

	/*
	*  TODO2: Vom astepta de la fiecare proces vecin care nu este parintele procesului curent vectorul de parinti sau o sonda.
            Daca primim un ecou (vector de parinti), actualizam vectorul propriu de parinti daca exista informatii aditionale.
	    HINT: Pentru simplitate, puteti face mereu recv ca pentru vectorul de parinti si sa verificati size-ul receptiei sau tag-ul
            pentru a determina daca este sonda sau ecou.
	*/
	for (int i = 0; i < num_neigh; i++) {
		if (neigh[i] != v[rank]) {
			MPI_Status status;

			MPI_Recv(partial_v, numProcs, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
			
			/* Daca mesajul primit este un vector partial, il agregam. */
			if (status.MPI_TAG == 0) {
				for (int i = 0; i < numProcs; i++) {
					if (partial_v[i] != -1) {
						v[i] = partial_v[i];
					}
				}
			}
		}
	}

	/*
	*  TODO2: Orice proces ce nu este lider va propaga vectorul de vecini parintelui lui si va astepta topologia completa de la acesta
	*/
	if (rank != leader) {
		MPI_Send(v, numProcs, MPI_INT, v[rank], 0, MPI_COMM_WORLD);
	}


	/*
	*  TODO2: Procesul curent va trimite doar copiilor lui topologia completa
	*/
	for (int i = 0; i < numProcs; i++) {
		if (v[i] == rank) {
			MPI_Send(v, numProcs, MPI_INT, i, 2, MPI_COMM_WORLD);
		}
	}

	if (rank != leader) {
		MPI_Recv(v, numProcs, MPI_INT, v[rank], 2, MPI_COMM_WORLD, NULL);
	}

	for (int i = 0; i < numProcs && rank == leader; i++) {
		printf("The node %d has the parent %d\n", i, v[i]);
	}

	return v;
}

int leader_chosing(int rank, int nProcesses) {
	int leader = -1;
	int q;
	leader = rank;
	
	/* Executam acest pas pana ajungem la convergenta */
	for (int k = 0; k < CONVERGENCE_COEF; k++) {
		/* TODO1: Pentru fiecare vecin, vom trimite liderul pe care il cunosc 
		* 		 si voi astepta un mesaj de la orice vecin
		* 		 Daca liderul e mai mare decat al meu, il actualizez pe al meu
		*/
		for (int i = 0; i < num_neigh; i++) {
			/* Send your leader to the niegh. */
			MPI_Send(&leader, 1, MPI_INT, neigh[i], 0, MPI_COMM_WORLD);
			/* Recv leader from any neigh */
			int neigh_leader;

			MPI_Recv(&neigh_leader, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, NULL);
			
			/* Update leader if necessary. */
			if (neigh_leader > leader) {
				leader = neigh_leader;
			}
		}
	}

	MPI_Barrier(MPI_COMM_WORLD);

	return leader;
}

double get_number_of_nodes(int rank, int leader) {
	
	double val;
	if (leader == rank) {
		val = 1.0;
	} else {
		val = 0.0;
	}

	double recvd = 0;
	double neigh_val = 0;
	/* Executam acest pas pana ajungem la convergenta */
	for (int k = 0; k < CONVERGENCE_COEF; k++) {
		/* TODO3: Pentru fiecare vecin, vom trimite valoarea pe care o cunosc
		* 		 si voi astepta un mesaj de la el
		* 		 Cu valoarea primita, actualizam valoarea cunoscuta ca fiind
		* 		 media dintre cele 2
		*/
		for (int i = 0; i < num_neigh; i++) {
			/* Trimit valoarea mea vecinului. */
			MPI_Send(&val, 1, MPI_DOUBLE, neigh[i], 0, MPI_COMM_WORLD);
			/* Vecinul imi trimite valoarea lui. */
			MPI_Recv(&neigh_val, 1, MPI_DOUBLE, neigh[i], 0, MPI_COMM_WORLD, NULL);

			val = (val + neigh_val) / 2;
		}
	}
	
	MPI_Barrier(MPI_COMM_WORLD);
	
	return (double)(1 / val);
}

int ** get_topology(int rank, int nProcesses, int * parents, int leader) {
	int ** topology = malloc(sizeof(int*) * nProcesses);
	int ** vTopology = malloc(sizeof(int*) * nProcesses);
	
	for (size_t i = 0; i < nProcesses; i++) {
		topology[i] = calloc(sizeof(int), nProcesses);
		vTopology[i] = calloc(sizeof(int), nProcesses);
	}

	for (size_t i = 0; i < num_neigh; i++) {
		topology[rank][neigh[i]] = 1;
	}

	/* TODO4: Primim informatii de la toti copii si actualizam matricea de topologie */
	for (int i = 0; i < num_neigh; i++) {
		/* Daca vecinul este si copilul meu, astept matricea de topologie de la el. */
		if (parents[neigh[i]] == rank) {
			/* O voi primi linie cu linie. */
			for (int j = 0; j < nProcesses; j++) {
				MPI_Recv(vTopology[j], nProcesses, MPI_INT, neigh[i], 0, MPI_COMM_WORLD, NULL);
			}
			
			/* Agreg rezultatele primite de la copil. */
			for (int j = 0; j < nProcesses; j++) {
				for (int k = 0; k < nProcesses; k++) {
					if (vTopology[j][k] == 1) {
						topology[j][k] = 1;
					}
				}
			}
		}
	}

	/* TODO4: Propagam matricea proprie catre parinte */
	if (rank != leader) {
		for (int j = 0; j < nProcesses; j++) {
			MPI_Send(topology[j], nProcesses, MPI_INT, parents[rank], 0, MPI_COMM_WORLD);
		}
	}

	/* TODO4: Daca nu suntem liderul, asteptam topologia completa de la parinte  */
	if (rank != leader) {
		for (int j = 0; j < nProcesses; j++) {
			MPI_Recv(topology[j], nProcesses, MPI_INT, parents[rank], 0, MPI_COMM_WORLD, NULL);
		}
	}
	
	/* TODO4: Trimitem topologia completa copiilor */
	for (int i = 0; i < num_neigh; i++) {
		if (parents[neigh[i]] == rank) {
			for (int j = 0; j < nProcesses; j++) {
				MPI_Send(topology[j], nProcesses, MPI_INT, neigh[i], 0, MPI_COMM_WORLD);
			}
		}
	}

	return topology;
}

int main(int argc, char * argv[]) {
	int rank, nProcesses, leader;
	int *parents, **topology;

	MPI_Init(&argc, &argv);
	MPI_Status status;
	MPI_Request request;

	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &nProcesses);

	if (nProcesses != 12) {
		printf("please run with: mpirun --oversubscribe -np 12 %s\n", argv[0]);
		MPI_Finalize();	
		exit(0);
	}
	 
	read_neighbours(rank);
	leader = leader_chosing(rank, nProcesses);
	
	MPI_Barrier(MPI_COMM_WORLD);

	parents = get_dst(rank, nProcesses, leader);

	MPI_Barrier(MPI_COMM_WORLD);

	double num_procs = get_number_of_nodes(rank, leader);
	
	// printf("%d/%d There are %f processes\n", rank, nProcesses, num_procs);

	topology = get_topology(rank, nProcesses, parents, leader);

	for (size_t i = 0; i < nProcesses && rank == 0; i++)
	{
		for (size_t j = 0; j < nProcesses; j++)
		{
			printf("%2d ", topology[i][j]);	
		}
		printf("\n");
	}
	
	MPI_Finalize();
	return 0;
}