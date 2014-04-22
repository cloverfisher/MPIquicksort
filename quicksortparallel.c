#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <mpi.h>
#include <time.h> 



int quicksort(int *dataset, int q, int r);
int changedata(int *a, int *b);
int parallel_quicksort(int *dataset,int pivot,int q,int r);
int parallel_quicksort_plus(int *dataset, int length, int pivot,MPI_Comm comm);
//int parallel_quicksort_plus_test(int *dataset, int length, int pivot,MPI_Comm comm);
int prifixsum = 0;
	double start,end;


int main(int argc, char** argv)
{
	int length = 1000000;
	int i,j,k;
	int nodenum,node,nodeplus;
	int prefix;
	int *recvnum;
	int recvPrefix[20];
	int prefixplus;
	int smallP=0;//,largeP=0;
	int currentdatalength;

	int smallLength,largeLength;
	int *smallDataset,*largeDataset;
	int *dataset;
	int pivot = 0;
	MPI_Comm smallComm,largeComm;
	MPI_Comm orginalComm;
	MPI_Status status;
	

	freopen("dataset.txt","r",stdin);
	dataset = (int *)malloc(2000000*sizeof(int));
//	memset(dataset,0,2000000*sizeof(int));
	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD,&node);




	MPI_Comm_size(MPI_COMM_WORLD,&nodenum);
	for(i=0;i<length;i++)
	{
		scanf("%d",&dataset[i]);	
	}
	if(node == 0)
		start = MPI_Wtime();
	if(nodenum == 1)
	{
		quicksort(dataset,0,length-1);
		end = MPI_Wtime();
		printf("time is  %lf s\n",(double)(end - start));
		printf("\n===============\n");
		for(i=0;i<length;i++)
		{
			 printf("%d ",dataset[i]);
		}
		printf("\n===============\n");
	}
	else
	{
		recvnum = (int *)malloc(2000000*sizeof(int));
		 srand (time(NULL));
		pivot = rand()%length;
		pivot = dataset[pivot];
		 parallel_quicksort_plus(dataset,length,pivot,MPI_COMM_WORLD);

		end = MPI_Wtime();
		if(node == 0)
		{
			printf("time is  %lf s\n",(double)(end - start));
			printf("\n===============\n");
			 for(i=0;i<length;i++)
			 {
			 	printf("%d ",dataset[i]);
			 }
			 printf("\n===============\n");
		}

	}
	free(dataset);
//	printf("%d\n",nodenum);
//	parallel_quicksort(dataset,0,a);

	MPI_Finalize();
	return 0;
}

int parallel_quicksort_plus(int *dataset, int length, int pivot,MPI_Comm comm)
{
	int i,j,k;
	int nodenum,node,nodeplus;
	int prefix = 0;
	int *recvnum;
	int recvPrefix[20];
	int prefixplus = 0;
	int smallP=0;//,largeP=0;
	int currentdatalength = 0;
	int largePivot = 0,smallPivot=0;
	int smallLength,largeLength;
	int *smallDataset,*largeDataset;
	MPI_Comm smallComm,largeComm;
	MPI_Comm orginalComm;
		MPI_Status status;
	

	MPI_Comm_rank(comm,&node);

	MPI_Comm_size(comm,&nodenum);
	if(nodenum == 1)
	{
		quicksort(dataset,0,length-1);
		return 0;
	}
	
	recvnum = (int *)malloc(length*sizeof(int));
	//	free(recvnum);


//	printf("%d\n",nodenum);
//	parallel_quicksort(dataset,0,a);
	
	if(node!=nodenum-1)
		currentdatalength = length/nodenum;
	else
		currentdatalength = length - length/nodenum*(nodenum-1);
	if(node!=nodenum-1)
		prefix = parallel_quicksort(dataset,pivot,length/nodenum*node,length/nodenum*(node+1)-1);
	else
		prefix = parallel_quicksort(dataset,pivot,length/nodenum*node,length-1);
	
	MPI_Gather(&prefix , 1, MPI_INT,recvPrefix,1,MPI_INT,0,comm);
	prefixplus = 0;
	if(node == 0)
	{
		prefixplus = recvPrefix[0];
		memcpy(recvnum,dataset,prefix*sizeof(int));
		for(i=1;i<nodenum;i++)
		{
			MPI_Recv(recvnum+prefixplus, recvPrefix[i], MPI_INT, i,0,comm,&status);
			prefixplus+=recvPrefix[i];
		}
	}
	else
		MPI_Send(dataset+length/nodenum*node,prefix,MPI_INT,0,0,comm);

	if(node == 0)
	{
		memcpy(recvnum+prefixplus,dataset+prefix,(currentdatalength - prefix)*sizeof(int));
		for(i=1;i<nodenum;i++)
		{
			prefixplus += length/nodenum - recvPrefix[i-1];
			if(i!=nodenum-1)
				MPI_Recv(recvnum+prefixplus,length/nodenum-recvPrefix[i],MPI_INT,i,0,comm,&status);
			else
				MPI_Recv(recvnum+prefixplus,length - length/nodenum*(nodenum-1)-recvPrefix[i],MPI_INT,i,0,comm,&status);
		}
	}
	else
		MPI_Send(dataset+length/nodenum*node + prefix, currentdatalength - prefix, MPI_INT , 0, 0, comm);

	if(node == 0)
	{
		prefixplus = 0;
		for(i=0;i<nodenum;i++)
		{
		//	printf("recvprefix %d  ",recvPrefix[i]);
			prefixplus += recvPrefix[i];
		}
		smallP = prefixplus * nodenum / length + 0.5;		

	}
			MPI_Bcast(&smallP,1,MPI_INT,0,comm);
		MPI_Bcast(&prefixplus,1,MPI_INT,0,comm);
		MPI_Bcast(recvnum,length,MPI_INT,0,comm);
	smallDataset = (int *)malloc(prefixplus*sizeof(int));
	largeDataset = (int *)malloc((length-prefixplus)*sizeof(int));

	if(node == 0)
	{
		memcpy(smallDataset,recvnum,prefixplus*sizeof(int));
		memcpy(largeDataset,recvnum+prefixplus,(length-prefixplus)*sizeof(int));	
	}

		MPI_Bcast(smallDataset,prefixplus,MPI_INT,0,comm);
		MPI_Bcast(largeDataset,length-prefixplus,MPI_INT,0,comm);
//	printf("\n%d\n",smallP);

	orginalComm = comm;

	if(node<smallP)
		MPI_Comm_split(orginalComm,0,node,&smallComm);
	else
		MPI_Comm_split(orginalComm,1,node,&smallComm);

//	printf("\nsmall2P:%d node :%d\n",smallP,nodenum);


	// if(node == 0)
	// {
	// 	for(i=0;i<length;i++)
	// 	{
	// 		printf("%d ", recvnum[i]);
	// 	}	
	// }

	// MPI_Comm_rank(smallComm,&nodeplus);
	// MPI_Comm_size(smallComm,&nodenum);

//	printf("%d\n",nodenum);

 	// if(node <= smallP)
 	// 	parallel_quicksort_plus_test(smallDataset, prefixplus, smallDataset[0],smallComm);
 	// else
 	// 	parallel_quicksort_plus_test(largeDataset, length-prefixplus,largeDataset[0], smallComm);

		if(smallP>nodenum-1)
		{
	//		printf("ss\n");
			quicksort(largeDataset,0,length-prefixplus-1);
	//		printf("length-prefixplus-1 %d  ",length-prefixplus-1);
			// for(i=0;i<length-prefixplus-1;i++)
			// {
			// 	printf("%d ",largeDataset[i]);
			// }
		}
		if(smallP == 0)
		{
	//		printf("ll\n");
			quicksort(smallDataset,0,prefixplus-1);
		}

	if(node ==0)
	{
		if(prefixplus > 1)
 	 	{
 	  	 	smallPivot = rand()%prefixplus;
 	 		smallPivot = smallDataset[smallPivot];
 	 	}
 	 	if(prefixplus < length-1)
 	 	{
 	  	 	largePivot = rand()%(length - prefixplus);
 	  //	 	printf("largePivot %d ",largePivot);
 	 		largePivot = largeDataset[largePivot];
 	 //		printf("largePivot %d \n",largePivot);
 	 	}

	}
	MPI_Bcast(&smallPivot,1,MPI_INT,0,comm);
	MPI_Bcast(&largePivot,1,MPI_INT,0,comm);
	//printf("prefixplus %d ",prefixplus);
 	 if(node < smallP)
 	 {
 	 	if(prefixplus >1)
 	 	{
 	 	 	parallel_quicksort_plus(smallDataset, prefixplus, smallPivot,smallComm);		
 	 	}

 	 }
 	else
 	{
 		if(prefixplus < length-1)
 	 	{
			parallel_quicksort_plus(largeDataset, length-prefixplus,largePivot, smallComm);
 	 	}
 	}
 	MPI_Bcast(smallDataset,prefixplus,MPI_INT,0,comm);
	MPI_Bcast(largeDataset,length-prefixplus,MPI_INT,nodenum-1,comm);

 	memset(dataset,0,length*sizeof(int));
 	memcpy(dataset,smallDataset,prefixplus*sizeof(int));
 	memcpy(dataset+prefixplus,largeDataset, (length-prefixplus)*sizeof(int));

	free(recvnum);
	free(smallDataset);
	free(largeDataset);
	return 0;
}

int changedata(int *a, int *b)
{
	int temp;
	temp = *a;
	*a = *b;
	*b = temp;
	return 0;
}

int parallel_quicksort(int *dataset,int pivot,int q,int r)
{
	int x,s;
	int i,j,k;
	if(r<=q)
	{
		return 1;
	}
	else
	{
		x=pivot;
		s = q;
		for(i=q;i<=r;i++)
		{
			if(dataset[i] <= x)
			{

					//		printf("%d,%d\n",i,dataset[i]);

				changedata(&dataset[i],&dataset[s]);
								s = s+1;
			}
		}
	//	changedata(&dataset[pivot],&dataset[s]);
	}
	return s-q;
}

int quicksort(int *dataset, int q, int r)
{
	int x,s;
	int i,j,k;
	int tempdata;
//	printf("%d %d %d %d %d;",q,r,dataset[0],dataset[1],dataset[2]);
	if(r<=q)
	{
	//	printf("lala ");
		return 1;
	}
	else
	{
		x = dataset[q];
		s = q;
		for(i=q+1;i<=r;i++)
		{
			if (dataset[i]<x)
			{
				s = s+1;
				tempdata = dataset[s];
				dataset[s] = dataset[i];
				dataset[i] = tempdata;
			}
		}
		tempdata = dataset[q];
		dataset[q] = dataset[s];
		dataset[s] = tempdata;
//		printf("%d,%d",q,s);
		quicksort(dataset,q,s);
		quicksort(dataset,s+1,r);
	}
	return 0;
}




