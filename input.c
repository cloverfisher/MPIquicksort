#include <stdio.h>
#include <stdlib.h>

int main()
{
	freopen("inputdata.txt","w",stdout);
	int number = 100;
	int i;
	for(i=0;i<number;i++)
	{
		printf("%d ", number-i);	
	}
	return 0;
}
