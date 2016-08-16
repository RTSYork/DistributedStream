/* NUMA memory access times */
#define _GNU_SOURCE

#include <sched.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#define NCPU		16
#define DATA_SIZE	(1 << 28) /* Must be larger than cache */

void set_cpu_affinity(int cpu)
{
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(cpu, &cpuset);
	sched_setaffinity(0, sizeof cpuset, &cpuset);
}

long timediff(struct timeval *t1, struct timeval *t2)
{
	return (t1->tv_sec - t2->tv_sec) * 1000000 + t1->tv_usec - t2->tv_usec;
}

int main()
{
	int alloc, cpu;
	printf("Alloc\tCPU\tTime\n");
	for (alloc = 0; alloc < NCPU; alloc++)
	{
		set_cpu_affinity(alloc);
		uint8_t *data = malloc(DATA_SIZE);
		int i;
		for (i = 0; i < DATA_SIZE; i++)
			data[i] = (uint8_t) i;
		for (cpu = 0; cpu < NCPU; cpu++)
		{
			struct timeval t1, t2;
			int count;
			set_cpu_affinity(cpu);
			uint8_t xor = 0;
			gettimeofday(&t1, NULL);
			for (count = 0; count < 10; count++)
				for (i = 0; i < DATA_SIZE; i++)
					xor ^= data[i];
			gettimeofday(&t2, NULL);
			printf("%d\t%d\t%ld\n", alloc, cpu, timediff(&t2, &t1));
		}
		free(data);
	}
	return 0;
}
