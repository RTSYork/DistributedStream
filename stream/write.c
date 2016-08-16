/* This program prepares the input file for the experiment. */
#define _BSD_SOURCE
#include <endian.h>
#include <stdio.h>

int main()
{
	FILE *f = fopen("input", "wb");
	long i, w;
	for (i = 1; i <= 131072; i++)
	{
		w = htobe64(i); /* Java is big-endian */
		fwrite(&w, sizeof (long), 1, f);
	}
	fclose(f);
	return 0;
}
