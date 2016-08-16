package stored;

public class LongStoredWork
{
	public long[] data;
	public int size;
	public long min, max;

	public LongStoredWork()
	{
		data = new long[LongStoredSpliterator.BLOCKSIZE];
		size = 0;
		min = max = 0;
	}
}
