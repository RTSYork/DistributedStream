package disk;

import java.nio.*;

public class LongStoredWork
{
	public long[] data;
	public long[] tmpbuf;
	public int size;
	public long min, max;

	public LongStoredWork()
	{
		data = new long[LongStoredSpliterator.BLOCKSIZE];
		tmpbuf = new long[LongStoredSpliterator.BLOCKSIZE];
		size = 0;
		min = max = 0;
	}
}
