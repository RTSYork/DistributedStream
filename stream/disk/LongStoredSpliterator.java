package disk;

import java.nio.*;
import java.util.*;
import java.util.function.*;

public class LongStoredSpliterator extends Spliterators.AbstractLongSpliterator
{
	public static final int BLOCKSIZE = 1048576;

	private LongStoredSource src;
	private static ThreadLocal<LongStoredWork> work = new ThreadLocal<LongStoredWork>();
	private long min;
	private long max;
	private boolean gotWork;

	public LongStoredSpliterator(LongStoredSource src, long min, long max)
	{
		super(Long.MAX_VALUE, 0);
		this.src = src;
		this.min = min;
		if (max < 0)
			max = src.length();
		this.max = max;
		gotWork = false;
	}

	@Override
	public int characteristics()
	{
		return 0;
	}

	@Override
	public boolean tryAdvance(LongConsumer action)
	{
		LongStoredWork w = work.get();
		if (w == null)
			work.set(w = new LongStoredWork());
		if (!gotWork)
			gotWork = src.getWork(w);
		if (min >= max)
			return false;
		action.accept(w.data[(int) min++]);
		return true;
	}

	@Override
	public void forEachRemaining(LongConsumer action)
	{
		LongStoredWork w = work.get();
		if (w == null)
			work.set(w = new LongStoredWork());
		if (!gotWork)
			gotWork = src.getWork(w);
		int len = (int) (w.max - w.min);
		long[] buf = w.data;
		for (int i = 0; i < len; i++)
			action.accept(buf[i]);
	}

	// Round up to the next highest power of 2
	private long NPOW2(long n)
	{
		n--;
		n |= n >> 1;
		n |= n >> 2;
		n |= n >> 4;
		n |= n >> 8;
		n |= n >> 16;
		n |= n >> 32;
		return n + 1;
	}

	@Override
	public Spliterator.OfLong trySplit()
	{
		if (max - min <= BLOCKSIZE)
			return null;
		long mid = NPOW2((max - min) >>> 1);
		if (mid < BLOCKSIZE)
			mid = BLOCKSIZE;
		long newMin = min;
		long newMax = min + mid;
		if (newMax - newMin >= BLOCKSIZE)
		{
			min = newMax;
			return new LongStoredSpliterator(src, newMin, newMax);
		}
		return null;
	}

	@Override
	public long estimateSize()
	{
		return Long.MAX_VALUE;
	}
}
