package dstream;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

/**
 * Spliterator implementation for receiving data items in a distribute operation.
 */
class DistributedSpliterator<T> implements Spliterator<T>
{
	private static final int BATCH = 256;

	private int tag;
	private int end;
	private Runnable cleanup;

	static final class Holder<U> implements Consumer<U>
	{
		U obj;

		@Override
		public void accept(U value)
		{
			obj = value;
		}
	}

	public DistributedSpliterator(int tag, int grpSize)
	{
		this.tag = tag;
		end = grpSize;
		cleanup = null;
	}

	public DistributedSpliterator(int tag, int grpSize, Runnable cleanup)
	{
		this(tag, grpSize);
		this.cleanup = cleanup;
	}

	@Override
	public int characteristics()
	{
		return IMMUTABLE | SUBSIZED;
	}

	@Override
	public boolean tryAdvance(Consumer<? super T> action)
	{
		while (end > 0)
		{
			T obj = (T) ComputeNode.comm.recvObject(tag);
			if (obj != null)
			{
				action.accept(obj);
				return true;
			}
			end--;
		}
		if (cleanup != null)
			cleanup.run();
		return false;
	}

	@Override
	public Spliterator<T> trySplit()
	{
		List<T> li = new ArrayList<T>(BATCH);
		Holder<T> hold = new Holder<T>();
		for (int i = 0; end > 0 && i < BATCH; i++)
			if (tryAdvance(hold))
				li.add(hold.obj);
			else
				break;
		if (li.size() > 0)
			return Spliterators.spliterator(li.iterator(), li.size(), characteristics() | SIZED);
		return null;
	}

	@Override
	public long estimateSize()
	{
		return Long.MAX_VALUE;
	}
}
