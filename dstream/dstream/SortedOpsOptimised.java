package dstream;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.stream.*;

import dstream.util.*;

/**
 * Improved implementation of the sorted operation.
 * Sorts local data first, then merges the data.
 */
class SortedOpsOptimised
{
	public static <T extends Comparable<T>> DistributedStream<T> sorted(ReferencePipeline<T> upstream)
	{
		return sorted(upstream, Comparator.<T>naturalOrder());
	}

	public static <T> DistributedStream<T> sorted(ReferencePipeline<T> upstream, Comparator<? super T> cmp)
	{
		final int rank = upstream.grp.indexOf(ComputeNode.getSelf());
		final int buckets = upstream.grp.size();
		int bcastTag = ComputeNode.nextTag(upstream.grp);
		int[] distribTag = new int[buckets];
		for (int i = 0; i < buckets; i++)
			distribTag[i] = ComputeNode.nextTag(upstream.grp);
		// Get all local items and sort
		ArrayList<T> local = upstream.localCollect(Collectors.toCollection(() -> new ArrayList<>()));
		local.sort(cmp);
		// Find value boundaries
		Object[] search;
		int[] pos = new int[buckets];
		if (rank == 0)
		{
			search = new Object[buckets - 1];
			for (int i = 1; i < buckets; i++)
			{
				int p = (int) ((long) local.size() * (long) i / (long) buckets);
				search[i - 1] = local.get(p);
				pos[i - 1] = p;
			}
			for (int i = 1; i < buckets; i++)
				ComputeNode.comm.sendObject(search, upstream.grp.get(i).rank, bcastTag);
		}
		else
		{
			search = (Object[]) ComputeNode.comm.recvObject(bcastTag);
			for (int i = 1; i < buckets; i++)
			{
				int p = Collections.<T>binarySearch(local, (T) search[i - 1], cmp);
				pos[i - 1] = (p >= 0 ? p : -p - 1);
			}
		}
		pos[buckets - 1] = local.size();
		// Send values to other nodes
		Thread[] th = new Thread[buckets];
		for (int b = 0; b < buckets; b++)
		{
			if (b == rank)
			{
				th[b] = null;
				continue;
			}
			final int t = b;
			th[t] = new Thread(() ->
			{
				int i = (t > 0 ? pos[t - 1] : 0);
				ComputeNode n = upstream.grp.get(t);
				while (i < pos[t])
				{
					ComputeNode.comm.sendObject(local.get(i), n.rank, distribTag[rank]);
					local.set(i, null);
					i++;
				}
				ComputeNode.comm.sendObject(null, n.rank, distribTag[rank]);
			});
			th[t].start();
		}
		// Receive values in spliterator
		SortedSpliteratorOptimised<T> sp = new SortedSpliteratorOptimised<>(local, (rank == 0 ? 0 : pos[rank - 1]), pos[rank], rank, distribTag, th, cmp);
		return DistributedStreamSupport.stream(sp, upstream.isParallel());
	}
}

class SortedSpliteratorOptimised<T> implements Spliterator<T>
{
	private static final int BATCH = 256;

	private ArrayList<T> local;
	private int localIndex;
	private int localTo;
	private int rank;
	private int[] distribTag;
	private PriorityQueue<QItem<T>> q;
	private LinkedBlockingQueue[] tq;
	private QItem<T> nextItem;
	private Thread[] sendThreads;
	private Thread[] recvThreads;
	private Object stop;
	private Comparator<? super T> cmp;

	static final class Holder<U> implements Consumer<U>
	{
		U obj;

		@Override
		public void accept(U value)
		{
			obj = value;
		}
	}

	static final class QItem<T>
	{
		int rank;
		T obj;

		public QItem(int rank, T obj)
		{
			this.rank = rank;
			this.obj = obj;
		}
	}

	public SortedSpliteratorOptimised(ArrayList<T> local, int localFrom, int localTo, int rank, int[] distribTag, Thread[] th, Comparator<? super T> cmp)
	{
		this.local = local;
		localIndex = localFrom;
		this.localTo = localTo;
		this.rank = rank;
		this.distribTag = distribTag;
		sendThreads = th;
		this.cmp = cmp;
		q = new PriorityQueue<>(distribTag.length, Comparator.<QItem<T>, T>comparing(e -> e.obj, cmp));
		recvThreads = new Thread[distribTag.length];
		tq = new LinkedBlockingQueue[distribTag.length];
		stop = new Object();
		for (int i = 0; i < distribTag.length; i++)
		{
			recvThreads[i] = null;
			if (i == rank)
				continue;
			tq[i] = new LinkedBlockingQueue();
			final int t = i;
			recvThreads[i] = new Thread(() ->
			{
				try
				{
					while (true)
					{
						T obj = (T) ComputeNode.comm.recvObject(distribTag[t]);
						if (obj != null)
						{
							tq[t].put(obj);
							continue;
						}
						tq[t].put(stop);
						break;
					}
				}
				catch (InterruptedException e)
				{
					e.printStackTrace();
					System.exit(1);
				}
			});
			recvThreads[i].start();
		}
		nextItem = null;
		try
		{
			for (int i = 0; i < distribTag.length; i++)
			{
				T obj;
				if (i != rank)
				{
					Object o = tq[i].take();
					if (o != stop)
						obj = (T) o;
					else
						obj = null;
				}
				else if (localIndex < localTo)
					obj = local.get(localIndex++);
				else
					obj = null;
				if (obj == null)
					continue;
				q.add(new QItem<>(i, obj));
			}
			if (!q.isEmpty())
				nextItem = q.remove();
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}

	@Override
	public int characteristics()
	{
		return IMMUTABLE | SUBSIZED | SORTED;
	}

	@Override
	public Comparator<? super T> getComparator()
	{
		return cmp;
	}

	@Override
	public boolean tryAdvance(Consumer<? super T> action)
	{
		if (nextItem != null)
		{
			action.accept(nextItem.obj);
			if (nextItem.rank != rank)
			{
				try
				{
					Object o = tq[nextItem.rank].take();
					if (o != stop)
						nextItem.obj = (T) o;
					else
						nextItem.obj = null;
				}
				catch (InterruptedException e)
				{
					e.printStackTrace();
					System.exit(1);
				}
			}
			else if (localIndex < localTo)
				nextItem.obj = local.get(localIndex++);
			else
				nextItem.obj = null;
			if (nextItem.obj != null)
				q.add(nextItem);
			nextItem = (!q.isEmpty() ? q.remove() : null);
			return true;
		}
		local = null;
		try
		{
			for (int i = 0; i < sendThreads.length; i++)
				if (sendThreads[i] != null)
					sendThreads[i].join();
			for (int i = 0; i < recvThreads.length; i++)
				if (recvThreads[i] != null)
					recvThreads[i].join();
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		return false;
	}

	@Override
	public Spliterator<T> trySplit()
	{
		List<T> li = new ArrayList<T>(BATCH);
		Holder<T> hold = new Holder<T>();
		for (int i = 0; i < BATCH; i++)
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
