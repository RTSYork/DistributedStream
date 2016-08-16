package dstream;

import java.io.*;
import java.nio.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.util.function.*;
import java.util.stream.*;
import java.util.zip.*;

/**
 * Implementation of the sorted operation.
 * Uses an external merge sort to handle streams that are larger than memory.
 */
class SortedOps
{
	private static final int SAMPLES = 1024;
	private static final int BATCH = 1024;

	public static <T extends Comparable<T>> DistributedStream<T> sorted(ReferencePipeline<T> upstream)
	{
		return sorted(upstream, Comparator.<T>naturalOrder());
	}

	public static <T> DistributedStream<T> sorted(ReferencePipeline<T> upstream, Comparator<? super T> cmp)
	{
		final boolean parallel = upstream.isParallel();
		final int buckets = upstream.grp.size();
		final int rank = upstream.grp.indexOf(ComputeNode.getSelf());
		// Sample data to establish bucket ranges
		int localSamples = SAMPLES / buckets + (rank < SAMPLES % buckets ? 1 : 0);
		int bcastTag = ComputeNode.nextTag(upstream.grp);
		int distribTag = ComputeNode.nextTag(upstream.grp);
		AtomicLong count = new AtomicLong(0);
		AtomicLong count2 = new AtomicLong(0);
		List<T> samples = new ArrayList<T>(SAMPLES);
		Object[] search = new Object[buckets - 1];
		ReadWriteLock rwlock = new ReentrantReadWriteLock();
		Lock rlock = rwlock.readLock();
		Lock wlock = rwlock.writeLock();
		wlock.lock();
		Thread sender = new Thread(() ->
		{
			upstream.localForEach(e ->
			{
				long c = count.incrementAndGet();
				if (c > localSamples) // Send remaining data
				{
					rlock.lock();
					int pos = Arrays.binarySearch(search, e, null);
					if (pos < 0)
						pos = -pos - 1;
					ComputeNode.comm.sendObject(e, upstream.grp.get(pos).rank, distribTag);
					rlock.unlock();
				}
				else // Broadcast samples
				{
					for (int i = 0; i < buckets; i++)
						ComputeNode.comm.sendObject(e, upstream.grp.get(i).rank, bcastTag);
					if (c == localSamples)
						for (int i = 0; i < buckets; i++)
							ComputeNode.comm.sendObject(null, upstream.grp.get(i).rank, bcastTag);
				}
			});
			if (count.get() < localSamples)
				for (int i = 0; i < buckets; i++)
					ComputeNode.comm.sendObject(null, upstream.grp.get(i).rank, bcastTag);
			for (int i = 0; i < buckets; i++)
				ComputeNode.comm.sendObject(null, upstream.grp.get(i).rank, distribTag);
		});
		sender.start();
		int end = 0;
		while (true)
		{
			T obj = (T) ComputeNode.comm.recvObject(bcastTag);
			if (obj != null)
				samples.add(obj);
			else if (++end >= buckets)
				break;
		}
		samples.sort(cmp);
		List<T> ours = new ArrayList<>();
		for (int i = 1; i < buckets; i++)
			search[i - 1] = samples.get(i * samples.size() / buckets);
		wlock.unlock();
		int from = rank * samples.size() / buckets;
		int to = (rank + 1) * samples.size() / buckets;
		for (int i = from; i < to; i++)
			ours.add(samples.get(i));
		MergeState<T> state = new MergeState<>(cmp);
		List<T> li = new ArrayList<>(BATCH);
		state.add(ours);
		samples.clear();
		samples = null;
		ours.clear();
		ours = null;
		// Sort rest of data
		end = 0;
		while (true)
		{
			if (li.size() > 16 && MemoryMonitor.lowMemory())
			{
				System.err.println(ComputeNode.getSelf().getName() + ": Spilling " + li.size() + " items to disk");
				li.sort(cmp);
				state.add(li);
				li = null;
				MemoryMonitor.reclaimMemory();
				li = new ArrayList<>(BATCH);
			}
			T obj = (T) ComputeNode.comm.recvObject(distribTag);
			if (obj != null)
				li.add(obj);
			else if (++end >= buckets)
				break;
		}
		if (!li.isEmpty())
		{
			li.sort(cmp);
			state.add(li);
			li = null;
		}
		try
		{
			li = null;
			sender.join();
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		Spliterator<T> sp = state.merge();
		if (sp != null)
			return DistributedStreamSupport.stream(sp, parallel);
		if (parallel)
			return DistributedStream.<T>empty().parallel();
		return DistributedStream.<T>empty();
	}
}

class MergeItem<T>
{
	public int index;
	public T obj;

	public MergeItem(int index, T obj)
	{
		this.index = index;
		this.obj = obj;
	}
}

class MergeState<T>
{
	private static final int MERGE_BATCH = 64; // Number of files to merge per pass
	private static final int RESET_BATCH = 128;

	private static AtomicLong gid = new AtomicLong(0);

	private Queue<Part> mq;
	private Comparator<? super T> cmp;
	private File spillDir;

	private class Part
	{
		public long id;
		public long length;

		public Part(long id, long length)
		{
			this.id = id;
			this.length = length;
		}
	}

	public MergeState(Comparator<? super T> cmp)
	{
		mq = new PriorityQueue<>(MERGE_BATCH, Comparator.comparingLong(fl -> fl.length));
		this.cmp = cmp;
		try
		{
			spillDir = new File(Files.createTempDirectory("spill").toString());
			spillDir.deleteOnExit();
		}
		catch (IOException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}

	// Adds an already-sorted list of items
	public void add(List<T> li)
	{
		long id = gid.incrementAndGet();
		long length = 0;
		try
		{
			File f = new File(Paths.get(spillDir.toString(), String.valueOf(id)).toString());
			ObjectOutputStream os = new ObjectOutputStream(new DeflaterOutputStream(new FileOutputStream(f), new Deflater(Deflater.BEST_SPEED)));
			int count = 0;
			for (T i: li)
			{
				os.writeObject(i);
				if (++count >= RESET_BATCH)
				{
					count = 0;
					os.reset();
				}
			}
			os.close();
			length = f.length();
		}
		catch (IOException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		synchronized (mq)
		{
			mq.add(new Part(id, length));
		}
	}

	Part merge(List<Long> ids)
	{
		long outID = -1;
		long length = 0;
		int size = ids.size();
		System.err.println(ComputeNode.getSelf().getName() + ": Merging (" + size + "-way)");
		PriorityQueue<MergeItem<T>> q = new PriorityQueue<>(size, Comparator.<MergeItem<T>, T>comparing(e -> e.obj, cmp));
		ObjectInputStream[] is = new ObjectInputStream[size];
		try
		{
			for (int i = 0; i < size; i++)
			{
				is[i] = new ObjectInputStream(new InflaterInputStream(new FileInputStream(Paths.get(spillDir.toString(), String.valueOf(ids.get(i))).toString())));
				try
				{
					T obj = (T) is[i].readObject();
					if (obj != null)
						q.add(new MergeItem<T>(i, obj));
				}
				catch (IOException e)
				{
					continue;
				}
			}
			outID = gid.incrementAndGet();
			File f = new File(Paths.get(spillDir.toString(), String.valueOf(outID)).toString());
			ObjectOutputStream os = new ObjectOutputStream(new DeflaterOutputStream(new FileOutputStream(f), new Deflater(Deflater.BEST_SPEED)));
			int count = 0;
			while (!q.isEmpty())
			{
				MergeItem<T> i = q.remove();
				os.writeObject(i.obj);
				if (++count >= RESET_BATCH)
				{
					count = 0;
					os.reset();
				}
				try
				{
					i.obj = (T) is[i.index].readObject();
				}
				catch (IOException e)
				{
					continue;
				}
				q.add(i);
			}
			os.close();
			for (int i = 0; i < size; i++)
			{
				is[i].close();
				Files.deleteIfExists(Paths.get(spillDir.toString(), String.valueOf(ids.get(i))));
			}
			length = f.length();
		}
		catch (IOException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		catch (ClassNotFoundException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		return new Part(outID, length);
	}

	public Spliterator<T> merge()
	{
		synchronized (mq)
		{
			List<Long> ids = new ArrayList<>(MERGE_BATCH);
			while (mq.size() > MERGE_BATCH - 1)
			{
				ids.add(mq.remove().id);
				if (ids.size() >= MERGE_BATCH)
				{
					mq.add(merge(ids));
					ids.clear();
				}
			}
			while (!mq.isEmpty())
				ids.add(mq.remove().id);
			long id;
			if (!ids.isEmpty())
				return new SortedSpliterator<T>(ids, cmp, spillDir);
			return null;
		}
	}
}

class SortedSpliterator<T> implements Spliterator<T>
{
	private static final int BATCH = 256;

	private File spillDir;
	private List<Long> ids;
	private ObjectInputStream[] is;
	private PriorityQueue<MergeItem<T>> q;
	private MergeItem<T> nextItem;
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

	public SortedSpliterator(List<Long> ids, Comparator<? super T> cmp, File spillDir)
	{
		this.spillDir = spillDir;
		this.ids = ids;
		this.cmp = cmp;
		int size = ids.size();
		is = new ObjectInputStream[size];
		q = new PriorityQueue<>(size, Comparator.<MergeItem<T>, T>comparing(e -> e.obj, cmp));
		nextItem = null;
		try
		{
			for (int i = 0; i < size; i++)
			{
				long id = ids.get(i);
				is[i] = new ObjectInputStream(new InflaterInputStream(new FileInputStream(Paths.get(spillDir.toString(), String.valueOf(id)).toString())));
				try
				{
					T obj = (T) is[i].readObject();
					if (obj != null)
						q.add(new MergeItem<T>(i, obj));
				}
				catch (IOException e)
				{
					continue;
				}
			}
			nextItem = q.remove();
		}
		catch (IOException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		catch (ClassNotFoundException e)
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
			try
			{
				nextItem.obj = (T) is[nextItem.index].readObject();
				q.add(nextItem);
			}
			catch (IOException e)
			{
				try
				{
					is[nextItem.index].close();
					Files.deleteIfExists(Paths.get(spillDir.toString(), String.valueOf(ids.get(nextItem.index)).toString()));
					if (q.isEmpty())
						Files.deleteIfExists(spillDir.toPath());
				}
				catch (IOException x)
				{
					x.printStackTrace();
					System.exit(1);
				}
			}
			catch (ClassNotFoundException e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			nextItem = (!q.isEmpty() ? q.remove() : null);
			return true;
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
