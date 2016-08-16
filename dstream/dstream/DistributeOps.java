package dstream;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.stream.*;
import java.util.function.*;

class DistributeOps
{
	private DistributeOps() { } // Do not instantiate

	public static <T> DistributedStream<T> distribute(ReferencePipeline<T> upstream, ComputeGroup dst, Partitioner<? super T> p)
	{
		LinkedHashSet<ComputeNode> unionSet = new LinkedHashSet<ComputeNode>(upstream.grp);
		unionSet.addAll(dst);
		ComputeGroup union = new ComputeGroup(unionSet);
		final int size = dst.size();
		final int tag = ComputeNode.nextTag(union);
		Thread th = new Thread(() ->
		{
			upstream.localForEach(e ->
			{
				ComputeNode.comm.sendObject(e, dst.get(Math.abs(p.partition(e)) % size).rank, tag);
			});
			// No more data: send NULL to each node in current and destination groups
			for (ComputeNode node: union)
				ComputeNode.comm.sendObject(null, node.rank, tag);
		});
		th.start();
		DistributedSpliterator<T> sp = new DistributedSpliterator<T>(tag, upstream.grp.size(), () ->
		{
			try
			{
				th.join();
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		});
		return new ReferencePipeline<T>(StreamSupport.stream(sp, upstream.isParallel()), dst);
	}

	public static <T> ReferencePipeline<T> broadcast(ReferencePipeline<T> upstream)
	{
		final ComputeGroup grp = upstream.grp;
		final int size = grp.size();
		final int tag = ComputeNode.nextTag(grp);
		Thread th = new Thread(() ->
		{
			upstream.localForEach(e ->
			{
				for (ComputeNode n: grp)
					ComputeNode.comm.sendObject(e, n.rank, tag);
			});
			// No more data: send NULL to each node in group
			for (ComputeNode n: grp)
				ComputeNode.comm.sendObject(null, n.rank, tag);
		});
		th.start();
		DistributedSpliterator<T> sp = new DistributedSpliterator<T>(tag, upstream.grp.size(), () ->
		{
			try
			{
				th.join();
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		});
		return new ReferencePipeline<T>(StreamSupport.stream(sp, upstream.isParallel()), grp);
	}

	public static <T> T reduce(T identity, ComputeGroup grp, BinaryOperator<T> combiner)
	{
		T result = identity;
		final int tag = ComputeNode.nextTag(grp);
		if (grp.get(0).isSelf())
		{
			for (int i = 1; i < grp.size(); i++)
			{
				T more = (T) ComputeNode.comm.recvObject(tag);
				result = combiner.apply(result, more);
			}
			for (int i = 1; i < grp.size(); i++)
				ComputeNode.comm.sendObject(result, grp.get(i).rank, tag);
		}
		else
		{
			ComputeNode.comm.sendObject(identity, grp.get(0).rank, tag);
			result = (T) ComputeNode.comm.recvObject(tag);
		}
		return result;
	}

	static <T> T combine(T identity, ComputeGroup grp, BiConsumer<T,T> combiner)
	{
		T result = identity;
		final int tag = ComputeNode.nextTag(grp);
		if (grp.get(0).isSelf())
		{
			for (int i = 1; i < grp.size(); i++)
			{
				T more = (T) grp.get(i).comm.recvObject(tag);
				combiner.accept(result, more);
			}
			for (int i = 1; i < grp.size(); i++)
				ComputeNode.comm.sendObject(result, grp.get(i).rank, tag);
		}
		else
		{
			ComputeNode.comm.sendObject(identity, grp.get(0).rank, tag);
			result = (T) ComputeNode.comm.recvObject(tag);
		}
		return result;
	}

	public static <T> DistributedStream<T>[] split(ReferencePipeline<T> upstream, int num)
	{
		// TODO
		return null;
	}

	public static DistributedIntStream[] split(IntPipeline upstream, int num)
	{
		// TODO
		return null;
	}

	public static DistributedLongStream[] split(LongPipeline upstream, int num)
	{
		// TODO
		return null;
	}

	public static DistributedDoubleStream[] split(DoublePipeline upstream, int num)
	{
		// TODO
		return null;
	}

	public static <T> DistributedStream<T> join(ReferencePipeline<T> upstream, DistributedStream<T>[] streams)
	{
		// TODO
		return null;
	}

	public static DistributedIntStream join(IntPipeline upstream, DistributedIntStream[] streams)
	{
		// TODO
		return null;
	}

	public static DistributedLongStream join(LongPipeline upstream, DistributedLongStream[] streams)
	{
		// TODO
		return null;
	}

	public static DistributedDoubleStream join(DoublePipeline upstream, DistributedDoubleStream[] streams)
	{
		// TODO
		return null;
	}
}
