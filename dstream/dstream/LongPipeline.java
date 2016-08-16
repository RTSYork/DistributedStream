package dstream;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

class LongPipeline implements DistributedLongStream
{
	LongStream s;
	ComputeGroup grp;

	LongPipeline(LongStream s)
	{
		this(s, ComputeGroup.getCluster());
	}

	LongPipeline(LongStream s, ComputeGroup grp)
	{
		this.s = s;
		this.grp = grp;
	}

	LongPipeline(LongPipeline upstream)
	{
		s = upstream.s;
		grp = upstream.grp;
	}

	LongPipeline(LongPipeline upstream, LongStream s)
	{
		this.s = s;
		grp = upstream.grp;
	}

	LongPipeline(LongPipeline upstream, LongStream s, ComputeGroup grp)
	{
		this.s = s;
		this.grp = grp;
	}

	@Override
	public ComputeGroup getComputeGroup()
	{
		return grp;
	}

	@Override
	public void setComputeGroup(ComputeGroup grp)
	{
		if (grp != null)
			this.grp = grp;
		else
			this.grp = ComputeGroup.getCluster();
	}

	// Data distribution operations

	@Override
	public DistributedLongStream distribute()
	{
		return distribute(grp, d -> Long.valueOf(d).hashCode());
	}

	@Override
	public DistributedLongStream distribute(LongPartitioner p)
	{
		return distribute(ComputeGroup.getCluster());
	}

	@Override
	public DistributedLongStream distribute(ComputeGroup grp)
	{
		return distribute(grp, d -> Long.valueOf(d).hashCode());
	}

	@Override
	public DistributedLongStream distribute(ComputeGroup grp, LongPartitioner p)
	{
		ReferencePipeline<Long> objs = new ReferencePipeline<Long>(s.mapToObj(n -> Long.valueOf(n)), grp);
		return new LongPipeline(
			DistributeOps.<Long>distribute(objs, grp, n -> p.partition(n)).mapToLong(n -> n),
			grp);
	}

	@Override
	public DistributedLongStream distribute(ComputeNode node)
	{
		ComputeGroup single = new ComputeGroup(node);
		return distribute(single);
	}

	@Override
	public DistributedLongStream[] split(int numStreams)
	{
		return DistributeOps.split(this, numStreams);
	}

	@Override
	public DistributedLongStream join(DistributedLongStream... streams)
	{
		return DistributeOps.join(this, streams);
	}

	// Local operations

	@Override
	public OptionalDouble localAverage()
	{
		return s.average();
	}

	@Override
	public <R> R localCollect(Supplier<R> supplier, ObjLongConsumer<R> accumulator, BiConsumer<R, R> combiner)
	{
		return s.collect(supplier, accumulator, combiner);
	}

	@Override
	public long localCount()
	{
		return s.count();
	}

	@Override
	public DistributedLongStream localDistinct()
	{
		return new LongPipeline(s.distinct(), grp);
	}

	@Override
	public void localForEach(LongConsumer action)
	{
		s.forEach(action);
	}

	@Override
	public void localForEachOrdered(LongConsumer action)
	{
		s.forEachOrdered(action);
	}

	@Override
	public DistributedLongStream localLimit(long maxSize)
	{
		return new LongPipeline(s.limit(maxSize), grp);
	}

	@Override
	public OptionalLong localMax()
	{
		return s.max();
	}

	@Override
	public OptionalLong localMin()
	{
		return s.min();
	}

	@Override
	public DistributedLongStream localPeek(LongConsumer action)
	{
		return new LongPipeline(s.peek(action), grp);
	}

	@Override
	public OptionalLong localReduce(LongBinaryOperator op)
	{
		return s.reduce(op);
	}

	@Override
	public long localReduce(long identity, LongBinaryOperator op)
	{
		return s.reduce(identity, op);
	}

	@Override
	public DistributedLongStream localSkip(long n)
	{
		return new LongPipeline(s.skip(n), grp);
	}

	@Override
	public DistributedLongStream localSorted()
	{
		return new LongPipeline(s.sorted(), grp);
	}

	@Override
	public long localSum()
	{
		return s.sum();
	}

	@Override
	public LongSummaryStatistics localSummaryStatistics()
	{
		return s.summaryStatistics();
	}

	@Override
	public long[] localToArray()
	{
		return s.toArray();
	}

	// Overrides

	@Override
	public DistributedDoubleStream asDoubleStream()
	{
		return new DoublePipeline(s.asDoubleStream(), grp);
	}

	@Override
	public DistributedStream<Long> boxed()
	{
		return new ReferencePipeline<Long>(s.boxed(), grp);
	}

	@Override
	public DistributedLongStream distinct()
	{
		return null; // TODO
	}

	@Override
	public DistributedLongStream filter(LongPredicate predicate)
	{
		return new LongPipeline(s.filter(predicate), grp);
	}

	@Override
	public DistributedLongStream flatMap(LongFunction<? extends LongStream> mapper)
	{
		return new LongPipeline(s.flatMap(mapper), grp);
	}

	@Override
	public DistributedLongStream limit(long maxSize)
	{
		return null; // TODO
	}

	@Override
	public DistributedLongStream map(LongUnaryOperator mapper)
	{
		return new LongPipeline(s.map(mapper), grp);
	}

	@Override
	public DistributedDoubleStream mapToDouble(LongToDoubleFunction mapper)
	{
		return new DoublePipeline(s.mapToDouble(mapper), grp);
	}

	@Override
	public DistributedIntStream mapToInt(LongToIntFunction mapper)
	{
		return new IntPipeline(s.mapToInt(mapper), grp);
	}

	@Override
	public <U> DistributedStream<U> mapToObj(LongFunction<? extends U> mapper)
	{
		return new ReferencePipeline<U>(s.mapToObj(mapper), grp);
	}

	@Override
	public DistributedLongStream peek(LongConsumer action)
	{
		return new LongPipeline(s.peek(action), grp);
	}

	@Override
	public DistributedLongStream skip(long n)
	{
		return null; // TODO
	}

	@Override
	public DistributedLongStream sorted()
	{
		return null; // TODO
	}

	// Stream operations

	@Override
	public boolean allMatch(LongPredicate predicate)
	{
		return s.allMatch(predicate); // TODO
	}

	@Override
	public boolean anyMatch(LongPredicate predicate)
	{
		return s.anyMatch(predicate); // TODO
	}

	@Override
	public OptionalDouble average()
	{
		return s.average(); // TODO
	}

	@Override
	public <R> R collect(Supplier<R> supplier, ObjLongConsumer<R> accumulator, BiConsumer<R,R> combiner)
	{
		return s.collect(supplier, accumulator, combiner); // TODO
	}

	@Override
	public long count()
	{
		return s.count(); // TODO
	}

	@Override
	public OptionalLong findAny()
	{
		return s.findAny(); // TODO
	}

	@Override
	public OptionalLong findFirst()
	{
		return s.findFirst(); // TODO
	}

	@Override
	public void forEach(LongConsumer action)
	{
		s.forEach(action);
	}

	@Override
	public void forEachOrdered(LongConsumer action)
	{
		s.forEachOrdered(action);
	}

	@Override
	public OptionalLong max()
	{
		return s.max(); // TODO
	}

	@Override
	public OptionalLong min()
	{
		return s.min(); // TODO
	}

	@Override
	public boolean noneMatch(LongPredicate predicate)
	{
		return s.noneMatch(predicate); // TODO
	}

	@Override
	public OptionalLong reduce(LongBinaryOperator op)
	{
		return s.reduce(op); // TODO
	}

	@Override
	public long reduce(long identity, LongBinaryOperator op)
	{
		return s.reduce(identity, op); // TODO
	}

	@Override
	public long sum()
	{
		return s.sum(); // TODO
	}

	@Override
	public LongSummaryStatistics summaryStatistics()
	{
		return s.summaryStatistics(); // TODO
	}

	@Override
	public long[] toArray()
	{
		return s.toArray(); // TODO
	}

	// BaseStream overrides

	@Override
	public void close()
	{
		s.close();
	}

	@Override
	public boolean isParallel()
	{
		return s.isParallel();
	}

	@Override
	public PrimitiveIterator.OfLong iterator()
	{
		return s.iterator();
	}

	@Override
	public DistributedLongStream onClose(Runnable closeHandler)
	{
		return new LongPipeline(s.onClose(closeHandler), grp);
	}

	@Override
	public DistributedLongStream parallel()
	{
		return new LongPipeline(s.parallel(), grp);
	}

	@Override
	public DistributedLongStream sequential()
	{
		return new LongPipeline(s.sequential(), grp);
	}

	@Override
	public Spliterator.OfLong spliterator()
	{
		return s.spliterator();
	}

	@Override
	public DistributedLongStream unordered()
	{
		return new LongPipeline(s.unordered(), grp);
	}
}
