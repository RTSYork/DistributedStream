package dstream;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

class IntPipeline implements DistributedIntStream
{
	IntStream s;
	ComputeGroup grp;

	IntPipeline(IntStream s)
	{
		this(s, ComputeGroup.getCluster());
	}

	IntPipeline(IntStream s, ComputeGroup grp)
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
	public DistributedIntStream distribute()
	{
		return distribute(grp, d -> Integer.valueOf(d).hashCode());
	}

	@Override
	public DistributedIntStream distribute(IntPartitioner p)
	{
		return distribute(ComputeGroup.getCluster());
	}

	@Override
	public DistributedIntStream distribute(ComputeGroup grp)
	{
		return distribute(grp, d -> Integer.valueOf(d).hashCode());
	}

	@Override
	public DistributedIntStream distribute(ComputeGroup grp, IntPartitioner p)
	{
		ReferencePipeline<Integer> objs = new ReferencePipeline<Integer>(s.mapToObj(n -> Integer.valueOf(n)), grp);
		return new IntPipeline(
			DistributeOps.<Integer>distribute(objs, grp, n -> p.partition(n)).mapToInt(n -> n),
			grp);
	}

	@Override
	public DistributedIntStream distribute(ComputeNode node)
	{
		ComputeGroup single = new ComputeGroup(node);
		return distribute(single);
	}

	@Override
	public DistributedIntStream[] split(int numStreams)
	{
		return DistributeOps.split(this, numStreams);
	}

	@Override
	public DistributedIntStream join(DistributedIntStream... streams)
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
	public <R> R localCollect(Supplier<R> supplier, ObjIntConsumer<R> accumulator, BiConsumer<R, R> combiner)
	{
		return s.collect(supplier, accumulator, combiner);
	}

	@Override
	public long localCount()
	{
		return s.count();
	}

	@Override
	public DistributedIntStream localDistinct()
	{
		return new IntPipeline(s.distinct(), grp);
	}

	@Override
	public DistributedIntStream localLimit(long maxSize)
	{
		return new IntPipeline(s.limit(maxSize), grp);
	}

	@Override
	public OptionalInt localMax()
	{
		return s.max();
	}

	@Override
	public OptionalInt localMin()
	{
		return s.min();
	}

	@Override
	public DistributedIntStream localPeek(IntConsumer action)
	{
		return new IntPipeline(s.peek(action), grp);
	}

	@Override
	public OptionalInt localReduce(IntBinaryOperator op)
	{
		return s.reduce(op);
	}

	@Override
	public int localReduce(int identity, IntBinaryOperator op)
	{
		return s.reduce(identity, op);
	}

	@Override
	public DistributedIntStream localSkip(long n)
	{
		return new IntPipeline(s.skip(n), grp);
	}

	@Override
	public DistributedIntStream localSorted()
	{
		return new IntPipeline(s.sorted(), grp);
	}

	@Override
	public int localSum()
	{
		return s.sum();
	}

	@Override
	public IntSummaryStatistics localSummaryStatistics()
	{
		return s.summaryStatistics();
	}

	@Override
	public int[] localToArray()
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
	public DistributedLongStream asLongStream()
	{
		return new LongPipeline(s.asLongStream(), grp);
	}

	@Override
	public DistributedStream<Integer> boxed()
	{
		return new ReferencePipeline<Integer>(s.boxed(), grp);
	}

	@Override
	public DistributedIntStream distinct()
	{
		return null; // TODO
	}

	@Override
	public DistributedIntStream filter(IntPredicate predicate)
	{
		return new IntPipeline(s.filter(predicate), grp);
	}

	@Override
	public DistributedIntStream flatMap(IntFunction<? extends IntStream> mapper)
	{
		return new IntPipeline(s.flatMap(mapper), grp);
	}

	@Override
	public DistributedIntStream limit(long maxSize)
	{
		return null; // TODO
	}

	@Override
	public DistributedIntStream map(IntUnaryOperator mapper)
	{
		return new IntPipeline(s.map(mapper), grp);
	}

	@Override
	public DistributedDoubleStream mapToDouble(IntToDoubleFunction mapper)
	{
		return new DoublePipeline(s.mapToDouble(mapper), grp);
	}

	@Override
	public DistributedLongStream mapToLong(IntToLongFunction mapper)
	{
		return new LongPipeline(s.mapToLong(mapper), grp);
	}

	@Override
	public <U> DistributedStream<U> mapToObj(IntFunction<? extends U> mapper)
	{
		return new ReferencePipeline<U>(s.mapToObj(mapper), grp);
	}

	@Override
	public DistributedIntStream peek(IntConsumer action)
	{
		return new IntPipeline(s.peek(action), grp);
	}

	@Override
	public DistributedIntStream skip(long n)
	{
		return null; // TODO
	}

	@Override
	public DistributedIntStream sorted()
	{
		return null; // TODO
	}

	// Stream operations

	@Override
	public boolean allMatch(IntPredicate predicate)
	{
		return s.allMatch(predicate); // TODO
	}

	@Override
	public boolean anyMatch(IntPredicate predicate)
	{
		return s.anyMatch(predicate); // TODO
	}

	@Override
	public OptionalDouble average()
	{
		return s.average(); // TODO
	}

	@Override
	public <R> R collect(Supplier<R> supplier, ObjIntConsumer<R> accumulator, BiConsumer<R,R> combiner)
	{
		return s.collect(supplier, accumulator, combiner); // TODO
	}

	@Override
	public long count()
	{
		return s.count(); // TODO
	}

	@Override
	public OptionalInt findAny()
	{
		return s.findAny(); // TODO
	}

	@Override
	public OptionalInt findFirst()
	{
		return s.findFirst(); // TODO
	}

	@Override
	public void forEach(IntConsumer action)
	{
		s.forEach(action);
	}

	@Override
	public void forEachOrdered(IntConsumer action)
	{
		s.forEachOrdered(action);
	}

	@Override
	public OptionalInt max()
	{
		return s.max(); // TODO
	}

	@Override
	public OptionalInt min()
	{
		return s.min(); // TODO
	}

	@Override
	public boolean noneMatch(IntPredicate predicate)
	{
		return s.noneMatch(predicate); // TODO
	}

	@Override
	public OptionalInt reduce(IntBinaryOperator op)
	{
		return s.reduce(op); // TODO
	}

	@Override
	public int reduce(int identity, IntBinaryOperator op)
	{
		return s.reduce(identity, op); // TODO
	}

	@Override
	public int sum()
	{
		return s.sum(); // TODO
	}

	@Override
	public IntSummaryStatistics summaryStatistics()
	{
		return s.summaryStatistics(); // TODO
	}

	@Override
	public int[] toArray()
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
	public PrimitiveIterator.OfInt iterator()
	{
		return s.iterator();
	}

	@Override
	public DistributedIntStream onClose(Runnable closeHandler)
	{
		return new IntPipeline(s.onClose(closeHandler), grp);
	}

	@Override
	public DistributedIntStream parallel()
	{
		return new IntPipeline(s.parallel(), grp);
	}

	@Override
	public DistributedIntStream sequential()
	{
		return new IntPipeline(s.sequential(), grp);
	}

	@Override
	public Spliterator.OfInt spliterator()
	{
		return s.spliterator();
	}

	@Override
	public DistributedIntStream unordered()
	{
		return new IntPipeline(s.unordered(), grp);
	}
}
