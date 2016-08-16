package dstream;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

class DoublePipeline implements DistributedDoubleStream
{
	DoubleStream s;
	ComputeGroup grp;

	DoublePipeline(DoubleStream s)
	{
		this(s, ComputeGroup.getCluster());
	}

	DoublePipeline(DoubleStream s, ComputeGroup grp)
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
	public DistributedDoubleStream distribute()
	{
		return distribute(grp, d -> Double.valueOf(d).hashCode());
	}

	@Override
	public DistributedDoubleStream distribute(DoublePartitioner p)
	{
		return distribute(ComputeGroup.getCluster());
	}

	@Override
	public DistributedDoubleStream distribute(ComputeGroup grp)
	{
		return distribute(grp, d -> Double.valueOf(d).hashCode());
	}

	@Override
	public DistributedDoubleStream distribute(ComputeGroup grp, DoublePartitioner p)
	{
		ReferencePipeline<Double> objs = new ReferencePipeline<Double>(s.mapToObj(n -> Double.valueOf(n)), grp);
		return new DoublePipeline(
			DistributeOps.<Double>distribute(objs, grp, n -> p.partition(n)).mapToDouble(n -> n),
			grp);
	}

	@Override
	public DistributedDoubleStream distribute(ComputeNode node)
	{
		ComputeGroup single = new ComputeGroup(node);
		return distribute(single);
	}

	@Override
	public DistributedDoubleStream[] split(int numStreams)
	{
		return DistributeOps.split(this, numStreams);
	}

	@Override
	public DistributedDoubleStream join(DistributedDoubleStream... streams)
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
	public <R> R localCollect(Supplier<R> supplier, ObjDoubleConsumer<R> accumulator, BiConsumer<R, R> combiner)
	{
		return s.collect(supplier, accumulator, combiner);
	}

	@Override
	public long localCount()
	{
		return s.count();
	}

	@Override
	public DistributedDoubleStream localDistinct()
	{
		return new DoublePipeline(s.distinct(), grp);
	}

	@Override
	public DistributedDoubleStream localLimit(long maxSize)
	{
		return new DoublePipeline(s.limit(maxSize), grp);
	}

	@Override
	public OptionalDouble localMax()
	{
		return s.max();
	}

	@Override
	public OptionalDouble localMin()
	{
		return s.min();
	}

	@Override
	public DistributedDoubleStream localPeek(DoubleConsumer action)
	{
		return new DoublePipeline(s.peek(action), grp);
	}

	@Override
	public OptionalDouble localReduce(DoubleBinaryOperator op)
	{
		return s.reduce(op);
	}

	@Override
	public double localReduce(double identity, DoubleBinaryOperator op)
	{
		return s.reduce(identity, op);
	}

	@Override
	public DistributedDoubleStream localSkip(long n)
	{
		return new DoublePipeline(s.skip(n), grp);
	}

	@Override
	public DistributedDoubleStream localSorted()
	{
		return new DoublePipeline(s.sorted(), grp);
	}

	@Override
	public double localSum()
	{
		return s.sum();
	}

	@Override
	public DoubleSummaryStatistics localSummaryStatistics()
	{
		return s.summaryStatistics();
	}

	@Override
	public double[] localToArray()
	{
		return s.toArray();
	}

	// Overrides

	@Override
	public DistributedStream<Double> boxed()
	{
		return new ReferencePipeline<Double>(s.boxed(), grp);
	}

	@Override
	public DistributedDoubleStream distinct()
	{
		return null; // TODO
	}

	@Override
	public DistributedDoubleStream filter(DoublePredicate predicate)
	{
		return new DoublePipeline(s.filter(predicate), grp);
	}

	@Override
	public DistributedDoubleStream flatMap(DoubleFunction<? extends DoubleStream> mapper)
	{
		return new DoublePipeline(s.flatMap(mapper), grp);
	}

	@Override
	public DistributedDoubleStream limit(long maxSize)
	{
		return null; // TODO
	}

	@Override
	public DistributedDoubleStream map(DoubleUnaryOperator mapper)
	{
		return new DoublePipeline(s.map(mapper), grp);
	}

	@Override
	public DistributedIntStream mapToInt(DoubleToIntFunction mapper)
	{
		return new IntPipeline(s.mapToInt(mapper), grp);
	}

	@Override
	public DistributedLongStream mapToLong(DoubleToLongFunction mapper)
	{
		return new LongPipeline(s.mapToLong(mapper), grp);
	}

	@Override
	public <U> DistributedStream<U> mapToObj(DoubleFunction<? extends U> mapper)
	{
		return new ReferencePipeline<U>(s.mapToObj(mapper), grp);
	}

	@Override
	public DistributedDoubleStream peek(DoubleConsumer action)
	{
		return new DoublePipeline(s.peek(action), grp);
	}

	@Override
	public DistributedDoubleStream skip(long n)
	{
		return null; // TODO
	}

	@Override
	public DistributedDoubleStream sorted()
	{
		return null; // TODO
	}

	// Stream operations

	@Override
	public boolean allMatch(DoublePredicate predicate)
	{
		return s.allMatch(predicate); // TODO
	}

	@Override
	public boolean anyMatch(DoublePredicate predicate)
	{
		return s.anyMatch(predicate); // TODO
	}

	@Override
	public OptionalDouble average()
	{
		return s.average(); // TODO
	}

	@Override
	public <R> R collect(Supplier<R> supplier, ObjDoubleConsumer<R> accumulator, BiConsumer<R,R> combiner)
	{
		return s.collect(supplier, accumulator, combiner); // TODO
	}

	@Override
	public long count()
	{
		return s.count(); // TODO
	}

	@Override
	public OptionalDouble findAny()
	{
		return s.findAny(); // TODO
	}

	@Override
	public OptionalDouble findFirst()
	{
		return s.findFirst(); // TODO
	}

	@Override
	public void forEach(DoubleConsumer action)
	{
		s.forEach(action);
	}

	@Override
	public void forEachOrdered(DoubleConsumer action)
	{
		s.forEachOrdered(action);
	}

	@Override
	public OptionalDouble max()
	{
		return s.max(); // TODO
	}

	@Override
	public OptionalDouble min()
	{
		return s.min(); // TODO
	}

	@Override
	public boolean noneMatch(DoublePredicate predicate)
	{
		return s.noneMatch(predicate); // TODO
	}

	@Override
	public OptionalDouble reduce(DoubleBinaryOperator op)
	{
		return s.reduce(op); // TODO
	}

	@Override
	public double reduce(double identity, DoubleBinaryOperator op)
	{
		return s.reduce(identity, op); // TODO
	}

	@Override
	public double sum()
	{
		return s.sum(); // TODO
	}

	@Override
	public DoubleSummaryStatistics summaryStatistics()
	{
		return s.summaryStatistics(); // TODO
	}

	@Override
	public double[] toArray()
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
	public PrimitiveIterator.OfDouble iterator()
	{
		return s.iterator();
	}

	@Override
	public DistributedDoubleStream onClose(Runnable closeHandler)
	{
		return new DoublePipeline(s.onClose(closeHandler), grp);
	}

	@Override
	public DistributedDoubleStream parallel()
	{
		return new DoublePipeline(s.parallel(), grp);
	}

	@Override
	public DistributedDoubleStream sequential()
	{
		return new DoublePipeline(s.sequential(), grp);
	}

	@Override
	public Spliterator.OfDouble spliterator()
	{
		return s.spliterator();
	}

	@Override
	public DistributedDoubleStream unordered()
	{
		return new DoublePipeline(s.unordered(), grp);
	}
}
