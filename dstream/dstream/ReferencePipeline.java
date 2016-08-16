package dstream;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

class ReferencePipeline<T> implements DistributedStream<T>
{
	Stream<T> s;
	ComputeGroup grp;

	ReferencePipeline(Stream<T> s)
	{
		this(s, ComputeGroup.getCluster());
	}

	ReferencePipeline(Stream<T> s, ComputeGroup grp)
	{
		this.s = s;
		this.grp = grp;
	}

	ReferencePipeline(ReferencePipeline<T> upstream)
	{
		s = upstream.s;
		grp = upstream.grp;
	}

	ReferencePipeline(ReferencePipeline<T> upstream, Stream<T> s)
	{
		this.s = s;
		grp = upstream.grp;
	}

	ReferencePipeline(ReferencePipeline<T> upstream, Stream<T> s, ComputeGroup grp)
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
		this.grp = (grp != null ? grp : ComputeGroup.getCluster());
	}

	// Data distribution operations

	@Override
	public DistributedStream<T> distribute()
	{
		return distribute(grp, i -> i.hashCode());
	}

	@Override
	public DistributedStream<T> distribute(Partitioner<? super T> p)
	{
		return distribute(ComputeGroup.getCluster(), p);
	}

	@Override
	public DistributedStream<T> distribute(ComputeGroup grp)
	{
		return distribute(grp, i -> i.hashCode());
	}

	@Override
	public DistributedStream<T> distribute(ComputeGroup grp, Partitioner<? super T> p)
	{
		return DistributeOps.distribute(this, grp, p);
	}

	@Override
	public DistributedStream<T> distribute(ComputeNode node)
	{
		ComputeGroup single = new ComputeGroup(node);
		return distribute(single);
	}

	@Override
	public DistributedStream<T>[] split(int numStreams)
	{
		return DistributeOps.split(this, numStreams);
	}

	@Override
	public DistributedStream<T> join(DistributedStream... streams)
	{
		return DistributeOps.join(this, streams);
	}

	// Local operations

	@Override
	public <R, A> R localCollect(Collector<? super T, A, R> collector)
	{
		return s.collect(collector);
	}

	@Override
	public <R> R localCollect(Supplier<R> supplier, BiConsumer<R, ? super T> accumulator, BiConsumer<R, R> combiner)
	{
		return s.collect(supplier, accumulator, combiner);
	}

	@Override
	public long localCount()
	{
		return s.count();
	}

	@Override
	public DistributedStream<T> localDistinct()
	{
		return new ReferencePipeline<>(this, s.distinct());
	}

	@Override
	public void localForEach(Consumer<? super T> action)
	{
		s.forEach(action);
	}

	@Override
	public void localForEachOrdered(Consumer<? super T> action)
	{
		s.forEachOrdered(action);
	}

	@Override
	public DistributedStream<T> localLimit(long maxSize)
	{
		return new ReferencePipeline<T>(this, s.limit(maxSize));
	}

	@Override
	public DistributedStream<T> localPeek(Consumer<? super T> action)
	{
		return new ReferencePipeline<T>(this, s.peek(action));
	}

	@Override
	public Optional<T> localReduce(BinaryOperator<T> accumulator)
	{
		return s.reduce(accumulator);
	}

	@Override
	public T localReduce(T identity, BinaryOperator<T> accumulator)
	{
		return s.reduce(identity, accumulator);
	}

	@Override
	public <U> U localReduce(U identity, BiFunction<U, ? super T, U> accumulator, BinaryOperator<U> combiner)
	{
		return s.reduce(identity, accumulator, combiner);
	}

	@Override
	public DistributedStream<T> localSkip(long n)
	{
		return new ReferencePipeline<T>(this, s.skip(n));
	}

	@Override
	public DistributedStream<T> localSorted()
	{
		Stream<T> str = s.sorted();
		s = str;
		return new ReferencePipeline<T>(this, str);
		//return new ReferencePipeline<T>(this, SortedOps.sorted((ReferencePipeline) this));
	}

	@Override
	public DistributedStream<T> localSorted(Comparator<? super T> comparator)
	{
		Stream<T> str = s.sorted(comparator);
		s = str;
		return new ReferencePipeline<T>(this, str);
		//return new ReferencePipeline<T>(this, SortedOps.sorted(this, comparator));
	}

	@Override
	public Object[] localToArray()
	{
		return s.toArray();
	}

	@Override
	public <A> A[] localToArray(IntFunction<A[]> generator)
	{
		return s.toArray(generator);
	}

	// Overrides

	@Override
	public DistributedStream<T> distinct()
	{
		return distribute()
			.localDistinct();
	}

	@Override
	public DistributedStream<T> filter(Predicate<? super T> predicate)
	{
		return new ReferencePipeline<T>(this, s.filter(predicate));
	}

	@Override
	public <R> DistributedStream<R> flatMap(Function<? super T,? extends Stream<? extends R>> mapper)
	{
		return new ReferencePipeline<R>((ReferencePipeline<R>) this, s.flatMap(mapper));
	}

	@Override
	public DistributedDoubleStream flatMapToDouble(Function<? super T,? extends DoubleStream> mapper)
	{
		return new DoublePipeline(s.flatMapToDouble(mapper), grp);
	}

	@Override
	public DistributedIntStream flatMapToInt(Function<? super T,? extends IntStream> mapper)
	{
		return new IntPipeline(s.flatMapToInt(mapper), grp);
	}

	@Override
	public DistributedLongStream flatMapToLong(Function<? super T,? extends LongStream> mapper)
	{
		return new LongPipeline(s.flatMapToLong(mapper), grp);
	}

	@Override
	public DistributedStream<T> limit(long maxSize)
	{
		return null; // TODO
	}

	@Override
	public <R> DistributedStream<R> map(Function<? super T,? extends R> mapper)
	{
		return new ReferencePipeline<R>((ReferencePipeline<R>) this, s.map(mapper));
	}

	@Override
	public DistributedDoubleStream mapToDouble(ToDoubleFunction<? super T> mapper)
	{
		return new DoublePipeline(s.mapToDouble(mapper), grp);
	}

	@Override
	public DistributedIntStream mapToInt(ToIntFunction<? super T> mapper)
	{
		return new IntPipeline(s.mapToInt(mapper), grp);
	}

	@Override
	public DistributedLongStream mapToLong(ToLongFunction<? super T> mapper)
	{
		return new LongPipeline(s.mapToLong(mapper), grp);
	}

	@Override
	public DistributedStream<T> peek(Consumer<? super T> action)
	{
		return new ReferencePipeline<T>(this, s.peek(action));
	}

	@Override
	public DistributedStream<T> skip(long n)
	{
		return null; // TODO
	}

	@Override
	public DistributedStream<T> sorted()
	{
		return SortedOpsOptimised.sorted((ReferencePipeline) this);
	}

	@Override
	public DistributedStream<T> sorted(Comparator<? super T> comparator)
	{
		return SortedOpsOptimised.sorted(this, comparator);
	}

	// Stream operations

	@Override
	public boolean allMatch(Predicate<? super T> predicate)
	{
		boolean result = s.allMatch(predicate);
		return DistributeOps.reduce(result, grp, (a, b) -> (a && b));
	}

	@Override
	public boolean anyMatch(Predicate<? super T> predicate)
	{
		boolean result = s.anyMatch(predicate);
		return DistributeOps.reduce(result, grp, (a, b) -> (a || b));
	}

	@Override
	public <R,A> R collect(Collector<? super T,A,R> collector)
	{
		BinaryOperator<A> combiner = collector.combiner();
		A container = s.collect(collector.supplier(), collector.accumulator(), (a, b) -> combiner.apply(a, b));
		container = DistributeOps.reduce(container, grp, (a, b) -> combiner.apply(a, b));
		return collector.characteristics().contains(Collector.Characteristics.IDENTITY_FINISH)
			? (R) container
			: collector.finisher().apply(container);
	}

	@Override
	public <R> R collect(Supplier<R> supplier, BiConsumer<R,? super T> accumulator, BiConsumer<R,R> combiner)
	{
		return DistributeOps.combine(s.collect(supplier, accumulator, combiner), grp, combiner);
	}

	@Override
	public long count()
	{
		Long result = s.count();
		return DistributeOps.reduce(result, grp, (a, b) -> a + b);
	}

	@Override
	public Optional<T> findAny()
	{
		T result = s.findAny().orElse(null);
		result = DistributeOps.reduce(result, grp, (a, b) -> (a != null ? a : b));
		return result != null ? Optional.of(result) : Optional.empty();
	}

	@Override
	public Optional<T> findFirst()
	{
		T result = s.findFirst().orElse(null);
		result = DistributeOps.reduce(result, grp, (a, b) -> (a != null ? a : b));
		return result != null ? Optional.of(result) : Optional.empty();
	}

	@Override
	public void forEach(Consumer<? super T> action)
	{
		DistributeOps.broadcast(this).localForEach(action);
	}

	@Override
	public void forEachOrdered(Consumer<? super T> action)
	{
		DistributeOps.broadcast(this).localForEach(action);
	}

	@Override
	public Optional<T> max(Comparator<? super T> comparator)
	{
		T result = s.max(comparator).orElse(null);
		result = DistributeOps.reduce(result, grp, (a, b) -> (comparator.compare(a, b) >= 0 ? a : b));
		return result != null ? Optional.of(result) : Optional.empty();
	}

	@Override
	public Optional<T> min(Comparator<? super T> comparator)
	{
		T result = s.min(comparator).orElse(null);
		result = DistributeOps.reduce(result, grp, (a, b) -> (comparator.compare(a, b) <= 0 ? a : b));
		return result != null ? Optional.of(result) : Optional.empty();
	}

	@Override
	public boolean noneMatch(Predicate<? super T> predicate)
	{
		Boolean result = s.noneMatch(predicate);
		return DistributeOps.reduce(result, grp, (a, b) -> (a && b));
	}

	@Override
	public Optional<T> reduce(BinaryOperator<T> accumulator)
	{
		T result = s.reduce(accumulator).orElse(null);
		result = DistributeOps.reduce(result, grp, accumulator);
		return result != null ? Optional.of(result) : Optional.empty();
	}

	@Override
	public T reduce(T identity, BinaryOperator<T> accumulator)
	{
		T result = s.reduce(identity, accumulator);
		return DistributeOps.reduce(result, grp, accumulator);
	}

	@Override
	public <U> U reduce(U identity, BiFunction<U,? super T,U> accumulator, BinaryOperator<U> combiner)
	{
		U result = s.reduce(identity, accumulator, combiner);
		return DistributeOps.reduce(result, grp, combiner);
	}

	@Override
	public Object[] toArray()
	{
		return DistributeOps.broadcast(this).localToArray();
	}

	@Override
	public <A> A[] toArray(IntFunction<A[]> generator)
	{
		return DistributeOps.broadcast(this).localToArray(generator);
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
	public Iterator<T> iterator()
	{
		return s.iterator();
	}

	@Override
	public DistributedStream<T> onClose(Runnable closeHandler)
	{
		return new ReferencePipeline<T>(this, s.onClose(closeHandler));
	}

	@Override
	public DistributedStream<T> parallel()
	{
		return new ReferencePipeline<T>(this, s.parallel());
	}

	@Override
	public DistributedStream<T> sequential()
	{
		return new ReferencePipeline<T>(this, s.sequential());
	}

	@Override
	public Spliterator<T> spliterator()
	{
		return s.spliterator();
	}

	@Override
	public DistributedStream<T> unordered()
	{
		return new ReferencePipeline<T>(this, s.unordered());
	}
}
