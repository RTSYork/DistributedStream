package dstream;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

/**
 * A distributed extension of Java 8 Streams.
 * The associated pipeline is replicated on each participating compute node.
 * @see java.util.stream.Stream
 */
public interface DistributedStream<T> extends Stream<T>
{
	/**
	 * Returns the current Distributed Stream's compute group.
	 * This group determines the participating nodes in the computation.
	 * @return Current Distributed Stream's compute group.
	 */
	public ComputeGroup getComputeGroup();

	/**
	 * Changes the current Distributed Stream's compute group.
	 * @param grp New compute group.
	 */
	public void setComputeGroup(ComputeGroup grp);

	// Data distribution operations

	/**
	 * Sends data elements between nodes in the current compute group
	 * according to a hash-based partitioner.
	 * A stateful eager intermediate operation.
	 * Hash function is the Object.hashCode method.
	 * Elements with the same hash value are sent to the same destination.
	 * Suitable for MapReduce-style shuffling of data.
	 * @return Distributed Stream consisting of all elements.
	 */
	public DistributedStream<T> distribute();

	/**
	 * Sends data elements between nodes in the current compute group
	 * according to the specified partitioner.
	 * A stateful eager intermediate operation.
	 * @param p Programmer-defined partitioner.
	 * @return Distributed Stream consisting of all elements.
	 */
	public DistributedStream<T> distribute(Partitioner<? super T> p);

	/**
	 * Sends data elements from nodes in the current compute group to
	 * another compute group according to a hash-based partitioner.
	 * A stateful eager intermediate operation.
	 * Suitable for MapReduce-style shuffling of data.
	 * @param grp Destination compute group.
	 * @return Distributed Stream consisting of all elements.
	 */
	public DistributedStream<T> distribute(ComputeGroup grp);

	/**
	 * Sends data elements from nodes in the current compute group to
	 * another compute group according to the specified partitioner.
	 * A stateful eager intermediate operation.
	 * @param grp Destination compute group.
	 * @param p Programmer-defined partitioner.
	 * @return Distributed Stream consisting of all elements.
	 */
	public DistributedStream<T> distribute(ComputeGroup grp,
		Partitioner<? super T> p);

	/**
	 * Sends data elements from nodes in the current compute group to
	 * the specified node.
	 * A stateful eager intermediate operation.
	 * @param node Destination compute node.
	 * @return Distributed Stream consisting of all elements.
	 */
	public DistributedStream<T> distribute(ComputeNode node);

	/**
	 * Splits the current stream into multiple streams, with each
	 * stream containing all elements of the current stream.
	 * A stateful eager intermediate operation.
	 * @param numStreams Number of streams to create.
	 * @return Array of Distributed Streams.
	 */
	public DistributedStream<T>[] split(int numStreams);

	/**
	 * Merges multiple streams into a single stream containing all
	 * elements from the source streams.
	 * The resulting stream's compute group is the union of all source
	 * stream compute groups.
	 * A stateful eager intermediate operation.
	 * @param streams Array or argument list of streams to merge.
	 * @return A single Distributed Stream.
	 */
	public DistributedStream<T> join(DistributedStream<T>... streams);

	// Local operations

	/**
	 * Accumulates local data elements on each node into a container.
	 * Equivalent to executing collect() on each local stream.
	 * A terminal operation.
	 * @param collector An operation which includes supplier, accumulator
	 * and combiner functions. @see java.util.stream.Collector
	 * @return The resulting container.
	 */
	public <R, A> R localCollect(Collector<? super T, A, R> collector);

	/**
	 * Accumulates local data elements on each node into a container.
	 * Equivalent to executing collect() on each local stream.
	 * A terminal operation.
	 * @param supplier Function returning a new container.
	 * @param accumulator Function to add a new element into a container.
	 * @param combiner Function for combining two containers.
	 * @return The resulting container.
	 */
	public <R> R localCollect(Supplier<R> supplier,
		BiConsumer<R, ? super T> accumulator, BiConsumer<R, R> combiner);

	/**
	 * Returns the number of data elements in the local stream.
	 * Equivalent to executing count() on each local stream.
	 * A terminal operation.
	 * @return The number of local data elements.
	 */
	public long localCount();

	/**
	 * Removes duplicate elements in the local stream.
	 * Equivalent to executing distinct() on each local stream.
	 * An intermediate operation.
	 * @return Distributed Stream without duplicate local data elements.
	 */
	public DistributedStream<T> localDistinct();

	/**
	 * Executes an action on each local element.
	 * Encounter order is not preserved.
	 * Equivalent to executing forEach() on each local stream.
	 * A terminal operation.
	 * @param action Action to apply on each element. Must be non-interfering.
	 */
	public void localForEach(Consumer<? super T> action);

	/**
	 * Executes an action on each local element.
	 * Encounter order is preserved.
	 * Equivalent to executing forEachOrdered() on each local stream.
	 * A terminal operation.
	 * @param action Action to apply on each element. Must be non-interfering.
	 */
	public void localForEachOrdered(Consumer<? super T> action);

	/**
	 * Keeps only the specified number of elements in each local stream.
	 * Equivalent to executing limit() on each local stream.
	 * An intermediate operation.
	 * @param maxSize Maximum number of elements to keep.
	 * @return Distributed Stream with each local stream
	 * truncated to maxSize elements.
	 */
	public DistributedStream<T> localLimit(long maxSize);

	/**
	 * Performs an action on each local element and returns the same
	 * Distributed Stream.
	 * Equivalent to executing peek() on each local stream.
	 * An intermediate operation.
	 * @param action A non-interfering action.
	 * @return The same Distributed Stream.
	 */
	public DistributedStream<T> localPeek(Consumer<? super T> action);

	/**
	 * Accumulates local elements into a single value of the same type.
	 * Equivalent to executing reduce() on each local stream.
	 * A terminal operation.
	 * @param accumulator Associative accumulating function.
	 * @return An Optional of either the reduced value or an empty value.
	 */
	public Optional<T> localReduce(BinaryOperator<T> accumulator);

	/**
	 * Accumulates local elements into a single value of the same type.
	 * Equivalent to executing reduce() on each local stream.
	 * A terminal operation.
	 * @param identity The accumulating function's identity value.
	 * @param accumulator Associative accumulating function.
	 * @return The reduced value.
	 */
	public T localReduce(T identity, BinaryOperator<T> accumulator);

	/**
	 * Accumulates local elements into a single value.
	 * Equivalent to executing reduce() on each local stream.
	 * A terminal operation.
	 * @param identity The accumulating function's identity value.
	 * @param accumulator Function to accumulate elements into the value.
	 * @param combiner Function to combine two values.
	 * @return The reduced value.
	 */
	public <U> U localReduce(U identity,
		BiFunction<U, ? super T, U> accumulator, BinaryOperator<U> combiner);

	/**
	 * Removes the first n elements in each local stream.
	 * Equivalent to executing skip() on each local stream.
	 * An intermediate operation.
	 * @param n Number of local elements to skip.
	 * @return Distributed Stream with the elements removed.
	 */
	public DistributedStream<T> localSkip(long n);

	/**
	 * Sorts all elements in each local stream.
	 * Equivalent to executing sorted() on each local stream.
	 * An intermediate operation.
	 * @return Distributed Stream with the elements locally sorted.
	 */
	public DistributedStream<T> localSorted();

	/**
	 * Sorts all elements in each local stream using the specified comparator.
	 * Equivalent to executing sorted() on each local stream.
	 * An intermediate operation.
	 * @param comparator Function to compare two values.
	 * @return Distributed Stream with the elements locally sorted.
	 */
	public DistributedStream<T> localSorted(Comparator<? super T> comparator);

	/**
	 * Collects all local elements into an array.
	 * Equivalent to executing toArray() on each local stream.
	 * A terminal operation.
	 * @return Array containing all local elements.
	 */
	public Object[] localToArray();

	/**
	 * Collects all local elements into an array.
	 * Equivalent to executing toArray() on each local stream.
	 * A terminal operation.
	 * @param generator Array allocation function.
	 * @return Array containing all local elements.
	 */
	public <A> A[] localToArray(IntFunction<A[]> generator);

	// Overrides

	/**
	 * Removes duplicate elements in the Distributed Stream.
	 * A stateful eager intermediate operation.
	 * @return Distributed Stream without duplicate data elements.
	 */
	@Override public DistributedStream<T> distinct();

	/**
	 * Removes elements in the Distributed Stream that do not satisfy the
	 * specified predicate.
	 * An intermediate operation.
	 * @param predicate Boolean function that decides whether an element
	 * should remain in the Distributed Stream.
	 * @return Distributed Stream where all elements satisfy the predicate.
	 */
	@Override public DistributedStream<T> filter(
		Predicate<? super T> predicate);

	/**
	 * Replaces each element of the Distributed Stream with elements in the
	 * stream returned by the specified mapping function.
	 * An intermediate operation.
	 * @param mapper Mapping function returning a stream.
	 * @return Distributed Stream with the applied mapping.
	 */
	@Override public <R> DistributedStream<R> flatMap(
		Function<? super T,? extends Stream<? extends R>> mapper);

	/**
	 * Replaces each element of the Distributed Stream with elements in the
	 * stream returned by the specified mapping function.
	 * An intermediate operation.
	 * @param mapper Mapping function returning a stream of doubles.
	 * @return Distributed DoubleStream with the applied mapping.
	 */
	@Override public DistributedDoubleStream flatMapToDouble(
		Function<? super T,? extends DoubleStream> mapper);

	/**
	 * Replaces each element of the Distributed Stream with elements in the
	 * stream returned by the specified mapping function.
	 * An intermediate operation.
	 * @param mapper Mapping function returning a stream of integers.
	 * @return Distributed IntStream with the applied mapping.
	 */
	@Override public DistributedIntStream flatMapToInt(
		Function<? super T,? extends IntStream> mapper);

	/**
	 * Replaces each element of the Distributed Stream with elements in the
	 * stream returned by the specified mapping function.
	 * An intermediate operation.
	 * @param mapper Mapping function returning a stream of long integers.
	 * @return Distributed LongStream with the applied mapping.
	 */
	@Override public DistributedLongStream flatMapToLong(
		Function<? super T,? extends LongStream> mapper);

	/**
	 * Keeps only the specified number of elements in the Distributed Stream.
	 * An intermediate operation.
	 * @param maxSize Maximum number of elements to keep.
	 * @return Distributed Stream truncated to maxSize elements.
	 */
	@Override public DistributedStream<T> limit(long maxSize);

	/**
	 * Replaces each element of the Distributed Stream with values from the
	 * specified mapping function.
	 * An intermediate operation.
	 * @param mapper Mapping function returning values of an arbitrary type.
	 * @return Distributed Stream with the applied mapping.
	 */
	@Override public <R> DistributedStream<R> map(
		Function<? super T,? extends R> mapper);

	/**
	 * Replaces each element of the Distributed Stream with values from the
	 * specified mapping function.
	 * An intermediate operation.
	 * @param mapper Mapping function returning values of type double.
	 * @return Distributed Stream with the applied mapping.
	 */
	@Override public DistributedDoubleStream mapToDouble(
		ToDoubleFunction<? super T> mapper);

	/**
	 * Replaces each element of the Distributed Stream with values from the
	 * specified mapping function.
	 * An intermediate operation.
	 * @param mapper Mapping function returning values of type integer.
	 * @return Distributed Stream with the applied mapping.
	 */
	@Override public DistributedIntStream mapToInt(
		ToIntFunction<? super T> mapper);

	/**
	 * Replaces each element of the Distributed Stream with values from the
	 * specified mapping function.
	 * An intermediate operation.
	 * @param mapper Mapping function returning values of type long integer.
	 * @return Distributed Stream with the applied mapping.
	 */
	@Override public DistributedLongStream mapToLong(
		ToLongFunction<? super T> mapper);

	/**
	 * Returns a parallel Distributed Stream with an otherwise identical state.
	 * An intermediate operation.
	 * @return A parallel Distributed Stream.
	 */
	@Override public DistributedStream<T> parallel();

	/**
	 * Performs an action on each element on all nodes and returns the same
	 * Distributed Stream.
	 * An intermediate operation.
	 * @param action A non-interfering action.
	 * @return The same Distributed Stream.
	 */
	@Override public DistributedStream<T> peek(Consumer<? super T> action);

	/**
	 * Returns a sequential Distributed Stream with an otherwise identical
	 * state.
	 * An intermediate operation.
	 * @return A sequential Distributed Stream.
	 */
	@Override public DistributedStream<T> sequential();

	/**
	 * Removes the first n elements in the Distributed Stream.
	 * A stateful intermediate operation.
	 * @param n Number of local elements to skip.
	 * @return Distributed Stream with the elements removed.
	 */
	@Override public DistributedStream<T> skip(long n);

	/**
	 * Sorts all elements in the Distributed Stream.
	 * A stateful eager intermediate operation.
	 * @return Distributed Stream with the elements sorted.
	 */
	@Override public DistributedStream<T> sorted();

	/**
	 * Sorts all elements in the Distributed Stream using the specified
	 * comparator.
	 * A stateful eager intermediate operation.
	 * @param comparator Function to compare two values.
	 * @return Distributed Stream with the elements locally sorted.
	 */
	@Override public DistributedStream<T> sorted(Comparator<? super T> comparator);

	// Generators

	/**
	 * Concatenates two streams.
	 * @param a First stream.
	 * @param b Second stream.
	 * @return A new Distributed Stream containing all elements of both
	 * streams.
	 */
	public static <T> DistributedStream<T> concat(
		Stream<? extends T> a, Stream<? extends T> b)
	{
		return new ReferencePipeline(Stream.<T>concat(a, b));
	}

	/**
	 * Creates an empty stream.
	 * @return An new empty Distributed Stream.
	 */
	public static <T> DistributedStream<T> empty()
	{
		return new ReferencePipeline(Stream.<T>empty());
	}

	/**
	 * Creates an unbounded stream from the specified supplier.
	 * @param s Function for supplying values to the stream.
	 * @return A new Distributed Stream containing elements returned from
	 * the supplier.
	 */
	public static <T> DistributedStream<T> generate(Supplier<T> s)
	{
		return new ReferencePipeline(Stream.<T>generate(s));
	}

	/**
	 * Creates an unbounded stream from applying the specified function
	 * on the previously computed element.
	 * @param seed The first element of the stream.
	 * @param f Function to be applied iteratively.
	 * @return A new Distributed Stream containing
	 * {seed, f(seed), f(f(seed)), ...}.
	 */
	public static <T> DistributedStream<T> iterate(T seed, UnaryOperator<T> f)
	{
		return new ReferencePipeline(Stream.<T>iterate(seed, f));
	}

	/**
	 * Creates a stream consisting of the specified values.
	 * @param values Stream elements.
	 * @return A new Distributed Stream with the specified values.
	 */
	public static <T> DistributedStream<T> of(T... values)
	{
		return new ReferencePipeline(Stream.<T>of(values));
	}

	/**
	 * Creates a stream consisting of the specified value as its only element.
	 * @param t A single stream element.
	 * @return A new Distributed Stream with the specified value.
	 */
	public static <T> DistributedStream<T> of(T t)
	{
		return new ReferencePipeline(Stream.<T>of(t));
	}
}
