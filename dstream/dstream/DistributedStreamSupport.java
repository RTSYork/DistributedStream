package dstream;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

/**
 * Contains methods for creating Distributed Streams.
 * Distributed equivalent of the StreamSupport class.
 */
public final class DistributedStreamSupport
{
	private DistributedStreamSupport() { } // Do not instantiate

	public static DistributedDoubleStream doubleStream(Spliterator.OfDouble spliterator, boolean parallel)
	{
		return new DoublePipeline(StreamSupport.doubleStream(spliterator, parallel));
	}

	public static DistributedDoubleStream doubleStream(Supplier<? extends Spliterator.OfDouble> supplier, int characteristics, boolean parallel)
	{
		return new DoublePipeline(StreamSupport.doubleStream(supplier, characteristics, parallel));
	}

	public static DistributedIntStream intStream(Spliterator.OfInt spliterator, boolean parallel)
	{
		return new IntPipeline(StreamSupport.intStream(spliterator, parallel));
	}

	public static DistributedIntStream intStream(Supplier<? extends Spliterator.OfInt> supplier, int characteristics, boolean parallel)
	{
		return new IntPipeline(StreamSupport.intStream(supplier, characteristics, parallel));
	}

	public static DistributedLongStream longStream(Spliterator.OfLong spliterator, boolean parallel)
	{
		return new LongPipeline(StreamSupport.longStream(spliterator, parallel));
	}

	public static DistributedLongStream longStream(Supplier<? extends Spliterator.OfLong> supplier, int characteristics, boolean parallel)
	{
		return new LongPipeline(StreamSupport.longStream(supplier, characteristics, parallel));
	}

	public static <T> DistributedStream<T> stream(Spliterator<T> spliterator, boolean parallel)
	{
		return new ReferencePipeline<T>(StreamSupport.stream(spliterator, parallel));
	}

	public static <T> DistributedStream<T> stream(Supplier<? extends Spliterator<T>> supplier, int characteristics, boolean parallel)
	{
		return new ReferencePipeline<T>(StreamSupport.stream(supplier, characteristics, parallel));
	}
}
