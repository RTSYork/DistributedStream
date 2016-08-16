package disk;

import java.io.*;
import java.nio.channels.*;
import java.util.*;
import java.util.stream.*;

public class LongStoredCollection extends AbstractCollection<Long>
{
	String filename;
	int items;

	public LongStoredCollection(String filename, int items)
	{
		super();
		this.filename = filename;
		this.items = items;
	}

	@Override
	public final Iterator<Long> iterator()
	{
		return null;
	}

	@Override
	public final Spliterator.OfLong spliterator()
	{
		LongStoredSource src = new LongStoredSource(filename, items);
		return new LongStoredSpliterator(src, 0, items);
	}

	public LongStream parallelLongStream()
	{
		return StreamSupport.longStream(spliterator(), true);
	}

	@Override
	public int size()
	{
		return 0;
	}
}
