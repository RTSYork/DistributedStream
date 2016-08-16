package dstream.util;

import java.io.*;
import java.util.*;

/**
 * Represents an on-disk collection.
 */
public interface StoredCollection<E> extends Collection<E>
{
	public default int size()
	{
		return 0;
	}

	public default Iterator<E> iterator()
	{
		return new IteratorFromSpliterator<E>(spliterator());
	}
}
