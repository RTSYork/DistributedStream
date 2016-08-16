package dstream.util;

import java.util.*;

class IteratorFromSpliterator<E> implements Iterator<E>
{
	private Spliterator<E> sp;
	private boolean checked;
	private boolean hasNextItem;
	private E nextItem;

	public IteratorFromSpliterator(Spliterator<E> sp)
	{
		this.sp = sp;
		checked = false;
		hasNextItem = false;
		nextItem = null;
	}

	public boolean hasNext()
	{
		if (checked)
			return hasNextItem;
		try
		{
			nextItem = next();
			checked = true;
			hasNextItem = true;
			return true;
		}
		catch (NoSuchElementException e)
		{
			checked = true;
			hasNextItem = false;
			return false;
		}
	}

	public E next()
	{
		if (checked)
		{
			checked = false;
			return nextItem;
		}
		checked = false;
		if (!sp.tryAdvance(i -> { this.nextItem = i; }))
		{
			nextItem = null;
			throw new NoSuchElementException();
		}
		E item = nextItem;
		nextItem = null;
		return item;
	}
}
