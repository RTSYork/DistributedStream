package stored;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.charset.*;
import java.nio.file.*;
import java.util.*;
import java.util.function.*;
import java.util.regex.*;


public class StringStoredCollectionImproved extends AbstractCollection<String>
{
	protected String filename;
	protected char delimiter;

	public StringStoredCollectionImproved(String filename, char delimiter)
	{
		super();
		this.filename = filename;
		this.delimiter = delimiter;
	}

	public StringStoredCollectionImproved(String filename)
	{
		this(filename, '\n');
	}

	@Override
	public final Iterator<String> iterator()
	{
		return null;
	}

	@Override
	public Spliterator<String> spliterator()
	{
		return new StringStoredSpliteratorImproved(filename, delimiter);
	}

	@Override
	public int size()
	{
		return 0; // TODO: read file and return number of delimiters (possibly +1)
	}
}

class StringStoredSpliteratorImproved implements Spliterator<String>
{
	public static final int BUFSIZE = 1048576;

	private String filename;
	private RandomAccessFile f;
	private char delimiter;
	private byte[] buf;
	private int bpos, bmax;
	private boolean readBlock;

	static final class Holder<U> implements Consumer<U>
	{
		U obj;

		@Override
		public void accept(U value)
		{
			obj = value;
		}
	}

	StringStoredSpliteratorImproved(String filename, char delimiter)
	{
		this.filename = filename;
		this.delimiter = delimiter;
		try
		{
			f = new RandomAccessFile(filename, "r");
		}
		catch (FileNotFoundException e)
		{
			e.printStackTrace();
			System.exit(1);
			f = null;
		}
		buf = new byte[BUFSIZE];
		bpos = bmax = 0;
		readBlock = true;
	}

	@Override
	public int characteristics()
	{
		return IMMUTABLE | SUBSIZED; // Stream.limit(n) etc. will not work without SUBSIZED
	}

	@Override
	public boolean tryAdvance(Consumer<? super String> action)
	{
		if (buf == null)
			return false;
		int p = bpos;
		int max = bmax;
		int r;
		do
		{
			while (p < max)
			{
				if (buf[p++] == delimiter)
				{
					action.accept(new String(buf, bpos, p - 1 - bpos));
					bpos = p;
					return true;
				}
			}
			p = max - bpos;
			if (bpos > 0 && p > 0)
				System.arraycopy(buf, bpos, buf, 0, p);
			try
			{
				r = f.read(buf, p, buf.length - p);
			}
			catch (IOException e)
			{
				e.printStackTrace();
				System.exit(1);
				r = 0;
			}
			readBlock = !readBlock;
			bmax = max = p + r;
			bpos = 0;
		}
		while (r > 0);
		buf = null;
		return false;
	}

	@Override
	public Spliterator<String> trySplit()
	{
		if (buf == null)
			return null;
		List<String> li = new ArrayList<String>();
		Holder<String> hold = new Holder<String>();
		while (tryAdvance(hold))
		{
			li.add(hold.obj);
			if (readBlock)
				break; // Stop after new block has been read
		}
		readBlock = false;
		if (li.size() > 0)
			return Spliterators.spliterator(li.iterator(), li.size(), characteristics());
		return null;
	}

	@Override
	public long estimateSize()
	{
		return Long.MAX_VALUE;
	}
}
