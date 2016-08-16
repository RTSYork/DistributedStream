package stored;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.charset.*;
import java.util.*;
import java.util.function.*;
import java.util.regex.*;

public class StringStoredCollection extends AbstractCollection<String>
{
	private String filename;
	private char delimiter;

	public StringStoredCollection(String filename, char delimiter)
	{
		super();
		this.filename = filename;
		this.delimiter = delimiter;
	}

	@Override
	public final Iterator<String> iterator()
	{
		return null;
	}

	@Override
	public final Spliterator<String> spliterator()
	{
		long len = 0;
		try
		{
			RandomAccessFile f = new RandomAccessFile(filename, "r");
			len = f.length();
			f.close();
		}
		catch (IOException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		return new StringStoredSpliterator(filename, delimiter, 0, len);
	}

	@Override
	public int size()
	{
		return 0;
	}
}

class StringStoredSpliterator implements Spliterator<String>
{
	private String filename;
	private char delimiter;
	private RandomAccessFile f;
	private byte[] buf;
	private int start;
	private int end;
	private byte[] sbuf;
	private int slen;
	private long min;
	private long max;

	private String read()
	{
		if (min >= max)
			return null;
		try
		{
			do
			{
				for (int i = start; i < end; i++)
					if (buf[i] == delimiter)
					{
						if (slen == 0)
						{
							int st = start;
							int ln = i - start;
							min += ln + 1;
							start = i + 1;
							return new String(buf, st, ln);
						}
						int newlen = slen + i - start;
						if (newlen > sbuf.length)
						{
							byte[] sbuf2 = new byte[newlen];
							System.arraycopy(sbuf, 0, sbuf2, 0, slen);
							sbuf = sbuf2;
						}
						System.arraycopy(buf, start, sbuf, slen, i - start);
						slen = 0;
						min += newlen + 1;
						start = i + 1;
						return new String(sbuf, 0, newlen);
					}
				if (end - start > 0)
				{
					int oldlen = slen;
					slen += end - start;
					if (slen > sbuf.length)
					{
						byte[] sbuf2 = new byte[slen];
						System.arraycopy(sbuf, 0, sbuf2, 0, oldlen);
						sbuf = sbuf2;
					}
					System.arraycopy(buf, start, sbuf, oldlen, end - start);
				}
				start = 0;
				end = f.read(buf);
				if (end < 0)
				{
					end = 0;
					return null;
				}
			}
			while (true);
		}
		catch (IOException e)
		{
			e.printStackTrace();
			System.exit(1);
			return null;
		}
	}
	
	private void skip()
	{
		try
		{
			do
			{
				for (int i = start; i < end; i++)
					if (buf[i] == delimiter)
					{
						min += i + 1 - start;
						start = i + 1;
						return;
					}
				start = 0;
				end = f.read(buf);
				if (end < 0)
				{
					end = 0;
					return;
				}
			}
			while (true);
		}
		catch (IOException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}

	private void init()
	{
		try
		{
			f = new RandomAccessFile(filename, "r");
			f.seek(min);
			buf = new byte[65536];
			sbuf = new byte[4096];
			start = end = slen = 0;
			if (min > 0)
				skip();
		}
		catch (IOException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}

	StringStoredSpliterator(String filename, char delimiter, long min, long max)
	{
		this.filename = filename;
		this.delimiter = delimiter;
		this.min = min;
		this.max = max;
	}

	@Override
	public int characteristics()
	{
		return ORDERED;
	}

	@Override
	public boolean tryAdvance(Consumer<? super String> action)
	{
		if (f == null)
			init();
		String s = read();
		if (s == null)
		{
			try
			{
				f.close();
			}
			catch (IOException e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			return false;
		}
		action.accept(s);
		return true;
	}

	@Override
	public void forEachRemaining(Consumer<? super String> action)
	{
		if (f == null)
			init();
		String s = read();
		while (s != null)
		{
			action.accept(s);
			s = read();
		}
		try
		{
			f.close();
		}
		catch (IOException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}

	@Override
	public Spliterator<String> trySplit()
	{
		long newMin = min;
		long newMax = (min + max) >>> 1;
		if (newMin + 2097152 < newMax)
		{
			min = newMax;
			return new StringStoredSpliterator(filename, delimiter, newMin, newMax);
		}
		return null; // Do not split child spliterators
	}

	@Override
	public long estimateSize()
	{
		return Long.MAX_VALUE;
	}
}
