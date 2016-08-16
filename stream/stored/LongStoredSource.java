package stored;

import java.io.*;

public class LongStoredSource
{
	private String filename;
	private long pos;
	private long len;

	public LongStoredSource(String filename, long len)
	{
		pos = 0;
		this.len = len;
	}

	public long length()
	{
		return len;
	}

	public synchronized boolean getWork(LongStoredWork work)
	{
		long[] data = work.data;
		long pos = this.pos;
		int len = (int) (data.length < this.len - this.pos ? data.length : this.len - this.pos);
		for (int i = 0, pos2 = (int) (pos + 1); i < len; i++, pos2++)
			data[i] = pos2;
		work.min = pos;
		work.max = pos + len;
		this.pos += len;
		return true;
	}
}
