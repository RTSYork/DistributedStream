package disk;

import java.io.*;
import java.nio.*;
import java.nio.file.*;
import java.nio.channels.*;

public class LongStoredSource
{
	private String filename;
	private FileChannel f;
	private long pos;
	private long len;

	public LongStoredSource(String filename, long len)
	{
		this.len = len;
		try
		{
			this.filename = filename;
			f = FileChannel.open(Paths.get(filename), StandardOpenOption.READ);
			pos = 0;
		}
		catch (IOException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}

	public long length()
	{
		return len;
	}

	public boolean getWork(LongStoredWork work)
	{
		synchronized (this)
		{
			work.size = (int) (pos + LongStoredSpliterator.BLOCKSIZE >= len ? len - pos : LongStoredSpliterator.BLOCKSIZE);
			if (work.size <= 0)
				return false;
			try
			{
				MappedByteBuffer bb = f.map(FileChannel.MapMode.READ_ONLY, pos << 3, work.size << 3);
				bb.asLongBuffer().get(work.tmpbuf, 0, (int) work.size);
				System.arraycopy(work.tmpbuf, 0, work.data, 0, (int) work.size);
			}
			catch (IOException e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			work.min = pos;
			work.max = pos + work.size;
			pos += work.size;
		}
		return work.size > 0;
	}
}
