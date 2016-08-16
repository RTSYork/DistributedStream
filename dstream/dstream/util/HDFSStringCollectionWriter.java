package dstream.util;

import dstream.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * A Distributed Collection that writes lines of text to a HDFS path.
 */
public class HDFSStringCollectionWriter extends HDFSStringCollection
{
	protected BufferedWriter bw;
	protected AtomicLong items;

	public HDFSStringCollectionWriter(ComputeGroup grp, String filename)
	{
		super(grp, filename + "/part-" + String.format("%05d", ComputeNode.getSelf().getCommunicator().getRank()));
		try
		{
			bw = new BufferedWriter(new OutputStreamWriter(fs.create(path, false))); // Do not overwrite
		}
		catch (IOException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		items = new AtomicLong(0);
	}

	public HDFSStringCollectionWriter(String filename)
	{
		this(ComputeGroup.getCluster(), filename);
	}

	@Override
	public int size()
	{
		long n = items.get();
		return (int) (n > Integer.MAX_VALUE ? Integer.MAX_VALUE : n);
	}

	public boolean add(String t)
	{
		try
		{
			bw.write(t.concat("\n"));
			bw.flush();
			items.incrementAndGet();
			return true;
		}
		catch (IOException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		return false;
	}

	public void finish()
	{
		try
		{
			bw.close();
		}
		catch (IOException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}
}
