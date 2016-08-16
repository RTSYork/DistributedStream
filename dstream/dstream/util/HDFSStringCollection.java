package dstream.util;

import dstream.*;

import java.io.*;
import java.util.*;
import java.util.function.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * A Distributed Collection that reads lines of text from a HDFS path.
 */
public class HDFSStringCollection extends AbstractCollection<String> implements DistributedCollection<String>, StoredCollection<String>
{
	protected FileSystem fs;
	protected Path path;
	protected ComputeGroup grp;
	protected String filename;

	public HDFSStringCollection(ComputeGroup grp, String filename)
	{
		super();
		this.grp = grp;
		this.filename = filename;
		Configuration conf = new Configuration();
		conf.addResource(new Path(System.getenv("HADOOP_HOME") + "/conf/core-site.xml"));
		try
		{
			fs = FileSystem.get(conf);
		}
		catch (IOException e)
		{
			e.printStackTrace();
			System.exit(1);
			fs = null;
		}
		path = new Path(filename); // "hdfs://10.0.0.1/..."
	}

	public HDFSStringCollection(String filename)
	{
		this(ComputeGroup.getCluster(), filename);
	}

	@Override
	public ComputeGroup getComputeGroup()
	{
		return grp;
	}

	@Override
	public final Iterator<String> iterator()
	{
		return new IteratorFromSpliterator<String>(spliterator());
	}

	@Override
	public final Spliterator<String> spliterator()
	{
		return new HDFSStringSpliterator(path, grp);
	}

	@Override
	public int size()
	{
		return 0; // TODO: read entire file and return size
	}
}

class HDFSStringSpliterator implements Spliterator<String>
{
	private static final int BATCH = 256;

	private JobConf job;
	private TextInputFormat txt;
	private List<InputSplit> splits;
	private RecordReader reader;
	private LongWritable key;
	private Text value;
	private int at;

	public HDFSStringSpliterator(Path path, ComputeGroup grp)
	{
		try
		{
			at = 0;

			splits = new ArrayList<InputSplit>();
			job = new JobConf();
			FileInputFormat.addInputPath(job, path);
			txt = new TextInputFormat();
			txt.configure(job);
			String hostname = ComputeNode.getSelf().getHostname();
			int grpRank = grp.indexOf(ComputeNode.getSelf());
			HashSet<String> hosts = new HashSet<>();
			for (ComputeNode n: grp)
				hosts.add(n.getHostname());
			for (InputSplit s: txt.getSplits(job, 0))
			{
				String h = s.getLocations()[0].split("\\.")[0];
				int hash = h.hashCode();
				if (hash < 0)
					hash = -hash;
				// Accept split if it's local, or not part of compute group provided the hashcode matches
				if (h.equals(hostname) || (!hosts.contains(h) && hash % grp.size() == grpRank))
					splits.add(s);
			}
			if (splits.size() > 0)
			{
				reader = txt.getRecordReader(splits.get(0), job, Reporter.NULL);
				System.err.println(ComputeNode.getSelf().getName() + ": Reading from " + splits.get(0));
				key = (LongWritable) reader.createKey();
				value = (Text) reader.createValue();
			}
			else
			{
				System.err.println(ComputeNode.getSelf().getName() + ": Nothing to read");
				reader = null;
				key = null;
				value = null;
			}
		}
		catch (IOException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}

	@Override
	public int characteristics()
	{
		return IMMUTABLE | SUBSIZED;
	}

	@Override
	public boolean tryAdvance(Consumer<? super String> action)
	{
		try
		{
			while (reader != null)
			{
				if (reader.next(key, value))
				{
					action.accept(value.toString());
					return true;
				}
				at++;
				reader = at < splits.size() ? txt.getRecordReader(splits.get(at), job, Reporter.NULL) : null;
				if (reader != null)
					System.err.println(ComputeNode.getSelf().getName() + ": Reading from " + splits.get(at));
				else
					System.err.println(ComputeNode.getSelf().getName() + ": Finished reading");
			}
		}
		catch (IOException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		return false;
	}

	@Override
	public Spliterator<String> trySplit()
	{
		try
		{
			List<String> li = new ArrayList<String>(BATCH);
			int count = 0;
			if (reader != null)
			{
				while (reader.next(key, value))
				{
					li.add(value.toString());
					if (++count >= BATCH)
						return Spliterators.spliterator(li.iterator(), li.size(), characteristics() | SIZED);
				}
				at++;
				reader = at < splits.size() ? txt.getRecordReader(splits.get(at), job, Reporter.NULL) : null;
				return Spliterators.spliterator(li.iterator(), li.size(), characteristics() | SIZED);
			}
		}
		catch (IOException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		return null;
	}

	@Override
	public long estimateSize()
	{
		return Long.MAX_VALUE;
	}
}
