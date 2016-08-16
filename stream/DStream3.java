import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.*;

/*
Reads values from disk
Uses Java streams
Thread affinity: 4 threads per Locale
Array allocated by main thread
*/
public class DStream3
{
	static final int BUFSIZE = 1048576;

	public static void main(String[] argv) throws InterruptedException
	{
		try { Class.forName("ThreadAffinity"); } catch (Exception e) { e.printStackTrace(); System.exit(1); }
		int max = Integer.parseInt(argv[0]);
		BitSet cpuset = new BitSet();
		cpuset.set(0, 4);
		ThreadAffinity.set(cpuset);
		long[] arr = new long[max];
		for (int i = 0; i < max; i++)
			arr[i] = 1;
		long sum = Arrays.stream(arr).parallel().sum(); // Do a dry-run to create all worker threads
		if (sum != max)
			System.err.println("Error: incorrect sum");
		Thread.sleep(10);
		ThreadLocal<SHA256> tlmd = new ThreadLocal<SHA256>();
		ThreadLocal<byte[]> tld = new ThreadLocal<byte[]>();
		try
		{
			FileChannel f = FileChannel.open(Paths.get("/mnt/disk/stream/input"), StandardOpenOption.READ);
			long maxl = (long) max << 3;
			long t1 = System.currentTimeMillis();
			for (long i = 0; i < maxl; i += BUFSIZE)
			{
				int len = (int) (i + BUFSIZE >= maxl ? maxl - i : BUFSIZE);
				MappedByteBuffer bb = f.map(FileChannel.MapMode.READ_ONLY, i, len);
				bb.asLongBuffer().get(arr, (int) (i >> 3), len >> 3);
			}
			Arrays.stream(arr).parallel().forEach(n ->
			{
				SHA256 md = tlmd.get();
				if (md == null)
					tlmd.set(md = new SHA256());
				md.reset();
				md.hash((byte) (n >>> 56));
				md.hash((byte) ((n >>> 48) & 0xff));
				md.hash((byte) ((n >>> 40) & 0xff));
				md.hash((byte) ((n >>> 32) & 0xff));
				md.hash((byte) ((n >>> 24) & 0xff));
				md.hash((byte) ((n >>> 16) & 0xff));
				md.hash((byte) ((n >>> 8) & 0xff));
				md.hash((byte) (n & 0xff));
				byte[] d = tld.get();
				if (d == null)
					tld.set(d = new byte[32]);
				md.digest(d);
			});
			long t2 = System.currentTimeMillis();
			f.close();
			System.err.println((t2 - t1) + " ms elapsed");
		}
		catch (IOException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}
}
