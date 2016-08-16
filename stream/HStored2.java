import java.util.*;
import java.util.stream.*;

import stored.*;

/*
Uses stored collections/streams
Thread affinity: 1 thread per CPU
Uses buffers only (entire array is not allocated)
*/
public class HStored2
{
	public static void main(String[] argv) throws InterruptedException
	{
		try { Class.forName("ThreadAffinity"); } catch (Exception e) { e.printStackTrace(); System.exit(1); }
		int max = Integer.parseInt(argv[0]);
		BitSet cpuset = new BitSet();
		cpuset.set(1);
		ThreadAffinity.set(cpuset);
		LongStoredSource src = new LongStoredSource(null, max);
		LongStream s = StreamSupport.longStream(new LongStoredSpliterator(src, 0, max), true);
		long sum = s.sum(); // Do a dry-run to create all worker threads
		Thread.sleep(10);
		ThreadLocal<SHA256> tlmd = new ThreadLocal<SHA256>();
		ThreadLocal<byte[]> tld = new ThreadLocal<byte[]>();
		src = new LongStoredSource(null, max);
		s = StreamSupport.longStream(new LongStoredSpliterator(src, 0, max), true);
		long t1 = System.currentTimeMillis();
		s.forEach(n ->
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
		System.err.println((t2 - t1) + " ms elapsed");
	}
}
