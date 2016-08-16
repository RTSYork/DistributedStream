import java.util.*;

/*
Uses Java streams
Thread affinity: 1 thread per CPU
Array allocated by main thread
*/
public class HStream2
{
	public static void main(String[] argv) throws InterruptedException
	{
		try { Class.forName("ThreadAffinity"); } catch (Exception e) { e.printStackTrace(); System.exit(1); }
		int max = Integer.parseInt(argv[0]);
		BitSet cpuset = new BitSet();
		cpuset.set(1);
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
		long t1 = System.currentTimeMillis();
		for (int i = 0, i2 = 1; i < max; i++, i2++)
			arr[i] = i2;
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
		System.err.println((t2 - t1) + " ms elapsed");
	}
}
