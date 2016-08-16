import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

class HybridForkJoinWorkerThread extends ForkJoinWorkerThread
{
	private int thread_id;

	public HybridForkJoinWorkerThread(ForkJoinPool pool, int thread_id)
	{
		super(pool);
		this.thread_id = thread_id + 2; // avoid CPU0 (interrupt-prone) and CPU1 (main thread)
	}

	protected void onStart()
	{
		try { Class.forName("ThreadAffinity"); } catch (Exception e) { e.printStackTrace(); System.exit(1); }
		BitSet cpuset = new BitSet();
		int smpBase = thread_id >> 3;
		cpuset.set(smpBase, smpBase + 8);
		//cpuset.set(thread_id);
		ThreadAffinity.set(cpuset);
	}
}

public class HybridThreadFactory implements ForkJoinPool.ForkJoinWorkerThreadFactory
{
	AtomicInteger thread_id = new AtomicInteger(0);

	public ForkJoinWorkerThread newThread(ForkJoinPool pool)
	{
		int id = thread_id.getAndIncrement();
		if (id >= 14)
			return null;
		return new SMPForkJoinWorkerThread(pool, id);
	}
}
