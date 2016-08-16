import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

class MyForkJoinWorkerThread extends ForkJoinWorkerThread
{
	private int thread_id;

	public MyForkJoinWorkerThread(ForkJoinPool pool, int thread_id)
	{
		super(pool);
		this.thread_id = thread_id + 2; // avoid CPU0 (interrupt-prone) and CPU1 (main thread)
	}

	protected void onStart()
	{
		try { Class.forName("ThreadAffinity"); } catch (Exception e) { e.printStackTrace(); System.exit(1); }
		//System.err.println(Thread.currentThread().getName());
		BitSet cpuset = new BitSet();
		cpuset.set(thread_id);
		ThreadAffinity.set(cpuset);
	}
}

public class MyThreadFactory implements ForkJoinPool.ForkJoinWorkerThreadFactory
{
	static AtomicInteger thread_id = new AtomicInteger(0);

	public ForkJoinWorkerThread newThread(ForkJoinPool pool)
	{
		int id = thread_id.getAndIncrement();
		if (id >= 14)
			return null;
		//System.err.println(id + " " + Thread.currentThread().getName());
		return new MyForkJoinWorkerThread(pool, id);
	}
}
