import java.io.*;
import java.util.*;

public class Profiler
{
	private static long t1;

	private static long maxUsedHeap = 0;

	static
	{
		String prof = System.getenv("PROFILE");
		if (prof != null)
		{
			if (prof.indexOf('t') >= 0)
			{
				Runtime.getRuntime().addShutdownHook(new Thread(() ->
				{
					System.err.println((System.currentTimeMillis() - t1) + "ms elapsed");
				}));
				t1 = System.currentTimeMillis();
			}
			if (prof.indexOf('m') >= 0)
			{
				monitorHeap();
				Runtime.getRuntime().addShutdownHook(new Thread(() ->
				{
					System.err.println(maxUsedHeap + " bytes max heap usage");
				}));
			}
		}
	}

	public static void init()
	{
	}

	private static void monitorHeap()
	{
		Runtime r = Runtime.getRuntime();
		System.gc();
		maxUsedHeap = r.totalMemory() - r.freeMemory();
		Timer t = new Timer(true);
		t.schedule(new TimerTask()
		{
			public void run()
			{
				System.gc();
				long heap = r.totalMemory() - r.freeMemory();
				if (heap > maxUsedHeap)
					maxUsedHeap = heap;
			}
		}, 1000, 1000);
	}

	public static void pause()
	{
		System.err.println("Press enter to continue...");
		try
		{
			while (System.in.read() != '\n')
				;
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}
}
