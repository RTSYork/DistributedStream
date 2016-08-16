package dstream;

public class MemoryMonitor extends Thread
{
	private static Thread th = new MemoryMonitor();

	private static volatile boolean lowMem = false;

	static
	{
		th.setDaemon(true);
		th.start();
	}

	private MemoryMonitor()
	{
		super();
	}

	public void run()
	{
		Runtime r = Runtime.getRuntime();
		long curMem = r.freeMemory(), prevMem;
		while (true)
		{
			prevMem = curMem;
			try
			{
				Thread.sleep(1000);
			}
			catch (InterruptedException e)
			{
				System.gc();
			}
			curMem = r.freeMemory();
			// Set low memory flag if free memory is projected to drop below 16 MB in the next second
			lowMem = (curMem - (prevMem - curMem) < 16777216);
		}
	}

	public static boolean lowMemory()
	{
		return lowMem;
	}

	public static void reclaimMemory()
	{
		th.interrupt();
	}
}
