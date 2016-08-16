import java.util.*;

public class ThreadAffinity
{
	static
	{
		System.loadLibrary("threadaffinity");
	}

	private static native void setThreadAffinity(BitSet cpuset);

	public static void set(BitSet cpuset)
	{
		setThreadAffinity(cpuset);
		//System.err.println("Thread affinity set to " + cpuset.toString());
	}
}
