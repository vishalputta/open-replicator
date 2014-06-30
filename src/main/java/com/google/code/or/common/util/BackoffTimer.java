package com.google.code.or.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author yogesh.dahiya
 */
/** Unit of Time Used through out is Ms */
public class BackoffTimer
{
	private static final Logger LOGGER = LoggerFactory.getLogger(BackoffTimer.class);

	public static final long NO_MORE_RETRIES = -1L;
	private BackoffTimerConfig config;
	private String name;
	private long startTime;
	private long retryCount;
	private long currentSleep;

	public BackoffTimer(BackoffTimerConfig config, String name)
	{
		super();
		this.config = config;
		this.name = name;
		reset();
	}

	/** Reset the counter; e.g. after a success */
	public void reset()
	{
		currentSleep = config.getInitSleep();
		retryCount = 0;
		startTime = -1;
	}

	/**
	 * Increments retries number and sleep
	 * @return the new sleep or NO_MORE_RETRIES if no more retries are allowed
	 */
	public long backoff()
	{
		if (0 == retryCount)
			startTime = System.currentTimeMillis();
		++retryCount;
		currentSleep = config.calculateNextSleep(currentSleep);
		if (config.getMaxRetryNum() >= 0 && retryCount > config.getMaxRetryNum())
			return NO_MORE_RETRIES;

		return currentSleep;
	}

	/**
	 * Sleep for the current sleep time, including any backoff that has occurred.
	 * @return true if we slept for the total time; false if we were interrupted
	 */
	public boolean sleep()
	{
		if (0L >= currentSleep)
			return true;
		try
		{
			Thread.sleep(currentSleep);
			return true;
		}
		catch (InterruptedException ex)
		{
			LOGGER.info(name + ": sleep interrupted");
			return false;
		}
	}

	public long getRemainingRetriesCount()
	{
		return config.getMaxRetryNum() >= 0 ? (config.getMaxRetryNum() - retryCount) : Long.MAX_VALUE;
	}

	public long getTotalRetryTime()
	{
		return startTime > 0 ? System.currentTimeMillis() - startTime : 0;
	}

	public static class BackoffTimerConfig
	{
		/** init sleep 1ms , max sleep 1min , delta 5ms */
		public static final BackoffTimerConfig LINEAR_BACKOFF_UNLIMITED_RETRIES = new BackoffTimerConfig(1, 60000, 1,
		        5, -1);
		public static final BackoffTimerConfig EXPONENTIAL_BACKOFF_UNLIMITED_RETRIES = new BackoffTimerConfig(1, 60000,
		        2, 5, -1);

		private final long initSleep;
		private final long maxSleep;
		private final double sleepIncrementFactor;
		private final long sleepIncrementDelta;
		private final long maxRetryNum;

		public BackoffTimerConfig(long initSleep, long maxSleep, double sleepIncrementFactor, long sleepInrementDelta,
		        long maxRetryNum)
		{
			super();
			this.initSleep = initSleep;
			this.maxSleep = maxSleep;
			this.sleepIncrementFactor = sleepIncrementFactor;
			this.sleepIncrementDelta = sleepInrementDelta;
			this.maxRetryNum = maxRetryNum;
		}

		/** Sleep calculation : S' = min(maxSleep, S * sleepIncrementFactor + sleepIncrementDelta) */
		public long calculateNextSleep(long curSleep)
		{
			if (curSleep < 0)
				return initSleep;

			long newSleep = (long) (sleepIncrementFactor * curSleep) + sleepIncrementDelta;
			if (newSleep <= curSleep)
				newSleep = curSleep + 1;

			return Math.min(maxSleep, newSleep);
		}

		public long getInitSleep()
		{
			return initSleep;
		}

		public long getMaxSleep()
		{
			return maxSleep;
		}

		public double getSleepIncrementFactor()
		{
			return sleepIncrementFactor;
		}

		public long getSleepIncrementDelta()
		{
			return sleepIncrementDelta;
		}

		public long getMaxRetryNum()
		{
			return maxRetryNum;
		}

		public String toString()
		{
			return "BackoffTimerStaticConfig [initSleep=" + initSleep + ", maxSleep=" + maxSleep
			        + ", sleepIncrementFactor=" + sleepIncrementFactor + ", sleepIncrementDelta=" + sleepIncrementDelta
			        + ", maxRetryNum=" + maxRetryNum + "]";
		}

	}

	public BackoffTimerConfig getConfig()
	{
		return config;
	}

	public void setConfig(BackoffTimerConfig config)
	{
		this.config = config;
	}

	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name;
	}

	public long getStartTime()
	{
		return startTime;
	}

	public void setStartTime(long startTime)
	{
		this.startTime = startTime;
	}

	public long getRetryCount()
	{
		return retryCount;
	}

	public void setRetryCount(long retryCount)
	{
		this.retryCount = retryCount;
	}

	public long getCurrentSleep()
	{
		return currentSleep;
	}

	public void setCurrentSleep(long currentSleep)
	{
		this.currentSleep = currentSleep;
	}

}
