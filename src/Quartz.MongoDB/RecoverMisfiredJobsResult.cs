using System;

namespace Quartz.Impl
{
	/// <summary>
	/// Helper class for returning the composite result of trying
	/// to recover misfired jobs.
	/// </summary>
	public class RecoverMisfiredJobsResult
	{
		public static readonly RecoverMisfiredJobsResult NoOp = new RecoverMisfiredJobsResult(false, 0, DateTimeOffset.MaxValue);

		private readonly bool hasMoreMisfiredTriggers;
		private readonly int processedMisfiredTriggerCount;
		private readonly DateTimeOffset earliestNewTimeUtc;

		/// <summary>
		/// Initializes a new instance of the <see cref="RecoverMisfiredJobsResult"/> class.
		/// </summary>
		/// <param name="hasMoreMisfiredTriggers">if set to <c>true</c> [has more misfired triggers].</param>
		/// <param name="processedMisfiredTriggerCount">The processed misfired trigger count.</param>
		/// <param name="earliestNewTimeUtc"></param>
		public RecoverMisfiredJobsResult(bool hasMoreMisfiredTriggers, int processedMisfiredTriggerCount, DateTimeOffset earliestNewTimeUtc)
		{
			this.hasMoreMisfiredTriggers = hasMoreMisfiredTriggers;
			this.processedMisfiredTriggerCount = processedMisfiredTriggerCount;
			this.earliestNewTimeUtc = earliestNewTimeUtc;
		}

		/// <summary>
		/// Gets a value indicating whether this instance has more misfired triggers.
		/// </summary>
		/// <value>
		/// 	<c>true</c> if this instance has more misfired triggers; otherwise, <c>false</c>.
		/// </value>
		public bool HasMoreMisfiredTriggers
		{
			get { return hasMoreMisfiredTriggers; }
		}

		/// <summary>
		/// Gets the processed misfired trigger count.
		/// </summary>
		/// <value>The processed misfired trigger count.</value>
		public int ProcessedMisfiredTriggerCount
		{
			get { return processedMisfiredTriggerCount; }
		}

		public DateTimeOffset EarliestNewTime
		{
			get { return earliestNewTimeUtc; }
		}
	}
}