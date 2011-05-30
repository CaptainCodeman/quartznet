namespace Quartz.Impl
{
	public class JobInfo
	{
		public JobKey Id { get; private set; }
		public JobDetailImpl Job { get; private set; }
		public TriggerInfo[] Triggers { get; private set; }
		public int TriggerCount { get; private set; }

		public JobInfo(JobKey id, JobDetailImpl job)
		{
			Id = id;
			Job = job;
			Triggers = new TriggerInfo[0];
			TriggerCount = 0;
		}
	}
}