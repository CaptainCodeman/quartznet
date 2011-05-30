using System;

namespace Quartz.Impl
{
	internal class MisfireHandler : QuartzThread
	{
		private volatile bool shutdown;
		private int numFails;

		internal MisfireHandler(MongoJobStore jobStore)
		{
		}

		public virtual void Initialize()
		{
			//this.Manage();
			Start();
		}

		public virtual void Shutdown()
		{
			shutdown = true;
			Interrupt();
		}

		private RecoverMisfiredJobsResult Manage()
		{
			try
			{
				numFails = 0;
				return null;
			}
			catch (Exception e)
			{
				if (numFails%4 == 0)
				{
				}
				numFails++;
			}
			return RecoverMisfiredJobsResult.NoOp;
		}

		public override void Run()
		{
                
		}

		// EOF
	}
}