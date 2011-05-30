using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;

using NUnit.Framework;

using Quartz.Impl;
using Quartz.Impl.Calendar;
using Quartz.Impl.Triggers;
using Quartz.Job;
using Quartz.Spi;
using Quartz.Tests.Integration.Impl;
using Quartz.Tests.Integration.Impl.AdoJobStore;

namespace Quartz.Tests.MongoDB
{
	[System.ComponentModel.Category("integration")]
	[TestFixture]
	public class MongoJobStoreSmokeTest
	{
		private static readonly string _connectionString = "server=localhost;database=quartzScheduler;safe=true;";
		private bool clearJobs = true;
		private bool scheduleJobs = true;
		private bool clustered = false;

		[Test]
		public void TestMongoDB()
		{
			NameValueCollection properties = new NameValueCollection();

			properties["quartz.scheduler.instanceName"] = "TestScheduler";
			properties["quartz.scheduler.instanceId"] = "instance_one";
			properties["quartz.threadPool.type"] = "Quartz.Simpl.SimpleThreadPool, Quartz";
			properties["quartz.threadPool.threadCount"] = "10";
			properties["quartz.threadPool.threadPriority"] = "Normal";
			properties["quartz.jobStore.misfireThreshold"] = "60000";
			properties["quartz.jobStore.type"] = "Quartz.Impl.MongoJobStore, Quartz.MongoDB";
			properties["quartz.jobStore.connectionString"] = _connectionString;

			// First we must get a reference to a scheduler
			ISchedulerFactory sf = new StdSchedulerFactory(properties);
			IScheduler sched = sf.GetScheduler();
			SmokeTestPerformer performer = new SmokeTestPerformer();
			performer.Test(sched, clearJobs, scheduleJobs);
		}
	}
}
