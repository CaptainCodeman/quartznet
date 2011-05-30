using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

using Common.Logging;

using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using MongoDB.Driver.Builders;

using Quartz.Impl.AdoJobStore;
using Quartz.Impl.Calendar;
using Quartz.Impl.Matchers;
using Quartz.Impl.Triggers;
using Quartz.Simpl;
using Quartz.Spi;

namespace Quartz.Impl
{
	public class MongoJobStore : IJobStore
	{
		private MongoDatabase database;

		private MongoCollection<BsonDocument> CalendarCollection
		{
			get { return database.GetCollection<BsonDocument>(CollectionPrefix + "Calendar"); }
		}

		private MongoCollection<JobInfo> JobCollection
		{
			get { return database.GetCollection<JobInfo>(CollectionPrefix + "Job"); }
		}

		private MongoCollection<BsonDocument> PausedJobGroupCollection
		{
			get { return database.GetCollection<BsonDocument>(CollectionPrefix + "PausedJobGroup"); }
		}

		//private MongoCollection<TriggerInfo> TriggerCollection
		//{
		//    get { return database.GetCollection<TriggerInfo>(CollectionPrefix + "Trigger"); }
		//}

		private MongoCollection<BsonDocument> PausedTriggerGroupCollection
		{
			get { return database.GetCollection<BsonDocument>(CollectionPrefix + "PausedTriggerGroup"); }
		}

		private MongoCollection<FiredTriggerRecord> FiredTriggerCollection
		{
			get { return database.GetCollection<FiredTriggerRecord>(CollectionPrefix + "FiredTrigger"); }
		}

		private ISchedulerSignaler signaler;
		private MisfireHandler misfireHandler;

		private TimeSpan misfireThreshold = TimeSpan.FromMinutes(1); // one minute;

		private readonly object lockObject = new object();

		private const string AllGroupsPaused = "_$_ALL_GROUPS_PAUSED_$_";

		private readonly ILog log;

		public string CollectionPrefix { get; set; }
		public string ConnectionString { get; set; }
		
        /// <summary>
        /// Initializes a new instance of the <see cref="MongoJobStore"/> class.
        /// </summary>
		public MongoJobStore()
	    {
	        log = LogManager.GetLogger(GetType());
	    }

		/// <summary> 
		/// The time span by which a trigger must have missed its
		/// next-fire-time, in order for it to be considered "misfired" and thus
		/// have its misfire instruction applied.
		/// </summary>
		[TimeSpanParseRule(TimeSpanParseRule.Milliseconds)]
		public TimeSpan MisfireThreshold
		{
			get { return misfireThreshold; }
			set
			{
				if (value.TotalMilliseconds < 1)
				{
					throw new ArgumentException("MisfireThreshold must be larger than 0");
				}
				misfireThreshold = value;
			}
		}

		/// <summary>
		/// Called by the QuartzScheduler before the <see cref="IJobStore" /> is
		/// used, in order to give the it a chance to Initialize.
		/// </summary>
		public void Initialize(ITypeLoadHelper loadHelper, ISchedulerSignaler s)
		{
			signaler = s;

			BsonSerializer.RegisterSerializer(typeof(JobKey), KeySerializer<JobKey>.Instance);

			BsonSerializer.RegisterSerializer(typeof(TriggerKey), KeySerializer<TriggerKey>.Instance);

			BsonClassMap.RegisterClassMap<AbstractTrigger>(cm =>
			{
				cm.AutoMap();
				cm.UnmapProperty(c => c.Key);
				cm.UnmapProperty(c => c.JobKey);
				cm.UnmapProperty(c => c.Group);
				cm.UnmapProperty(c => c.Name);
				cm.UnmapProperty(c => c.JobGroup);
				cm.UnmapProperty(c => c.JobName);
			});

			BsonClassMap.RegisterClassMap<JobDetailImpl>(cm =>
			{
				cm.AutoMap();
				cm.MapProperty(c => c.JobType).SetSerializer(TypeSerializer.Instance);
				cm.UnmapProperty(c => c.Key);
				cm.UnmapProperty(c => c.Group);
				cm.UnmapProperty(c => c.Name);
			});

			BsonClassMap.RegisterClassMap<BaseCalendar>(cm =>
			{
				cm.MapProperty(c => c.Description);
				cm.MapProperty(c => c.TimeZone);
				cm.SetDiscriminatorIsRequired(true);
				cm.SetIsRootClass(true);
			});

			database = MongoDatabase.Create(ConnectionString);

			// TODO: Ensure indexes ...
			// TODO: Get collections ...
		}

		/// <summary>
		/// Called by the QuartzScheduler to inform the <see cref="IJobStore" /> that
		/// the scheduler has started.
		/// </summary>
		public void SchedulerStarted()
		{
			if (Clustered)
			{
				// TODO: Support cluserting
			}
			else
			{
				try
				{
					RecoverJobs();
				}
				catch (SchedulerException se)
				{
					throw new SchedulerConfigException("Failure occured during job recovery.", se);
				}
			}

			misfireHandler = new MisfireHandler(this);
			misfireHandler.Initialize();
		}

		/// <summary>
		/// Called by the QuartzScheduler to inform the <see cref="IJobStore" /> that
		/// it should free up all of it's resources because the scheduler is
		/// shutting down.
		/// </summary>
		public void Shutdown() { }

		/// <summary>
		/// Indicates whether job store supports persistence.
		/// </summary>
		/// <returns></returns>
		public bool SupportsPersistence
		{
			get { return true; }
		}

		/// <summary>
		/// How long (in milliseconds) the <see cref="IJobStore" /> implementation 
		/// estimates that it will take to release a trigger and acquire a new one. 
		/// </summary>
		public long EstimatedTimeToReleaseAndAcquireTrigger
		{
			get { return 10; }
		}

		/// <summary>
		/// Whether or not the <see cref="IJobStore" /> implementation is clustered.
		/// </summary>
		/// <returns></returns>
		public bool Clustered
		{
			get { return false; }	// TODO: Add support for clustering
		}

		/// <summary>
		/// Store the given <see cref="IJobDetail" /> and <see cref="ITrigger" />.
		/// </summary>
		/// <param name="newJob">The <see cref="IJobDetail" /> to be stored.</param>
		/// <param name="newTrigger">The <see cref="ITrigger" /> to be stored.</param>
		/// <throws>  ObjectAlreadyExistsException </throws>
		public void StoreJobAndTrigger(IJobDetail newJob, IOperableTrigger newTrigger)
		{
			StoreJob(newJob, false);
			StoreTrigger(newTrigger, newJob, false, InternalTriggerState.Waiting, false, false);
		}

		/// <summary>
		/// returns true if the given JobGroup is paused
		/// </summary>
		/// <param name="groupName"></param>
		/// <returns></returns>
		public bool IsJobGroupPaused(string groupName)
		{
			var query = Query.EQ("_id", groupName);
			var count = PausedJobGroupCollection.Count(query);
			return (count > 0);
		}

		/// <summary>
		/// returns true if the given TriggerGroup
		/// is paused
		/// </summary>
		/// <param name="groupName"></param>
		/// <returns></returns>
		public bool IsTriggerGroupPaused(string groupName)
		{
			var query = Query.EQ("_id", groupName);
			var count = PausedTriggerGroupCollection.Count(query);
			return (count > 0);
		}

		/// <summary>
		/// Store the given <see cref="IJobDetail" />.
		/// </summary>
		/// <param name="newJob">The <see cref="IJobDetail" /> to be stored.</param>
		/// <param name="replaceExisting">
		/// If <see langword="true" />, any <see cref="IJob" /> existing in the
		/// <see cref="IJobStore" /> with the same name and group should be
		/// over-written.
		/// </param>
		public void StoreJob(IJobDetail newJob, bool replaceExisting)
		{
			bool existingJob = CheckExists(newJob.Key);
			try
			{
				var jobInfo = new JobInfo(newJob.Key, (JobDetailImpl)newJob);
				if (existingJob)
				{
					if (!replaceExisting)
					{
						throw new ObjectAlreadyExistsException(newJob);
					}
					JobCollection.Save(jobInfo);
				}
				else
				{
					JobCollection.Insert(jobInfo);
				}
			}
			catch (IOException e)
			{
				throw new JobPersistenceException("Couldn't store job: " + e.Message, e);
			}
			catch (Exception e)
			{
				throw new JobPersistenceException("Couldn't store job: " + e.Message, e);
			}
		}

		public void StoreJobsAndTriggers(IDictionary<IJobDetail, IList<ITrigger>> triggersAndJobs, bool replace)
		{
			foreach (IJobDetail job in triggersAndJobs.Keys)
			{
				StoreJob(job, replace);

				foreach (ITrigger trigger in triggersAndJobs[job])
				{
					StoreTrigger((IOperableTrigger)trigger, replace);
				}
			}
		}

		/// <summary>
		/// Remove (delete) the <see cref="IJob" /> with the given
		/// key, and any <see cref="ITrigger" /> s that reference
		/// it.
		/// </summary>
		/// <remarks>
		/// If removal of the <see cref="IJob" /> results in an empty group, the
		/// group should be removed from the <see cref="IJobStore" />'s list of
		/// known group names.
		/// </remarks>
		/// <returns>
		/// 	<see langword="true" /> if a <see cref="IJob" /> with the given name and
		/// group was found and removed from the store.
		/// </returns>
		public bool RemoveJob(JobKey jobKey)
		{
			try
			{
				DeleteJobAndTriggers(jobKey);
				return true;
			}
			catch (Exception e)
			{
				throw new JobPersistenceException("Couldn't remove job: " + e.Message, e);
			}
		}

		private void DeleteJobAndTriggers(JobKey key)
		{
			DeleteJob(key);
		}

		public bool RemoveJobs(IList<JobKey> jobKeys)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Retrieve the <see cref="IJobDetail" /> for the given
		/// <see cref="IJob" />.
		/// </summary>
		/// <returns>
		/// The desired <see cref="IJob" />, or null if there is no match.
		/// </returns>
		public IJobDetail RetrieveJob(JobKey jobKey)
		{
			try
			{
				var jobInfo = JobCollection.FindOneById(jobKey.ToBsonDocument());
				return jobInfo.Job;
			}
			catch (Exception e)
			{
				throw new JobPersistenceException("Couldn't retrieve job: " + e.Message, e);
			}
		}

		/// <summary>
		/// Store the given <see cref="ITrigger" />.
		/// </summary>
		/// <param name="newTrigger">The <see cref="ITrigger" /> to be stored.</param>
		/// <param name="replaceExisting">If <see langword="true" />, any <see cref="ITrigger" /> existing in
		/// the <see cref="IJobStore" /> with the same name and group should
		/// be over-written.</param>
		/// <throws>  ObjectAlreadyExistsException </throws>
		public void StoreTrigger(IOperableTrigger newTrigger, bool replaceExisting)
		{
			StoreTrigger(newTrigger, null, replaceExisting, InternalTriggerState.Waiting, false, false);
		}

		protected virtual void StoreTrigger(IOperableTrigger newTrigger, IJobDetail job, bool replaceExisting, InternalTriggerState state, bool forceState, bool recovering)
		{
			bool existingTrigger = CheckExists(newTrigger.Key);

			if ((existingTrigger) && (!replaceExisting))
			{
				throw new ObjectAlreadyExistsException(newTrigger);
			}

			try
			{
				if (!forceState)
				{
					bool shouldBepaused = IsTriggerGroupPaused(newTrigger.Key.Group);

					if (!shouldBepaused)
					{
						shouldBepaused = IsTriggerGroupPaused(AllGroupsPaused);

						if (shouldBepaused)
						{
							InsertPausedTriggerGroup(newTrigger.Key.Group);
						}
					}

					if (shouldBepaused && (state.Equals(InternalTriggerState.Waiting) || state.Equals(InternalTriggerState.Acquired)))
					{
						state = InternalTriggerState.Paused;
					}
				}

				if (job == null)
				{
					var jobId = newTrigger.JobKey.ToBsonDocument();
					var jobInfo = JobCollection.FindOneById(jobId);
					job = jobInfo.Job;
				}

				if (job == null)
				{
					throw new JobPersistenceException("The job (" + newTrigger.JobKey + ") referenced by the trigger does not exist.");
				}

				if (job.ConcurrentExectionDisallowed && !recovering)
				{
					state = CheckBlockedState(job.Key, state);
				}

				var triggerInfo = new TriggerInfo(newTrigger.Key, state, newTrigger);

				if (existingTrigger)
				{
					var id = job.Key.ToBsonDocument();
					var query = Query.And(Query.EQ("_id", id), Query.EQ("Triggers._id", newTrigger.Key.ToBsonDocument()));
					var update = Update.Set("Triggers.$", triggerInfo.ToBsonDocument());
					JobCollection.Update(query, update);
				}
				else
				{
					var id = job.Key.ToBsonDocument();
					var query = Query.EQ("_id", id);
					var update = Update.Push("Triggers", triggerInfo.ToBsonDocument()).Inc("TriggerCount", 1);
					JobCollection.Update(query, update);
				}
			}
			catch (Exception ex)
			{
				// throw new ObjectAlreadyExistsException(newJob)
				string message = String.Format("Couldn't store trigger '{0}' for '{1}' job: {2}", newTrigger.Key, newTrigger.JobKey, ex.Message);
				throw new JobPersistenceException(message, ex);
			}
		}

		//private bool TriggerExists(JobKey jobKey, TriggerKey triggerKey)
		//{
		//    var query = Query.And(Query.EQ("_id", jobKey.ToBsonDocument()), Query.EQ("Triggers.Key", triggerKey.ToBsonDocument()));
		//    var count = JobCollection.Count(query);
		//    return (count > 0);
		//}

		private void InsertPausedTriggerGroup(string groupName)
		{
			var doc = new BsonDocument {{"_id", groupName}};
			PausedTriggerGroupCollection.Update(Query.EQ("_id", groupName), Update.Replace(doc), UpdateFlags.Upsert);
		}

		/// <summary>
		/// Determines if a Trigger for the given job should be blocked.  
		/// State can only transition to StatePausedBlocked/StateBlocked from 
		/// StatePaused/StateWaiting respectively.
		/// </summary>
		/// <returns>StatePausedBlocked, StateBlocked, or the currentState. </returns>
		private InternalTriggerState CheckBlockedState(JobKey jobKey, InternalTriggerState currentState)
		{
			// State can only transition to BLOCKED from PAUSED or WAITING.
			if ((currentState.Equals(InternalTriggerState.Waiting) == false) && (currentState.Equals(InternalTriggerState.Paused) == false))
			{
				return currentState;
			}

			try
			{
				IList<FiredTriggerRecord> lst = SelectFiredTriggerRecordsByJob(jobKey.Name, jobKey.Group);

				if (lst.Count > 0)
				{
					FiredTriggerRecord rec = lst[0];
					if (rec.JobDisallowsConcurrentExecution) // TODO: worry about failed/recovering/volatile job  states?
					{
						return (currentState == InternalTriggerState.Paused) ? InternalTriggerState.PausedAndBlocked : InternalTriggerState.Blocked;
					}
				}

				return currentState;
			}
			catch (Exception e)
			{
				throw new JobPersistenceException("Couldn't determine if trigger should be in a blocked state '" + jobKey + "': " + e.Message, e);
			}
		}

		/// <summary>
		/// Select the states of all fired-trigger records for a given job, or job
		/// group if job name is <see langword="null" />.
		/// </summary>
		/// <param name="jobName">Name of the job.</param>
		/// <param name="groupName">Name of the group.</param>
		/// <returns>a List of <see cref="FiredTriggerRecord" /> objects.</returns>
		protected virtual IList<FiredTriggerRecord> SelectFiredTriggerRecordsByJob(string jobName, string groupName)
		{
			IList<FiredTriggerRecord> lst = new List<FiredTriggerRecord>();

			QueryComplete query;
			if (jobName != null)
			{
				query = Query.EQ("_id", new JobKey(jobName, groupName).ToBsonDocument());
			}
			else
			{
				query = Query.EQ("_id.Group", groupName);
			}

			var documents = FiredTriggerCollection.Find(query);
			return documents.ToList();
			
			//using (IDataReader rs = cmd.ExecuteReader())
			//{
			//    while (rs.Read())
			//    {
			//        FiredTriggerRecord rec = new FiredTriggerRecord();

			//        rec.FireInstanceId = rs.GetString(ColumnEntryId);
			//        rec.FireInstanceState = rs.GetString(ColumnEntryState);
			//        rec.FireTimestamp = Convert.ToInt64(rs[ColumnFiredTime], CultureInfo.InvariantCulture);
			//        rec.Priority = Convert.ToInt32(rs[ColumnPriority], CultureInfo.InvariantCulture);
			//        rec.SchedulerInstanceId = rs.GetString(ColumnInstanceName);
			//        rec.TriggerKey = new TriggerKey(rs.GetString(ColumnTriggerName), rs.GetString(ColumnTriggerGroup));
			//        if (!rec.FireInstanceState.Equals(StateAcquired))
			//        {
			//            rec.JobDisallowsConcurrentExecution = rs.GetBoolean(ColumnIsNonConcurrent);
			//            rec.JobRequestsRecovery = rs.GetBoolean(ColumnRequestsRecovery);
			//            rec.JobKey = new JobKey(rs.GetString(ColumnJobName), rs.GetString(ColumnJobGroup));
			//        }
			//        lst.Add(rec);
			//    }
			//}
			//return lst;
		}

		/// <summary>
		/// Remove (delete) the <see cref="ITrigger" /> with the given key.
		/// </summary>
		/// <remarks>
		/// <p>
		/// If removal of the <see cref="ITrigger" /> results in an empty group, the
		/// group should be removed from the <see cref="IJobStore" />'s list of
		/// known group names.
		/// </p>
		/// <p>
		/// If removal of the <see cref="ITrigger" /> results in an 'orphaned' <see cref="IJob" />
		/// that is not 'durable', then the <see cref="IJob" /> should be deleted
		/// also.
		/// </p>
		/// </remarks>
		/// <returns>
		/// 	<see langword="true" /> if a <see cref="ITrigger" /> with the given
		/// name and group was found and removed from the store.
		/// </returns>
		public bool RemoveTrigger(TriggerKey triggerKey)
		{
			try
			{
				var triggerId = triggerKey.ToBsonDocument();
				var query = Query.EQ("Triggers._id", triggerId);
				
				// remove the trigger
				var update = Update.Inc("TriggerCount", -1).Pull("Triggers", Query.EQ("_id", triggerId));
				JobCollection.Update(query, update);

				var nonDurableWithoutTriggers = Query.And(Query.EQ("TriggerCount", 0), Query.EQ("Durable", false));
				JobCollection.Remove(nonDurableWithoutTriggers);

				return true;
			}
			catch (Exception e)
			{
				throw new JobPersistenceException("Couldn't remove trigger: " + e.Message, e);
			}
		}

		private void DeleteJob(JobKey key)
		{
			var query = Query.EQ("_id", key.ToBsonDocument());
			JobCollection.Remove(query);
		}

		public bool RemoveTriggers(IList<TriggerKey> triggerKeys)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Remove (delete) the <see cref="ITrigger" /> with the
		/// given name, and store the new given one - which must be associated
		/// with the same job.
		/// </summary>
		/// <param name="triggerKey">The <see cref="ITrigger"/> to be replaced.</param>
		/// <param name="newTrigger">The new <see cref="ITrigger" /> to be stored.</param>
		/// <returns>
		/// 	<see langword="true" /> if a <see cref="ITrigger" /> with the given
		/// name and group was found and removed from the store.
		/// </returns>
		public bool ReplaceTrigger(TriggerKey triggerKey, IOperableTrigger newTrigger)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Retrieve the given <see cref="ITrigger" />.
		/// </summary>
		/// <returns>
		/// The desired <see cref="ITrigger" />, or null if there is no
		/// match.
		/// </returns>
		public IOperableTrigger RetrieveTrigger(TriggerKey triggerKey)
		{
			var id = triggerKey.ToBsonDocument();
			var query = Query.EQ("Triggers._id", id);
			var job = JobCollection.FindOne(query);
			if (job == null)
				return null;

			var triggerInfo = job.Triggers.FirstOrDefault(x => x.Id.Equals(triggerKey));
			if (triggerInfo == null)
				return null;

			return triggerInfo.Trigger;
		}

		/// <summary>
		/// Determine whether a <see cref="IJob" /> with the given identifier already
		/// exists within the scheduler.
		/// </summary>
		/// <remarks>
		/// </remarks>
		/// <param name="jobKey">the identifier to check for</param>
		/// <returns>true if a job exists with the given identifier</returns>
		public bool CheckExists(JobKey jobKey)
		{
			var id = jobKey.ToBsonDocument();
			var count = JobCollection.Count(Query.EQ("_id", id));
			return (count > 0);
		}

		/// <summary>
		/// Determine whether a <see cref="ITrigger" /> with the given identifier already
		/// exists within the scheduler.
		/// </summary>
		/// <remarks>
		/// </remarks>
		/// <param name="triggerKey">the identifier to check for</param>
		/// <returns>true if a trigger exists with the given identifier</returns>
		public bool CheckExists(TriggerKey triggerKey)
		{
			var id = triggerKey.ToBsonDocument();
			var count = JobCollection.Count(Query.EQ("Triggers._id", id));
			return (count > 0);
		}

		/// <summary>
		/// Clear (delete!) all scheduling data - all <see cref="IJob"/>s, <see cref="ITrigger" />s
		/// <see cref="ICalendar" />s.
		/// </summary>
		/// <remarks>
		/// </remarks>
		public void ClearAllSchedulingData()
		{
			lock (lockObject)
			{
				CalendarCollection.RemoveAll();
				JobCollection.RemoveAll();
				PausedJobGroupCollection.RemoveAll();
				PausedTriggerGroupCollection.RemoveAll();
				FiredTriggerCollection.RemoveAll();
			}
		}

		/// <summary>
		/// Store the given <see cref="ICalendar" />.
		/// </summary>
		/// <param name="name">The name.</param>
		/// <param name="calendar">The <see cref="ICalendar" /> to be stored.</param>
		/// <param name="replaceExisting">If <see langword="true" />, any <see cref="ICalendar" /> existing
		/// in the <see cref="IJobStore" /> with the same name and group
		/// should be over-written.</param>
		/// <param name="updateTriggers">If <see langword="true" />, any <see cref="ITrigger" />s existing
		/// in the <see cref="IJobStore" /> that reference an existing
		/// Calendar with the same name with have their next fire time
		/// re-computed with the new <see cref="ICalendar" />.</param>
		/// <throws>  ObjectAlreadyExistsException </throws>
		public void StoreCalendar(string name, ICalendar calendar, bool replaceExisting, bool updateTriggers)
		{
			var doc = calendar.ToBsonDocument();
			CalendarCollection.Update(Query.EQ("_id", name), Update.Set("Calendar", doc), UpdateFlags.Upsert);
			
		}

		/// <summary>
		/// Remove (delete) the <see cref="ICalendar" /> with the
		/// given name.
		/// </summary>
		/// <remarks>
		/// If removal of the <see cref="ICalendar" /> would result in
		/// <see cref="ITrigger" />s pointing to non-existent calendars, then a
		/// <see cref="JobPersistenceException" /> will be thrown.
		/// </remarks>
		/// <param name="calName">The name of the <see cref="ICalendar" /> to be removed.</param>
		/// <returns>
		/// 	<see langword="true" /> if a <see cref="ICalendar" /> with the given name
		/// was found and removed from the store.
		/// </returns>
		public bool RemoveCalendar(string calName)
		{
			try
			{
				if (CalendarIsReferenced(calName))
				{
					throw new JobPersistenceException("Calender cannot be removed if it referenced by a trigger!");
				}

				return DeleteCalendar(calName);
			}
			catch (Exception e)
			{
				throw new JobPersistenceException("Couldn't remove calendar: " + e.Message, e);
			}
		}

		private bool CalendarIsReferenced(string calName)
		{
			var result = JobCollection.Find(Query.EQ("Triggers.Trigger.CalendarName", calName)).SetFields("_id").SetLimit(1).FirstOrDefault();
			return result != null;
		}

		private bool DeleteCalendar(string calName)
		{
			CalendarCollection.Remove(Query.EQ("_id", calName));
			return true;
		}

		/// <summary>
		/// Retrieve the given <see cref="ITrigger" />.
		/// </summary>
		/// <param name="calName">The name of the <see cref="ICalendar" /> to be retrieved.</param>
		/// <returns>
		/// The desired <see cref="ICalendar" />, or null if there is no
		/// match.
		/// </returns>
		public ICalendar RetrieveCalendar(string calName)
		{
			var value = CalendarCollection.FindOneById(calName);
			var doc = value["Calendar"].AsBsonDocument;
			var obj = BsonSerializer.Deserialize<BaseCalendar>(doc);
			return obj;
		}

		/// <summary>
		/// Get the number of <see cref="IJob" />s that are
		/// stored in the <see cref="IJobStore" />.
		/// </summary>
		/// <returns></returns>
		public int GetNumberOfJobs()
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Get the number of <see cref="ITrigger" />s that are
		/// stored in the <see cref="IJobStore" />.
		/// </summary>
		/// <returns></returns>
		public int GetNumberOfTriggers()
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Get the number of <see cref="ICalendar" /> s that are
		/// stored in the <see cref="IJobStore" />.
		/// </summary>
		/// <returns></returns>
		public int GetNumberOfCalendars()
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Get the names of all of the <see cref="IJob" /> s that
		/// have the given group name.
		/// <p>
		/// If there are no jobs in the given group name, the result should be a
		/// zero-length array (not <see langword="null" />).
		/// </p>
		/// </summary>
		/// <param name="matcher"></param>
		/// <returns></returns>
		public Collection.ISet<JobKey> GetJobKeys(GroupMatcher<JobKey> matcher)
		{
			QueryComplete query;
			
			if (StringOperator.Equality.Equals(matcher.CompareWithOperator))
			{
				query = Query.EQ("_id.Group", matcher.CompareToValue);
			}
			else if (StringOperator.Contains.Equals(matcher.CompareWithOperator))
			{
				query = Query.EQ("_id.Group", BsonRegularExpression.Create(Regex.Escape(matcher.CompareToValue)));
			}
			else if (StringOperator.EndsWith.Equals(matcher.CompareWithOperator))
			{
				query = Query.EQ("_id.Group", BsonRegularExpression.Create(Regex.Escape(matcher.CompareToValue) + "$"));
			}
			else if (StringOperator.StartsWith.Equals(matcher.CompareWithOperator))
			{
				query = Query.EQ("_id.Group", BsonRegularExpression.Create("^" + Regex.Escape(matcher.CompareToValue)));
			}
			else
			{
				throw new ArgumentOutOfRangeException("Don't know how to translate " + matcher.CompareWithOperator + " into Regex");
			}

			var keys = JobCollection.FindAs<BsonDocument>(query).SetFields("_id").Select(x => BsonSerializer.Deserialize<JobKey>(x["_id"].AsBsonDocument));
			return new Collection.HashSet<JobKey>(keys);
		}

		/// <summary>
		/// Get the names of all of the <see cref="ITrigger" />s
		/// that have the given group name.
		/// <p>
		/// If there are no triggers in the given group name, the result should be a
		/// zero-length array (not <see langword="null" />).
		/// </p>
		/// </summary>
		public Collection.ISet<TriggerKey> GetTriggerKeys(GroupMatcher<TriggerKey> matcher)
		{
			var query = GetTriggerGroupMatcherQuery(matcher);
			var expression = GetTriggerGroupMatcherExpression(matcher);
			var map = BsonJavaScript.Create(@"function Map() {
			    for(var i = 0; i < this.Triggers.length; i++) {
			        var id = this.Triggers[i]._id;
			        if (id.Group" + expression + @") {
			            emit(id,true);
			        }
			    }
			}");

			var reduce = BsonJavaScript.Create(@"function(key, values) {
			  return true;
			}");

			var docs = JobCollection.MapReduce(query, map, reduce).GetInlineResultsAs<BsonDocument>();
			var keys = docs.Select(x => BsonSerializer.Deserialize<TriggerKey>(x["_id"].AsBsonDocument));
			return new Collection.HashSet<TriggerKey>(keys);
		}

		private string GetTriggerGroupMatcherExpression(GroupMatcher<TriggerKey> matcher)
		{
			string value;
			if (StringOperator.Equality.Equals(matcher.CompareWithOperator))
			{
				value = string.Format("='{0}'", matcher.CompareToValue);
			}
			else if (StringOperator.Contains.Equals(matcher.CompareWithOperator))
			{
				value = string.Format(".match({0})", BsonRegularExpression.Create(Regex.Escape(matcher.CompareToValue)));
			}
			else if (StringOperator.EndsWith.Equals(matcher.CompareWithOperator))
			{
				value = string.Format(".match({0})", BsonRegularExpression.Create(Regex.Escape(matcher.CompareToValue) + "$"));
			}
			else if (StringOperator.StartsWith.Equals(matcher.CompareWithOperator))
			{
				value = string.Format(".match({0})", BsonRegularExpression.Create("^" + Regex.Escape(matcher.CompareToValue)));
			}
			else
			{
				throw new ArgumentOutOfRangeException("Don't know how to translate " + matcher.CompareWithOperator + " into Regex");
			}
			return value;
		}

		private static QueryComplete GetTriggerGroupMatcherQuery(GroupMatcher<TriggerKey> matcher)
		{
			QueryComplete query;

			if (StringOperator.Equality.Equals(matcher.CompareWithOperator))
			{
				query = Query.EQ("Triggers._id.Group", matcher.CompareToValue);
			}
			else if (StringOperator.Contains.Equals(matcher.CompareWithOperator))
			{
				query = Query.EQ("Triggers._id.Group", BsonRegularExpression.Create(Regex.Escape(matcher.CompareToValue)));
			}
			else if (StringOperator.EndsWith.Equals(matcher.CompareWithOperator))
			{
				query = Query.EQ("Triggers._id.Group", BsonRegularExpression.Create(Regex.Escape(matcher.CompareToValue) + "$"));
			}
			else if (StringOperator.StartsWith.Equals(matcher.CompareWithOperator))
			{
				query = Query.EQ("Triggers._id.Group", BsonRegularExpression.Create("^" + Regex.Escape(matcher.CompareToValue)));
			}
			else
			{
				throw new ArgumentOutOfRangeException("Don't know how to translate " + matcher.CompareWithOperator + " into Regex");
			}

			return query;
		}

		private static QueryComplete GetPausedTriggerGroupMatcherQuery(GroupMatcher<TriggerKey> matcher)
		{
			QueryComplete query;

			if (StringOperator.Equality.Equals(matcher.CompareWithOperator))
			{
				query = Query.EQ("_id", matcher.CompareToValue);
			}
			else if (StringOperator.Contains.Equals(matcher.CompareWithOperator))
			{
				query = Query.EQ("_id", BsonRegularExpression.Create(Regex.Escape(matcher.CompareToValue)));
			}
			else if (StringOperator.EndsWith.Equals(matcher.CompareWithOperator))
			{
				query = Query.EQ("_id", BsonRegularExpression.Create(Regex.Escape(matcher.CompareToValue) + "$"));
			}
			else if (StringOperator.StartsWith.Equals(matcher.CompareWithOperator))
			{
				query = Query.EQ("_id", BsonRegularExpression.Create("^" + Regex.Escape(matcher.CompareToValue)));
			}
			else
			{
				throw new ArgumentOutOfRangeException("Don't know how to translate " + matcher.CompareWithOperator + " into Regex");
			}

			return query;
		}

		//private QueryComplete GetGroupMatcherQuery<T>(GroupMatcher<T> matcher) where T : Key<T>
		//{
		//    QueryComplete query;

		//    if (StringOperator.Equality.Equals(matcher.CompareWithOperator))
		//    {
		//        query = Query.EQ("_id.Group", matcher.CompareToValue);
		//    }
		//    else if (StringOperator.Contains.Equals(matcher.CompareWithOperator))
		//    {
		//        query = Query.EQ("_id.Group", BsonRegularExpression.Create(Regex.Escape(matcher.CompareToValue)));
		//    }
		//    else if (StringOperator.EndsWith.Equals(matcher.CompareWithOperator))
		//    {
		//        query = Query.EQ("_id.Group", BsonRegularExpression.Create(Regex.Escape(matcher.CompareToValue) + "$"));
		//    }
		//    else if (StringOperator.StartsWith.Equals(matcher.CompareWithOperator))
		//    {
		//        query = Query.EQ("_id.Group", BsonRegularExpression.Create("^" + Regex.Escape(matcher.CompareToValue)));
		//    }
		//    else
		//    {
		//        throw new ArgumentOutOfRangeException("Don't know how to translate " + matcher.CompareWithOperator + " into Regex");
		//    }
		//}

		/// <summary>
		/// Get the names of all of the <see cref="IJob" />
		/// groups.
		/// <p>
		/// If there are no known group names, the result should be a zero-length
		/// array (not <see langword="null" />).
		/// </p>
		/// </summary>
		public IList<string> GetJobGroupNames()
		{
			var result = JobCollection.Distinct("_id.Group").Select(x => x.AsString);
			return result.ToList();
		}

		/// <summary>
		/// Get the names of all of the <see cref="ITrigger" />
		/// groups.
		/// <p>
		/// If there are no known group names, the result should be a zero-length
		/// array (not <see langword="null" />).
		/// </p>
		/// </summary>
		public IList<string> GetTriggerGroupNames()
		{
			var result = JobCollection.Distinct("Triggers._id.Group").Select(x => x.AsString);
			return result.ToList();
		}

		/// <summary>
		/// Get the names of all of the <see cref="ICalendar" /> s
		/// in the <see cref="IJobStore" />.
		/// 
		/// <p>
		/// If there are no Calendars in the given group name, the result should be
		/// a zero-length array (not <see langword="null" />).
		/// </p>
		/// </summary>
		public IList<string> GetCalendarNames()
		{
			var result = CalendarCollection.FindAllAs<BsonDocument>().Select(x => x["_id"].AsString);
			return result.ToList();
		}

		/// <summary>
		/// Get all of the Triggers that are associated to the given Job.
		/// </summary>
		/// <remarks>
		/// If there are no matches, a zero-length array should be returned.
		/// </remarks>
		public IList<IOperableTrigger> GetTriggersForJob(JobKey jobKey)
		{
			var id = jobKey.ToBsonDocument();
			var jobInfo = JobCollection.FindOneById(id);
			foreach (var triggerInfo in jobInfo.Triggers)
			{
				triggerInfo.Trigger.Key = triggerInfo.Id;
				triggerInfo.Trigger.JobKey = jobKey;
			}
			var triggers = jobInfo.Triggers.Select(x => x.Trigger);
			return triggers.ToList();
		}

		/// <summary>
		/// Get the current state of the identified <see cref="ITrigger" />.
		/// </summary>
		/// <seealso cref="TriggerState" />
		public TriggerState GetTriggerState(TriggerKey triggerKey)
		{
			lock (lockObject)
			{
				var state = GetInternalTriggerState(triggerKey);

				if ((int)state == int.MaxValue)
					return TriggerState.None;

				switch (state)
				{
					case InternalTriggerState.Complete:
						return TriggerState.Complete;
					case InternalTriggerState.Paused:
						return TriggerState.Paused;
					case InternalTriggerState.PausedAndBlocked:
						return TriggerState.Paused;
					case InternalTriggerState.Blocked:
						return TriggerState.Blocked;
					case InternalTriggerState.Error:
						return TriggerState.Error;
					default:
						return TriggerState.Normal;
				}
			}
		}

		/// <summary>
		/// Pause the <see cref="ITrigger" /> with the given key.
		/// </summary>
		public void PauseTrigger(TriggerKey triggerKey)
		{
			try
			{
				var oldState = GetInternalTriggerState(triggerKey);

				if (oldState.Equals(InternalTriggerState.Waiting) || oldState.Equals(InternalTriggerState.Acquired))
				{
					UpdateTriggerState(triggerKey, InternalTriggerState.Paused);
				}
				else if (oldState.Equals(InternalTriggerState.Blocked))
				{
					UpdateTriggerState(triggerKey, InternalTriggerState.PausedAndBlocked);
				}
			}
			catch (Exception e)
			{
				throw new JobPersistenceException("Couldn't pause trigger '" + triggerKey + "': " + e.Message, e);
			}
		}

		private void UpdateTriggerState(TriggerKey triggerKey, InternalTriggerState newState)
		{
			var id = triggerKey.ToBsonDocument();
			var query = Query.ElemMatch("Triggers", Query.EQ("_id", id));
			var update = Update.Set("Triggers.$.State", newState);
			JobCollection.Update(query, update);
		}

		public InternalTriggerState GetInternalTriggerState(TriggerKey triggerKey)
		{
			var triggerInfo = GetTriggerInfo(triggerKey);
			return triggerInfo.State;
		}

		public TriggerInfo GetTriggerInfo(TriggerKey triggerKey)
		{
			var id = triggerKey.ToBsonDocument();
			var query = Query.EQ("Triggers._id", id);
			var job = JobCollection.FindOne(query);
			if (job == null)
				return null;

			var triggerInfo = job.Triggers.FirstOrDefault(x => x.Id.Equals(triggerKey));
			triggerInfo.Trigger.Key = triggerInfo.Id;
			triggerInfo.Trigger.JobKey = job.Id;
			return triggerInfo;
		}

		/// <summary>
		/// Pause all of the <see cref="ITrigger" />s in the
		/// given group.
		/// </summary>
		/// <remarks>
		/// The JobStore should "remember" that the group is paused, and impose the
		/// pause on any new triggers that are added to the group while the group is
		/// paused.
		/// </remarks>
		public IList<string> PauseTriggers(GroupMatcher<TriggerKey> matcher)
		{
			try
			{
				UpdateTriggerGroupStateFromOtherStates(matcher, InternalTriggerState.Paused, InternalTriggerState.Acquired, InternalTriggerState.Waiting, InternalTriggerState.Waiting);
				UpdateTriggerGroupStateFromOtherStates(matcher, InternalTriggerState.PausedAndBlocked, InternalTriggerState.Blocked);

				IList<string> groups = GetTriggerGroups(matcher);

				foreach (string group in groups)
				{
					InsertPausedTriggerGroup(group);
				}

				return groups;
			}
			catch (Exception e)
			{
				throw new JobPersistenceException("Couldn't pause trigger group '" + matcher + "': " + e.Message, e);
			}
		}

		private IList<string> GetTriggerGroups(GroupMatcher<TriggerKey> matcher)
		{
			var query = GetTriggerGroupMatcherQuery(matcher);
			var expression = GetTriggerGroupMatcherExpression(matcher);
			var map = BsonJavaScript.Create(@"function Map() {
			    for(var i = 0; i < this.Triggers.length; i++) {
			        var id = this.Triggers[i]._id;
			        if (id.Group" + expression + @") {
			            emit(id.Group,true);
			        }
			    }
			}");

			var reduce = BsonJavaScript.Create(@"function(key, values) {
			  return true;
			}");

			var docs = JobCollection.MapReduce(query, map, reduce).GetInlineResultsAs<BsonDocument>();
			var keys = docs.Select(x => x["_id"].AsString);
			return keys.ToList();
		}

		private void UpdateTriggerGroupStateFromOtherStates(GroupMatcher<TriggerKey> matcher, InternalTriggerState newState, params InternalTriggerState[] oldStates)
		{
			var values = oldStates.Select(x => BsonValue.Create(x)).ToArray();
			var query = Query.ElemMatch("Triggers", Query.And(GetTriggerGroupMatcherQuery(matcher), Query.In("State", values)));
			var update = Update.Set("Triggers.$.State", newState);
			JobCollection.Update(query, update, UpdateFlags.Multi);
		}

		/// <summary>
		/// Pause the <see cref="IJob" /> with the given key - by
		/// pausing all of its current <see cref="ITrigger" />s.
		/// </summary>
		public void PauseJob(JobKey jobKey)
		{
			IList<IOperableTrigger> triggers = GetTriggersForJob(jobKey);
			foreach (IOperableTrigger trigger in triggers)
			{
				PauseTrigger(trigger.Key);
			}
		}

		/// <summary>
		/// Pause all of the <see cref="IJob" />s in the given
		/// group - by pausing all of their <see cref="ITrigger" />s.
		/// <p>
		/// The JobStore should "remember" that the group is paused, and impose the
		/// pause on any new jobs that are added to the group while the group is
		/// paused.
		/// </p>
		/// </summary>
		/// <seealso cref="string">
		/// </seealso>
		public IList<string> PauseJobs(GroupMatcher<JobKey> matcher)
		{
			Collection.ISet<string> groupNames = new Collection.HashSet<string>();
			Collection.ISet<JobKey> jobKeys = GetJobKeys(matcher);

			foreach (JobKey jobKey in jobKeys)
			{
				PauseJob(jobKey);
				groupNames.Add(jobKey.Group);
			}

			return new List<string>(groupNames);
		}

		/// <summary>
		/// Resume (un-pause) the <see cref="ITrigger" /> with the
		/// given key.
		/// 
		/// <p>
		/// If the <see cref="ITrigger" /> missed one or more fire-times, then the
		/// <see cref="ITrigger" />'s misfire instruction will be applied.
		/// </p>
		/// </summary>
		/// <seealso cref="string">
		/// </seealso>
		public void ResumeTrigger(TriggerKey triggerKey)
		{
			try
			{
				var triggerInfo = GetTriggerInfo(triggerKey);

				if (triggerInfo == null || !triggerInfo.NextFireTime.HasValue || triggerInfo.NextFireTime == DateTimeOffset.MinValue)
				{
					return;
				}

				bool blocked = false;
				if (triggerInfo.State == InternalTriggerState.PausedAndBlocked)
				{
					blocked = true;
				}

				var newState = CheckBlockedState(triggerInfo.Trigger.JobKey, InternalTriggerState.Waiting);

				bool misfired = false;

				if ((triggerInfo.NextFireTime.Value < SystemTime.UtcNow()))
				{
					misfired = UpdateMisfiredTrigger(triggerKey, newState, true);
				}

				if (!misfired)
				{
					UpdateTriggerStateFromOtherState(triggerKey, newState, blocked ? InternalTriggerState.PausedAndBlocked : InternalTriggerState.Paused);
				}
			}
			catch (Exception e)
			{
				throw new JobPersistenceException("Couldn't resume trigger '" + triggerKey + "': " + e.Message, e);
			}
		}

		/// <summary>
		/// Update the given trigger to the given new state, if it is in the given
		/// old state.
		/// </summary>
		/// <param name="triggerKey">the key of the trigger</param>
		/// <param name="newState">the new state for the trigger</param>
		/// <param name="oldState">the old state the trigger must be in</param>
		/// <returns>
		/// int the number of rows updated
		/// </returns>
		private void UpdateTriggerStateFromOtherState(TriggerKey triggerKey, InternalTriggerState newState, InternalTriggerState oldState)
		{
			var query = Query.ElemMatch("Triggers", Query.And(Query.EQ("_id", triggerKey.ToBsonDocument()), Query.EQ("State", oldState)));
			var update = Update.Set("Triggers.$.State", newState);
			JobCollection.Update(query, update);
		}

		protected virtual bool UpdateMisfiredTrigger(TriggerKey triggerKey, InternalTriggerState newStateIfNotComplete, bool forceState)
		{
			try
			{
				IOperableTrigger trig = RetrieveTrigger(triggerKey);

				DateTimeOffset misfireTime = SystemTime.UtcNow();
				if (MisfireThreshold > TimeSpan.Zero)
				{
					misfireTime = misfireTime.AddMilliseconds(-1 * MisfireThreshold.TotalMilliseconds);
				}

				if (trig.GetNextFireTimeUtc().Value > misfireTime)
				{
					return false;
				}

				DoUpdateOfMisfiredTrigger(trig, forceState, newStateIfNotComplete, false);

				signaler.NotifySchedulerListenersFinalized(trig);

				return true;
			}
			catch (Exception e)
			{
				throw new JobPersistenceException(
					string.Format("Couldn't update misfired trigger '{0}': {1}", triggerKey, e.Message), e);
			}
		}

		private void DoUpdateOfMisfiredTrigger(IOperableTrigger trig, bool forceState, InternalTriggerState newStateIfNotComplete, bool recovering)
		{
			ICalendar cal = null;
			if (trig.CalendarName != null)
			{
				cal = RetrieveCalendar(trig.CalendarName);
			}

			signaler.NotifyTriggerListenersMisfired(trig);

			trig.UpdateAfterMisfire(cal);

			// TODO: Decide if we need to replace the whole trigger or could just update the status and next-fire-time
			if (!trig.GetNextFireTimeUtc().HasValue)
			{
				StoreTrigger(trig, null, true, InternalTriggerState.Complete, forceState, recovering);
			}
			else
			{
				StoreTrigger(trig, null, true, newStateIfNotComplete, forceState, false);
			}
		}

		/// <summary>
		/// Resume (un-pause) all of the <see cref="ITrigger" />s
		/// in the given group.
		/// <p>
		/// If any <see cref="ITrigger" /> missed one or more fire-times, then the
		/// <see cref="ITrigger" />'s misfire instruction will be applied.
		/// </p>
		/// </summary>
		public IList<string> ResumeTriggers(GroupMatcher<TriggerKey> matcher)
		{
			try
			{
				DeletePausedTriggerGroup(matcher);
				Collection.HashSet<string> groups = new Collection.HashSet<string>();

				Collection.ISet<TriggerKey> keys = GetTriggerKeys(matcher);

				foreach (TriggerKey key in keys)
				{
					ResumeTrigger(key);
					groups.Add(key.Group);
				}

				return new List<string>(groups);
			}
			catch (Exception e)
			{
				throw new JobPersistenceException("Couldn't resume trigger group '" + matcher + "': " + e.Message, e);
			}
		}

		private void DeletePausedTriggerGroup(GroupMatcher<TriggerKey> matcher)
		{
			var query = GetPausedTriggerGroupMatcherQuery(matcher);
			PausedTriggerGroupCollection.Remove(query);
		}

		private void DeletePausedTriggerGroup(string groupName)
		{
			var query = Query.EQ("_id", groupName);
			PausedTriggerGroupCollection.Remove(query);
		}

		/// <summary>
		/// Gets the paused trigger groups.
		/// </summary>
		/// <returns></returns>
		public Collection.ISet<string> GetPausedTriggerGroups()
		{
			var docs = PausedTriggerGroupCollection.FindAll();
			var groups = docs.Select(x => x["_id"].AsString);
			return new Collection.HashSet<string>(groups);
		}

		/// <summary> 
		/// Resume (un-pause) the <see cref="IJob" /> with the
		/// given key.
		/// <p>
		/// If any of the <see cref="IJob" />'s<see cref="ITrigger" /> s missed one
		/// or more fire-times, then the <see cref="ITrigger" />'s misfire
		/// instruction will be applied.
		/// </p>
		/// </summary>
		public void ResumeJob(JobKey jobKey)
		{
			IList<IOperableTrigger> triggers = GetTriggersForJob(jobKey);
			foreach (IOperableTrigger trigger in triggers)
			{
				ResumeTrigger(trigger.Key);
			}
		}

		/// <summary>
		/// Resume (un-pause) all of the <see cref="IJob" />s in
		/// the given group.
		/// <p>
		/// If any of the <see cref="IJob" /> s had <see cref="ITrigger" /> s that
		/// missed one or more fire-times, then the <see cref="ITrigger" />'s
		/// misfire instruction will be applied.
		/// </p> 
		/// </summary>
		public Collection.ISet<string> ResumeJobs(GroupMatcher<JobKey> matcher)
		{
			Collection.ISet<String> groupNames = new Collection.HashSet<string>();
			Collection.ISet<JobKey> jobKeys = GetJobKeys(matcher);

			foreach (JobKey jobKey in jobKeys)
			{
				ResumeJob(jobKey);
				groupNames.Add(jobKey.Group);
			}
			return groupNames;
		}

		/// <summary>
		/// Pause all triggers - equivalent of calling <see cref="IJobStore.PauseTriggers" />
		/// on every group.
		/// <p>
		/// When <see cref="IJobStore.ResumeAll" /> is called (to un-pause), trigger misfire
		/// instructions WILL be applied.
		/// </p>
		/// </summary>
		/// <seealso cref="IJobStore.ResumeAll" />
		public void PauseAll()
		{
			lock (lockObject)
			{
				IList<string> triggerGroupNames = GetTriggerGroupNames();

				foreach (string groupName in triggerGroupNames)
				{
					PauseTriggers(GroupMatcher<TriggerKey>.GroupEquals(groupName));
				}
			}
		}

		/// <summary>
		/// Resume (un-pause) all triggers - equivalent of calling <see cref="IJobStore.ResumeTriggers" />
		/// on every group.
		/// <p>
		/// If any <see cref="ITrigger" /> missed one or more fire-times, then the
		/// <see cref="ITrigger" />'s misfire instruction will be applied.
		/// </p>
		/// 
		/// </summary>
		/// <seealso cref="IJobStore.PauseAll" />
		public void ResumeAll()
		{
			lock (lockObject)
			{
				IList<string> triggerGroupNames = GetTriggerGroupNames();

				foreach (string groupName in triggerGroupNames)
				{
					ResumeTriggers(GroupMatcher<TriggerKey>.GroupEquals(groupName));
				}

				DeletePausedTriggerGroup(AllGroupsPaused);
			}
		}

		/// <summary>
		/// Get a handle to the next trigger to be fired, and mark it as 'reserved'
		/// by the calling scheduler.
		/// </summary>
		/// <param name="noLaterThan">If &gt; 0, the JobStore should only return a Trigger
		/// that will fire no later than the time represented in this value as
		/// milliseconds.</param>
		/// <param name="maxCount"></param>
		/// <param name="timeWindow"></param>
		/// <returns></returns>
		/// <seealso cref="ITrigger">
		/// </seealso>
		public IList<IOperableTrigger> AcquireNextTriggers(DateTimeOffset noLaterThan, int maxCount, TimeSpan timeWindow)
		{
			throw new NotImplementedException();
		}

		/// <summary> 
		/// Inform the <see cref="IJobStore" /> that the scheduler no longer plans to
		/// fire the given <see cref="ITrigger" />, that it had previously acquired
		/// (reserved).
		/// </summary>
		public void ReleaseAcquiredTrigger(IOperableTrigger trigger)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Inform the <see cref="IJobStore" /> that the scheduler is now firing the
		/// given <see cref="ITrigger" /> (executing its associated <see cref="IJob" />),
		/// that it had previously acquired (reserved).
		/// </summary>
		/// <returns>
		/// May return null if all the triggers or their calendars no longer exist, or
		/// if the trigger was not successfully put into the 'executing'
		/// state.  Preference is to return an empty list if none of the triggers
		/// could be fired.
		/// </returns>
		public IList<TriggerFiredResult> TriggersFired(IList<IOperableTrigger> triggers)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Inform the <see cref="IJobStore" /> that the scheduler has completed the
		/// firing of the given <see cref="ITrigger" /> (and the execution its
		/// associated <see cref="IJob" />), and that the <see cref="JobDataMap" />
		/// in the given <see cref="IJobDetail" /> should be updated if the <see cref="IJob" />
		/// is stateful.
		/// </summary>
		public void TriggeredJobComplete(IOperableTrigger trigger, IJobDetail jobDetail, SchedulerInstruction triggerInstCode)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Inform the <see cref="IJobStore" /> of the Scheduler instance's Id, 
		/// prior to initialize being invoked.
		/// </summary>
		public string InstanceId { get; set; }

		/// <summary>
		/// Inform the <see cref="IJobStore" /> of the Scheduler instance's name, 
		/// prior to initialize being invoked.
		/// </summary>
		/// <summary>
		/// Get or set the instance Id of the Scheduler (must be unique within this server instance).
		/// </summary>
		public string InstanceName { get; set; }

		/// <summary>
		/// Tells the JobStore the pool size used to execute jobs.
		/// </summary>
		public int ThreadPoolSize { get; set; }

		/// <summary>
		/// Will recover any failed or misfired jobs and clean up the data store as
		/// appropriate.
		/// </summary>
		protected virtual void RecoverJobs()
		{
			//try
			//{
			//    // update inconsistent job states
			//    int rows = Delegate.UpdateTriggerStatesFromOtherStates(conn, StateWaiting, StateAcquired, StateBlocked);

			//    string.Format(CultureInfo.InvariantCulture, "UPDATE {0}{1} SET {2} = @newState WHERE {3} = {4} AND ({5} = @oldState1 OR {6} = @oldState2)",
			//  TablePrefixSubst,
			//  TableTriggers, ColumnTriggerState,
			//  ColumnSchedulerName, SchedulerNameSubst,
			//  ColumnTriggerState, ColumnTriggerState);

			//    rows += Delegate.UpdateTriggerStatesFromOtherStates(conn, StatePaused, StatePausedBlocked, StatePausedBlocked);

			//    log.Info("Freed " + rows + " triggers from 'acquired' / 'blocked' state.");

			//    // clean up misfired jobs
			//    RecoverMisfiredJobs(conn, true);

			//    // recover jobs marked for recovery that were not fully executed
			//    IList<IOperableTrigger> recoveringJobTriggers = Delegate.SelectTriggersForRecoveringJobs(conn);
			//    log.Info("Recovering " + recoveringJobTriggers.Count + " jobs that were in-progress at the time of the last shut-down.");

			//    foreach (IOperableTrigger trigger in recoveringJobTriggers)
			//    {
			//        if (JobExists(conn, trigger.JobKey))
			//        {
			//            trigger.ComputeFirstFireTimeUtc(null);
			//            StoreTrigger(conn, trigger, null, false, StateWaiting, false, true);
			//        }
			//    }
			//    log.Info("Recovery complete.");

			//    // remove lingering 'complete' triggers...
			//    IList<TriggerKey> triggersInState = Delegate.SelectTriggersInState(conn, StateComplete);
			//    for (int i = 0; triggersInState != null && i < triggersInState.Count; i++)
			//    {
			//        RemoveTrigger(conn, triggersInState[i]);
			//    }
			//    if (triggersInState != null)
			//    {
			//        log.Info(string.Format(CultureInfo.InvariantCulture, "Removed {0} 'complete' triggers.", triggersInState.Count));
			//    }

			//    // clean up any fired trigger entries
			//    int n = Delegate.DeleteFiredTriggers(conn);
			//    log.Info("Removed " + n + " stale fired job entries.");
			//}
			//catch (JobPersistenceException)
			//{
			//    throw;
			//}
			//catch (Exception e)
			//{
			//    throw new JobPersistenceException("Couldn't recover jobs: " + e.Message, e);
			//}
		}
	}
}
