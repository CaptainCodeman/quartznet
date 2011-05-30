using System;

using Quartz.Impl.Triggers;
using Quartz.Simpl;
using Quartz.Spi;

namespace Quartz.Impl
{
	public class TriggerInfo
	{
		public TriggerKey Id { get; private set; }
		public InternalTriggerState State { get; private set; }
		public IOperableTrigger Trigger { get; private set; }
		public DateTimeOffset? NextFireTime { get; private set; }

		public TriggerInfo(TriggerKey id, InternalTriggerState state, IOperableTrigger trigger)
		{
			Id = id;
			State = state;
			Trigger = trigger;
		}
	}
}