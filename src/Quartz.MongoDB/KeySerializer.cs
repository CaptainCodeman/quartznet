using System;

using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;

using Quartz.Util;

namespace Quartz.Impl
{
	public class KeySerializer<T> : BsonBaseSerializer
	{
		private static readonly KeySerializer<T> instance = new KeySerializer<T>();
		public static KeySerializer<T> Instance
		{
			get { return instance; }
		}

		public override object Deserialize(
			BsonReader bsonReader,
			Type nominalType,
			IBsonSerializationOptions options
			)
		{
			bsonReader.ReadStartDocument();
			var group = bsonReader.ReadString("Group");
			var name = bsonReader.ReadString("Name");
			bsonReader.ReadEndDocument();
			return (T)Activator.CreateInstance(typeof (T), name, group);
		}

		public override void Serialize(
			BsonWriter bsonWriter,
			Type nominalType,
			object value,
			IBsonSerializationOptions options
			)
		{
			var key = (Key<T>)value;
			bsonWriter.WriteStartDocument();
			bsonWriter.WriteString("Group", key.Group);
			bsonWriter.WriteString("Name", key.Name);
			bsonWriter.WriteEndDocument();
		}
	}
}