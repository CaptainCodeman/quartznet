using System;

using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;

namespace Quartz.Impl
{
	public class TypeSerializer : BsonBaseSerializer
	{
		private static readonly TypeSerializer instance = new TypeSerializer();
		public static TypeSerializer Instance
		{
			get { return instance; }
		}

		public override object Deserialize(
			BsonReader bsonReader,
			Type nominalType,
			IBsonSerializationOptions options
			)
		{
			var typeName = bsonReader.ReadString();
			return Type.GetType(typeName);
		}

		public override void Serialize(
			BsonWriter bsonWriter,
			Type nominalType,
			object value,
			IBsonSerializationOptions options
			)
		{
			var type = (Type)value;
			bsonWriter.WriteString(type.AssemblyQualifiedName);
		}
	}
}