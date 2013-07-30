package cascading.avro;

import java.io.IOException;
import java.util.Collection;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroSerialization;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import parquet.avro.AvroParquetOutputCommitter;
import parquet.avro.AvroParquetSupportInputFormat;
import parquet.avro.AvroParquetSupportOutputFormat;
import parquet.avro.AvroReadSupport;
import cascading.avro.serialization.AvroSpecificRecordSerialization;
import cascading.flow.FlowProcess;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Tuple;

public class ParquetScheme extends AvroScheme {
	
	public ParquetScheme(Schema schema)
	{
		super(schema);
	}
	
	 @Override
	public boolean source(FlowProcess<JobConf> flowProcess,
			SourceCall<Object[], RecordReader> sourceCall) throws IOException {

		RecordReader<Void, GenericRecord> input = sourceCall.getInput();
		
		GenericRecord genericRecord = input.createValue();
		
		if (!input.next(input.createKey(), genericRecord)) {
			return false;
		}		
		
		Record record = new Record(genericRecord.getSchema());
		
		for(Field field : genericRecord.getSchema().getFields())
		{
			record.put(field.name(), genericRecord.get(field.name()));
		}		
		
		Tuple tuple = sourceCall.getIncomingEntry().getTuple();
		tuple.clear();

		Object[] split = AvroToCascading.parseRecord(record, schema);
		tuple.addAll(split);

		return true;
	}
	
	@Override
	public void sourceConfInit(FlowProcess<JobConf> flowProcess,
			Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
		
		retrieveSourceFields(flowProcess, tap);
		
		// Set the input schema and input class
		conf.set(AvroReadSupport.AVRO_REQUESTED_PROJECTION, schema.toString());	
		
		conf.setInputFormat(AvroParquetSupportInputFormat.class);
	
		// add AvroSerialization to io.serializations
		addAvroSerializations(conf);			
	}

	@Override
	public void sinkConfInit(FlowProcess<JobConf> flowProcess,
			Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {

		if (schema == null) {
			throw new RuntimeException("Must provide sink schema");
		}
		
		// Set the input schema and input class
		conf.set("parquet.avro.schema", schema.toString());	
		conf.setOutputCommitter(AvroParquetOutputCommitter.class);
	
		conf.setOutputFormat(AvroParquetSupportOutputFormat.class);		
	
		// add AvroSerialization to io.serializations
		addAvroSerializations(conf);
		
	}
	

	private void addAvroSerializations(JobConf conf) {
		Collection<String> serializations = conf
				.getStringCollection("io.serializations");
		if (!serializations.contains(AvroSerialization.class.getName())) {
			serializations.add(AvroSerialization.class.getName());
			serializations.add(AvroSpecificRecordSerialization.class.getName());
		}

		conf.setStrings("io.serializations",
				serializations.toArray(new String[serializations.size()]));
	}

}
