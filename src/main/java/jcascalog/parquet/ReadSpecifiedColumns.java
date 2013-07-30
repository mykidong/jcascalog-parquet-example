package jcascalog.parquet;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import parquet.avro.AvroParquetInputFormat;
import parquet.avro.AvroParquetOutputFormat;


public class ReadSpecifiedColumns extends Configured implements Tool{	
	
	private static final String AVRO_INPUT_SCHEMA = "/META-INF/avro/electric-power-usage2.avsc";
	private static final String AVRO_OUTPUT_SCHEMA = "/META-INF/avro/sub-electric-power-usage.avsc";
	
	private Schema inputSchema;
	private Schema outSchema;	
	
	public static class ReadSpecifiedColumnsMapper extends Mapper<Void, GenericRecord, Void, GenericRecord> {
		
		private Schema outSchema;
		
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);	
			
			try {
				outSchema = new Schema.Parser().parse(getClass().getResourceAsStream(AVRO_OUTPUT_SCHEMA));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}		
		
		public void map(Void key, GenericRecord record, Context context) throws IOException, InterruptedException {			
			String addressCode = (String) record.get("addressCode");	
			
			GenericData.Array<GenericRecord> devicePowerEventArray = (GenericData.Array<GenericRecord>)record.get("devicePowerEventList");
			
			for(GenericRecord devicePowerEvent : devicePowerEventArray)
			{
				GenericRecord datum = new GenericData.Record(outSchema);
				datum.put("addressCode", addressCode);			
				datum.put("power", devicePowerEvent.get("power"));			
				
				context.write(null, datum);          	    			
			}
		}
	}
	

	public int run(String[] args) throws Exception {
		
		String codec = "snappy";
		
		if(args.length == 4)
		{
			codec = args[3];
		}	
	
		this.getConf().set("tmpjars", args[2]);
		
		this.getConf().set("parquet.compression", codec);		
		
		Job job = new Job(this.getConf());	
		
		job.setNumReduceTasks(0);		
	
		FileOutputFormat.setOutputPath(job, new Path(args[1]));		
		
		inputSchema = new Schema.Parser().parse(getClass().getResourceAsStream(AVRO_INPUT_SCHEMA));
		outSchema = new Schema.Parser().parse(getClass().getResourceAsStream(AVRO_OUTPUT_SCHEMA));	
			
		job.setMapperClass(ReadSpecifiedColumnsMapper.class);	
		
		job.setInputFormatClass(AvroParquetInputFormat.class);
	    AvroParquetInputFormat.setInputPaths(job, new Path(args[0]));
	    
	    AvroParquetInputFormat.setRequestedProjection(job, inputSchema);
	  
	    job.setOutputFormatClass(AvroParquetOutputFormat.class);
	    AvroParquetOutputFormat.setOutputPath(job, new Path(args[1]));
	    AvroParquetOutputFormat.setSchema(job, outSchema);
	
		setCompression(job.getConfiguration(), true);
		
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://hadoop01:9000");
		conf.set("mapred.job.tracker", "hadoop01:9001");
		
		ToolRunner.run(conf, new ReadSpecifiedColumns(), args);
	}
	
	
	public static void setCompression(Configuration conf, boolean compress) {			
		conf.setBoolean("mapred.output.compress", compress);
		conf.setBoolean("mapred.compress.map.output", compress);
		try {
			conf.setClass("mapred.output.compression.codec", Class.forName("com.hadoop.compression.lzo.LzopCodec"), CompressionCodec.class);
			conf.setClass("io.compression.codecs", Class.forName("com.hadoop.compression.lzo.LzopCodec"), CompressionCodec.class);
			conf.set("io.compression.codec.lzo.class", "com.hadoop.compression.lzo.LzopCodec");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}
}
