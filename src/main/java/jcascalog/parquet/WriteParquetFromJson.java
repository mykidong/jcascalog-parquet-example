package jcascalog.parquet;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import parquet.avro.AvroParquetOutputFormat;

public class WriteParquetFromJson extends Configured implements Tool{	
	
	private static final String AVRO_OUTPUT_SCHEMA = "/META-INF/avro/electric-power-usage.avsc";
	
	private Schema outSchema;	
	
	public static class WriteParquetFromJsonMapper extends Mapper<Object, Text, Void, GenericRecord> {
		
		private Schema outSchema;
		
		private ObjectMapper mapper = new ObjectMapper();
		
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);	
			
			try {
				outSchema = new Schema.Parser().parse(getClass().getResourceAsStream(AVRO_OUTPUT_SCHEMA));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}		
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {	
			
			ElectricPowerUsage electricPowerUsage = mapper.readValue(value.toString(),
					new TypeReference<ElectricPowerUsage>() {
					});
			
			GenericRecord datum = new GenericData.Record(outSchema);
        	
        	datum.put("addressCode", electricPowerUsage.getAddressCode());	
        	datum.put("timestamp", electricPowerUsage.getTimestamp());	
        	
        	List<DevicePowerEvent> devicePowerEventList = electricPowerUsage.getDevicePowerEventList();
        	
    		GenericData.Array<GenericRecord> devicePowerEventArray =  new GenericData.Array<GenericRecord>(devicePowerEventList.size(), outSchema.getField("devicePowerEventList").schema());
    		
    		for(DevicePowerEvent devicePowerEvent : electricPowerUsage.getDevicePowerEventList())
    		{
    			GenericRecord devicePowerEventRecord = new GenericData.Record(devicePowerEventArray.getSchema().getElementType());
    			devicePowerEventRecord.put("power", devicePowerEvent.getPower());
    			devicePowerEventRecord.put("deviceType", devicePowerEvent.getDeviceType());
    			devicePowerEventRecord.put("deviceId", devicePowerEvent.getDeviceId());
    			devicePowerEventRecord.put("status", devicePowerEvent.getStatus());
    			
    			devicePowerEventArray.add(devicePowerEventRecord);			
    		}
    		
    		datum.put("devicePowerEventList", devicePowerEventArray);		            
    		
    		context.write(null, datum); 
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
				
		outSchema = new Schema.Parser().parse(getClass().getResourceAsStream(AVRO_OUTPUT_SCHEMA));	
			
		job.setMapperClass(WriteParquetFromJsonMapper.class);	
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));		
		job.setInputFormatClass(TextInputFormat.class);			
	
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
		
		ToolRunner.run(conf, new WriteParquetFromJson(), args);
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
