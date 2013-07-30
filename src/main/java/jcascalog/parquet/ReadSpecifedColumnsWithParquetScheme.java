package jcascalog.parquet;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import jcascalog.Api;
import jcascalog.Subquery;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cascading.avro.ParquetScheme;
import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;

public class ReadSpecifedColumnsWithParquetScheme extends Configured
	implements Tool{
	
	private static final String AVRO_INPUT_SCHEMA = "/META-INF/avro/electric-power-usage2.avsc";
	private static final String AVRO_OUTPUT_SCHEMA = "/META-INF/avro/sub-electric-power-usage.avsc";

	public int run(String[] args) throws Exception {
		
		String codec = "snappy";
		
		if(args.length == 4)
		{
			codec = args[3];
		}
	
		Configuration hadoopConf = this.getConf();
		hadoopConf.set("tmpjars", args[2]);		
		
		hadoopConf.set("parquet.compression", codec);		

		setCompression(hadoopConf, true);

		Map<String, String> confMap = new HashMap<String, String>();
		Iterator<Entry<String, String>> iter = hadoopConf.iterator();
		while (iter.hasNext()) {
			Entry<String, String> entry = iter.next();
			confMap.put(entry.getKey(), entry.getValue());
		}

		Api.setApplicationConf(confMap);

		String inputPath = args[0];
		String outputPath = args[1];

		final Schema inputSchema = new Schema.Parser().parse(getClass()
				.getResourceAsStream(AVRO_INPUT_SCHEMA));
		
		final Schema outputSchema = new Schema.Parser().parse(getClass()
				.getResourceAsStream(AVRO_OUTPUT_SCHEMA));
		

		ParquetScheme inputScheme = new ParquetScheme(inputSchema);
		inputScheme.setSourceFields(new Fields("?address-code", "?event-list"));
		
		ParquetScheme outputScheme = new ParquetScheme(outputSchema);
		outputScheme.setSinkFields(new Fields("?address-code", "?power"));
		
		Tap inTap = new Hfs(inputScheme, inputPath);
		Tap outTap = new Hfs(outputScheme, outputPath);	
		
		Subquery query = new Subquery("?address-code", "?power")
				 .predicate(inTap, "?address-code", "?event-list")
				 .predicate(new SplitAndFlatten(), "?event-list").out("?power");
		
		Api.execute(outTap, query);
		
		return 0;
	}
	
	public static class SplitAndFlatten extends CascalogFunction {
		@Override
		public void operate(FlowProcess flow_process, FunctionCall fnCall) {
			
			List<Tuple> tupleList = (List<Tuple>)fnCall.getArguments().getObject(0);
		
			for(Tuple tuple : tupleList) {

				double power = tuple.getDouble(0);
				
				fnCall.getOutputCollector().add(new Tuple(power));					
			}			
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://hadoop01:9000");
		conf.set("mapred.job.tracker", "hadoop01:9001");
		
		ToolRunner.run(conf, new ReadSpecifedColumnsWithParquetScheme(), args);
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
