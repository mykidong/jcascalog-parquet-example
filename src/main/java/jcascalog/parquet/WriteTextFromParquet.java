package jcascalog.parquet;

import java.util.HashMap;
import java.util.Iterator;
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
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

public class WriteTextFromParquet extends Configured
	implements Tool{
	
	private static final String AVRO_INPUT_SCHEMA = "/META-INF/avro/sub-electric-power-usage.avsc";

	public int run(String[] args) throws Exception {
		
		String codec = "snappy";
		
		if(args.length == 4)
		{
			codec = args[3];
		}
	
		Configuration hadoopConf = this.getConf();
		hadoopConf.set("tmpjars", args[2]);		
		
		hadoopConf.set("parquet.compression", codec);		

		setCompression(hadoopConf, false);

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
		
	
		ParquetScheme inputScheme = new ParquetScheme(inputSchema);
		inputScheme.setSourceFields(new Fields("?address-code", "?power"));
		
		
	
		Tap inTap = new Hfs(inputScheme, inputPath);
		Tap outTap = new Hfs(new TextDelimited(false, "\t"), outputPath);	
		
		Subquery query = new Subquery("?address-code", "?power")
				 .predicate(inTap, "?address-code", "?power");			
		
		Api.execute(outTap, query);
		
		return 0;
	}
	
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://hadoop01:9000");
		conf.set("mapred.job.tracker", "hadoop01:9001");
		
		ToolRunner.run(conf, new WriteTextFromParquet(), args);
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
