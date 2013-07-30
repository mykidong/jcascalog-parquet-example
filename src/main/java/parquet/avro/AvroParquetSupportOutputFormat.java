package parquet.avro;

import static parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE;
import static parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.metadata.CompressionCodecName;

public class AvroParquetSupportOutputFormat extends FileOutputFormat<AvroWrapper<GenericRecord>, Writable>{

	@Override
	public RecordWriter<AvroWrapper<GenericRecord>, Writable> getRecordWriter(FileSystem ignore, final JobConf job,
            final String name, Progressable prog) throws IOException {
		
		Path dir = FileOutputFormat.getTaskOutputPath(job, name);	  
	    String schemaString = job.get("parquet.avro.schema");	    
	  
	    String extension = ".parquet";
	   
	    String compressionCodecName = job.get(ParquetOutputFormat.COMPRESSION, UNCOMPRESSED.name()); 	    
	    CompressionCodecName codec = CompressionCodecName.fromConf(compressionCodecName);
	    
	    int blockSize = job.getInt(ParquetOutputFormat.BLOCK_SIZE, DEFAULT_BLOCK_SIZE);
	    
	    int pageSize = job.getInt(ParquetOutputFormat.PAGE_SIZE, DEFAULT_PAGE_SIZE);
	    
	    extension = codec.getExtension() + extension;	   
	    dir = new Path(dir.toString() + extension);	   
	
		final AvroParquetWriter writer = new AvroParquetWriter(dir, 
																Schema.parse(schemaString), 
																codec, 
																blockSize, 
																pageSize);	
		
	
		return new RecordWriter<AvroWrapper<GenericRecord>, Writable>() {

			@Override
			public void close(Reporter arg0) throws IOException {
				writer.close();				
			}

			@Override
			public void write(AvroWrapper<GenericRecord> wrapper, Writable arg1)
					throws IOException {
				writer.write(wrapper.datum());				
			}			
		};
	}	
}
