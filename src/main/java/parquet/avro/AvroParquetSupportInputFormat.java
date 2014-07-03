package parquet.avro;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import parquet.filter.UnboundRecordFilter;
import parquet.hadoop.BadConfigurationException;
import parquet.hadoop.ParquetInputFormat;

public class AvroParquetSupportInputFormat extends
		FileInputFormat<Void, GenericRecord> {

	@Override
	protected boolean isSplitable(FileSystem fs, Path filename) {
		return false;
	}

	@Override
	protected FileStatus[] listStatus(JobConf job) throws IOException {
		List<FileStatus> result = new ArrayList<FileStatus>();		
		for (FileStatus file : super.listStatus(job))
			if (file.getPath().getName().endsWith("parquet"))
				result.add(file);
		return result.toArray(new FileStatus[0]);
	}
	
	private static Class<?> getClassFromConfig(JobConf jobConf, String configName, Class<?> assignableFrom) {
	    final String className = jobConf.get(configName);
	    if (className == null) {
	      return null;
	    }
	    try {
	      final Class<?> foundClass = Class.forName(className);
	      if (!assignableFrom.isAssignableFrom(foundClass)) {
	        throw new BadConfigurationException("class " + className + " set in job conf at "
	                + configName + " is not a subclass of " + assignableFrom.getCanonicalName());
	      }
	      return foundClass;
	    } catch (ClassNotFoundException e) {
	      throw new BadConfigurationException("could not instantiate class " + className + " set in job conf at " + configName, e);
	    }
	  }

	@Override
	public RecordReader<Void, GenericRecord> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException {
		
		final FileSplit file = (FileSplit)split;
	    reporter.setStatus(file.toString());
	    
	    String schemaStr = job.get(AvroReadSupport.AVRO_REQUESTED_PROJECTION);
	    final Schema schema = Schema.parse(schemaStr);	 	    
	    
	    UnboundRecordFilter filter = null;
	    try {
	    	Class<?> unboundRecordFilterClass =  
		    		getClassFromConfig(job, ParquetInputFormat.UNBOUND_RECORD_FILTER, UnboundRecordFilter.class);
	    	if(unboundRecordFilterClass != null)
	    	{
	    		filter = (UnboundRecordFilter)unboundRecordFilterClass.newInstance();
	    	}
		} catch (InstantiationException e) {		
			e.printStackTrace();
		} catch (IllegalAccessException e) {		
			e.printStackTrace();
		}
	    
	  
		final AvroParquetReader reader = (filter == null) ? new AvroParquetReader(job, file.getPath())
															: new AvroParquetReader(job, file.getPath(), filter);
		
		return new RecordReader<Void, GenericRecord>() {

			@Override
			public void close() throws IOException {
				reader.close();
			}

			@Override
			public Void createKey() {				
				return null;
			}

			@Override
			public GenericRecord createValue() {			
				return new GenericRecord(){
					
					private GenericRecord currentRecord;

					@Override
					public void put(int i, Object v) {	
						throw new UnsupportedOperationException();
					}

					@Override
					public Object get(int i) {						
						throw new UnsupportedOperationException();
					}

					@Override
					public Schema getSchema() {
						return schema;
					}

					@Override
					public void put(String key, Object v) {
						this.currentRecord = (GenericRecord) v;						
					}

					@Override
					public Object get(String key) {						
						return this.currentRecord.get(key);
					}					
				};
			}

			@Override
			public long getPos() throws IOException {			
				return 0;
			}

			@Override
			public float getProgress() throws IOException {			
				return 0;
			}

			@Override
			public boolean next(Void key, GenericRecord value)
					throws IOException {
				Object tmpValue = reader.read();
				
				if(tmpValue == null)
					return false;
				
				GenericRecord genericRecord = (GenericRecord)tmpValue;
				
				value.put(null, tmpValue);
				
				return true;
			}

		};
	}

}
