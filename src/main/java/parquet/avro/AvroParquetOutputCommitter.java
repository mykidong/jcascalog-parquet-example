package parquet.avro;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobContext;

import parquet.hadoop.Footer;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetFileWriter;

public class AvroParquetOutputCommitter extends FileOutputCommitter {

	@Override
	public void commitJob(JobContext context) throws IOException {
		super.commitJob(context);

		Path outputPath = FileOutputFormat.getOutputPath(context.getJobConf());
		try {
			Configuration configuration = context.getConfiguration();
			final FileSystem fileSystem = outputPath
					.getFileSystem(configuration);
			FileStatus outputStatus = fileSystem.getFileStatus(outputPath);
			List<Footer> footers = ParquetFileReader.readAllFootersInParallel(
					configuration, outputStatus);
			try {
				ParquetFileWriter.writeMetadataFile(configuration, outputPath,
						footers);
			} catch (Exception e) {

				System.err.println("could not write summary file for "
						+ outputPath + ", " + e.getMessage());

				final Path metadataPath = new Path(outputPath,
						ParquetFileWriter.PARQUET_METADATA_FILE);
				if (fileSystem.exists(metadataPath)) {
					fileSystem.delete(metadataPath, true);
				}
			}
		} catch (Exception e) {
			System.err.println("could not write summary file for " + outputPath
					+ ", " + e.getMessage());
		}

	}
}
