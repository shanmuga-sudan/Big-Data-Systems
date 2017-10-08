package org.hadoop.xmlDataset;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * Reads records that are delimited by a specific begin/end tag.
 */
public class XmlInputFormat extends FileInputFormat<LongWritable, Text> {
	public static final String START_TAG_KEY = "xmlinput.start";
	public static final String END_TAG_KEY = "xmlinput.end";

	@Override
	public RecordReader<LongWritable, Text> getRecordReader(
			InputSplit inputSplit, JobConf jobConf, Reporter reporter)
			throws IOException {

		reporter.setStatus(inputSplit.toString());
		return new XmlRecordReader(jobConf, (FileSplit) inputSplit);
	}

	public static class XmlRecordReader implements
			RecordReader<LongWritable, Text> {
		private byte[] startTag;
		private byte[] endTag;
		private long start;
		private long end;
		private FSDataInputStream fsin;
		private DataOutputBuffer buffer = new DataOutputBuffer();
		
		public XmlRecordReader(JobConf jobConf, FileSplit inputSplit)
				throws IOException {
			FileSplit fileSplit = (FileSplit) inputSplit;
			startTag = jobConf.get(START_TAG_KEY).getBytes("utf-8");
			endTag = jobConf.get(END_TAG_KEY).getBytes("utf-8");

			start = fileSplit.getStart();
			end = start + fileSplit.getLength();
			Path path = fileSplit.getPath();
			
			try {
				FileSystem fs = path.getFileSystem(jobConf);
				fsin = fs.open(path);
				fsin.seek(start);

			} catch (IOException ex) {
				System.out.println("IO exception in Input Formatter");
				Logger.getLogger(XmlInputFormat.class.getName()).log(Level.SEVERE,
						null, ex);
			}
		}

		@Override
		public LongWritable createKey() {
			return new LongWritable();
		}

		@Override
		public Text createValue() {
			return new Text();
		}

		@Override
		public long getPos() throws IOException {
			return fsin.getPos();
		}

		@Override
		public boolean next(LongWritable key, Text value) throws IOException {
			if (fsin.getPos() < end) {
				if (readUntilMatch(startTag, false)) {
					try {
						buffer.write(startTag);
						if (readUntilMatch(endTag, true)) {
							value.set(buffer.getData(), 0, buffer.getLength());
//							System.out.println("The value is : "+value.toString());
							key.set(fsin.getPos());
							return true;
						}
					} finally {
						buffer.reset();
					}
				}
			}
			return false;
		}

		@Override
		public float getProgress() throws IOException {
			return (fsin.getPos() - start) / (float) (end - start);
		}

		@Override
		public void close() throws IOException {
			fsin.close();
		}

		private boolean readUntilMatch(byte[] match, boolean withinBlock)
				throws IOException {
			int i = 0;
			while (true) {
				int b = fsin.read();
				// end of file:
				if (b == -1)
					return false;
				// save to buffer:
				if (withinBlock)
					buffer.write(b);

				// check if we're matching:
				if (b == match[i]) {
					i++;
					if (i >= match.length)
						return true;
				} else
					i = 0;

				// see if we've passed the stop point:
				if (!withinBlock && i == 0 && fsin.getPos() >= end)
					return false;
			}
		}
	}

}