package com.cloudwick.pig;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;

public class LoadUDF extends LoadFunc{

	private RecordReader reader;
	private String[] range;
	public LoadUDF(String arg)
	{
		range= arg.split(",");
	}
	
	public void setLocation(String location, Job job) throws IOException {
		// TODO Auto-generated method stub
		
		FileInputFormat.setInputPaths(job,location);
		}

	@Override
	public InputFormat getInputFormat() throws IOException {
		// TODO Auto-generated method stub
		return new TextInputFormat();
	}

	@Override
	public void prepareToRead(RecordReader reader, PigSplit split)
			throws IOException {
		// TODO Auto-generated method stub
		this.reader= reader;
	}

	@Override
	public Tuple getNext() throws IOException {
		// TODO Auto-generated method stub
        try {
			Text value= (Text) reader.getCurrentValue();
		String line= value.toString();
		
        
        
        } catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
		
		return null;
	}

}
