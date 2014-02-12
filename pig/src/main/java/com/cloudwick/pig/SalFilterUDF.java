package com.cloudwick.pig;

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

public class SalFilterUDF extends FilterFunc {

	@Override
	public Boolean exec(Tuple input) throws IOException {
		// TODO Auto-generated method stub
	 if(input == null || input.size()==0)
	 {
		 return false;
	 }
	Object obj=  input.get(0);
	 if (obj == null)
	 {
		 return false;
	 }
	 long sal = (Long) obj;
	 if (sal>500 && sal<1000)
	 {
		 return true;
	 }
	 else
	 {
		 return false;
	 }
	}
}
