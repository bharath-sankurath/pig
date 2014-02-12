package com.cloudwick.pig;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class EmpEvalUDF extends EvalFunc<Integer> {

	@Override
	public Integer exec(Tuple input) throws IOException {
		// TODO Auto-generated method stub
		if (input == null || input.size() == 0) {
			return 0;
		}
//		bag obj = input.get(0);

		 String obj = (String) input.get(0);
	 long sal = (Long) input.get(1);
		String[] dt = obj.split("-");

		if (dt[2].equals("1980")) {
			return (int) (sal+1000);
		} else if (dt[2].equals("1981")) {
			return (int) (sal+2000);
		}

		else if (dt[2].equals("1982")) {
			return (int) (sal+3000);
		} else if (dt[2].equals("1983")) {
			return (int) (sal+4000);
		}
		return 0;

	}

}
