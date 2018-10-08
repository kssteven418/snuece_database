
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Table {
	
	/* stats */
	String tname;
	int nrow;
	int ncol;
	int strLen=21; //string length is 20, 21 including \0
	int tupleLen; // ncol*strLen
	
	/* data info */
	ArrayList<String> names;
	ArrayList<Integer> types; // 0 for int, 1 for string
	
	/* data */
	ArrayList<char[]> data; // list of data records(tuples)
	
	
	
	// initiation constructor : read data from the file in the given path
	Table(String path, String name){
		
		tname = name;
		
		names = new ArrayList<String>();
		types = new ArrayList<Integer>();
		data = new ArrayList<char[]>();
		
	
		try {
		    BufferedReader in = new BufferedReader(new FileReader(path));
	     	String s;

			// read name  type from the first line
			s = in.readLine();
			
			String[] parse = s.split(" ");
			ncol = parse.length; // number of attributes
			tupleLen = ncol * strLen; // tuple length
			
			/* initiate data info : lists of names and types */
			for (int i=0; i<parse.length; i++) {
				String[] temp = parse[i].split("\\(|\\)");
				names.add(temp[0]);
				types.add(temp.equals("string")?1:0);
			}
			
			/* then read and insert data tuples to data list */
			while ((s = in.readLine()) != null) {
				char[] tuple = new char[tupleLen]; 
	        	String[] temp = s.split(" ");
	        	for (int i=0; i<ncol; i++) {
	        		CharStr.copyString(tuple, i*strLen, temp[i], strLen-1/*20*/);
	        	}
	        	data.add(tuple);
	        	nrow++;
	      	}
			
		}
		catch(IOException e) {
			System.err.println(e);
		}
		
		//debugging
		print();
		
	}
	
	void print() {
		
		System.out.println(tname+" "+ncol+" "+nrow);
		
		if(names!=null) {
			for(String n:names)
				System.out.print(n+'\t');
			System.out.println();
		}
		
		if(types!=null) {
			for(int t:types)
				System.out.print((t==0?"integer":"string")+'\t');
			System.out.println();
		}
		
		if(data!=null) {
			for(char[] tuple:data) {
				for(int i=0; i<ncol; i++)
					System.out.print(CharStr.getString(tuple, i*strLen)+"\t");
				System.out.println();
			}
		}
		System.out.println();
		
	}
	
}
