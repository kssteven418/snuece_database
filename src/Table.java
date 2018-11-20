
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
	ArrayList<String> table;
	
	/* data */
	ArrayList<char[]> data; // list of data records(tuples)
	

	
	// initiation constructor : read data from the file in the given path
	Table(String path, String _tname){
		
		tname = _tname;
		
		names = new ArrayList<String>();
		types = new ArrayList<Integer>();
		data = new ArrayList<char[]>();
		table = new ArrayList<String>();
		
	
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
				types.add(temp[1].equals("string")?1:0);
				table.add(tname);
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
		//print();
		
	}
	

	// empty table constructor
	Table(String _tname, int _ncol, ArrayList<String> _names, ArrayList<Integer> _types, ArrayList<String> _table){
		tname = new String(_tname);
		nrow = 0;
		ncol = _ncol;
		tupleLen = strLen * ncol;
		names = new ArrayList<String>(_names);
		types = new ArrayList<Integer>(_types);
		table = new ArrayList<String>(_table);
		data = new ArrayList<char[]>();
		//print();
	}
	
	// join empty table constructor
	Table(String _tname, int _ncol, int _ncol2, 
			ArrayList<String> _names, ArrayList<Integer> _types, ArrayList<String> _table,
			ArrayList<String> _names2, ArrayList<Integer> _types2, ArrayList<String> _table2){
		tname = new String(_tname);
		nrow = 0;
		ncol = _ncol+_ncol2;
		tupleLen = strLen*ncol;
		names = new ArrayList<String>(_names);
		names.addAll(new ArrayList<String>(_names2));
		types = new ArrayList<Integer>(_types);
		types.addAll(new ArrayList<Integer>(_types2));
		table = new ArrayList<String>(_table);
		table.addAll(new ArrayList<String>(_table2));
		data = new ArrayList<char[]>();
	
	}
	
	
	
	//insert tuple into the data list
	void insert(char[] tup) {
		if(tup.length != tupleLen) {// length does not match
			System.out.println("Error : Input tuple length does not match");
			return;
		}
		char[] temp = new char[tup.length];
		System.arraycopy(tup, 0, temp, 0, tup.length);
		data.add(temp);
		nrow++;
	}
	
	
	void print() {
		
		System.out.println(tname+" : "+nrow+" by "+ncol);

		if(table!=null) {
			for(String n:table)
				System.out.print(n+'\t');
			System.out.println();
		}

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
