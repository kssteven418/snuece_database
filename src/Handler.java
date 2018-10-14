import java.util.ArrayList;
import java.util.List;

public class Handler {
	
	/* query lists */
	QueryList selectList;
	QueryList fromList;
	QueryList whereList;
	QueryList orderbyList;
	
	/* tables to handle */
	ArrayList<Table> tables;
	
	/* External Sorter */
	ExtSort sorter;

	// print mode for debugging
	boolean printMode;
	
	Handler(QueryList s, QueryList f, QueryList w, QueryList ob, 
			ArrayList<Table> _tables, ExtSort _sorter, boolean pm) {
		
		// all inputs are references, copy references only
		selectList = s;
		fromList = f;
		whereList = w;
		orderbyList = ob;
		
		tables = _tables;
		sorter = _sorter;
		
		printMode = pm;
		
	}
	
	
	/* For debugging */
	void print(){
      
	  if(selectList == null) {
		  System.out.print("SELECT Query List : ");
	      System.out.println("*");
	  }
	  else {
	    selectList.print();
	  }
	  
	  fromList.print();
	  
	  if(whereList!=null)
		  whereList.print();
	  
	  if(orderbyList!=null)
		  orderbyList.print();
	  
	  System.out.println();
	}
	
	
	/*********************** MAIN HANDLER *************************/
	
	int handle() {
		//1st, check the syntax
		int checkValue = check();
		if(checkValue<0) return -1; // parsing error
		
		//2nd, check the from statement
		//for project 2, only one table is possible in the from statement
		if(fromList.list.size()!=1) {
			System.out.println("Error : For project 2, one table is acceptable for the From statement input");
			return -1;
		}
		if(!handleFrom()) return -1;
		
		// the only table stated in the from statement
		Table temp = tables.get(findTable(fromList.get(0).ta.table));
		
		//3rd, handle OrderBy statement
		//by sorting the table
		if(orderbyList!=null) { 
			// if order by statement is null, then simply use the table stated in the from statement
			temp = handleOrderby();
			if(temp==null) return -1;
		}
		
		//4th, handle Where statement
		if(whereList!=null) {
			temp = handleWhere(temp);
			if(temp==null) return -1;
		}
		
		//finally, handle Select statement
		temp = handleSelect(temp);
		if(temp==null) return -1;
		
		//print the final table
		if(printMode) temp.print();
		
		return 0;
	}
	
	
	/********************** SUB HANDLERS **********************/
	
	//check correctness of the query
	//tables in select and where commands should be declared in from command
	int check() {
		List<String> fromTables = new ArrayList<String>();

		for (Query q : fromList.list) {
			fromTables.add(q.ta.table);
		}
		
		if(selectList != null) {
			for (Query q : selectList.list) {
				if(!fromTables.contains(q.ta.table)) {
					System.out.println("ERROR : Table name "+q.ta.table+" is invalid!");
					return -1;
				}
			}
		}

		if(whereList != null) {
			for (Query q : whereList.list) {
				if(!fromTables.contains(q.ta.table)) {
					System.out.println("ERROR : Table name "+q.ta.table+" is invalid!");
					return -1;
				}
				if(q.op_type == 4 && !fromTables.contains(q.ta2.table)) {
					System.out.println("ERROR : Table name "+q.ta2.table+" is invalid!");
					return -1;
				}
					
			}
		}
		
		if(orderbyList != null) {
			for (Query q : orderbyList.list) {
				if(!fromTables.contains(q.ta.table)) {
					System.out.println("ERROR : Table name "+q.ta.table+" is invalid!");
					return -1;
				}
			}
		}
		
		return 0;
	}
	
	
	boolean handleFrom() {
		for(Query q : fromList.list) {
			String table = q.ta.table;
			int table_index = findTable(table);
			if(table_index<0) {
				System.out.println("Error : Invalid From Statement (Not a valid table name)");
				return false;
			}
		}
		return true;
	}
	
	Table handleOrderby() {
		
		
		//find the appropriate column
		Query q = orderbyList.get(0);
		String table = q.ta.table;
		String attr = q.ta.attr;
		int table_index = findTable(table);
		if(table_index<0) {
			System.out.println("Error : Invalid Order By Statement, Not a valid table name");
			return null;
		}
		
		int attr_index = findAttr(table_index, attr);
		if(attr_index<0) {
			System.out.println("Error : Invalid Order By Statement, Not a valid attribute name");
			return null;
		}
		
		int attr_type = tables.get(table_index).types.get(attr_index);
		
		Table output = null;
		output = sorter.run(tables.get(table_index), attr_index, attr_type, false);
		
		//sorter.runTest(tables.get(0));
		//sorter.run(tables.get(1), 1, 1, true);
		//sorter.run(tables.get(1), 2, 0, true);
		return output;
	}
	
	Table handleWhere(Table input) {
		Table temp = input;
		Table output = null;
		
		for(Query q: whereList.list) { // assume one from statement input
			output = new Table(input.tname, input.ncol, input.names, input.types);
			
			String attr_name = q.ta.attr;
			int attr = findAttr(temp, attr_name); // index of attr
			if(attr<0) { // no such attribute
				System.out.println("Error : Invalid Where Statement, no attribute");
				return null;
			}
			int type = temp.types.get(attr);
			
			/* Compare with value i.e. R.sid < 30 */
			if(q.op_type==3) { 
				String value = q.value;
				/* integer type */
				if(type==0) { 
					try {
						int intVal = Integer.parseInt(value);
						//scan the table
						for(char[] x:temp.data) {
							int x_val = Integer.parseInt(CharStr.getString(x, temp.strLen*attr));
							if(compare(x_val, q.operation, intVal)) {
								// insert the tuple, if meets the condition
								output.insert(x);
							}
						}
					// parse error if type does not match
					} catch(Exception e) {
						System.out.println("Error : Invalid Where Statement, type mismatch");
						return null;
					}
				}
				/* string type */
				else { 
					//type does not match if the value does not start with ' or "
					char startChar = value.charAt(0);
					if (startChar != '\"' && startChar != '\'') {
						System.out.println("Error : Invalid Where Statement, type mismatch");
						return null;
					}
					// scan the table
					String strVal = value.substring(1, value.length() - 1);
					for(char[] x:temp.data) {
						String x_val = CharStr.getString(x, temp.strLen*attr);
						if(compare(x_val, q.operation, strVal)) {
							// insert the tuple, if meets the condition
							output.insert(x);
						}
					}
				}
			}
			
			/* compare with another value i.e. R.x < R.y */
			else if(q.op_type==4) { 
				String attr_name2 = q.ta2.attr;
				int attr2 = findAttr(temp, attr_name2); // index of attr2 
				if(attr2<0) { // no such attribute
					System.out.println("Error : Invalid Where Statement, no attribute");
					return null;
				}
				int type2 = temp.types.get(attr2);
				/* integer type */
				if(type==0) { 
					if(type2==1) {
						System.out.println("Error : Invalid Where Statement, type mismatch");
						return null; // type mismatch
					}
					//scan the table
					try {
						for(char[] x:temp.data) {
							int x_val1 = Integer.parseInt(CharStr.getString(x, temp.strLen*attr));
							int x_val2 = Integer.parseInt(CharStr.getString(x, temp.strLen*attr2));
							if(compare(x_val1, q.operation, x_val2)) {
								// insert the tuple, if meets the condition
								output.insert(x);
							}
						}
					} catch(Exception e) {
						System.out.println("Error : Type casting error");
						return null;
					}
				}
				/* string type */
				else { 
					if(type2==0) {
						System.out.println("Error : Invalid Where Statement, type mismatch");
						return null; // type mismatch
					}
					//scan the table
					for(char[] x:temp.data) {
						String x_val1 = CharStr.getString(x, temp.strLen*attr);
						String x_val2 = CharStr.getString(x, temp.strLen*attr2);
						if(compare(x_val1, q.operation, x_val2)) {
							// insert the tuple, if meets the condition
							output.insert(x);
						}
					}
				}
			}
			
			//use the output as the input for the next where statement
			//output.print();
			temp = output;
		}
		
		return output;
	}
	
	Table handleSelect(Table input) {
		
		if(selectList==null) {
			//input as *
			return input;
		}
		
		ArrayList<String> names = new ArrayList<String>();
		ArrayList<Integer> names_ind = new ArrayList<Integer>();
		ArrayList<Integer> types = new ArrayList<Integer>();
		int strLen = input.strLen;
		
		for(Query q: selectList.list) {
			String attr_name = q.ta.attr;
			int attr = findAttr(input, attr_name); // index
			if(attr<0) { // no such attribute
				System.out.println("Error : Invalid Select Statement, no attribute");
				return null;
			}
			int type = input.types.get(attr);
			names.add(attr_name);
			names_ind.add(attr);
			types.add(type);
			
		}
		Table output = new Table(input.tname, names.size(), names, types);
		
		char[] temp = new char[names.size()*strLen];
		
		for(char[] x: input.data) {
			int i=0;
			for(int ind: names_ind) {
				System.arraycopy(x, strLen*ind, temp, strLen*i, strLen);
				i++;
			}
			output.insert(temp);
		}
		
		return output;
	}
	
	
	
	/************************* HELPER FUNCTIONS ***************************/
	
	// Helper function
	// compare two values
	boolean compare(int left, char op, int right) {
		switch (op) {
		case '<':
			if (left < right)
				return true;
			return false;
		case '>':
			if (left > right)
				return true;
			return false;
		case '=':
			if (left == right)
				return true;
			return false;
		}
		return false;
	}

	boolean compare(String left, char op, String right) {
		switch (op) {
		case '<':
			if (left.compareTo(right) < 0)
				return true;
			return false;
		case '>':
			if (left.compareTo(right) > 0)
				return true;
			return false;
		case '=':
			if (left.compareTo(right) == 0)
				return true;
			return false;
		}
		return false;
	}

	
	// find table index from the table list
	int findTable(String name) {
		for(int i=0; i<tables.size(); i++) {
			if (tables.get(i).tname.equals(name)) return i;
		}
		return -1;
	}
	
	// find attr index from the table(index)
	int findAttr(int index, String name) {
		Table t = tables.get(index);
		for(int i=0; i<t.names.size(); i++) {
			if(t.names.get(i).equals(name)) return i;
		}
		return -1;
	}
	
	int findAttr(Table t, String name) {
		for(int i=0; i<t.names.size(); i++) {
			if(t.names.get(i).equals(name)) return i;
		}
		return -1;
	}

}
