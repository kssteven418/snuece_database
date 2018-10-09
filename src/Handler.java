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

	
	Handler(QueryList s, QueryList f, QueryList w, QueryList ob, 
			ArrayList<Table> _tables, ExtSort _sorter) {
		
		// all inputs are references, copy references only
		selectList = s;
		fromList = f;
		whereList = w;
		orderbyList = ob;
		
		tables = _tables;
		sorter = _sorter;
		
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
	
	int handle() {
		int checkValue = check();
		if(checkValue<0) return -1; // parsing error
		
		//otherwise, handle OrderBy operation
		handleOrderby();
		return 0;
	}
	
	void handleOrderby() {
		//tentatively, test for the first table..
		sorter.init(tables.get(0).tupleLen, tables.get(0).strLen, 1, 0);
		sorter.run(tables.get(0), 0);
	}

}
