import java.util.ArrayList;

public class Join {
	int jmode = 2; // join mode, 0 for bnj, 1 for smj, 2 for hj
	
	int bsize; // B : number of buffers
	int rnum; // number of records per page
	boolean timing;
	
	int strLen;
	int tupleLenIn;
	int tupleLenOut;
	
	int colIn;
	int typeIn;
	int colIndexIn;
	
	int colOut;
	int typeOut;
	int colIndexOut;
	
	char op;
	
	// buffers
	char[][] inbuf_out; // input buffer for outer table
	char[][] inbuf_in; // input buffer for inner table
	char[][] outbuf; // output buffer
	
	int inbuf_out_max;
	int inbuf_in_max;
	
	Table in; // inner table
	Table out; // outer table
	
	//position in out table
	int out_pos = 0;
	//position in in table	
	int in_pos = 0;
	
	//position in outbuf
	int outbuf_pos = 0;
	
	Join(){
		bsize = 4;
		rnum = 7;		
		timing = false;
	}
	
	//for debugging
	void print_buffer(char[][] arr) {
		for(int i=0; i<arr.length; i++) {
			for(int j=0; j<arr[i].length; j++) {
				System.out.print(arr[i][j]);
			}
			System.out.println();
		}
	}
	
	/* Setters */
	void setBsize(int _bsize){
		if(_bsize<3) {
			System.out.println("Error : Bsize should be at least 3");
			return;
		}
		bsize = _bsize;
	}
	
	void setRnum(int _rnum) {
		if(_rnum <1) {
			System.out.println("Error : Rnum should be at least 1");
			return;
		}
		rnum = _rnum;
	}
	
	void setTiming(boolean _timing) {
		timing = _timing;
	}
	
	
	void init(int tlout, int tlin, int sl, int colin, int typein, int colout, int typeout, char _op) {
		/* initiate the buffer size */
		/* # buffer = bsize, records per buffer = rnum, record length = tupleLen */
		tupleLenIn = tlin;
		tupleLenOut = tlout;
		
		strLen = sl;
		
		colIn = colin;
		typeIn = typein;
		colIndexIn = colIn*strLen;
		
		colOut = colout;
		typeOut = typeout;
		colIndexOut = colOut*strLen;

		op = _op;

		
		/* for convenience, let's assume that the absolute size of buffers can be different,
		 * depending on the tuple size they contain */
		
		// in buffer for the outer
		inbuf_out = new char[rnum][tupleLenOut];
		// in buffer for the inner
		inbuf_in= new char[rnum*(bsize-2)][tupleLenIn];
		// out buffer
		outbuf = new char[rnum][tupleLenIn+tupleLenOut];
		
		
		out_pos = 0;
		in_pos = 0;
		outbuf_pos = 0;
		
	}
	
	
	// inner.attr=outer.attr
	Table run(Table inner, Table outer, int colin, int typein, int colout, int typeout, char op) {
		in = inner;
		out = outer;
		
		init(outer.tupleLen, inner.tupleLen, inner.strLen, colin, typein, colout, typeout, op);
		// check for join-ability
		// must have same types
		if(typeIn!=typeOut) {
			System.out.println("Error : Joining columns have different types");
			return null;
		}
		// for sort merge join and hash join, operation must be '='
		if(op!='=' && jmode!=0) {
			System.out.println("Error : Sort merge join and hash join only support equality operation");
			return null;
		}
		

		
		Table output = new Table(outer.tname+inner.tname, outer.ncol, inner.ncol,
				outer.names, outer.types, outer.table,
				inner.names, inner.types, inner.table);

		
		if(jmode==0) {
			output = blockNestedJoin(output);
		}
		else if (jmode==1) {
			output = sortMergeJoin(output);
		}
		else if (jmode==2) {
			output = hashJoin(output);
		}
		else {
			System.out.println("Error : Invalid join mode");
			output = null;
		}
		
		return output;
	}
	
	Table blockNestedJoin(Table output) {

		// scan outer table
		out_pos = 0;
		while(true) {
			if(out_pos==out.data.size()) break; // scanned all outer table
			inbuf_out_max = fill(0);
			
			// scan total inner table
			in_pos = 0;
			while(true) {
				if(in_pos==in.data.size()) break;
				inbuf_in_max = fill(1);
				
				// join by scanning tuple by tuple
				for (int i=0; i<inbuf_out_max; i++) {
					for(int j=0; j<inbuf_in_max; j++) {
						
						/* TODO : join condition check !! */
						if(checkJoinCond(inbuf_in[j], inbuf_out[i], op)) {
							outputTuple(inbuf_out[i], inbuf_in[j], output);
						}
					}
				}
				
			}
		}
		flush(outbuf_pos, output); // flush remaining
		
		//output.print();
		return output;
	}
	
	
	Table sortMergeJoin(Table output) {
		
		// initialize external sorting module
		ExtSort sort = new ExtSort();
		sort.setBsize(bsize);
		sort.setRnum(rnum);
		sort.setTiming(false);
		sort.setWrite(false);
		
		// sort by external sorting module
		in = sort.run(in, colIn, typeIn, false);
		out = sort.run(out, colOut, typeOut, false);
		in.print();
		out.print();
		
		// pointers pointing table addr
		int out_start = 0;
		int in_start = 0;
		int in_finish = 0;
		
		//pointers pointing buffer addr
		int i = 0;
		int o = 0;
		
		out_pos = 0;
		in_pos = 0;
		
		inbuf_out_max = fill(0);
		inbuf_in_max = fill(1);
		
		while(true) {
						
			// searching phase
			if(out_start==out.data.size()) break;
			if(in_start==in.data.size()) break;
			
			// if out of inbuf_in boundary
			if(i==inbuf_in_max) {
				i = 0;
				inbuf_in_max = fill(1);
			}
			
			// if out of inbuf_out boundary
			if(o==inbuf_out_max) {
				o = 0;
				inbuf_out_max = fill(0);
			}
			
			boolean cond = checkJoinCond(inbuf_in[i], inbuf_out[o], '=');
			
			// A. if the keys of the inner table and the outer table match 
			if(cond) {
				
				outputTuple(inbuf_out[o], inbuf_in[i], output);	
				
				/******** 1. proceed in_finish pointer *********/
				
				// initialize in_finish pointer
				in_finish = in_start+1;
				i++;
				
				while(true) {
					if(in_finish==in.data.size()) break; // out of bound
					
					// if not on the memory
					if(i==inbuf_in_max) {
						i = 0;
						inbuf_in_max = fill(1);
					}
					
					// check if matching
					// if matching, proceed in_finish pointer and store the joined tuple
					if(checkJoinCond(inbuf_in[i], inbuf_out[o], '=')) {
						
						// output the joined tuple
						outputTuple(inbuf_out[o], inbuf_in[i], output);	

						// proceed
						in_finish++;
						i++;
						
					}
					// if not matching, break the loop
					else {
						break;
					}
				}
				
				//System.out.println(out_start+" "+in_start+" "+in_finish);
				
				
				/********* 2. Join! **********/
				//scan outer table and join with inner table entries from in_start to in_finish 
				while(true) {
					
					out_start++;
					o++;
					
					if(out_start==out.data.size()) break; // index out of bound
					
					// if out of inbuf_out boundary
					if(o==inbuf_out_max) {
						o = 0;
						inbuf_out_max = fill(0);
					}
					
					// initialization
					in_pos = in_start;
					i=0;
					inbuf_in_max = fill(1);
					
					// if the keys still match, then must scan inner table
					// from in_start to in_finish
					if(checkJoinCond(inbuf_in[i], inbuf_out[o], '=')) {
						
						//scan from in_start to in_finish
						for(int pos=in_start; pos<in_finish; pos++) {
							// if out of outbuf boundary
							if(i==inbuf_in_max) {
								i = 0;
								inbuf_in_max = fill(1);
							}
							outputTuple(inbuf_out[o], inbuf_in[i], output);	
							i++;
						}
					}
					// if mismatch, then stop scanning outer table
					else {
						break;
					}
				}
				
				// restart from the in_finish
				in_start = in_finish;
				in_pos = in_finish;
				i = 0;
				inbuf_in_max = fill(1);
				
			}

			// B. if the keys of the inner table and the outer table mismatch 
			// then, move in_start or out_start, according to key comparison result
			else {
				// in_key < out_key
				// proceed in pointer
				if(checkJoinCond(inbuf_in[i], inbuf_out[o], '<')) {
					in_start++;
					i++;
				}
				// in_key > out_key
				// proceed out pointer
				else {
					out_start++;
					o++;
				}
			}
		}
		
		flush(outbuf_pos, output); // flush remaining
		
		return output;
	}
	
	Table hashJoin(Table output) {
		
		HashJoin hashJoin = new HashJoin(this);
		// partitioning phase
		
		return null;
	}
	
	void partitioningPhase() {
		
	}
	
	
	/*************************HELPER FUNCTIONS************************/
	
	// fill inbuf from the inner or outer table 
	int fill(int mode) { // 0 if out->inbuf_out, 1 if in->inbuf_in
		int pos = 0;

		while(true) {
			if(mode==0) {
				if(out_pos==out.data.size()) {
					break;
				}
				if(pos==inbuf_out.length) {
					break;
				}
				System.arraycopy(out.data.get(out_pos), 0, inbuf_out[pos], 0, out.tupleLen);
				pos++;
				out_pos++;
			}
			else {
				if(in_pos==in.data.size()) {
					break;
				}
				if(pos==inbuf_in.length) {
					break;
				}
				System.arraycopy(in.data.get(in_pos), 0, inbuf_in[pos], 0, in.tupleLen);
				pos++;
				in_pos++;
			}
		}
		
		return pos;
	}
	
	// output tuple to the output buffer
	// if the buffer is full, then flush
	void outputTuple(char[] tup_out, char[] tup_in, Table output) {
		char[] temp = joinArray(tup_out, tup_in);
		outbuf[outbuf_pos] = temp;
		outbuf_pos ++;
		//if outbuf full, flush
		if(outbuf_pos==outbuf.length) {
			flush(outbuf_pos, output);
			outbuf_pos = 0; // reset
		}
	}
	
	// flush the outbuf into the output table(disk)
	void flush(int max, Table output) {
		int pos = 0;
		while(true) {
			if(pos==max) break;
			output.insert(outbuf[pos]);
			pos++;
		}
		
	}
	
	// join two string into one string
	char[] joinArray(char[] arr1, char[] arr2) {
		char[] temp = new char[arr1.length+arr2.length];
		int k = 0;
		for(int i=0; i<arr1.length; i++) {
			temp[k] = arr1[i];
			k++;
		}
		for(int i=0; i<arr2.length; i++) {
			temp[k] = arr2[i];
			k++;
		}
		return temp;
	}
	
	boolean checkJoinCond(char[] src_in, char[] src_out, char op) { // data from inner table and outer table, respectively
		String str_in = CharStr.getString(src_in, colIndexIn);
		String str_out = CharStr.getString(src_out, colIndexOut);
		return checkOperation(str_in, str_out, op);
	}
	
	// result of "inner.attr op outer.attr"
	boolean checkOperation(String str_in, String str_out, char op) {
		//System.out.println(str_in+" "+str_out);
		//integer type
		if(typeIn==0) {
			int i_in = Integer.parseInt(str_in);
			int i_out = Integer.parseInt(str_out);
			if(op=='=') return i_in==i_out;
			if(op=='<') return i_in<i_out;
			if(op=='>') return i_in>i_out;
		}
		else {
			int cmp = str_in.compareTo(str_out);
			if(op=='=') return cmp==0;
			if(op=='<') return cmp<0;
			if(op=='>') return cmp>0;
		}
		return false;
	}
	
	
}


class HashJoin{
	
	int bsize; // B : number of buffers
	int rnum; // number of records per page
	
	int strLen;
	int tupleLenIn;
	int tupleLenOut;
	
	int colIn;
	int typeIn;
	int colIndexIn;
	
	int colOut;
	int typeOut;
	int colIndexOut;
	
	Table in; // inner table
	Table out; // outer table
	
	char[][][] buffer;
	char[][] inbuffer;
	char[][] outbuffer;
	
	int pos;
	int max_pos;
	
	HashJoin(Join j) {
		bsize = j.bsize;
		rnum = j.rnum;
		
		strLen = j.strLen;
		tupleLenIn = j.tupleLenIn;
		tupleLenOut = j.tupleLenOut;
		
		colIn = j.colIn;
		typeIn = j.typeIn;
		colIndexIn = j.colIndexIn;
		
		colOut = j.colOut;
		typeOut = j.typeOut;
		colIndexOut = j.colIndexOut;
		
		in = j.in;
		out = j.out;
	}
	
	Table run() {
		
	}
	
	void partitioningPhase() {
		Table[] in_partition = new Table[bsize-1];
		Table[] out_partition = new Table[bsize-1];
		
		for(int i=0; i<bsize-1; i++) {
			in_partition[i] = new Table(in.tname+i, in.ncol, in.names, in.types, in.table);
			out_partition[i] = new Table(out.tname+i, out.ncol, out.names, out.types, out.table);
		}
		
		/*********** partition inner table *************/
		// use bsize-1 number of buffers(as output buffers) and one inbuffer
		inbuffer = new char[rnum][tupleLenIn];
		buffer = new char[bsize-1][rnum][tupleLenIn];
		
		//indexing for buffers and outbuffer
		int[] index_buffer = new int[bsize-1];
		for(int i=0; i<bsize-1; i++) index_buffer[i] = 0;
		int index_outbuffer = 0;
		
		max_pos = fill(in, inbuffer);
		while(true) {
			// renew inbuffer
			if(index_outbuffer==max_pos) {
				if(pos==in.data.size()) break; // inner table all done
				max_pos = fill(in, inbuffer);
			}
			
		}
		
		/*********** partition outer table *************/
		// use bsize-1 number of buffers(as output buffers) and one inbuffer
		inbuffer = new char[rnum][tupleLenOut];
		buffer = new char[bsize-1][rnum][tupleLenOut];
		
		//indexing for buffers and outbuffer
		for(int i=0; i<bsize-1; i++) index_buffer[i] = 0;
		index_outbuffer = 0;
		
	}
	
	int hash(String str, int n, int size) {
		String s = new String(str);
		for(int i=0; i<n; i++) {
			int h = Math.abs(s.hashCode());
			s = s+Integer.toString(h);
		}
		return Math.abs(s.hashCode())%size;
	}
	
	int add(char[][] buffer, char[] str, int index) {
		buffer[index] = str;
		return index+1;
	}
	
	int flush(char[][] src, int index, Table dest){
		for(int i=0; i<index; i++) {
			dest.insert(src[i]);
		}
		return 0;	
	}
	
	int fill(Table src, char[][] dest) {
		int i = 0;
		while(true) {
			
			if(pos==src.data.size()) {
				break;
			}
			if(i==dest.length) {
				break;
			}
			System.arraycopy(src.data.get(pos), 0, dest[i], 0, src.tupleLen);
			pos++;
			i++;
		}
		return i;
	}
}
