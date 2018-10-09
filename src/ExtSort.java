
public class ExtSort {
	
	/* modes : modified by setter functions only */
	int bsize; // B : number of buffers
	int rnum; // number of records per page
	boolean timing;
	
	/* belows are initiate by init function */
	
	/* tuple and string length */
	int tupleLen;
	int strLen;
	
	//column number to sort by and its type
	int col;
	int type;
	int colIndex;
	
	//output buffer position(index of the last page)
	int outbufPos;
	
	//length of current run : B -> B(B-1) -> B(B-1)^2 -> ...
	int runLen;
	
	// buffer
	char[] buffer;
	
	ExtSort(){
		bsize = 4;
		rnum = 2;		
		timing = false;
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
	
	// initiation
	// tl:tupleLen, sl:strLen(expecting 21)
	// col : column to sort by, type : type of col
	void init(int tl, int sl, int _col, int _type) {
		/* initiate the buffer size */
		/* # buffer = bsize, records per buffer = rnum, record length = tupleLen */
		tupleLen = tl;
		strLen = sl;
		
		col = _col;
		type = _type;
		colIndex = col*strLen;
		
		buffer = new char[bsize*rnum*tupleLen];
		// starting point of the output buffer (last page starting index)
		outbufPos = buffer.length - tupleLen*rnum; 
		
		runLen = 0;
	}
	
	Table run(Table input) {
		if(input.data.size()==0) return null;
		/* call run(input, pass) */
		return null;
	}
	
	Table run(Table input, int pass) {
		Table output = new Table(input.tname, input.ncol, input.names, input.types);
		
		//internal sort
		if(pass==0) {
			int inputStartPos = 0;
			while(true) {
				int start = 0;
				for(; start<bsize*rnum; start++) {
					if(inputStartPos+start >= input.data.size()) break; // index out of bound
					char[] temp = input.data.get(inputStartPos+start); // read from input
					System.arraycopy(temp, 0, buffer, tupleLen*start, tupleLen); // copy at the buffer
				}
				internalSort(0, start-1); // sort tuples in the buffer
				dumpBuffer(output, 0, start-1); // dump into the output table
				
				if(inputStartPos+start >= input.data.size()) break;// index out of bound
				
				inputStartPos += bsize*rnum;			
			}
			
			runLen = bsize;
			
		}
		
		//external merge sort
		else {
			//outer loop
			int inputStartPos = 0;
			boolean finish = false;
			while(!finish) {
				int[] pointerBuffer = new int[bsize-1]; // pointer. nth tuple at each input page?
				int[] pointerTable = new int[bsize-1];
				int[] intlist = new int[bsize-1]; //pointing value for type 0
				String[] strlist = new String[bsize-1]; // pointing value for type 1
				
				//initiation
				for(int i=0; i<bsize-1; i++) { 
					pointerBuffer[i] = rnum*i;
					pointerTable[i] = inputStartPos + rnum*runLen*i;
				}
				
				int lastpage = bsize-1;
				int lasttuple = rnum-1;
				
				for(int i=0; i<bsize-1; i++) { // for each run
					for(int j=0; j<rnum; j++) { // copy each tuple
						/* idea : set the last tuple '\0' (len=0 string) ?? */
					}
				}
				
			}
		}
		
		output.print();
		return output;
	}
	
	// helper function for the 0th pass
	// sort buffer from (startPos)th tuple to (endPos)th tuple
	void internalSort(int startPos, int endPos) {
		//use bubble sort
		for(int i=startPos; i<endPos; i++) {
			for(int j=i+1; j<=endPos; j++) {
				String x = CharStr.getString(buffer, i*tupleLen+colIndex);
				String y = CharStr.getString(buffer, j*tupleLen+colIndex);
				try {
					if((type==1 && x.compareTo(y)>0) || (type==0 && Integer.parseInt(x) > Integer.parseInt(y))) {
						swap(i, j);					}
				} catch(Exception e) {
					System.out.println("Error : Type casting error.");
				}
			}
		}
		
		//should adjust quicksort..??
		
	}
	
	// helper function for interalSort function
	// swap (x_pos)th tuple with (y_pos)th tuple
	void swap(int x_pos, int y_pos) {
		char[] temp = new char[tupleLen];
		System.arraycopy(buffer, x_pos*tupleLen, temp, 0, tupleLen); // x->temp
		System.arraycopy(buffer, y_pos*tupleLen, buffer, x_pos*tupleLen, tupleLen); // y->x
		System.arraycopy(temp, 0, buffer, y_pos*tupleLen, tupleLen); // temp->y
	}
	
	// dump (startPos)th ~ (endPos)th tuples in the buffer to the output table
	void dumpBuffer(Table table, int startPos, int endPos) {
		for(int i=startPos; i<=endPos; i++) {
			char[] temp = new char[tupleLen];
			System.arraycopy(buffer, i*tupleLen, temp, 0, tupleLen);
			table.insert(temp);
		}
	}
	
}


//helper functions
class CharStr{
	
	static void copyString(char[] dest, int destInd, String src, int len) {
		int temp = Math.min(src.length(), len);
		src.getChars(0, temp, dest, destInd);
		dest[destInd+temp] = '\0';
	}
	
	static String getString(char[] src, int srcInd) {
		int i = srcInd;
		for(; i<src.length; i++) {
			if(src[i]=='\0') break;
		}
		int len = i-srcInd;
		return new String(src, srcInd, len);
	}
	
}