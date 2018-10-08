
public class ExtSort {
	int bsize; // B : number of buffers
	int rnum; // number of records per page
	boolean timing;
	
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