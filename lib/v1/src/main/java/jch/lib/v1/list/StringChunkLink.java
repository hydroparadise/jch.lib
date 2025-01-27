package jch.lib.list;


/***
 * 
 * @author ChadHarrison
 *
 */
public class StringChunkLink extends StringChunk implements ChunkLink {
	private long index = 0;
	private ChunkLink nextChunk = null;
	private ChunkLink previousChunk = null;
	private boolean primeForReindex = false;
	
	public StringChunkLink(String newValue) {
		super(newValue);	
	}

	public ChunkLink getNextChunk() {
		// TODO Auto-generated method stub
		return this.nextChunk;
	}

	public boolean setNextChunk(ChunkLink nextChunk) {
		//if(nextChunk != null) {
			this.nextChunk = nextChunk;
			return true;
		//}
		//return false;
	}
	
	public ChunkLink getPreviousChunk() {
		// TODO Auto-generated method stub
		return this.previousChunk;
	}
	
	public boolean setPreviousChunk(ChunkLink previousChunk) {
		//if(previousChunk != null) {
			this.previousChunk = previousChunk;
			return true;
		//}
		//return false;
	}

	public String getChunkType() {
		// TODO Auto-generated method stub
		return this.getClass().getSimpleName();
	}

	/**
	 * Returns current index value of element
	 */
	public long getIndex() {
		// TODO Auto-generated method stub
		return index;
	}

	/**
	 * An index can be set if the the element has not yet been assigned an index or the 
	 * setPrimeForReindex() method was invoked prior to index reassignment. 
	 * @return True if index assignment was successful, False if index assignment was unsuccessful.
	 */
	public boolean setIndex(long newIndex) {
		if(index == 0 || this.primeForReindex == true) {
			index = newIndex;
			this.primeForReindex = false;
			return true;
		}
		else {
			return false;
		}
	}
	
	
	/**
	 * Used as a safety mechanism intended for to be used by the parent list to reindex elements
	 * for cases where elements are moved or removed.
	 * @return Current state of primeForReindex flag
	 */
	public boolean setPrimeForReindex() {
		this.primeForReindex = true;
		return this.primeForReindex;
	}
	
	@Override
	public String toString() {
		return this.getValue();
	}
	
	public String toString(long length)  {
		StringChunkLink t = this;
		StringBuilder output = new StringBuilder();
		if(length > 0) {
			output.append(t.getValue());
		}
		if(length > 1) {
			
			for(long i = 0; i < length - 1; i++) {
				if(t.getNextChunk() == null) {
					i = length;
				}
				else {
					t = (StringChunkLink)t.getNextChunk();
					output.append(t.getValue());
				}
			}
		}
		
		return output.toString();
	}
}
