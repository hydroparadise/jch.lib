package jch.lib.list;


/***
 * 
 * @author ChadHarrison
 *
 */
public interface ChunkLink {
	public ChunkLink getNextChunk();
	public boolean setNextChunk(ChunkLink nextChunkLink);
	public ChunkLink getPreviousChunk();
	public boolean setPreviousChunk(ChunkLink previousChunkLink);
	public String getChunkType();
	public long getIndex();
	public boolean setIndex(long newIndex);
	public boolean setPrimeForReindex();
	
}
