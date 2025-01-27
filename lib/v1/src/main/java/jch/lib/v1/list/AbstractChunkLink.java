package jch.lib.list;

/***
 * 
 * @author ChadHarrison
 *
 */
public interface AbstractChunkLink extends ChunkLink {
	public long getLength();
	public boolean setLength(long length);
	public ChunkLink getRefChunkLink();
	public boolean setRefChunkLink(ChunkLink refLink);
}
