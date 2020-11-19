package jch.lib.common.chunk;

public interface AbstractChunkLink extends ChunkLink {
	public long getLength();
	public boolean setLength(long length);
	public ChunkLink getRefChunkLink();
	public boolean setRefChunkLink(ChunkLink refLink);
}
