package jch.lib.analytics.text.book;

import jch.lib.list.ChunkLink;
import jch.lib.list.ChunkList;
import jch.lib.list.StringChunkLink;



/**
 * 
 * @author hydro
 *
 */
public class BookValue {
	StringChunkLink refLink;
	StringChunkLink refLinkExtent;
	long refLength = 0;
	
	void reCalcLen() {
		if(this.refLink != null &&
			this.refLinkExtent != null) {
			this.refLength = this.refLinkExtent.getIndex() - this.refLink.getIndex();
		}
	}
	
	public boolean setRefLength(long refLength) {
		if(refLength > 0) {
			this.refLength = refLength;
			//reCalcLen();
			return true;
		}
		else 
			return false;		
	}
	
	public boolean setRefLink(ChunkLink refLink) {
		if(refLink != null ) {
			this.refLink = (StringChunkLink)refLink;
			reCalcLen();
			return true;
		}
		else 
			return false;
	}
	
	public boolean setRefLinkExtent(ChunkLink refLinkExtent) {
		if(refLink != null ) {
			this.refLinkExtent = (StringChunkLink)refLinkExtent;

			if(this.refLink != null && this.refLinkExtent != null &&
					refLink.getIndex() < refLinkExtent.getIndex()) {
				reCalcLen();
					return true;
			}
			else 
				return false;
		}
		else 
			return false;
	}
	
	public boolean setRefLink(ChunkLink refLink, long refLength) {
		if(refLink != null &&
				refLength > 0) {
			this.refLink = (StringChunkLink)refLink;
			this.refLength = refLength;
			//reCalcLen();
			return true;
		}
		else 
			return false;
	}
	
	/**
	 * A potentially cleaner way to add link references and reference links.
	 * Make sure links are from same list
	 * 
	 * @param refLink
	 * @param refLinkExtent
	 * @return
	 */
	public boolean setRefLink(ChunkLink refLink, ChunkLink refLinkExtent) {
		
		if(refLink != null && refLinkExtent != null &&
			refLink.getIndex() < refLinkExtent.getIndex()) {
			this.refLink = (StringChunkLink)refLink;
			this.refLinkExtent = (StringChunkLink) refLinkExtent;
			reCalcLen();
			return true;
		}
		else {
			return false;
		}
			
	}
	
	public ChunkLink getRefLink() {
		return this.refLink;
	}
	
	public ChunkLink getRefLinkExtent() {
		return this.refLinkExtent;
	}
	
	public long getRefLength() {
		return this.refLength;
	}
	public String getRefValue() {
		ChunkList L1 = ChunkList.stringToChunks(this.refLink.toString(this.refLength));
		String output = "";
		
		//Cleans string a bit before printing
		if(L1 != null ) {
			output = L1.toString(L1.getChunkCount());
		}
		return output;
	}
	
	
	public String getCleanRefValue() {
		StringBuilder output = new StringBuilder("");
		if(refLink != null) {
			ChunkList L1 = ChunkList.stringToChunks(this.refLink.toString(this.refLength));
			
			//Cleans string a bit before printing
			if(L1 != null ) {
				
				//Removes multiple newlines
				L1.replaceAll("\r\n\r\n", "\r\n");
				//L1.reindex();
				//Changes newlines to spaces
				L1.replaceAll("\r\n", " ");
				//L1.reindex();
				//cleanup stray end lines and returns
				L1.replaceAll("\r", "");
				L1.replaceAll("\n", "");
				L1.reindex();
				//Tidy up the list by reindexing
				
				output.append(L1.toString(L1.getChunkCount()));
			}
		}
		return output.toString();
	}
	
	public BookValue() {
		// TODO Auto-generated constructor stub
	}

}
