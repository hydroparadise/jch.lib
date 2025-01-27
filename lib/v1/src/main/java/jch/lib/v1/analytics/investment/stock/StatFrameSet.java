package jch.lib.analytics.investment.stock;

public class StatFrameSet {

	public StatFrameSet() {
		// TODO Auto-generated constructor stub
	}

	public boolean addEntry(StatFrame newFrame) {
		boolean success = true;
		
		//First Entry
		if(firstFrame == null) {
			firstFrame = newFrame;
			lastFrame = newFrame;
		}
		else {
			if (firstFrame == lastFrame) {
				firstFrame.setNextFrame(newFrame);
				lastFrame.setPrevFrame(firstFrame);
			}
			else {
				lastFrame.setNextFrame(newFrame);	
				newFrame.setPrevFrame(lastFrame);
			}
			lastFrame = newFrame;
		}
		
		return success;
	}
	
	
	
	public StatFrame getFirstFrame() {
		return firstFrame;
	}

	public void setFirstFrame(StatFrame firstFrame) {
		this.firstFrame = firstFrame;
	}

	public StatFrame getLastFrame() {
		return lastFrame;
	}

	public void setLastFrame(StatFrame lastFrame) {
		this.lastFrame = lastFrame;
	}
	
	
	/***
	 * Calculates the number of frames in a given frame set
	 * @return
	 */
	public long calcFrameCount() {

		long output = 0;
		StatFrame curFrame = firstFrame;
		while(curFrame != null) {
			output++;
			curFrame = curFrame.getNextFrame();
		}
		return output;
	}
	
	public StatFrame toStatFrameFirstPrice() {
		StatFrame output = new StatFrame();
		StatFrame curFrame = this.firstFrame;
		while(curFrame != null) {
			StatEntry newEntry = new StatEntry(
						curFrame.getFirstEntry().getTime(),
						curFrame.getFirstEntry().getPrice(),
						(int) curFrame.calcSumVolume()
					);
			output.addEntry(newEntry);
			curFrame = curFrame.getNextFrame();
		}
		return output;
	}
	
	public StatFrame toStatFrameMaxPrice() {
		StatFrame output = new StatFrame();
		StatFrame curFrame = this.firstFrame;
		while(curFrame != null) {
			StatEntry newEntry = new StatEntry(
						curFrame.getFirstEntry().getTime(),
						curFrame.calcMaxPrice(),
						(int) curFrame.calcSumVolume()
					);
			output.addEntry(newEntry);
			curFrame = curFrame.getNextFrame();
		}
		return output;
	}
	
	public StatFrame toStatFrameMinPrice() {
		StatFrame output = new StatFrame();
		StatFrame curFrame = this.firstFrame;
		while(curFrame != null) {
			StatEntry newEntry = new StatEntry(
						curFrame.getFirstEntry().getTime(),
						curFrame.calcMinPrice(),
						(int) curFrame.calcSumVolume()
					);
			output.addEntry(newEntry);
			curFrame = curFrame.getNextFrame();
		}
		return output;
	}
	
	public StatFrame toStatFrameAvgPrice() {
		StatFrame output = new StatFrame();
		StatFrame curFrame = this.firstFrame;
		while(curFrame != null) {
			StatEntry newEntry = new StatEntry(
						curFrame.getFirstEntry().getTime(),
						curFrame.calcAvgPrice(),
						(int) curFrame.calcSumVolume()
					);
			output.addEntry(newEntry);
			curFrame = curFrame.getNextFrame();
		}
		return output;
	}
	
	public StatFrame toStatFrameWtdAvgPrice() {
		StatFrame output = new StatFrame();
		StatFrame curFrame = this.firstFrame;
		while(curFrame != null) {
			StatEntry newEntry = new StatEntry(
						curFrame.getFirstEntry().getTime(),
						curFrame.calcWtdStdDevVolDurPrice(),
						(int) curFrame.calcSumVolume()
					);
			output.addEntry(newEntry);
			curFrame = curFrame.getNextFrame();
		}
		return output;
	}
	
	StatFrame firstFrame;
	StatFrame lastFrame;
}
