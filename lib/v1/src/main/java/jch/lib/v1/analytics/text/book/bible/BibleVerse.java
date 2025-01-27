package jch.lib.analytics.text.book.bible;

import jch.lib.analytics.text.book.BookValue;

public class BibleVerse extends BookValue {
	int verseNumber;
	
	
	public String toString() {
		return this.getCleanRefValue();
	}
	public int getVerseNumber() {
		return verseNumber;
	}
	public void setVerseNumber(int verseNumber) {
		this.verseNumber = verseNumber;
	}
	public BibleVerse() {
		// TODO Auto-generated constructor stub
	}
	public BibleVerse(int verseNumber) {
		this.verseNumber = verseNumber;
	}

}
