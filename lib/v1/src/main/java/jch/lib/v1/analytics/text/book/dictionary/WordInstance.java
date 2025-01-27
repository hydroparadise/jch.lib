package jch.lib.analytics.text.book.dictionary;

import java.util.ArrayList;

import jch.lib.analytics.text.book.BookValue;

public class WordInstance extends BookValue{
	public BookValue WordHeader;
	public BookValue WordFooter;
	public ArrayList<BookValue> WordDefinitions;
	public ArrayList<WordToken> Tokens;
	public ArrayList<WordToken> Synonyms;
	public ArrayList<WordPartsOfSpeech> PartsOfSpeech;
	
	public WordInstance() {
		// TODO Auto-generated constructor stub
		WordHeader = new BookValue();
		WordFooter = new BookValue();
		WordDefinitions = new ArrayList<BookValue>();
		Tokens = new ArrayList<WordToken>();
		Synonyms = new ArrayList<WordToken>();
		PartsOfSpeech = new ArrayList<WordPartsOfSpeech>();
 	}

}
