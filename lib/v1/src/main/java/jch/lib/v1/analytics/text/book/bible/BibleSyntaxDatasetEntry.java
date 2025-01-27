package jch.lib.analytics.text.book.bible;

import jch.lib.analytics.text.book.SyntaxDatasetEntry;

public class BibleSyntaxDatasetEntry implements SyntaxDatasetEntry {
	
	public int getTestamentIdx() {
		return testamentIdx;
	}

	public void setTestamentIdx(int testamentIdx) {
		this.testamentIdx = testamentIdx;
	}

	public int getBookIdx() {
		return bookIdx;
	}

	public void setBookIdx(int bookIdx) {
		this.bookIdx = bookIdx;
	}

	public int getChapterIdx() {
		return chapterIdx;
	}

	public void setChapterIdx(int chapterIdx) {
		this.chapterIdx = chapterIdx;
	}

	public int getVerseIdx() {
		return verseIdx;
	}

	public void setVerseIdx(int verseIdx) {
		this.verseIdx = verseIdx;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public int getBeginOffset() {
		return beginOffset;
	}

	public void setBeginOffset(int beginOffset) {
		this.beginOffset = beginOffset;
	}

	public String getLemma() {
		return lemma;
	}

	public void setLemma(String lemma) {
		this.lemma = lemma;
	}

	public String getPartOfSpeechTag() {
		return partOfSpeechTag;
	}

	public void setPartOfSpeechTag(String partOfSpeechTag) {
		this.partOfSpeechTag = partOfSpeechTag;
	}

	public String getAspect() {
		return aspect;
	}

	public void setAspect(String aspect) {
		this.aspect = aspect;
	}

	public String getCasing() {
		return casing;
	}

	public void setCasing(String casing) {
		this.casing = casing;
	}

	public String getForm() {
		return form;
	}

	public void setForm(String form) {
		this.form = form;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public String getMood() {
		return mood;
	}

	public void setMood(String mood) {
		this.mood = mood;
	}

	public String getNumber() {
		return number;
	}

	public void setNumber(String number) {
		this.number = number;
	}

	public String getPerson() {
		return person;
	}

	public void setPerson(String person) {
		this.person = person;
	}

	public String getProper() {
		return proper;
	}

	public void setProper(String proper) {
		this.proper = proper;
	}

	public String getReciprocity() {
		return reciprocity;
	}

	public void setReciprocity(String reciprocity) {
		this.reciprocity = reciprocity;
	}

	public String getTense() {
		return tense;
	}

	public void setTense(String tense) {
		this.tense = tense;
	}

	public String getVoice() {
		return voice;
	}

	public void setVoice(String voice) {
		this.voice = voice;
	}

	public int getDependencyEdgeHeadToken() {
		return dependencyEdgeHeadToken;
	}

	public void setDependencyEdgeHeadToken(int dependencyEdgeHeadToken) {
		this.dependencyEdgeHeadToken = dependencyEdgeHeadToken;
	}

	public String getDependencyEdgeLabel() {
		return dependencyEdgeLabel;
	}

	public void setDependencyEdgeLabel(String dependencyEdgeLabel) {
		this.dependencyEdgeLabel = dependencyEdgeLabel;
	}

	public static String getHeaderString() {
		return headerString;
	}

	public static void setHeaderString(String headerString) {
		BibleSyntaxDatasetEntry.headerString = headerString;
	}

	int testamentIdx;
	int bookIdx;
	int chapterIdx;
	int verseIdx;
	String text = "";
	int beginOffset;
	String lemma = "";
	String partOfSpeechTag = "";
	String aspect = "";
	String casing = "";
	String form = "";
	String gender = "";
	String mood = "";
	String number = "";
	String person = "";
	String proper = "";
	String reciprocity = "";
	String tense = "";
	String voice = "";
	int dependencyEdgeHeadToken;
	String dependencyEdgeLabel = "";
	
	static String d = "\t";
	static String d1 = ""; //"\"";	
	
	static String headerString = "TesatementIdx" + d +
								 "BookIdx" +  d +
								 "ChapterIdx" +  d +
								 "VerseIdx" + d +
								 "TextContent" + d +
								 "BeginOffset" +  d +
								 "Lemma" +  d +
								 "PartOfSpeechTag" + d +
								 "Aspect" + d +
								 "Casing" + d +
								 "Form" + d +
								 "Gender" + d +
								 "Mood" + d +
								 "Number" + d +
								 "Person" +  d +
								 "Proper" + d +
								 "Reciprocity" + d +
								 "Tense" + d +
								 "Voice" + d +
								 "DependencyEdgeHeadToken" + d +
								 "DependencyEdgeLabel"; 
	


	
	public String getStringRecord() {
		String output = "";
		
		output = Integer.toString(this.testamentIdx) + d +
				Integer.toString( this.bookIdx) + d +
				Integer.toString(this.chapterIdx) + d + 
				Integer.toString(this.verseIdx) + d + 
				d1 + this.text + d1 + d +
				Integer.toString(this.beginOffset) + d +
				d1 + this.lemma + d1 + d +
				d1 + this.partOfSpeechTag + d1 + d +
				d1 + this.aspect + d1 + d +
				d1 + this.casing + d1 + d +
				d1 + this.form  + d1 + d +
				d1 + this.gender + d1 + d +
				d1 + this.mood  + d1 + d +
				d1 + this.number + d1 + d +
				d1 + this.person + d1 + d +
				d1 + this.proper + d1 + d +
				d1 + this.reciprocity + d1 + d +
				d1 + this.tense + d1 + d +
				d1 + this.voice + d1 + d +
				Integer.toString(this.dependencyEdgeHeadToken) + d + 
				d1 + this.dependencyEdgeLabel  + d1;
		return output;
	}
	
	public boolean setStringRecord(String record) {
		String[] values = record.split("\t");
		
		this.testamentIdx = Integer.valueOf(values[0]);
		this.bookIdx = Integer.valueOf(values[1]);
		this.chapterIdx = Integer.valueOf(values[2]);
		this.verseIdx = Integer.valueOf(values[3]);
		this.text = values[4];
		this.beginOffset = Integer.valueOf(values[5]);
		this.lemma = values[6];
		this.partOfSpeechTag = values[7];
		this.aspect = values[8];
		this.casing = values[9];
		this.form = values[10];
		this.gender = values[11];
		this.mood = values[12];
		this.number = values[13];
		this.person = values[14];
		this.proper = values[15];
		this.reciprocity = values[16];
		this.tense = values[17];
		this.voice = values[18];
		this.dependencyEdgeHeadToken = Integer.valueOf(values[19]);
		this.dependencyEdgeLabel = values[20];
		
		
		
		return true;
		
	}
	
	/**
	 * P - Period (.)
	 * Q - Question Mark (?)
	 * E - Ex (!)
	 * CM - Comma (,)
	 * CL - Collon (:)
	 * SM - SemiCollon (;)
	 * 
	 * @return
	 */
	public String getPunctuation() {
		
		if(this.partOfSpeechTag.equals("PUNCT") == true) {
			if(this.text.equals(".")) return "P";
			if(this.text.equals("!")) return "E";
			if(this.text.equals("?")) return "Q";
			if(this.text.equals(",")) return "CM";
			if(this.text.equals(":")) return "CL";
			if(this.text.equals(";")) return "SM";
			else return "U";
		}
		else  
			return null;
	}
	
	/**
	 * Returns a string describing most prominent features of the current entry.
	 * @return
	 */
	public String getMsrPDP() {
		String output = null;
		output = this.partOfSpeechTag + "-" +
		         this.dependencyEdgeLabel;
		if(this.getPunctuation() != null) {
			output = output + "-" + this.getPunctuation();
		}
		return output;
	}
	
	public BibleSyntaxDatasetEntry() {
		// TODO Auto-generated constructor stub
	}

}
