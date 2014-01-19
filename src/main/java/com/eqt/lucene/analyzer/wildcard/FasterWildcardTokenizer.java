package com.eqt.lucene.analyzer.wildcard;

import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.util.CharacterUtils;
import org.apache.lucene.util.Version;

/**
 * Variation of a typical ngram tokenizer. Will on average speed up all wildcarding query types
 * but the most improvement is leading wildcard. 
 * Will need some kind of query parsing logic to take full advantage of.
 * @author gman
 *
 */
public final class FasterWildcardTokenizer extends TokenFilter {

	CharTermAttribute termAttr = addAttribute(CharTermAttribute.class);
	PositionIncrementAttribute posAttr = addAttribute(PositionIncrementAttribute.class);
	boolean done = true;
	boolean started = false;
	char[] currBuffer = null;
	int currBuffSize = 0;
	int curr = 0;
	char DELIM = '$';
	CharacterUtils utils;
	
	protected FasterWildcardTokenizer(TokenStream input) {
		super(input);
		utils = CharacterUtils.getInstance(Version.LUCENE_46);
	}

	@Override
	public boolean incrementToken() throws IOException {
		clearAttributes();

		if(currBuffSize - curr >= 2) {
			currBuffer[curr] = DELIM;
			termAttr.copyBuffer(currBuffer, curr, currBuffSize-curr);
			posAttr.setPositionIncrement(0);
			termAttr.setLength(currBuffSize-curr);
			started = true;
			curr++;
//			System.out.println(new String(termAttr.buffer(),0,termAttr.length()));
			return true;
		}

		//this returns the origional term, for doing trailing wildcard searches
		if (input.incrementToken()) {
			done = false;
			started = false;
			curr = 0;
			currBuffer = termAttr.buffer().clone();
			currBuffSize = termAttr.length();
			posAttr.setPositionIncrement(1);
//			System.out.println("origonal term token: " + new String(termAttr.buffer(),0,termAttr.length()));
			return true;
		}
	
		//end of stream
		return false;
	}
}
