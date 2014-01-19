package com.eqt.lucene.analyzer.wildcard;

import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.util.Version;

public class FasterWildcardAnalyzer extends Analyzer {

	@Override
	protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
		Tokenizer stream = new WhitespaceTokenizer(Version.LUCENE_46, reader);
		TokenStream filter = new LowerCaseFilter(Version.LUCENE_46, stream);
		filter = new FasterWildcardTokenizer(filter);
		return new TokenStreamComponents(stream, filter);
	}
}
