package com.eqt.lucene.analyzer.wildcard;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.junit.Test;

public class FasterWildcardTest extends BaseTokenStreamTestCase {

	public static final String TEST_STR_1 = "cataract";
	public static final String TEST_STR_2 = "This is a test";
	
	@Test
	public void TestTokens() throws IOException {
		Analyzer fasterWildcardAnalyzer = new FasterWildcardAnalyzer();
//
//		assertTokenStreamContents(
//				fasterWildcardAnalyzer.tokenStream("my_keyword_field", new StringReader("ISO8859-1 and all that jazz")),
//				new String[] { "ISO8859-1 and all that jazz" });

		assertAnalyzesTo(fasterWildcardAnalyzer, TEST_STR_1, 
				new String[] { "cataract" , "$ataract", "$taract", "$aract", "$ract"
				, "$act", "$ct", "$t"});

		assertAnalyzesTo(fasterWildcardAnalyzer, TEST_STR_2, 
				new String[] { "this","$his","$is","$s" , "is","$s","a","test","$est","$dt","$t"});

	}

}
