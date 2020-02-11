/*
 * Copyright 2012-2020 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.helper.query;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;



/*
 * Tests to ensure that Qualifiers are built successfully for non indexed bins.
 */
public class RegexpBuilderTests {


	@Test
	public void testEscapesBackslash() {
		String inputStr = "a\\b";
		String expectedStr = "a\\\\b"; // a\\b

		String escapedStr = Qualifier.QualifierRegexpBuilder.escapeBRERegexp(inputStr);	
		assertThat(escapedStr).isEqualTo(expectedStr);
	}

	@Test
	public void testEscapesDot() {
		String inputStr = "a.b";
		String expectedStr = "a\\.b"; // a\.b

		String escapedStr = Qualifier.QualifierRegexpBuilder.escapeBRERegexp(inputStr);
		assertThat(escapedStr).isEqualTo(expectedStr);
	}
	
	@Test
	public void testEscapesSquareBracket() {
		String inputStr = "a[b";
		String expectedStr = "a\\[b"; // a\[b

		String escapedStr = Qualifier.QualifierRegexpBuilder.escapeBRERegexp(inputStr);	
		assertThat(escapedStr).isEqualTo(expectedStr);
	}

	@Test
	public void testEscapesAsterisk() {
		String inputStr = "a*b";
		String expectedStr = "a\\*b"; // a\*b

		String escapedStr = Qualifier.QualifierRegexpBuilder.escapeBRERegexp(inputStr);
		assertThat(escapedStr).isEqualTo(expectedStr);
	}

	@Test
	public void testEscapesCircumflex() {
		String inputStr = "a^b";
		String expectedStr = "a\\^b"; // a\^b

		String escapedStr = Qualifier.QualifierRegexpBuilder.escapeBRERegexp(inputStr);
		assertThat(escapedStr).isEqualTo(expectedStr);
	}

	@Test
	public void testEscapesDollar() {
		String inputStr = "a$b";
		String expectedStr = "a\\$b"; // a\$b

		String escapedStr = Qualifier.QualifierRegexpBuilder.escapeBRERegexp(inputStr);
		assertThat(escapedStr).isEqualTo(expectedStr);
	}

	@Test
	public void testDoesNotEscapeOtherCharacter() {
		String inputStr = "abcdefghijklmnopqrstuvwxyz";

		String escapedStr = Qualifier.QualifierRegexpBuilder.escapeBRERegexp(inputStr);
		assertThat(escapedStr).isEqualTo(inputStr);
	}
	
	@Test
	public void escapesMultiplSpecialCharacters() {
		String inputStr = "\\^aerospike$"; // \\\^aerospike\$
		String expectedStr = "\\\\\\^aerospike\\$"; // \^aerospike\$

		String escapedStr = Qualifier.QualifierRegexpBuilder.escapeBRERegexp(inputStr);
		assertThat(escapedStr).isEqualTo(expectedStr);
	}
	
	@Test
	public void buildsEscapedStartsWith() {
		String inputStr = "*aero";
		String expectedStr = "^\\*aero";
		String escapedStr = Qualifier.QualifierRegexpBuilder.getStartsWith(inputStr);
		
		assertThat(escapedStr).isEqualTo(expectedStr);
	}
	
	@Test
	public void buildsEscapedEndsWith() {
		String inputStr = "*spike*";
		String expectedStr = "\\*spike\\*$";
		String escapedStr = Qualifier.QualifierRegexpBuilder.getEndsWith(inputStr);

		assertThat(escapedStr).isEqualTo(expectedStr);
	}
	
	@Test
	public void buildsEscapedContaining() {
		String inputStr = "*erospi*";
		String expectedStr = "\\*erospi\\*"; // \*erospi\*
		String escapedStr = Qualifier.QualifierRegexpBuilder.getContaining(inputStr);

		assertThat(escapedStr).isEqualTo(expectedStr);
	}

	@Test
	public void buildsEscapedEquals() {
		String inputStr = "\\*aerospike*";
		String expectedStr = "^\\\\\\*aerospike\\*$"; // ^\\\*aerospike\*$
		String escapedStr = Qualifier.QualifierRegexpBuilder.getStringEquals(inputStr);

		assertThat(escapedStr).isEqualTo(expectedStr);
	}
}
