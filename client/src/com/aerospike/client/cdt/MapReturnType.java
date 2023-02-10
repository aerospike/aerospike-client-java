/*
 * Copyright 2012-2023 Aerospike, Inc.
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
package com.aerospike.client.cdt;

/**
 * Map return type. Type of data to return when selecting or removing items from the map.
 */
public final class MapReturnType {
	/**
	 * Do not return a result.
	 */
	public static final int NONE = 0;

	/**
	 * Return key index order.
	 * <ul>
	 * <li>0 = first key</li>
	 * <li>N = Nth key</li>
	 * <li>-1 = last key</li>
	 * </ul>
	 */
	public static final int INDEX = 1;

	/**
	 * Return reverse key order.
	 * <ul>
	 * <li>0 = last key</li>
	 * <li>-1 = first key</li>
	 * </ul>
	 */
	public static final int REVERSE_INDEX = 2;

	/**
	 * Return value order.
	 * <ul>
	 * <li>0 = smallest value</li>
	 * <li>N = Nth smallest value</li>
	 * <li>-1 = largest value</li>
	 * </ul>
	 */
	public static final int RANK = 3;

	/**
	 * Return reverse value order.
	 * <ul>
	 * <li>0 = largest value</li>
	 * <li>N = Nth largest value</li>
	 * <li>-1 = smallest value</li>
	 * </ul>
	 */
	public static final int REVERSE_RANK = 4;

	/**
	 * Return count of items selected.
	 */
	public static final int COUNT = 5;

	/**
	 * Return key for single key read and key list for range read.
	 */
	public static final int KEY = 6;

	/**
	 * Return value for single key read and value list for range read.
	 */
	public static final int VALUE = 7;

	/**
	 * Return key/value items. The possible return types are:
	 * <ul>
	 * <li>HashMap : Returned for unordered maps</li>
	 * <li>TreeMap : Returned for key ordered maps</li>
	 * <li>List&lt;Entry&gt; : Returned for range results where range order needs to be preserved.</li>
	 * </ul>
	 */
	public static final int KEY_VALUE = 8;

	/**
	 * Return true if count > 0.
	 */
	public static final int EXISTS = 13;

	/**
	 * Return an unordered map.
	 */
	public static final int UNORDERED_MAP = 16;

	/**
	 * Return an ordered map.
	 */
	public static final int ORDERED_MAP = 17;

	/**
	 * Invert meaning of map command and return values.  For example:
	 * <pre>{@code MapOperation.removeByKeyRange(binName, keyBegin, keyEnd, MapReturnType.KEY | MapReturnType.INVERTED);}</pre>
	 * With the INVERTED flag enabled, the keys outside of the specified key range will be removed and returned.
	 */
	public static final int INVERTED = 0x10000;
}
