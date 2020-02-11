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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.aerospike.client.Value;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.PredExp;
import com.aerospike.client.query.RegexFlag;

/**
 * Generic Bin qualifier. It acts as a filter to exclude records that do not met this criteria.
 * The operations supported are:
 * <ul>
 * <li>EQ - Equals</li>
 * <li>GT - Greater than</li>
 * <li>GTEQ - Greater than or equal to</li>
 * <li>LT - Less than</li>
 * <li>LTEQ - Less than or equal to</li>
 * <li>NOTEQ - Not equal</li>
 * <li>BETWEEN - Between two value (inclusive)</li>
 * <li>START_WITH - A string that starts with</li>
 * <li>ENDS_WITH - A string that ends with</li>
 * </ul><p>
 *
 * @author Peter Milne
 */
public class Qualifier implements Map<String, Object>, Serializable {
	private static final long serialVersionUID = -2689196529952712849L;
	private static final String listIterVar = "listIterVar";
	private static final String mapIterVar = "mapIterVar";
	private static final String FIELD = "field";
	private static final String IGNORE_CASE = "ignoreCase";
	private static final String VALUE2 = "value2";
	private static final String VALUE1 = "value1";
	private static final String QUALIFIERS = "qualifiers";
	private static final String OPERATION = "operation";
	private static final String AS_FILTER = "queryAsFilter";
	protected Map<String, Object> internalMap;

	public static class QualifierRegexpBuilder {
		private static Character BACKSLASH = '\\';
		private static Character DOT = '.';
		private static Character ASTERISK = '*';
		private static Character DOLLAR = '$';
		private static Character OPEN_BRACKET = '[';
		private static Character CIRCUMFLEX = '^';

		public static String escapeBRERegexp(String base) {
			StringBuilder builder = new StringBuilder();
			for (char stringChar: base.toCharArray()) {
				if (
				stringChar == BACKSLASH ||
				stringChar == DOT ||
				stringChar == ASTERISK ||
				stringChar == DOLLAR ||
				stringChar == OPEN_BRACKET ||
				stringChar == CIRCUMFLEX) {
						builder.append(BACKSLASH);
				}
				builder.append(stringChar);
			}
			return builder.toString();
		}

		/*
		 * This op is always in [START_WITH, ENDS_WITH, EQ, CONTAINING]
		 */
		private static String getRegexp(String base, FilterOperation op) {
			String escapedBase = escapeBRERegexp(base);
			if (op == FilterOperation.START_WITH) {
				return "^" + escapedBase;
			}
			if (op == FilterOperation.ENDS_WITH) {
				return escapedBase + "$";
			}
			if (op == FilterOperation.EQ) {
				return "^" + escapedBase + "$";
			}
			return escapedBase;
		}

		public static String getStartsWith(String base) {
			return getRegexp(base, FilterOperation.START_WITH);
		}

		public static String getEndsWith(String base) {
			return getRegexp(base, FilterOperation.ENDS_WITH);
		}

		public static String getContaining(String base) {
			return getRegexp(base, FilterOperation.CONTAINING);
		}
		public static String getStringEquals(String base) {
			return getRegexp(base, FilterOperation.EQ);
		}
	}


	public enum FilterOperation {
		EQ, GT, GTEQ, LT, LTEQ, NOTEQ, BETWEEN, START_WITH, ENDS_WITH, CONTAINING, IN,
		LIST_CONTAINS, MAP_KEYS_CONTAINS, MAP_VALUES_CONTAINS,
		LIST_BETWEEN, MAP_KEYS_BETWEEN, MAP_VALUES_BETWEEN, GEO_WITHIN,
		OR, AND
	}

	public Qualifier() {
		super();
		internalMap = new HashMap<String, Object>();
	}

	public Qualifier(FilterOperation operation, Qualifier... qualifiers) {
		this();
		internalMap.put(QUALIFIERS, qualifiers);
		internalMap.put(OPERATION, operation);
	}

	public Qualifier(String field, FilterOperation operation, Value value1) {
		this(field, operation, Boolean.FALSE, value1);
	}

	public Qualifier(String field, FilterOperation operation, Boolean ignoreCase, Value value1) {
		this();
		internalMap.put(FIELD, field);
		internalMap.put(OPERATION, operation);
		internalMap.put(VALUE1, value1);
		internalMap.put(IGNORE_CASE, ignoreCase);
	}

	public Qualifier(String field, FilterOperation operation, Value value1, Value value2) {
		this(field, operation, Boolean.FALSE, value1);
		internalMap.put(VALUE2, value2);
	}

	public FilterOperation getOperation() {
		return (FilterOperation) internalMap.get(OPERATION);
	}

	public String getField() {
		return (String) internalMap.get(FIELD);
	}

	public PredExp getFieldExpr(int valType) throws PredExpException {
		switch (valType) {
			case ParticleType.INTEGER:
				return PredExp.integerBin(getField());
			case ParticleType.STRING:
				return PredExp.stringBin(getField());
			case ParticleType.GEOJSON:
				return PredExp.geoJSONBin(getField());
			default:
				throw new PredExpException("PredExp Unsupported Particle Type: " + valType);
		}
	}

	public void asFilter(Boolean queryAsFilter) {
		internalMap.put(AS_FILTER, queryAsFilter);
	}

	public Boolean queryAsFilter() {
		return internalMap.containsKey(AS_FILTER) && (Boolean) internalMap.get(AS_FILTER);
	}

	public Qualifier[] getQualifiers() {
		return (Qualifier[]) internalMap.get(QUALIFIERS);
	}

	public Value getValue1() {
		return (Value) internalMap.get(VALUE1);
	}

	public Value getValue2() {
		return (Value) internalMap.get(VALUE2);
	}

	public Filter asFilter() {
		FilterOperation op = getOperation();
		switch (op) {
			case EQ:
				if (getValue1().getType() == ParticleType.INTEGER) {
					return Filter.equal(getField(), getValue1().toLong());
				} else {
					// There is no case insensitive string comparison filter.
					if(ignoreCase()) {
						return null;
					}
					return Filter.equal(getField(), getValue1().toString());
				}
			case GTEQ:
			case BETWEEN:
				return Filter.range(getField(), getValue1().toLong(), getValue2()==null?Long.MAX_VALUE:getValue2().toLong());
			case GT:
				return Filter.range(getField(), getValue1().toLong()+1, getValue2()==null?Long.MAX_VALUE:getValue2().toLong());
			case LT:
				return Filter.range(getField(), Long.MIN_VALUE, getValue1().toLong()-1);
			case LTEQ:
				return Filter.range(getField(),  Long.MIN_VALUE, getValue1().toLong());
			case LIST_CONTAINS:
				return collectionContains(IndexCollectionType.LIST);
			case MAP_KEYS_CONTAINS:
				return collectionContains(IndexCollectionType.MAPKEYS);
			case MAP_VALUES_CONTAINS:
				return collectionContains(IndexCollectionType.MAPVALUES);
			case LIST_BETWEEN:
				return collectionRange(IndexCollectionType.LIST);
			case MAP_KEYS_BETWEEN:
				return collectionRange(IndexCollectionType.MAPKEYS);
			case MAP_VALUES_BETWEEN:
				return collectionRange(IndexCollectionType.MAPVALUES);
			case GEO_WITHIN:
				return geoWithinRadius(IndexCollectionType.DEFAULT);
			default:
				return null;
		}
	}

	private Filter geoWithinRadius(IndexCollectionType collectionType) {
		return  Filter.geoContains(getField(), getValue1().toString());
	}

	private Filter collectionContains(IndexCollectionType collectionType) {
		Value val = getValue1();
		int valType = val.getType();
		switch (valType) {
			case ParticleType.INTEGER:
				return Filter.contains(getField(), collectionType, val.toLong());
			case ParticleType.STRING:
				return Filter.contains(getField(), collectionType, val.toString());
		}
		return null;
	}

	private Filter collectionRange(IndexCollectionType collectionType) {
		return Filter.range(getField(), collectionType, getValue1().toLong(), getValue2().toLong());
	}

	public List<PredExp> toPredExp() throws PredExpException{
		int regexFlags = ignoreCase() ? RegexFlag.ICASE : RegexFlag.NONE;
		List<PredExp> rs = new ArrayList<PredExp>();
		switch(getOperation()){
		case AND:
			Qualifier[] qs = (Qualifier[])get(QUALIFIERS);
			for(Qualifier q : qs) rs.addAll(q.toPredExp());
			rs.add(PredExp.and(qs.length));
			break;
		case OR:
			qs = (Qualifier[])get(QUALIFIERS);
			for(Qualifier q : qs) rs.addAll(q.toPredExp());
			rs.add(PredExp.or(qs.length));
			break;
		case IN: // Conver IN to a collection of or as Aerospike has not support for IN query
			Value val = getValue1();
			int valType = val.getType();
			if(valType != ParticleType.LIST)
				throw new IllegalArgumentException("FilterOperation.IN expects List argument with type: " + ParticleType.LIST + ", but got: " + valType);
			List<?> inList = (List<?>) val.getObject();
			for(Object value : inList) rs.addAll(new Qualifier(this.getField(), FilterOperation.EQ, Value.get(value)).toPredExp());
			rs.add(PredExp.or(inList.size()));
			break;
		case EQ:
			val = getValue1();
			valType = val.getType();
			switch (valType) {
				case ParticleType.INTEGER:
					rs.add(getFieldExpr(valType));
					rs.add(PredExp.integerValue(val.toLong()));
					rs.add(PredExp.integerEqual());
					break;
				case ParticleType.STRING:
					if (ignoreCase()) {
						String equalsRegexp = QualifierRegexpBuilder.getStringEquals(getValue1().toString());
						rs.add(getFieldExpr(getValue1().getType()));
						rs.add(PredExp.stringValue(equalsRegexp));
						rs.add(PredExp.stringRegex(RegexFlag.ICASE));
					} else {
						rs.add(getFieldExpr(valType));
						rs.add(PredExp.stringValue(val.toString()));
						rs.add(PredExp.stringEqual());
					}
					break;
				default:
					throw new PredExpException("PredExp Unsupported Particle Type: " + valType);
			}
			break;
		case NOTEQ:
			rs.addAll(Arrays.asList(valToPredExp(getValue1())));
			rs.add(getValue1().getType()==ParticleType.INTEGER?PredExp.integerUnequal():PredExp.stringUnequal());
			break;
		case GT:
			rs.addAll(Arrays.asList(valToPredExp(getValue1())));
			rs.add(PredExp.integerGreater());
			break;
		case GTEQ:
			rs.addAll(Arrays.asList(valToPredExp(getValue1())));
			rs.add(PredExp.integerGreaterEq());
			break;
		case LT:
			rs.addAll(Arrays.asList(valToPredExp(getValue1())));
			rs.add(PredExp.integerLess());
			break;
		case LTEQ:
			rs.addAll(Arrays.asList(valToPredExp(getValue1())));
			rs.add(PredExp.integerLessEq());
			break;
		case BETWEEN:
			return new Qualifier(FilterOperation.AND, new Qualifier(getField(), FilterOperation.GTEQ, getValue1()), new Qualifier(getField(), FilterOperation.LTEQ, getValue2())).toPredExp();
		case GEO_WITHIN:

			rs.addAll(Arrays.asList(valToPredExp(getValue1())));
			rs.add(PredExp.geoJSONWithin());

			break;
		case START_WITH:
			String startWithRegexp = QualifierRegexpBuilder.getStartsWith(getValue1().toString());
			rs.add(getFieldExpr(getValue1().getType()));
			rs.add(PredExp.stringValue(startWithRegexp));
			rs.add(PredExp.stringRegex(regexFlags));
			break;
		case ENDS_WITH:
			String endWithRegexp = QualifierRegexpBuilder.getEndsWith(getValue1().toString());
			rs.add(getFieldExpr(getValue1().getType()));
			rs.add(PredExp.stringValue(endWithRegexp));
			rs.add(PredExp.stringRegex(regexFlags));
			break;
		case CONTAINING:
			String containingRegexp = QualifierRegexpBuilder.getContaining(getValue1().toString());
			rs.add(getFieldExpr(getValue1().getType()));
			rs.add(PredExp.stringValue(containingRegexp));
			rs.add(PredExp.stringRegex(regexFlags));
			break;
		case LIST_CONTAINS:
			if (getValue1().getType() == ParticleType.STRING) {
				rs.add(PredExp.stringVar(listIterVar));
				rs.add(PredExp.stringValue(getValue1().toString()));
				rs.add(PredExp.stringEqual());
			} else {
				rs.add(PredExp.integerVar(listIterVar));
				rs.add(PredExp.integerValue(getValue1().toLong()));
				rs.add(PredExp.integerEqual());
			}
			rs.add(PredExp.listBin(getField()));
			rs.add(PredExp.listIterateOr(listIterVar));
			break;
		case MAP_KEYS_CONTAINS:
			if (getValue1().getType() == ParticleType.STRING) {
				rs.add(PredExp.stringVar(mapIterVar));
				rs.add(PredExp.stringValue(getValue1().toString()));
				rs.add(PredExp.stringEqual());
			} else {
				rs.add(PredExp.integerVar(mapIterVar));
				rs.add(PredExp.integerValue(getValue1().toLong()));
				rs.add(PredExp.integerEqual());
			}
			rs.add(PredExp.mapBin(getField()));
			rs.add(PredExp.mapKeyIterateOr(mapIterVar));
			break;
		case MAP_VALUES_CONTAINS:
			if (getValue1().getType() == ParticleType.STRING) {
				rs.add(PredExp.stringVar(mapIterVar));
				rs.add(PredExp.stringValue(getValue1().toString()));
				rs.add(PredExp.stringEqual());
			} else {
				rs.add(PredExp.integerVar(mapIterVar));
				rs.add(PredExp.integerValue(getValue1().toLong()));
				rs.add(PredExp.integerEqual());
			}
			rs.add(PredExp.mapBin(getField()));
			rs.add(PredExp.mapValIterateOr(mapIterVar));
			break;
		case LIST_BETWEEN:
			rs.add(PredExp.integerVar(listIterVar));
			rs.add(PredExp.integerValue(getValue1().toLong()));
			rs.add(PredExp.integerGreaterEq());
			
			rs.add(PredExp.integerVar(listIterVar));
			rs.add(PredExp.integerValue(getValue2().toLong()));
			rs.add(PredExp.integerLessEq());
			
			rs.add(PredExp.and(2));

			rs.add(PredExp.listBin(getField()));
			rs.add(PredExp.listIterateOr(listIterVar));
			break;
		case MAP_KEYS_BETWEEN:
			rs.add(PredExp.integerVar(mapIterVar));
			rs.add(PredExp.integerValue(getValue1().toLong()));
			rs.add(PredExp.integerGreaterEq());
			
			rs.add(PredExp.integerVar(mapIterVar));
			rs.add(PredExp.integerValue(getValue2().toLong()));
			rs.add(PredExp.integerLessEq());
			
			rs.add(PredExp.and(2));

		rs.add(PredExp.mapBin(getField()));
			rs.add(PredExp.mapKeyIterateOr(mapIterVar));
			break;
		case MAP_VALUES_BETWEEN:
			rs.add(PredExp.integerVar(mapIterVar));
			rs.add(PredExp.integerValue(getValue1().toLong()));
			rs.add(PredExp.integerGreaterEq());
			
			rs.add(PredExp.integerVar(mapIterVar));
			rs.add(PredExp.integerValue(getValue2().toLong()));
			rs.add(PredExp.integerLessEq());
			
			rs.add(PredExp.and(2));

		rs.add(PredExp.mapBin(getField()));
			rs.add(PredExp.mapValIterateOr(mapIterVar));
			break;
		default:
			throw new PredExpException("PredExp Unsupported Operation: " + getOperation());
		}
		return rs;
	}

    private PredExp[] valToPredExp(Value val) throws PredExpException {
        int valType = val.getType();
        switch (valType) {
            case ParticleType.INTEGER:
                return new PredExp[]{
                        getFieldExpr(valType),
                        PredExp.integerValue(val.toLong())};
            case ParticleType.STRING:
                return new PredExp[]{
                        getFieldExpr(valType),
                        PredExp.stringValue(val.toString())};
            case ParticleType.GEOJSON:
                return new PredExp[]{
                        getFieldExpr(valType),
                        PredExp.geoJSONValue(val.toString())};
            default:
                throw new PredExpException("PredExp Unsupported Particle Type: " + val.getType());
        }
    }



	

	private Boolean ignoreCase() {
		Boolean ignoreCase = (Boolean) internalMap.get(IGNORE_CASE);
		return (ignoreCase == null) ? false : ignoreCase;
	}

	protected String luaFieldString(String field) {
		return String.format("rec['%s']", field);
	}

	protected String luaValueString(Value value) {
		String res = null;
		if(null == value) return res;
		int type = value.getType();
		switch (type) {
			//		case ParticleType.LIST:
			//			res = value.toString();
			//			break;
			//		case ParticleType.MAP:
			//			res = value.toString();
			//			break;
			//		case ParticleType.DOUBLE:
			//			res = value.toString();
			//			break;
		case ParticleType.STRING:
			res = String.format("'%s'", value.toString());
			break;
		case ParticleType.GEOJSON:
			res = String.format("'%s'", value.toString());
			break;
		default:
				res = value.toString();
				break;
		}
		return res;
	}


	/*
	 * (non-Javadoc)
	 * @see java.util.Map#size()
	 */
	@Override
	public int size() {
		return internalMap.size();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#isEmpty()
	 */
	@Override
	public boolean isEmpty() {
		return internalMap.isEmpty();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#containsKey(java.lang.Object)
	 */
	@Override
	public boolean containsKey(java.lang.Object key) {
		return internalMap.containsKey(key);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#containsValue(java.lang.Object)
	 */
	@Override
	public boolean containsValue(java.lang.Object value) {
		return internalMap.containsValue(value);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#get(java.lang.Object)
	 */
	@Override
	public Object get(java.lang.Object key) {
		return internalMap.get(key);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#put(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Object put(String key, Object value) {
		return internalMap.put(key, value);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#remove(java.lang.Object)
	 */
	@Override
	public Object remove(java.lang.Object key) {
		return internalMap.remove(key);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#putAll(java.util.Map)
	 */
	@Override
	public void putAll(Map<? extends String, ? extends Object> m) {
		internalMap.putAll(m);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#clear()
	 */
	@Override
	public void clear() {
		internalMap.clear();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#keySet()
	 */
	@Override
	public Set<String> keySet() {
		return internalMap.keySet();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#values()
	 */
	@Override
	public Collection<Object> values() {
		return internalMap.values();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#entrySet()
	 */
	@Override
	public Set<java.util.Map.Entry<String, Object>> entrySet() {
		return internalMap.entrySet();
	}

	@Override
	public String toString() {
		String output = String.format("%s:%s:%s:%s", getField(), getOperation(), getValue1(), getValue2());
		return output;
	}
}
