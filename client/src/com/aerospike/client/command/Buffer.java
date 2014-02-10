/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
package com.aerospike.client.command;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.util.Arrays;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.util.Unpacker;

public final class Buffer {

	public static Object bytesToParticle(int type, byte[] buf, int offset, int len)
		throws AerospikeException {
		
		switch (type) {
		case ParticleType.STRING:
			return Buffer.utf8ToString(buf, offset, len);
			
		case ParticleType.INTEGER:
			return Buffer.bytesToNumber(buf, offset, len);
		
		case ParticleType.BLOB:
			return Arrays.copyOfRange(buf, offset, offset+len);
			
		case ParticleType.JBLOB:
			return Buffer.bytesToObject(buf, offset, len);
			
		case ParticleType.LIST:
			return Unpacker.unpackObjectList(buf, offset, len);

		case ParticleType.MAP:
			return Unpacker.unpackObjectMap(buf, offset, len);
		
		default:
			return null;
		}
	}
	
	/*
	private static Object parseList(byte[] buf, int offset, int len) throws AerospikeException {
		int limit = offset + len;
		int itemCount = Buffer.bytesToInt(buf, offset);
		offset += 4;
		ArrayList<Object> list = new ArrayList<Object>(itemCount);
		
		while (offset < limit) {
			int sz = Buffer.bytesToInt(buf, offset);
			offset += 4;
			int type = buf[offset];
			offset++;
			list.add(bytesToParticle(type, buf, offset, sz));
			offset += sz;
		}	
		return list;
	}
		
	private static Object parseMap(byte[] buf, int offset, int len) throws AerospikeException {
		Object key;
		Object value;
		
		int limit = offset + len;
		int n_items = Buffer.bytesToInt(buf, offset);
		offset += 4;
		HashMap<Object, Object> map = new HashMap<Object, Object>(n_items);
		
		while (offset < limit) {
			// read out the key
			int sz = Buffer.bytesToInt(buf, offset);
			offset += 4;
			int type = buf[offset];
			offset++;
			
			key = bytesToParticle(type, buf, offset, len);
			offset += sz;
			
			// read out the value
			sz = Buffer.bytesToInt(buf, offset);
			offset += 4;
			type = buf[offset];
			offset++;
			
			value = bytesToParticle(type, buf, offset, len);
			offset += sz;
			
			map.put(key, value);
		}
		return map;
	}
	*/
	
	/**
	 * Estimate size of Utf8 encoded bytes without performing the actual encoding. 
	 */
	public static int estimateSizeUtf8(String value) {
		if (value == null) {
			return 0;
		}
		
		int max = value.length();
		int count = 0;
		
		for (int i = 0; i < max; i++) {
			char ch = value.charAt(i);
			
			if (ch < 0x80) {
				count++;
			}
			else if (ch < 0x800) {
			    count += 2;
			} 
			else if (Character.isHighSurrogate(ch)) {
				count += 4;
				++i;
			} else {
				count += 3;
			}
		}
		return count;
	}
	
	/**
	 * Convert input string to UTF-8, copies into buffer (at given offset).
	 * Returns number of bytes in the string.
	 *
     * Java's internal UTF8 conversion is very, very slow.
     * This is, rather amazingly, 8x faster than the to-string method.
	 * Returns the number of bytes this translated into.
     */
    public static int stringToUtf8(String s, byte[] buf, int offset) {
        if (s == null) {
        	return 0;
        }
    	int length = s.length();
        int startOffset = offset;
        
        for (int i = 0; i < length; i++) {
            int c = s.charAt(i);
            if (c < 0x80) {
                buf[offset++] = (byte) c;
            }
            else if (c < 0x800) {
            	buf[offset++] = (byte)(0xc0 | ((c >> 6)));
            	buf[offset++] = (byte)(0x80 | (c & 0x3f));
            }
            else {
		    	// Encountered a different encoding other than 2-byte UTF8. Let java handle it.
            	try {
		    		byte[] value = s.getBytes("UTF8");
					System.arraycopy(value, 0, buf, startOffset, value.length);
					return value.length;
            	}
            	catch (UnsupportedEncodingException uee) {
            		throw new RuntimeException("UTF8 encoding is not supported.");
            	}
            }
        }
        return offset - startOffset;
    }

    public static String utf8ToString(byte[] buf, int offset, int length) {
    	// A Thread local implementation does not help here, so  
    	// allocate character buffer each time.  
		char[] charBuffer = new char[length];
    	int charCount = 0;
        int limit = offset + length;
    	int origoffset = offset;

        while (offset < limit ) {
        	int b1 = buf[offset];
        	
        	if (b1 >= 0) {
                charBuffer[charCount++] = (char)b1;
                offset++;        		
        	}
        	else if ((b1 >> 5) == -2) {
        		int b2 = buf[offset + 1];
        		charBuffer[charCount++] = (char) (((b1 << 6) ^ b2) ^ 0x0f80);
                offset += 2;
        	}
		    else {
		    	// Encountered an UTF encoding which uses more than 2 bytes. 
		    	// Use a native function to do the conversion.
		    	try {
		    		return new String(buf, origoffset, length, "UTF8");
		    	}
		    	catch (UnsupportedEncodingException uee) {
            		throw new RuntimeException("UTF8 decoding is not supported.");
		    	}
		    }
        }
        return new String(charBuffer, 0, charCount);
    }

    public static String utf8ToString(byte[] buf, int offset, int length, StringBuilder sb) {
    	// This method is designed to accommodate multiple string conversions on the same
    	// thread, but without the ThreadLocal overhead.  The StringBuilder instance is
    	// created on the stack and passed in each method invocation.
    	sb.setLength(0);
        int limit = offset + length;
    	int origoffset = offset;

        while (offset < limit ) {
            if ((buf[offset] & 0x80) == 0) { // 1 byte
                char c = (char) buf[offset];
                sb.append(c);
                offset++;
            }
            else if ((buf[offset] & 0xE0) == 0xC0) { // 2 bytes
                char c =  (char) (((buf[offset] & 0x1f) << 6) | (buf[offset+1] & 0x3f));
                sb.append(c);
                offset += 2;
            }
		    else {
		    	// Encountered an UTF encoding which uses more than 2 bytes. 
		    	// Use a native function to do the conversion.
		    	try {
		    		return new String(buf, origoffset, length, "UTF8");
		    	}
		    	catch (UnsupportedEncodingException uee) {
            		throw new RuntimeException("UTF8 decoding is not supported.");
		    	}
		    }
        }
        return sb.toString();
    }
    
    /**
     * Convert UTF8 numeric digits to an integer.  Negative integers are not supported.
     * 
     * Input format: 1234
     */
    public static int utf8DigitsToInt(byte[] buf, int begin, int end) {
    	int val = 0;
    	int mult = 1;
    	
    	for (int i = end - 1; i >= begin; i--) {
    		val += ((int)buf[i] - 48) * mult;
    		mult *= 10;
    	}
    	return val;
    }
    
    public static String bytesToHexString(byte[] buf) {
		StringBuilder sb = new StringBuilder(buf.length * 2);
		
		for (int i = 0; i < buf.length; i++) {
    		sb.append(String.format("%02x", buf[i]));
		}
		return sb.toString();
    }

    public static String bytesToHexString(byte[] buf, int offset, int length) {
		StringBuilder sb = new StringBuilder(length * 2);
		
		for (int i = offset; i < length; i++) {
    		sb.append(String.format("%02x", buf[i]));
		}
		return sb.toString();
    }

    public static Object bytesToObject(byte[] buf, int offset, int length)
		throws AerospikeException.Serialize {
    	
    	if (length <= 0) {
    		return null;
    	}
    	
		try {
			ByteArrayInputStream bastream = new ByteArrayInputStream(buf, offset, length);
			ObjectInputStream oistream = new ObjectInputStream(bastream);
			return oistream.readObject();
		} 
		catch (Exception e) {
    		throw new AerospikeException.Serialize(e);
		}
	}

	public static Object bytesToNumber(byte[] buf, int offset, int len) {		
		if (len == 0)
			return new Integer(0);
		
		if (len > 8)
			return bytesToBigInteger(buf, offset, len);
	
		// This will work for negative integers too which 
		// will be represented in two's compliment representation.
		long val = 0;
		
		for (int i = 0; i < len; i++) {
			val <<= 8;
			val |= buf[offset+i] & 0xFF;
		}
		
		if (val <= Integer.MAX_VALUE && val >= Integer.MIN_VALUE)
			return new Integer((int) val);
		
		return new Long(val);
	}

	public static Object bytesToBigInteger(byte[] buf, int offset, int len) {
		boolean negative = false;
		
		if ((buf[offset] & 0x80) != 0) {
			negative = true;
			buf[offset] &= 0x7f;
		}
		byte[] bytes = new byte[len];
		System.arraycopy(buf, offset, bytes, 0, len);
		
		BigInteger big = new BigInteger(bytes);
		
		if (negative) {
			big = big.negate();
		}
		return big;
	}

	public static void longToBytes(long v, byte[] buf, int offset) {
		for (int i = 7; i >= 0; i--) {
			buf[offset+i] = (byte) (v & 0xff);
			v >>>= 8;
		}
	}
  	
    public static long bytesToLong(byte[] buf, int offset) {
    	long a = 0;
    	
    	for (int i = 0; i < 8; i++) {
    		a <<= 8;
    		a |= getUnsigned(buf[offset+i]);
    	}
    	return a;
    }

	public static void intToBytes(int v, byte[] buf, int offset) {
		for (int i = 3; i >= 0; i--) {
			buf[offset+i] = (byte) (v & 0xff);
			v >>>= 8;
		}
	}

	public static int bytesToInt(byte[] buf, int offset) {
        return (
        	((buf[offset] & 0xFF) << 24) | 
        	((buf[offset+1] & 0xFF) << 16) | 
        	((buf[offset+2] & 0xFF) << 8) | 
        	(buf[offset+3] & 0xFF)
        );
    }

    public static int bytesToIntIntel(byte[] buf, int offset) {
        return 
        	((buf[offset+3] & 0xFF) << 24) | 
        	((buf[offset+2] & 0xFF) << 16) | 
        	((buf[offset+1] & 0xFF) << 8) | 
        	(buf[offset] & 0xFF);
    }
	
    public static void shortToBytes(int v, byte[] buf, int offset) {
        buf[offset] = (byte) ((v >>> 8) & 0xff);
        buf[offset+1] = (byte) (v & 0xff);
    }

    public static int bytesToShort(byte[] buf, int offset) {
        return ((buf[offset] & 0xFF) << 8) + (buf[offset+1] & 0xFF);
    }
    
    private static int getUnsigned(byte b) {
    	int r = b;
    	
    	if (r < 0) {
    		r = r & 0x7f;
    		r = r | 0x80;
    	}
    	return r;
    }
}
