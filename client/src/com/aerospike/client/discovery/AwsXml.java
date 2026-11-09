/*
 * Copyright 2012-2026 Aerospike, Inc.
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
package com.aerospike.client.discovery;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import com.aerospike.client.AerospikeException;

/**
 * Parser for EC2 and STS XML responses, with DTDs and external entities disabled. Used by one
 * provider instance on one thread at a time.
 */
final class AwsXml {
	private static final ErrorHandler ERRORS = new ErrorHandler() {
		@Override
		public void warning(SAXParseException e) {
		}

		@Override
		public void error(SAXParseException e) throws SAXException {
			throw e;
		}

		@Override
		public void fatalError(SAXParseException e) throws SAXException {
			throw e;
		}
	};

	private final DocumentBuilderFactory factory;

	AwsXml() {
		factory = DocumentBuilderFactory.newInstance();

		try {
			factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
			factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
			factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
			factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
			factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
		}
		catch (ParserConfigurationException e) {
			throw new AerospikeException("AWS EC2 seed provider: XML parser cannot be secured: " + e, e);
		}
		factory.setXIncludeAware(false);
		factory.setExpandEntityReferences(false);
		factory.setNamespaceAware(false);
	}

	/**
	 * Parse a document and return its root element.
	 */
	Element parse(byte[] body) throws Exception {
		DocumentBuilder builder = factory.newDocumentBuilder();
		builder.setErrorHandler(ERRORS);
		return builder.parse(new ByteArrayInputStream(body)).getDocumentElement();
	}

	/**
	 * Return the direct child elements with the given name.
	 */
	static List<Element> children(Element parent, String name) {
		List<Element> list = new ArrayList<>();

		if (parent == null) {
			return list;
		}

		for (Node node = parent.getFirstChild(); node != null; node = node.getNextSibling()) {
			if (node.getNodeType() == Node.ELEMENT_NODE && name.equals(node.getNodeName())) {
				list.add((Element)node);
			}
		}
		return list;
	}

	/**
	 * Return the first direct child element with the given name, or null.
	 */
	static Element child(Element parent, String name) {
		List<Element> list = children(parent, name);
		return list.isEmpty()? null : list.get(0);
	}

	/**
	 * Return the trimmed text of the first direct child element with the given name, or null.
	 */
	static String text(Element parent, String name) {
		Element e = child(parent, name);
		return (e == null)? null : e.getTextContent().trim();
	}

	/**
	 * Return the trimmed text of the first descendant element with the given name, or null.
	 */
	static String find(Element root, String name) {
		NodeList list = root.getElementsByTagName(name);
		return (list.getLength() == 0)? null : list.item(0).getTextContent().trim();
	}

	/**
	 * Return an exception for an AWS Query API error response, with the HTTP status, the AWS error
	 * code and message and the request id when present. Each secret is removed from the message.
	 */
	AerospikeException error(String action, AwsHttp.Response response, String... secrets) {
		String code = null;
		String message = null;
		String requestId = null;

		try {
			Element root = parse(response.body);
			code = find(root, "Code");
			message = find(root, "Message");
			requestId = find(root, "RequestID");

			if (requestId == null) {
				requestId = find(root, "RequestId");
			}
		}
		catch (Exception ignore) {
		}

		if (requestId == null) {
			requestId = response.headers.get("x-amzn-requestid");
		}

		StringBuilder sb = new StringBuilder(128);
		sb.append("AWS EC2 seed provider: ").append(action).append(" failed: HTTP ").append(response.status);

		if (code != null) {
			sb.append(' ').append(code);
		}

		if (message != null) {
			sb.append(": ").append(message);
		}

		if (requestId != null) {
			sb.append(" (request id ").append(requestId).append(')');
		}
		return new AerospikeException(redact(sb.toString(), secrets));
	}

	/**
	 * Replace each non-empty secret in the text.
	 */
	static String redact(String text, String... secrets) {
		for (String secret : secrets) {
			if (secret != null && ! secret.isEmpty()) {
				text = text.replace(secret, "<redacted>");
			}
		}
		return text;
	}
}
