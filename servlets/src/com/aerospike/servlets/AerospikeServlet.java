/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
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
package com.aerospike.servlets;

import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

public class AerospikeServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;

	protected AerospikeClient client;
    protected String host;
    protected int port;

    @Override
	protected void service(HttpServletRequest request, HttpServletResponse response) 
		throws ServletException, IOException {
    	
    	try {
            String method = request.getMethod();

            if (method.equals("OPTIONS")) {
                // Available HTTP options for this command
                response.setHeader("Access-Control-Allow-Origin", "*");
                response.setHeader("Access-Control-Allow-Methods", "DELETE,OPTIONS,GET,POST");
                response.setHeader("Access-Control-Max-Age", "1000");
                response.setStatus(HttpServletResponse.SC_OK);
                return;
            }

            if (! initConnection(request, response))
				return;

            processRequest(request, response);
    	}
    	catch (Exception e) {
    		throw new ServletException(e);
    	}
    }
    
	private boolean initConnection(HttpServletRequest request, HttpServletResponse response) 
		throws IOException {

		String hostString = request.getParameter("host");
		String portString = request.getParameter("port");
				
		if (client != null && client.isConnected() && hostString != null && portString != null && host.equals(hostString) && Integer.toString(port).equals(portString))
			return true;
				
		host = hostString;
		
	    if (host == null || host.length() == 0) {
	    	try {
		        host = getServletConfig().getInitParameter("clusterAddr").trim();
		    } 
		    catch (Exception e) {
		        host = "127.0.0.1";
		    }
	    }
	    
		if (portString == null || portString.length() == 0) {
		    try {
		        portString = getServletConfig().getInitParameter("clusterPort");
		    } 
		    catch (Exception e) {
		    	portString = "3000";
		    }
	    }
		
		try {
			port = Integer.parseInt(portString);
		}
		catch (Exception e) {
			displayLoginError(response, host, portString, null);	
			return false;
		}
		
		try {
			ClientPolicy policy = new ClientPolicy();
			client = new AerospikeClient(policy, host, port);						
			
			if (client.isConnected())
				return true;
			
			displayLoginError(response, host, portString, null);	
			return false;
		}
		catch (Exception ex) {
			displayLoginError(response, host, portString, ex.getMessage());
			return false;
		}
	}
	
	private void displayLoginError(HttpServletResponse response, String hostString, String portString, String message) 
		throws IOException {
        
		String errorMsg = "Invalid server: " + hostString + ':' + portString;
		
		if (message != null) {
			errorMsg += " " + message;
		}
		response.sendError(HttpServletResponse.SC_NOT_FOUND, errorMsg); 
	}
	
	private void processRequest(HttpServletRequest request, HttpServletResponse response) 
		throws ServletException, IOException {

        String method = request.getMethod();
        String jsonpCallback = null;

        response.setHeader("Access-Control-Allow-Origin", "*");
        response.setContentType("application/json");
        response.setHeader("Cache-Control", "no-cache");

        @SuppressWarnings("unchecked")
		Map<String, String[]> params = request.getParameterMap();
        String pathInfo = request.getPathInfo();

        final Writer writer = response.getWriter();

        // If no pathInfo, just bail now.
        if (pathInfo == null) {
            response.setStatus(HttpServletResponse.SC_OK);
            writer.flush();
            return;
        }

        // Method override - so handy when debugging, or using a brain-dead client
        if (params.containsKey("X-method")) {
            method = params.get("X-method")[0];
        }

        int responseCode = 200;
        String errorMsg = "";
        try {
            pathInfo = stripChar(pathInfo, '/');
            String[] pathArray = pathInfo.split("/");

            // Get namespace, set, and key from the path.
            // Path *must* be of form namespace/set/key
            if (pathArray.length != 3) {
                throw new ResourceNotFoundException("Must Specify Namespace, Key, and Value");
            }

            String namespace = pathArray[0];
            String set       = pathArray[1];
            String userKey   = pathArray[2];

            // check that we've got a key
            if (userKey == null) {
                log("bad key received");
                throw new BadParamException("No Key");
            }
            
            Key key = new Key(namespace, set, userKey);

            // Get other parameters from the parameters on the URL
            String[] values      = params.get("value");
            String[] bins        = params.get("bin");
            String[] generations = params.get("generation");
            String[] timeouts    = params.get("timeout");
            String[] ops         = params.get("op");
            String[] expirations = params.get("expiration");
            String[] callbacks   = params.get("callback");
            String[] jsonps      = params.get("jsonp");

            int timeout = 0;
            try {
                timeout = (timeouts == null || timeouts.length == 0) ? 0 : Integer.parseInt(timeouts[0]);
            }catch (NumberFormatException e) {
                log("bad timeout received");
                throw new BadParamException("Timeout Not Numeric");
            }

            int expiration = 0;
            try {
                expiration = (expirations == null || expirations.length == 0) ? 0 : Integer.parseInt(expirations[0]);
            }catch (NumberFormatException e) {
                log("bad expiration received");
                throw new BadParamException("Expiration Not Numeric");
            }

            int generation = 0;
            try {
                generation = (generations == null || generations.length == 0) ? 0 : Integer.parseInt(generations[0]);
            }catch (NumberFormatException e) {
                log("bad generation received");
                throw new BadParamException("Generation Not Numeric");
            }

            if (callbacks != null) {
                jsonpCallback = callbacks[0];
            } else if (jsonps != null) {
                jsonpCallback = jsonps[0];
            }

            Policy policy = new Policy();
            policy.timeout = timeout;

            if (method.equals("GET")) {
                Record record;
                if (bins == null)
                	record = this.client.get(policy, key);
                else
                	record = this.client.get(policy, key, bins);
                
                if (record != null) {
                    if (record.bins != null) {
                        Set<?> keyset = record.bins.keySet();
                        Iterator<?> iterator = keyset.iterator();
                        if (!iterator.hasNext()) {
                            throw new ResourceNotFoundException("Server Returns No Values");
                        }

                        String resultJSON = "{\"result\" : {";
                        while (iterator.hasNext()) {
                            Object binName = iterator.next();
                            Object binValue = record.bins.get(binName);
                            resultJSON += "\"" + binName.toString() + "\" : ";
                            if( binValue instanceof Integer ) {
                                resultJSON += binValue.toString();
                            } else if (binValue instanceof byte[]) {
                                System.out.println(" it's a blob!");
                            } else if (binValue instanceof String) {
                                resultJSON += "\"" +  binValue.toString() + "\"";
                            } else {
                                resultJSON += "\"\"";
                            }
                            if (iterator.hasNext()) {
                                resultJSON += ",";
                            }
                        }
                        resultJSON += "},";
                        resultJSON += "\"generation\" : " + record.generation + "}";
                        if (jsonpCallback != null) {
                            resultJSON = jsonpCallback + "(" + resultJSON + ")";
                            response.setContentType("application/javascript");
                        }
                        writer.write(resultJSON);
                    }
                } else {
                	throw new ResourceNotFoundException("Key Not Found");
                }

            }else if (method.equals("POST")) {
            
                if (values == null || values.length < 1) {
                    throw new BadParamException("Setting a key requires a value");
                }

                boolean isArithmeticAdd = false;
                boolean isAppend        = false;
                boolean isPrepend       = false;

                if (ops != null && ops.length > 0) {
                 	if (ops[0].equals("add")) {
                    	isArithmeticAdd = true;
                    } else if (ops[0].equals("append")){
                    	isAppend  = true;
                    } else if (ops[0].equals("prepend")){
                    	isPrepend = true;
                    }
                }
                
                WritePolicy writePolicy = new WritePolicy();
                writePolicy.timeout = timeout;
                writePolicy.expiration = expiration;
                if (generation != 0) {
                	writePolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
                    writePolicy.generation = generation;
                }
                
                if (values.length > 1) {
                    if (bins == null || bins.length != values.length) {
                        throw new BadParamException("Setting a key requires a bin name");
                    }
                    Bin[] binArray = new Bin[bins.length];                    
                    for (int i=0; i<bins.length; i++) {
                        Object value = getObject(values[i], isArithmeticAdd);
                        binArray[i] = new Bin(bins[i], value);
                    }

                    if (isArithmeticAdd) {
                        client.add(writePolicy, key, binArray);
                    } else if (isAppend) {
                        client.append(writePolicy, key, binArray);
                    } else if (isPrepend) {
                        client.prepend(writePolicy, key, binArray);
                    } else { // standard set
                        client.put(writePolicy, key, binArray);
                    }

                } else {
                    String binName = bins != null && bins.length > 0 ? bins[0] : "";
                    Object value = getObject(values[0], isArithmeticAdd);
                    Bin bin = new Bin(binName, value);
                    
                    if (isArithmeticAdd) {
                        client.add(writePolicy, key, bin);
                    } else if (isAppend) {
                        client.append(writePolicy, key, bin);
                    } else if (isPrepend) {
                        client.prepend(writePolicy, key, bin);
                    } else {
                        client.put(writePolicy, key, bin);
                    }
                }
            } else if (method.equals("DELETE")) {
                WritePolicy writePolicy = new WritePolicy();
                writePolicy.timeout = timeout;
                if (generation != 0) {
                	writePolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
                    writePolicy.generation = generation;
                }

                client.delete(writePolicy, key);                
            } else {
                throw new BadMethodException("Unknown Method");
            }

        } catch (AerospikeException.Timeout e) {
            responseCode = HttpServletResponse.SC_REQUEST_TIMEOUT;
            errorMsg = e.getMessage();

        } catch (BadMethodException e) {
            responseCode = HttpServletResponse.SC_METHOD_NOT_ALLOWED;
            errorMsg = e.getMessage();

        } catch (BadParamException e) {
            responseCode = HttpServletResponse.SC_BAD_REQUEST;
            errorMsg = e.getMessage();

        } catch (ResourceNotFoundException e) {
            responseCode = HttpServletResponse.SC_NOT_FOUND;
            errorMsg = e.getMessage();
            
        } catch (AerospikeException ae) {
            responseCode = HttpServletResponse.SC_NOT_FOUND;
            errorMsg = "Error " + ae.getResultCode() + ": " + ae.getMessage();
        }
        
        if (responseCode != HttpServletResponse.SC_OK) {
            try {
                response.sendError(responseCode, errorMsg); 

            } catch (java.io.IOException e) {
            }
        } else {
            response.setStatus(HttpServletResponse.SC_OK);
            writer.flush();
        }
    }

    private Object getObject(String valueStr, boolean isArithmeticAdd) 
    	throws BadParamException {
    	if (! isArithmeticAdd) {
    		return valueStr;
    	}
    	
        try {
            return Integer.parseInt(valueStr);
        } catch (NumberFormatException e) {
            throw new BadParamException("Integer Value Required"); 
        }
    }
    
    private static String stripChar(String inStr, char stripChar) {
        int i = 0;
        for (i = 0; i < inStr.length(); i++) {
            if (inStr.charAt(i) != stripChar) {
                break;
            }
        }
        if (i == inStr.length())
            return inStr;
        else
            return inStr.substring(i, inStr.length());
    }

    @Override
	public void destroy() {
		if (client != null) {
			client.close();
			client = null;
		}
	}
	    
	private class BadParamException extends Exception {
		private static final long serialVersionUID = 1L;
	
		public BadParamException(String message) {
			super(message);
		}
	}
	
	private class BadMethodException extends Exception {
		private static final long serialVersionUID = 1L;
	
		public BadMethodException(String message) {
			super(message);
		}
	}
	
	private class ResourceNotFoundException extends Exception {
		private static final long serialVersionUID = 1L;
	
		public ResourceNotFoundException(String message) {
			super(message);
		}
	}	
}
