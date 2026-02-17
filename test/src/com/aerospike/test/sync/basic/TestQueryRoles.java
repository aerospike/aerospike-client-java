/*
 * Copyright 2012-2025 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.aerospike.test.sync.basic;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.admin.Role;
import com.aerospike.client.policy.AdminPolicy;
import com.aerospike.client.util.Version;
import com.aerospike.test.sync.TestSync;

public class TestQueryRoles extends TestSync {
	private static boolean securityEnabled = false;
	private static boolean credentialsProvided = false;
	private static Version serverVersion = null;
	
	private static final Map<String, List<String>> EXPECTED_ROLES_BY_VERSION = createExpectedRolesMap();
	
	private static Map<String, List<String>> createExpectedRolesMap() {
		Map<String, List<String>> rolesMap = new HashMap<>();
		
		List<String> baseRoles = Arrays.asList(
			Role.UserAdmin,       
			Role.SysAdmin,        
			Role.DataAdmin,       
			Role.UDFAdmin,        
			Role.SIndexAdmin,     
			Role.Read,            
			Role.ReadWrite,       
			Role.ReadWriteUdf,    
			Role.Write,           
			Role.Truncate         
		);
		
		// Masking roles added in version 8.1.1
		List<String> maskingRoles = Arrays.asList(
			Role.MaskingAdmin,     
			Role.ReadMasked,       
			Role.WriteMasked       
		);
		
		// Store base roles for versions < 8.1.1
		rolesMap.put("pre-8.1.1", baseRoles);
		
		// Store all roles (base + masking) for versions >= 8.1.1
		List<String> allRoles = new java.util.ArrayList<>(baseRoles);
		allRoles.addAll(maskingRoles);
		rolesMap.put("8.1.1+", allRoles);
		
		return rolesMap;
	}

	@BeforeClass
	public static void setup() {
		// Step 1: Check credentials
		credentialsProvided = (args.user != null && !args.user.isEmpty() && 
		                       args.password != null && !args.password.isEmpty());
		
		if (!credentialsProvided) {
			securityEnabled = false;
			return;
		}
		
		// Step 2: Initialize client
		try {
			TestSync.init();
		} catch (AerospikeException.Connection e) {
			securityEnabled = false;
			return;
		}
		
		// Step 3: Verify security is enabled
		if (client == null) {
			return;
		}
	
		try {
			AdminPolicy policy = new AdminPolicy();
			client.queryRoles(policy);
			securityEnabled = true;
			
			serverVersion = client.getCluster().getRandomNode().getServerVersion();
		} catch (AerospikeException.InvalidNode e) {
			securityEnabled = false;
		} catch (AerospikeException e) {
			if (e.getResultCode() == ResultCode.SECURITY_NOT_ENABLED ||
				e.getResultCode() == ResultCode.SECURITY_NOT_SUPPORTED ||
				e.getResultCode() == ResultCode.NOT_AUTHENTICATED) {
				securityEnabled = false;
			} else {
				throw e;
			}
		}
	}

	@Test
	public void testQueryRolesAll() {
		Assume.assumeTrue("Skipping test: Credentials not provided", credentialsProvided);
		Assume.assumeTrue("Skipping test: Security is not enabled on the server", securityEnabled);
		
		AdminPolicy policy = new AdminPolicy();
		List<Role> roles = client.queryRoles(policy);
		assertNotNull("Roles list should not be null", roles);
	}

	@Test
	public void testPreVersion8_1_1_Roles() {
		Assume.assumeTrue("Skipping test: Credentials not provided", credentialsProvided);
		Assume.assumeTrue("Skipping test: Security is not enabled on the server", securityEnabled);
		Assume.assumeTrue("Skipping test: Server version is >= 8.1.1", 
			serverVersion.isLessThan(8, 1, 1, 0));
		
		AdminPolicy policy = new AdminPolicy();
		List<Role> roles = client.queryRoles(policy);
		assertNotNull("Roles list should not be null", roles);
		
		Set<String> roleNames = new HashSet<>();
		for (Role role : roles) {
			roleNames.add(role.name);
		}
		
		List<String> expectedRoles = EXPECTED_ROLES_BY_VERSION.get("pre-8.1.1");
		
		for (String expectedRole : expectedRoles) {
			assertTrue("Role '" + expectedRole + "' should exist for server version < 8.1.1", 
				roleNames.contains(expectedRole));
		}
		
		assertFalse("Masking role '" + Role.MaskingAdmin + "' should NOT exist for server version < 8.1.1",
			roleNames.contains(Role.MaskingAdmin));
		assertFalse("Masking role '" + Role.ReadMasked + "' should NOT exist for server version < 8.1.1",
			roleNames.contains(Role.ReadMasked));
		assertFalse("Masking role '" + Role.WriteMasked + "' should NOT exist for server version < 8.1.1",
			roleNames.contains(Role.WriteMasked));
	}

	@Test
	public void testVersion8_1_1_AndAbove_Roles() {
		Assume.assumeTrue("Skipping test: Credentials not provided", credentialsProvided);
		Assume.assumeTrue("Skipping test: Security is not enabled on the server", securityEnabled);
		Assume.assumeTrue("Skipping test: Server version is < 8.1.1", 
			serverVersion.isGreaterOrEqual(8, 1, 1, 0));
		
		AdminPolicy policy = new AdminPolicy();
		List<Role> roles = client.queryRoles(policy);
		assertNotNull("Roles list should not be null", roles);
		
		Set<String> roleNames = new HashSet<>();
		for (Role role : roles) {
			roleNames.add(role.name);
		}
		
		List<String> expectedRoles = EXPECTED_ROLES_BY_VERSION.get("8.1.1+");
		
		for (String expectedRole : expectedRoles) {
			assertTrue("Role '" + expectedRole + "' should exist for server version >= 8.1.1", 
				roleNames.contains(expectedRole));
		}
		
		assertTrue("Masking role '" + Role.MaskingAdmin + "' should exist for server version >= 8.1.1",
			roleNames.contains(Role.MaskingAdmin));
		assertTrue("Masking role '" + Role.ReadMasked + "' should exist for server version >= 8.1.1",
			roleNames.contains(Role.ReadMasked));
		assertTrue("Masking role '" + Role.WriteMasked + "' should exist for server version >= 8.1.1",
			roleNames.contains(Role.WriteMasked));
	}

	@Test
	public void testRolesByServerVersion() {
		Assume.assumeTrue("Skipping test: Credentials not provided", credentialsProvided);
		Assume.assumeTrue("Skipping test: Security is not enabled on the server", securityEnabled);
		
		AdminPolicy policy = new AdminPolicy();
		List<Role> roles = client.queryRoles(policy);
		assertNotNull("Roles list should not be null", roles);
		
		// Convert roles list to a set of role names for easy lookup
		Set<String> roleNames = new HashSet<>();
		for (Role role : roles) {
			roleNames.add(role.name);
		}
		
		String versionKey;
		List<String> expectedRoles;
		
		if (serverVersion.isGreaterOrEqual(8, 1, 1, 0)) {
			versionKey = "8.1.1+";
			expectedRoles = EXPECTED_ROLES_BY_VERSION.get(versionKey);
			
			for (String expectedRole : expectedRoles) {
				assertTrue("Role '" + expectedRole + "' should exist for server version " + serverVersion, 
					roleNames.contains(expectedRole));
			}
			
			assertTrue("Masking role '" + Role.MaskingAdmin + "' should exist for server version >= 8.1.1",
				roleNames.contains(Role.MaskingAdmin));
			assertTrue("Masking role '" + Role.ReadMasked + "' should exist for server version >= 8.1.1",
				roleNames.contains(Role.ReadMasked));
			assertTrue("Masking role '" + Role.WriteMasked + "' should exist for server version >= 8.1.1",
				roleNames.contains(Role.WriteMasked));
		} else {
			versionKey = "pre-8.1.1";
			expectedRoles = EXPECTED_ROLES_BY_VERSION.get(versionKey);
			
			for (String expectedRole : expectedRoles) {
				assertTrue("Role '" + expectedRole + "' should exist for server version " + serverVersion, 
					roleNames.contains(expectedRole));
			}
			
			assertFalse("Masking role '" + Role.MaskingAdmin + "' should NOT exist for server version < 8.1.1",
				roleNames.contains(Role.MaskingAdmin));
			assertFalse("Masking role '" + Role.ReadMasked + "' should NOT exist for server version < 8.1.1",
				roleNames.contains(Role.ReadMasked));
			assertFalse("Masking role '" + Role.WriteMasked + "' should NOT exist for server version < 8.1.1",
				roleNames.contains(Role.WriteMasked));
		}
	}
}
