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
package com.aerospike.client.admin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class PrivilegeCodeTest {
	@Test
	public void backupAndRestoreAdminIds() {
		// CLIENT-6001 / server codes 5 and 6.
		assertEquals(5, PrivilegeCode.BACKUP_ADMIN.id);
		assertEquals(6, PrivilegeCode.RESTORE_ADMIN.id);
		assertTrue(PrivilegeCode.BACKUP_ADMIN.id < PrivilegeCode.READ.id);
		assertTrue(PrivilegeCode.RESTORE_ADMIN.id < PrivilegeCode.READ.id);
	}

	@Test
	public void fromIdMapsBackupAndRestoreAdmin() {
		assertEquals(PrivilegeCode.BACKUP_ADMIN, PrivilegeCode.fromId(5));
		assertEquals(PrivilegeCode.RESTORE_ADMIN, PrivilegeCode.fromId(6));
	}

	@Test
	public void toStringMapsAdminRoleNames() {
		assertEquals(Role.BackupAdmin, PrivilegeCode.BACKUP_ADMIN.toString());
		assertEquals(Role.RestoreAdmin, PrivilegeCode.RESTORE_ADMIN.toString());
	}

	@Test
	public void canScopeIsFalseForGlobalAdminCodes() {
		assertFalse(PrivilegeCode.SYS_ADMIN.canScope());
		assertFalse(PrivilegeCode.BACKUP_ADMIN.canScope());
		assertFalse(PrivilegeCode.RESTORE_ADMIN.canScope());
		assertTrue(PrivilegeCode.READ.canScope());
	}
}
