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
package com.aerospike.examples;

public final class ExampleDefinition {
	private final String name;
	private final ExampleMode mode;
	private final Class<?> exampleClass;
	private final ExampleFixture fixture;
	private final ExampleServerRequirement serverRequirement;

	public ExampleDefinition(
		String name,
		ExampleMode mode,
		Class<?> exampleClass,
		ExampleFixture fixture
	) {
		this(name, mode, exampleClass, fixture, ExampleServerRequirement.NONE);
	}

	public ExampleDefinition(
		String name,
		ExampleMode mode,
		Class<?> exampleClass,
		ExampleFixture fixture,
		ExampleServerRequirement serverRequirement
	) {
		this.name = name;
		this.mode = mode;
		this.exampleClass = exampleClass;
		this.fixture = (fixture == null) ? ExampleFixture.NONE : fixture;
		this.serverRequirement = (serverRequirement == null) ? ExampleServerRequirement.NONE : serverRequirement;
	}

	public String name() {
		return name;
	}

	public ExampleMode mode() {
		return mode;
	}

	public Class<?> exampleClass() {
		return exampleClass;
	}

	public ExampleFixture fixture() {
		return fixture;
	}

	public ExampleServerRequirement serverRequirement() {
		return serverRequirement;
	}

	public boolean isAsync() {
		return mode == ExampleMode.ASYNC;
	}
}
