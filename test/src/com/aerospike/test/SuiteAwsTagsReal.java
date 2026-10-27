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
package com.aerospike.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.aerospike.test.discovery.TestAwsTagsReal;

/**
 * AWS EC2 tag seed discovery against real AWS. Run on an EC2 instance by
 * test/discovery/run_aws_tags_real.sh, which supplies the system properties and the environment.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
	TestAwsTagsReal.class
})
public class SuiteAwsTagsReal {
}
