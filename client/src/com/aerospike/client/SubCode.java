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
package com.aerospike.client;

/**
 * Server error detail subcodes.
 * <p>
 * When extended error detail is requested (see
 * {@link com.aerospike.client.policy.Policy#errorDetailVerbosity}), the server may
 * attach a numeric subcode to a failure response. The subcode is surfaced on
 * {@link AerospikeException#getSubCode()}.
 * <p>
 * <b>Match on the {@code (resultCode, subcode)} pair.</b> Subcode integer values are
 * scoped to their parent {@link ResultCode} and are <b>not</b> globally unique — the
 * value {@code 1}, for example, recurs under every parent status. A subcode is only
 * meaningful when interpreted together with the result code. Always check the result
 * code first:
 * <pre>{@code
 * catch (AerospikeException ae) {
 *     if (ae.getResultCode() == ResultCode.OP_NOT_APPLICABLE &&
 *         ae.getSubCode() == SubCode.OPNOT_CDT_BOUNDED_LIST_OVERFLOW) {
 *         // roll to a fresh partition / apply backpressure
 *     }
 * }
 * }</pre>
 * <p>
 * {@link #NONE} (0) means "no subcode" — it is reserved universally and is the value
 * returned when the server did not send a subcode (verbosity disabled, or the failing
 * branch had no dispatchable subcode).
 * <p>
 * This catalogue mirrors the server's per-status enums in
 * {@code as/include/base/proto.h} and is server-version-specific. It is pinned to
 * server master as of 2026-08-21 (39 subcodes); no released server version carries
 * the feature yet, so the pin advances to a release number once one ships. Re-verify
 * this catalogue against {@code proto.h} at every client release. It is append-only:
 * published values are immutable and are never renumbered or reused. New failure modes
 * get new values appended to their group. Treat any subcode value not declared here as
 * an opaque integer rather than assuming it is absent.
 */
public final class SubCode {
	/**
	 * No subcode (universal). Returned when the server did not supply a subcode.
	 */
	public static final int NONE = 0;

	//-------------------------------------------------------
	// Pairs with ResultCode.PARAMETER_ERROR (4)  [AS_ERR_PARAMETER]
	//-------------------------------------------------------

	/** Per-record TTL exceeds the namespace's max-ttl. */
	public static final int PARAM_TTL_INVALID = 1;

	/** Bit op offset lands past the blob (or above the proto cap). */
	public static final int PARAM_BITS_OFFSET_OUT_OF_RANGE = 2;

	/** Bit op size is out of range (e.g. zero, or too large). */
	public static final int PARAM_BITS_SIZE_OUT_OF_RANGE = 3;

	/** Blob resize would exceed the maximum blob size. */
	public static final int PARAM_BITS_RESIZE_EXCEEDED = 4;

	/** Write would exceed the per-record bin-count limit (write path). */
	public static final int PARAM_BIN_COUNT_TOO_LARGE = 5;

	/** String op wire/expression args malformed or out of range. */
	public static final int PARAM_STRING_OP_PARAMS_INVALID = 6;

	/** String op code or modifier/read class mismatch on the wire path. */
	public static final int PARAM_STRING_OP_INVALID = 7;

	/** String context-eval path malformed. */
	public static final int PARAM_STRING_CTX_NOT_APPLICABLE = 8;

	/** String modify/read index or code-point range out of bounds. */
	public static final int PARAM_STRING_INDEX_OUT_OF_BOUNDS = 9;

	/** String regex pattern invalid (compile / ICU failure). */
	public static final int PARAM_STRING_REGEX_INVALID = 10;

	/** String or string op argument is not valid UTF-8. */
	public static final int PARAM_STRING_UTF8_INVALID = 11;

	//-------------------------------------------------------
	// Pairs with ResultCode.PARTITION_UNAVAILABLE (11)  [AS_ERR_UNAVAILABLE]
	//-------------------------------------------------------

	/** Cluster is still resolving initial partition balance at startup. */
	public static final int UNAVAIL_INITIAL_BALANCE_UNRESOLVED = 1;

	/** A needed replica is unavailable (likely a partition split). */
	public static final int UNAVAIL_REPLICA_UNAVAILABLE = 2;

	//-------------------------------------------------------
	// Pairs with ResultCode.UNSUPPORTED_FEATURE (16)  [AS_ERR_UNSUPPORTED_FEATURE]
	//-------------------------------------------------------

	/** MRT attempted against a non-SC (AP) namespace. */
	public static final int UNSUPP_FEAT_MRT_REQUIRES_STRONG_CONSISTENCY = 1;

	/** Requested feature is unsupported in this context (generic). */
	public static final int UNSUPP_FEAT_GENERIC = 2;

	//-------------------------------------------------------
	// Pairs with ResultCode.BIN_NOT_FOUND (17)  [AS_ERR_BIN_NOT_FOUND]
	//-------------------------------------------------------

	/** HLL op needs an existing bin and can't auto-create one. */
	public static final int BIN_NOT_FOUND_HLL_CANNOT_CREATE_WITH_OP = 1;

	/** String modify on a missing bin (non-NO_FAIL path). */
	public static final int BIN_NOT_FOUND_STRING_VALUE_NOT_FOUND = 2;

	//-------------------------------------------------------
	// Pairs with ResultCode.BIN_NAME_TOO_LONG (21)  [AS_ERR_BIN_NAME]
	//-------------------------------------------------------

	/** Write would exceed the per-record bin-count limit (UDF path). */
	public static final int BIN_NAME_COUNT_TOO_LARGE = 1;

	//-------------------------------------------------------
	// Pairs with ResultCode.FAIL_FORBIDDEN (22)  [AS_ERR_FORBIDDEN]
	//-------------------------------------------------------

	/** Write bounced by an XDR ship filter at the destination. */
	public static final int FORBID_XDR_FILTER_BLOCKED = 1;

	/** Set-level record-count stop-writes limit reached. */
	public static final int FORBID_SET_COUNT_STOP_WRITES = 2;

	/** Set-level size stop-writes limit reached. */
	public static final int FORBID_SET_SIZE_STOP_WRITES = 3;

	/** Writes stopped due to cluster clock skew. */
	public static final int FORBID_CLOCK_SKEW_STOP_WRITES = 4;

	/** REPLACE / CREATE_OR_REPLACE forbidden while resolving conflicts. */
	public static final int FORBID_REPLACE_CONFLICT_RESOLVING = 5;

	/** Write forbidden because the set/namespace is mid-truncate. */
	public static final int FORBID_TRUNCATED = 6;

	// Note: server subcodes 7 and 9 in this family are retired (masking violations
	// return ROLE_VIOLATION, not FORBIDDEN) and are intentionally not declared.

	/** Non-durable delete forbidden (would violate durability). */
	public static final int FORBID_DURABILITY_VIOLATION = 8;

	//-------------------------------------------------------
	// Pairs with ResultCode.OP_NOT_APPLICABLE (26)  [AS_ERR_OP_NOT_APPLICABLE]
	//-------------------------------------------------------

	/** List index is outside the current element range. */
	public static final int OPNOT_CDT_INDEX_OUT_OF_BOUNDS = 1;

	/** Requested rank is past the current population. */
	public static final int OPNOT_CDT_RANK_OUT_OF_BOUNDS = 2;

	/** Insert would exceed an ordered+bounded list's cap. */
	public static final int OPNOT_CDT_BOUNDED_LIST_OVERFLOW = 3;

	/** HLL op needs index_bits but the sketch has none set. */
	public static final int OPNOT_HLL_INDEX_BITS_UNSET = 4;

	/** Union needs to reduce index_bits but folding isn't allowed. */
	public static final int OPNOT_HLL_CANNOT_REDUCE_INDEX_BITS = 5;

	/** As above, for the minhash dimension. */
	public static final int OPNOT_HLL_CANNOT_REDUCE_MINHASH_BITS = 6;

	/** Fold blocked because the sketch carries minhash bits. */
	public static final int OPNOT_HLL_CANNOT_FOLD_MINHASH = 7;

	/** Fold target index_bits >= current (fold can only reduce). */
	public static final int OPNOT_HLL_FOLD_INDEX_BITS_TOO_LARGE = 8;

	/** Intersect inputs have mismatched minhash parameters. */
	public static final int OPNOT_HLL_INTERSECT_MINHASH_MISMATCH = 9;

	/** String to numeric conversion failed (strtoll/strtod). */
	public static final int OPNOT_STRING_CONVERSION_FAILED = 10;

	/** Source blob/string is not valid UTF-8 for an OP_NOT_APPLICABLE path. */
	public static final int OPNOT_STRING_UTF8_INVALID = 11;

	// 12 is reserved server-side for a regex-limit subcode still in review.

	/**
	 * String is not valid base64 — a length that is not a multiple of 4, a character
	 * outside the alphabet, or misplaced {@code '='} padding.
	 */
	public static final int OPNOT_STRING_B64_INVALID = 13;

	//-------------------------------------------------------
	// ResultCode.FILTERED_OUT (27) [AS_ERR_FILTERED_OUT] carries NO subcode:
	// the server emits AS_SUB_NONE plus a contextual "filtered out ..." message.
	// (The as_sub_filtered_t enum was removed server-side and never shipped, so
	// no FILTERED_* constants are defined here. Match on the message, not a subcode.)
	//-------------------------------------------------------

	//-------------------------------------------------------
	// Pairs with ResultCode.MRT_BLOCKED (120)  [AS_ERR_MRT_BLOCKED]
	//-------------------------------------------------------

	/** Record is provisionally locked by another MRT. */
	public static final int MRT_BLOCKED_RECORD_LOCKED = 1;

	/** Op belongs to a different MRT than the one holding the lock. */
	public static final int MRT_BLOCKED_ID_MISMATCH = 2;

	private SubCode() {
	}
}
