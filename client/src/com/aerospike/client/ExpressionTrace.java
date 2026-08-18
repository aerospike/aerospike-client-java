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

import java.io.Serializable;
import java.util.Arrays;

/**
 * Structured expression build/eval trace surfaced at error-detail verbosity 3.
 * <p>
 * When extended error detail is requested at verbosity 3 (see
 * {@link com.aerospike.client.policy.Policy#errorDetailVerbosity}) and the server
 * fails to build an expression — a metadata/predicate filter ({@code filter_exp})
 * or an {@code exp_read}/{@code exp_write} operation — it attaches this trace as a
 * nested map under the field-45 error-detail key {@link #AS_ERROR_DETAIL_KEY_EXP_TRACE}.
 * It is surfaced on {@link AerospikeException#getExpressionTrace()}.
 * <p>
 * Expression build failures carry {@link ResultCode#PARAMETER_ERROR} and
 * {@link SubCode#NONE} (no subcode); the contextual message is on the exception.
 * The trace is purely additive diagnostic detail — it never changes the result
 * code, subcode, or message-string format.
 * <p>
 * <b>Every field is optional.</b> The server caps the whole error-detail payload
 * and drops {@code snippet} first, then {@code path}, when the budget is tight, so
 * those may be absent even within a present trace. Absent integer fields read as
 * {@code -1} (except {@link #getLang()}, which defaults to {@link #LANG_MSGPACK});
 * absent object fields read as {@code null}. Never require any field.
 * <p>
 * <b>Two coordinate spaces — do not conflate them.</b> {@link #getByteOffset()} is a
 * byte offset into the <i>msgpack expression payload</i> the client sent. The
 * {@link #getAelOffset()}/{@link #getAelSpan()} pair are offsets into <i>AEL source
 * text</i> — a different coordinate space, reserved for a future server branch and
 * absent on today's msgpack build traces.
 * <p>
 * The nested-map key/value constants below mirror the server's {@code proto.h} names
 * so they stay greppable across repositories. They are append-only.
 */
public final class ExpressionTrace implements Serializable {
	private static final long serialVersionUID = 1L;

	//-------------------------------------------------------
	// Wire constants (mirror server proto.h).
	//-------------------------------------------------------

	/** Top-level field-45 error-detail key carrying the nested expression-trace map. */
	public static final int AS_ERROR_DETAIL_KEY_EXP_TRACE = 3;

	/** Nested trace key: phase (uint; {@link #PHASE_BUILD} / {@link #PHASE_EVAL}). */
	public static final int KEY_PHASE = 1;
	/** Nested trace key: byte_offset into the msgpack expression payload (uint). */
	public static final int KEY_BYTE_OFFSET = 2;
	/** Nested trace key: failing op name (str). */
	public static final int KEY_OP = 3;
	/** Nested trace key: true nesting depth of the fault (uint). */
	public static final int KEY_DEPTH = 4;
	/** Nested trace key: op-name chain root&rarr;fault (array of str). */
	public static final int KEY_PATH = 5;
	/** Nested trace key: human-only rendered snippet of the failing element (str). */
	public static final int KEY_SNIPPET = 6;
	/** Nested trace key: eval-phase outcome (uint; {@code OUTCOME_*}). */
	public static final int KEY_OUTCOME = 7;
	/** Nested trace key: source language (uint; {@link #LANG_MSGPACK} / {@link #LANG_AEL}). */
	public static final int KEY_LANG = 8;
	/** Nested trace key: char offset into AEL source text (uint). */
	public static final int KEY_AEL_OFFSET = 9;
	/** Nested trace key: byte width of the offending AEL source region (uint). */
	public static final int KEY_AEL_SPAN = 10;
	/** Nested trace key: 1-based line in AEL source (uint; reserved). */
	public static final int KEY_AEL_LINE = 11;
	/** Nested trace key: 1-based column in AEL source (uint; reserved). */
	public static final int KEY_AEL_COL = 12;
	/** Nested trace key: decisive comparison's operand values (array of 2 str). */
	public static final int KEY_OPERANDS = 13;

	/** Phase value: expression build failed. */
	public static final int PHASE_BUILD = 1;
	/** Phase value: expression evaluation failed. */
	public static final int PHASE_EVAL = 2;

	/** Outcome value: evaluation faulted. */
	public static final int OUTCOME_FAULT = 1;
	/**
	 * Outcome value: the expression evaluated cleanly to FALSE — the record was not
	 * matched, and nothing went wrong.
	 */
	public static final int OUTCOME_FALSE = 2;
	/** Outcome value: a referenced bin or key was absent. */
	public static final int OUTCOME_ABSENT = 3;

	/** Source language: msgpack (the implied default when {@code lang} is absent). */
	public static final int LANG_MSGPACK = 1;
	/** Source language: AEL DSL. */
	public static final int LANG_AEL = 2;

	/**
	 * The {@code "..."} sentinel the server splices into {@link #getPath()} when the
	 * true nesting depth exceeds the path-frame cap. {@link #getDepth()} still reports
	 * the true count.
	 */
	public static final String PATH_TRUNCATION_SENTINEL = "...";

	//-------------------------------------------------------
	// Fields (all optional; sentinels mark "absent").
	//-------------------------------------------------------

	private final int phase;
	private final int byteOffset;
	private final String op;
	private final int depth;
	private final String[] path;
	private final String snippet;
	private final int lang;
	private final int aelOffset;
	private final int aelSpan;
	private final int outcome;
	private final String[] operands;

	/**
	 * Construct a trace without the eval-phase explainer fields. Equivalent to the
	 * full constructor with {@code outcome = -1} and {@code operands = null}.
	 *
	 * @param phase      {@link #PHASE_BUILD} / {@link #PHASE_EVAL}, or {@code -1} if absent
	 * @param byteOffset byte offset into the msgpack expression payload, or {@code -1}
	 * @param op         failing op name, or {@code null}
	 * @param depth      true nesting depth of the fault, or {@code -1}
	 * @param path       op-name chain root&rarr;fault, or {@code null}
	 * @param snippet    rendered snippet of the failing element, or {@code null}
	 * @param lang       {@link #LANG_MSGPACK} / {@link #LANG_AEL}, or {@code -1} (&rArr; msgpack)
	 * @param aelOffset  char offset into AEL source text, or {@code -1}
	 * @param aelSpan    byte width of the offending AEL source region, or {@code -1}
	 */
	public ExpressionTrace(int phase, int byteOffset, String op, int depth, String[] path,
		String snippet, int lang, int aelOffset, int aelSpan) {
		this(phase, byteOffset, op, depth, path, snippet, lang, aelOffset, aelSpan, -1, null);
	}

	/**
	 * Construct a trace. Use {@code -1} / {@code null} for any absent field.
	 *
	 * @param phase      {@link #PHASE_BUILD} / {@link #PHASE_EVAL}, or {@code -1} if absent
	 * @param byteOffset byte offset into the msgpack expression payload, or {@code -1}
	 * @param op         failing op name, or {@code null}
	 * @param depth      true nesting depth of the fault, or {@code -1}
	 * @param path       op-name chain root&rarr;fault, or {@code null}
	 * @param snippet    rendered snippet of the failing element, or {@code null}
	 * @param lang       {@link #LANG_MSGPACK} / {@link #LANG_AEL}, or {@code -1} (&rArr; msgpack)
	 * @param aelOffset  char offset into AEL source text, or {@code -1}
	 * @param aelSpan    byte width of the offending AEL source region, or {@code -1}
	 * @param outcome    {@code OUTCOME_*}, or {@code -1} when absent
	 * @param operands   decisive comparison's operand values, or {@code null}
	 */
	public ExpressionTrace(int phase, int byteOffset, String op, int depth, String[] path,
		String snippet, int lang, int aelOffset, int aelSpan, int outcome, String[] operands) {
		this.phase = phase;
		this.byteOffset = byteOffset;
		this.op = op;
		this.depth = depth;
		this.path = path;
		this.snippet = snippet;
		this.lang = lang;
		this.aelOffset = aelOffset;
		this.aelSpan = aelSpan;
		this.outcome = outcome;
		this.operands = operands;
	}

	/**
	 * Phase that failed: {@link #PHASE_BUILD} or {@link #PHASE_EVAL}. Returns {@code -1}
	 * when absent. Today the server emits build traces only ({@code PHASE_BUILD}).
	 */
	public int getPhase() {
		return phase;
	}

	/**
	 * Byte offset into the msgpack expression payload of the failing element, or
	 * {@code -1} when absent. This is a coordinate into the wire payload the client
	 * sent — not into AEL source text (see {@link #getAelOffset()}).
	 */
	public int getByteOffset() {
		return byteOffset;
	}

	/**
	 * Failing op name (pre-rendered server-side), or {@code null} when absent.
	 */
	public String getOp() {
		return op;
	}

	/**
	 * True nesting depth of the fault, or {@code -1} when absent. Reports the true
	 * count even when {@link #getPath()} was truncated to the frame cap.
	 */
	public int getDepth() {
		return depth;
	}

	/**
	 * Op-name chain from root to fault, or {@code null} when absent. May contain a
	 * {@link #PATH_TRUNCATION_SENTINEL} ({@code "..."}) element mid-array when the true
	 * nesting exceeded the server's path-frame cap; {@link #getDepth()} still reports
	 * the true count.
	 */
	public String[] getPath() {
		return path;
	}

	/**
	 * Human-only rendered snippet of the failing element, or {@code null} when absent
	 * (it is the first field the server drops under a tight byte budget).
	 */
	public String getSnippet() {
		return snippet;
	}

	/**
	 * Source language: {@link #LANG_MSGPACK} or {@link #LANG_AEL}. An absent {@code lang}
	 * key means msgpack (the default), so this returns {@link #LANG_MSGPACK} when the
	 * server omitted it.
	 */
	public int getLang() {
		return (lang < 0) ? LANG_MSGPACK : lang;
	}

	/**
	 * Char offset into the AEL source text, or {@code -1} when absent. Reserved for the
	 * AEL DSL branch; absent on today's msgpack build traces. A different coordinate
	 * space from {@link #getByteOffset()}.
	 */
	public int getAelOffset() {
		return aelOffset;
	}

	/**
	 * Byte width of the offending AEL source region, or {@code -1} when absent.
	 * Populated only when {@link #getLang()} is {@link #LANG_AEL}.
	 */
	public int getAelSpan() {
		return aelSpan;
	}

	/**
	 * Why the record was not matched, on an eval-phase trace: {@link #OUTCOME_FAULT},
	 * {@link #OUTCOME_FALSE} or {@link #OUTCOME_ABSENT}; {@code -1} when absent. The
	 * build phase never emits it.
	 */
	public int getOutcome() {
		return outcome;
	}

	/**
	 * The decisive comparison's operand values as {@code [lhs, rhs]} — e.g.
	 * {@code ["15", "18"]} — or {@code null} when absent. Emitted only for an
	 * {@link #OUTCOME_FALSE} trace whose decisive op is a comparison, and dropped
	 * first by the server's byte budget, so a FALSE outcome does not guarantee them.
	 * Server-rendered display strings capped at 48 bytes: numbers always fit,
	 * non-scalars render as a placeholder ({@code "<blob>"}, {@code "<collection>"}),
	 * and only a long string bin is silently clipped.
	 */
	public String[] getOperands() {
		return operands;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(128);
		sb.append("ExpressionTrace[phase=").append(phase);
		sb.append(", byteOffset=").append(byteOffset);
		if (op != null) {
			sb.append(", op=").append(op);
		}
		sb.append(", depth=").append(depth);
		if (path != null) {
			sb.append(", path=").append(Arrays.toString(path));
		}
		if (snippet != null) {
			sb.append(", snippet=").append(snippet);
		}
		// 0 is not a wire value, so this also skips a default-constructed trace.
		if (outcome > 0) {
			sb.append(", outcome=").append(outcome);
		}
		if (operands != null) {
			sb.append(", operands=").append(Arrays.toString(operands));
		}
		sb.append(", lang=").append(getLang());
		if (aelOffset >= 0) {
			sb.append(", aelOffset=").append(aelOffset);
		}
		if (aelSpan >= 0) {
			sb.append(", aelSpan=").append(aelSpan);
		}
		sb.append(']');
		return sb.toString();
	}
}
