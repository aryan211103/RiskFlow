package com.riskflow.scoring.model;

/**
 * Defines how a rule group's member rules are evaluated together.
 *
 * ALL — every rule in the group must match for the group score to apply.
 *       Think of it as a logical AND across all member expressions.
 *       Use this when you want to catch coordinated fraud signals that
 *       only mean something when they appear together.
 *       Example: high amount AND high-risk merchant AND country mismatch
 *
 * ANY — at least one rule in the group must match for the group score to apply.
 *       Think of it as a logical OR across all member expressions.
 *       Use this when any one of several signals is independently suspicious.
 *       Example: gambling MCC OR sanctioned country OR blocked device
 *
 * Stored as a VARCHAR in PostgreSQL via @Enumerated(EnumType.STRING) on Rule.
 * This means the database stores the literal text "ALL" or "ANY",
 * not a number like 0 or 1. String storage is safer — adding new enum
 * values later does not shift the ordinal positions of existing ones.
 */
public enum MatchMode {
    ALL,
    ANY
}