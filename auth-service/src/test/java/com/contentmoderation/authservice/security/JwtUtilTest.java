package com.contentmoderation.authservice.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for JwtUtil — token generation and validation logic.
 *
 * Why test this?
 * JwtUtil is the security gate for the auth service. If generateToken()
 * produces an unreadable token, or isTokenValid() returns true for expired
 * or wrong-user tokens, the entire auth layer is broken. These tests verify
 * the core contract of the JWT utility.
 *
 * No Spring context needed — JwtUtil is a plain @Component with no
 * Spring dependencies. Instantiated directly.
 */
class JwtUtilTest {

    private JwtUtil jwtUtil;

    @BeforeEach
    void setUp() {
        jwtUtil = new JwtUtil();
    }

    // =========================================================================
    // TOKEN GENERATION AND EXTRACTION
    // =========================================================================

    @Test
    void generateToken_extractUsername_returnsCorrectUsername() {
        // A token generated for "alice" must extract "alice" — not null, not "bob"
        String token = jwtUtil.generateToken("alice");

        assertNotNull(token, "Generated token should not be null");
        assertFalse(token.isBlank(), "Generated token should not be blank");

        String extracted = jwtUtil.extractUsername(token);
        assertEquals("alice", extracted,
            "extractUsername should return the exact username the token was generated for");
    }

    @Test
    void generateToken_differentUsersProduceDifferentTokens() {
        // Two users should get different tokens — sanity check that the
        // subject field is actually varying
        String tokenAlice = jwtUtil.generateToken("alice");
        String tokenBob   = jwtUtil.generateToken("bob");

        assertNotEquals(tokenAlice, tokenBob,
            "Tokens for different users should be different");
    }

    // =========================================================================
    // TOKEN VALIDATION — VALID CASES
    // =========================================================================

    @Test
    void isTokenValid_validTokenCorrectUser_returnsTrue() {
        String token = jwtUtil.generateToken("alice");
        assertTrue(jwtUtil.isTokenValid(token, "alice"),
            "A freshly generated token should be valid for its intended user");
    }

    // =========================================================================
    // TOKEN VALIDATION — INVALID CASES
    // =========================================================================

    @Test
    void isTokenValid_validTokenWrongUser_returnsFalse() {
        // A token generated for "alice" must not be valid for "bob".
        // This is the cross-user token reuse attack.
        String token = jwtUtil.generateToken("alice");
        assertFalse(jwtUtil.isTokenValid(token, "bob"),
            "A token for alice must not be valid for bob");
    }

    @Test
    void isTokenValid_tamperedToken_throwsOrReturnsFalse() {
        // If someone modifies the token payload, the signature check should fail.
        // jjwt throws a JwtException subclass in this case.
        // We accept either a thrown exception or a false return — both are safe behavior.
        String validToken = jwtUtil.generateToken("alice");

        // Tamper with the token: replace last 10 characters with garbage
        // JWT format is: header.payload.signature — modifying any part breaks the signature
        String tampered = validToken.substring(0, validToken.length() - 10) + "XXXXXXXXXX";

        // Either throws (signature invalid) or returns false — both are acceptable security responses
        try {
            boolean result = jwtUtil.isTokenValid(tampered, "alice");
            assertFalse(result, "Tampered token should not be valid");
        } catch (Exception e) {
            // Exception thrown by jjwt signature verification is also correct behavior
            assertTrue(true, "Exception thrown on tampered token is acceptable secure behavior");
        }
    }

    @Test
    void extractUsername_validToken_doesNotReturnNull() {
        String token = jwtUtil.generateToken("charlie");
        String username = jwtUtil.extractUsername(token);
        assertNotNull(username, "Extracted username should never be null for a valid token");
    }

    @Test
    void isTokenValid_emptyStringToken_throwsOrReturnsFalse() {
        // An empty string is not a valid JWT — should not return true
        try {
            boolean result = jwtUtil.isTokenValid("", "alice");
            assertFalse(result, "Empty token should not be valid");
        } catch (Exception e) {
            // jjwt throws on malformed tokens — that is correct behavior
            assertTrue(true, "Exception on empty token is acceptable");
        }
    }

    @Test
    void isTokenValid_nullToken_throwsOrReturnsFalse() {
        // Null token should never validate as true
        try {
            boolean result = jwtUtil.isTokenValid(null, "alice");
            assertFalse(result, "Null token should not be valid");
        } catch (Exception e) {
            // Acceptable — jjwt or our code may throw on null input
            assertTrue(true, "Exception on null token is acceptable");
        }
    }
}