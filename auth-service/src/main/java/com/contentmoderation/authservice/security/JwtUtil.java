package com.contentmoderation.authservice.security;

import java.util.Date;

import javax.crypto.SecretKey;

import org.springframework.stereotype.Component;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;

@Component
public class JwtUtil {

    // This secret string is used to create our signing key
    // In production, this would come from environment variables, NOT hardcoded
    private final String SECRET = "my-super-secret-key-that-is-at-least-32-characters-long";

    // Token validity: 24 hours in milliseconds
    private final long EXPIRATION_MS = 24 * 60 * 60 * 1000;

    // Create the signing key from our secret string
    private SecretKey getSigningKey() {
        return Keys.hmacShaKeyFor(SECRET.getBytes());
    }

    // Generate a JWT token for a given username
    public String generateToken(String username) {
        Date now = new Date();
        Date expiry = new Date(now.getTime() + EXPIRATION_MS);

        return Jwts.builder()
                .subject(username)       // WHO this token is for
                .issuedAt(now)           // WHEN it was created
                .expiration(expiry)      // WHEN it expires
                .signWith(getSigningKey()) // SIGN with our secret
                .compact();              // Build the final token string
    }

    // Extract the username from a token
    public String extractUsername(String token) {
        // TODO: What do you think goes here?
        // Hint: We need to parse the token, verify its signature,
        // and pull out the "subject" claim we set in generateToken()
        // The jjwt library uses: Jwts.parser()...
        return Jwts.parser()
            .verifyWith(getSigningKey())   // use our key to verify signature
            .build()
            .parseSignedClaims(token)      // parse and validate the token
            .getPayload()                  // get the payload section
            .getSubject();                 // get the "subject" claim (username)
    }

    // Check if a token is valid
    public boolean isTokenValid(String token, String username) {
        // TODO: What two things should we check here?
        // Think about: what makes a badge invalid?
        // 1. ???
        // 2. ???
        String tokenUsername = extractUsername(token);
        return tokenUsername.equals(username) && !isTokenExpired(token);
    }

    private boolean isTokenExpired(String token) {
        Date expiration = Jwts.parser()
                .verifyWith(getSigningKey())
                .build()
                .parseSignedClaims(token)
                .getPayload()
                .getExpiration();
        return expiration.before(new Date());
    }
}