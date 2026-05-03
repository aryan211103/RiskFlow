package com.contentmoderation.authservice.service;

import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Service;

import com.contentmoderation.authservice.model.User;
import com.contentmoderation.authservice.repository.UserRepository;
import com.contentmoderation.authservice.security.JwtUtil;

@Service
public class AuthService {

    private UserRepository userRepository;
    private JwtUtil jwtUtil;
    private BCryptPasswordEncoder passwordEncoder;

    public AuthService(UserRepository userRepository, JwtUtil jwtUtil) {
        this.userRepository = userRepository;
        this.jwtUtil = jwtUtil;
        this.passwordEncoder = new BCryptPasswordEncoder();
    }

    public User registerUser(String username, String email, String password) {
        // Step 1: Check if username already exists
        User existingUser = userRepository.findByUsername(username);
        if (existingUser != null) {
            throw new RuntimeException("Username already exists");
        }

        // Step 2: Create new user
        User newUser = new User();
        newUser.setUsername(username);     // BLANK 1
        newUser.setEmail(email);        // BLANK 2
        newUser.setPassword(passwordEncoder.encode(password));  // BLANK 3: what do we encode?
        newUser.setRole("USER");         // BLANK 4: what default role?

        // Step 3: Save to database and return
        return userRepository.save(newUser);
    }

    public String login(String username, String password) {
        User existingUser = userRepository.findByUsername(username);
        if (existingUser == null) {
            throw new RuntimeException("Username doesn't exists");
        }

        if (passwordEncoder.matches(password, existingUser.getPassword())) {
            return jwtUtil.generateToken(username);
        }

        else{
            throw new RuntimeException("Password doesn't match");
        }
    }
}