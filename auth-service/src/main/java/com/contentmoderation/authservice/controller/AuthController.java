package com.contentmoderation.authservice.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.contentmoderation.authservice.dto.LoginResponse;
import com.contentmoderation.authservice.model.User;
import com.contentmoderation.authservice.service.AuthService;

@RestController
@RequestMapping("/auth")
public class AuthController {

    private AuthService authService;

    public AuthController(AuthService authService) {
        this.authService = authService;
    }

    @PostMapping("/register")
    public User register(@RequestBody User user) {
        return authService.registerUser(
            user.getUsername(),
            user.getEmail(),
            user.getPassword()
        );
    }

    @PostMapping("/login")
    public LoginResponse login(@RequestBody User user) {

        String token = authService.login(
            user.getUsername(),
            user.getPassword()
        );

        return new LoginResponse(token);
    }

    @GetMapping("/me")
    public String me(Authentication authentication) {

        if (authentication == null) {
            return "Not authenticated";
        }

        return authentication.getName();
    }

    @ExceptionHandler(RuntimeException.class)
    public ResponseEntity<String> handleException(RuntimeException e) {
        return ResponseEntity.badRequest().body(e.getMessage());
    }
}