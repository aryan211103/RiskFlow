package com.contentmoderation.authservice.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.contentmoderation.authservice.model.User;

public interface UserRepository extends JpaRepository<User, Long> {

    User findByUsername(String username);

}