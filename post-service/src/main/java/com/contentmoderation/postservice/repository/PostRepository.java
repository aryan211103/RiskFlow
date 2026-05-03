package com.contentmoderation.postservice.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.contentmoderation.postservice.model.Post;

@Repository
public interface PostRepository extends JpaRepository<Post, Long> {
}