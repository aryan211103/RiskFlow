package com.contentmoderation.postservice.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.contentmoderation.postservice.dto.CreatePostRequest;
import com.contentmoderation.postservice.model.Post;
import com.contentmoderation.postservice.service.PostService;

@RestController
@RequestMapping("/posts")
public class PostController {

    private final PostService postService;

    // Spring sees this constructor and automatically injects PostService
    public PostController(PostService postService) {
        this.postService = postService;
    }

    @PostMapping
    public ResponseEntity<Post> createPost(@RequestBody CreatePostRequest request) {
        Post savedPost = postService.createPost(request.getContent(), request.getUserId());
        return ResponseEntity.status(HttpStatus.CREATED).body(savedPost);
    }
}