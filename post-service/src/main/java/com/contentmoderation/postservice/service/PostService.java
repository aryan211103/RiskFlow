package com.contentmoderation.postservice.service;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.contentmoderation.postservice.model.Post;
import com.contentmoderation.postservice.repository.PostRepository;

@Service
public class PostService {

    private final PostRepository postRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    // Constructor injection — Spring automatically provides both dependencies
    public PostService(PostRepository postRepository, KafkaTemplate<String, String> kafkaTemplate) {
        this.postRepository = postRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    public Post createPost(String content, Long userId) {
        // Step 1: Build the Post object — constructor sets status=PENDING and createdAt automatically
        Post post = new Post(content, userId);

        // Step 2: Save to DB — Hibernate generates INSERT SQL and returns the saved post with its new id
        Post savedPost = postRepository.save(post);

        // Step 3: Publish Kafka event to the "post.created" topic
        // We send a simple string message for now — in Phase 4 we'll use proper JSON
        String message = "postId:" + savedPost.getId() + ",userId:" + userId + ",content:" + content;
        kafkaTemplate.send("post.created", message);

        // Step 4: Return the saved post so the controller can send it back to the user
        return savedPost;
    }
}