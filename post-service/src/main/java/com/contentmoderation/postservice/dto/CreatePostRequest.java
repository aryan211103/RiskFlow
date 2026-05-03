package com.contentmoderation.postservice.dto;

public class CreatePostRequest {
    private String content;
    private Long userId;

    // getters and setters
    public String getContent() { return content; }
    public Long getUserId() { return userId; }
    public void setContent(String content) { this.content = content; }
    public void setUserId(Long userId) { this.userId = userId; }
}